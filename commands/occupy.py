"""功能: GPU 占卡 — 查询空闲节点并自动提交/删除占卡任务"""
import os
import re
import json
import glob
import yaml
import random
import time
import signal
import tempfile
from datetime import datetime
from typing import List, Dict, Tuple

from InquirerPy import inquirer
from rich.table import Table
from rich.panel import Panel

from raytool.utils.kube import run_kubectl, apply_yaml, get_pods, group_pods_by_job
from raytool.utils.ui import (
    console, confirm, confirm_with_input, print_success, print_error, print_warning, print_info,
    colorize_status,
)

# 占卡任务名前缀（新格式: run-{model}-{task}-{date}-{index}）
OCCUPY_PREFIX = "run-"
# 匹配占卡任务名的正则: run-{model}-{task}-{MMDD}-{NN}
OCCUPY_NAME_PATTERN = re.compile(r"^run-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}$")
# 可选的任务类型
TASK_TYPES = ["retool", "search", "swebench"]
# 可选的模型名
MODEL_NAMES = ["qwen3", "qwen25"]
# 默认每批占用的节点数（1 Master + 3 Worker = 4 节点）
DEFAULT_BATCH_SIZE = 4

# ── 不同 GPU 实例类型的资源配置 ──
# 每个实例类型对应: gpus_per_node, efa_count, 简短描述
GPU_INSTANCE_PROFILES: Dict[str, Dict] = {
    "ml.p5en.48xlarge": {
        "gpus": 8,
        "efa": 16,
        "gpu_model": "H200",
        "description": "8×H200 (p5en)",
    },
    "ml.p5e.48xlarge": {
        "gpus": 8,
        "efa": 32,
        "gpu_model": "H200",
        "description": "8×H200 (p5e)",
    },
    "ml.p5.48xlarge": {
        "gpus": 8,
        "efa": 32,
        "gpu_model": "H100",
        "description": "8×H100 (p5)",
    },
    "ml.p4d.24xlarge": {
        "gpus": 8,
        "efa": 4,
        "gpu_model": "A100",
        "description": "8×A100 (p4d)",
    },
    "ml.p4de.24xlarge": {
        "gpus": 8,
        "efa": 4,
        "gpu_model": "A100-80G",
        "description": "8×A100-80G (p4de)",
    },
    "ml.g5.48xlarge": {
        "gpus": 8,
        "efa": 1,
        "gpu_model": "A10G",
        "description": "8×A10G (g5)",
    },
}

# 未知实例类型的默认配置（兜底）
DEFAULT_GPU_PROFILE = {
    "gpus": 8,
    "efa": 16,
    "gpu_model": "Unknown",
    "description": "Unknown GPU",
}


def _get_instance_profile(instance_type: str) -> Dict:
    """获取实例类型对应的资源配置，未知类型使用默认值"""
    return GPU_INSTANCE_PROFILES.get(instance_type, DEFAULT_GPU_PROFILE)
# 自动巡逻默认间隔（秒）
DEFAULT_PATROL_INTERVAL = 300  # 5 分钟
# 占卡 YAML 存放目录
OCCUPY_YAML_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "occupy-jobs")
# 占卡 YAML 模板路径
OCCUPY_TEMPLATE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "ray-job", "zzj-gpu-occupy.yaml")


def occupy_gpus(namespace: str):
    """GPU 占卡交互式入口 — 先选择操作类型"""
    action = inquirer.select(
        message="主人，请选择占卡操作",
        choices=[
            {"name": "🚀 提交新的占卡任务", "value": "submit"},
            {"name": "🗑️  删除已有占卡任务", "value": "delete"},
            {"name": "👁️  自动巡逻 (定时检测空闲卡并自动占卡)", "value": "patrol"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
    ).execute()

    if action == "cancel":
        return
    elif action == "delete":
        _delete_occupy_jobs(namespace)
        return
    elif action == "patrol":
        _auto_patrol(namespace)
        return

    # ── 提交新占卡任务 ──
    _submit_occupy_jobs(namespace)


def _delete_occupy_jobs(namespace: str):
    """批量删除占卡任务（PyTorchJob）"""
    print_info("正在查询已有的占卡任务...")
    console.print()

    # 查询所有 PyTorchJob
    rc, stdout, stderr = run_kubectl(
        ["get", "pytorchjobs", "-o", "json"],
        namespace,
        timeout=15,
    )

    if rc != 0:
        print_error(f"查询 PyTorchJob 失败: {stderr.strip()}")
        return

    try:
        data = json.loads(stdout)
        items = data.get("items", [])
    except (json.JSONDecodeError, KeyError):
        print_error("解析 PyTorchJob 列表失败")
        return

    if not items:
        print_warning("当前没有任何 PyTorchJob")
        return

    # 按名称匹配占卡前缀，同时也列出所有任务让用户选
    occupy_jobs = []
    other_jobs = []
    for item in items:
        name = item.get("metadata", {}).get("name", "")
        creation = item.get("metadata", {}).get("creationTimestamp", "")
        # 统计 Pod 数和 GPU 数
        master_spec = item.get("spec", {}).get("pytorchReplicaSpecs", {}).get("Master", {})
        worker_spec = item.get("spec", {}).get("pytorchReplicaSpecs", {}).get("Worker", {})
        master_replicas = master_spec.get("replicas", 1)
        worker_replicas = worker_spec.get("replicas", 0)
        total_nodes = master_replicas + worker_replicas
        # 从容器资源请求中获取每节点 GPU 数
        try:
            gpu_per_node = int(
                master_spec.get("template", {}).get("spec", {}).get("containers", [{}])[0]
                .get("resources", {}).get("requests", {}).get("nvidia.com/gpu", 8)
            )
        except (IndexError, ValueError):
            gpu_per_node = 8
        total_gpus = total_nodes * gpu_per_node

        job_info = {
            "name": name,
            "creation": creation[:19].replace("T", " ") if creation else "-",
            "nodes": total_nodes,
            "gpus": total_gpus,
        }

        if OCCUPY_NAME_PATTERN.match(name):
            occupy_jobs.append(job_info)
        else:
            other_jobs.append(job_info)

    # 显示占卡任务列表
    if occupy_jobs:
        table = Table(title="🔥 占卡任务列表", show_lines=False, border_style="cyan")
        table.add_column("#", style="dim", width=4)
        table.add_column("任务名称", style="bold cyan", min_width=30)
        table.add_column("节点数", justify="center", width=8)
        table.add_column("GPU 数", justify="center", width=8)
        table.add_column("创建时间", width=22)

        for i, job in enumerate(occupy_jobs, 1):
            table.add_row(
                str(i),
                job["name"],
                str(job["nodes"]),
                str(job["gpus"]),
                job["creation"],
            )

        console.print(table)
        console.print()

    if not occupy_jobs and not other_jobs:
        print_warning("没有找到任何占卡任务")
        return

    # 构建多选列表
    all_choices = []

    if occupy_jobs:
        all_choices.append({"name": f"--- 占卡任务 ({len(occupy_jobs)} 个) ---", "value": "__separator_occupy__", "enabled": False})
        # 一键全选占卡任务
        total_occupy_gpus = sum(j["gpus"] for j in occupy_jobs)
        all_choices.append({
            "name": f"⚡ 全选所有占卡任务 ({len(occupy_jobs)} 个, {total_occupy_gpus} GPU)",
            "value": "__all_occupy__",
        })
        for job in sorted(occupy_jobs, key=lambda x: x["name"]):
            all_choices.append({
                "name": f"  {job['name']}  ({job['nodes']}节点, {job['gpus']}GPU, {job['creation']})",
                "value": job["name"],
            })

    if other_jobs:
        all_choices.append({"name": f"--- 其他任务 ({len(other_jobs)} 个) ---", "value": "__separator_other__", "enabled": False})
        for job in sorted(other_jobs, key=lambda x: x["name"]):
            all_choices.append({
                "name": f"  {job['name']}  ({job['nodes']}节点, {job['gpus']}GPU, {job['creation']})",
                "value": job["name"],
            })

    all_choices.append({"name": "❌ 取消", "value": "__cancel__"})

    selected = inquirer.checkbox(
        message="请选择要删除的任务 (空格选中, 回车确认)",
        choices=all_choices,
        pointer="❯",
    ).execute()

    if not selected or "__cancel__" in selected:
        print_warning("已取消")
        return

    # 处理「全选占卡任务」
    if "__all_occupy__" in selected:
        selected = [j["name"] for j in occupy_jobs]
    else:
        # 过滤掉分隔符
        selected = [s for s in selected if not s.startswith("__")]

    if not selected:
        print_warning("未选择任何任务")
        return

    # 显示待删除列表
    total_del_gpus = 0
    console.print()
    console.print("[bold yellow]⚠️  即将删除以下任务:[/bold yellow]")
    for name in selected:
        # 查找 GPU 数
        for j in occupy_jobs + other_jobs:
            if j["name"] == name:
                total_del_gpus += j["gpus"]
                console.print(f"  [bold red]✖[/bold red] {name}  ({j['nodes']}节点, {j['gpus']}GPU)")
                break
        else:
            console.print(f"  [bold red]✖[/bold red] {name}")

    console.print()
    console.print(f"[bold]共 {len(selected)} 个任务, {total_del_gpus} 张 GPU 将被释放[/bold]")
    console.print()

    if not confirm_with_input("确认删除? 请输入 'yes'"):
        print_warning("已取消删除")
        return

    # 逐个删除
    console.print()
    success_count = 0
    fail_count = 0
    for name in selected:
        print_info(f"正在删除: {name}")
        rc, stdout, stderr = run_kubectl(
            ["delete", "pytorchjob", name, "--ignore-not-found=true"],
            namespace,
        )
        if rc == 0:
            print_success(f"已删除: {name}")
            success_count += 1
            # 同时清理对应的 YAML 文件
            yaml_path = os.path.join(OCCUPY_YAML_DIR, f"{name}.yaml")
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
        else:
            print_error(f"删除失败 {name}: {stderr.strip()}")
            fail_count += 1

    console.print()
    if fail_count == 0:
        print_success(f"全部删除完成! 共删除 {success_count} 个任务, 释放 {total_del_gpus} 张 GPU")
    else:
        print_warning(f"删除完成: {success_count} 成功, {fail_count} 失败")


def _random_task_identity() -> Tuple[str, str]:
    """随机生成模型和任务类型组合，返回 (model, task)，已做 K8s 命名合规处理"""
    model = random.choice(MODEL_NAMES)
    task = random.choice(TASK_TYPES)
    # K8s 资源名只允许小写字母、数字、'-'
    model = model.lower().replace(".", "").replace("_", "-")
    task = task.lower().replace("_", "-")
    return model, task


def _submit_occupy_jobs(namespace: str):
    """提交新的占卡任务"""
    print_info("正在查询集群 GPU 节点信息...")
    console.print()

    # 1. 获取所有 GPU 节点
    all_nodes = _get_gpu_nodes(namespace)
    if not all_nodes:
        print_error("未找到任何 GPU 节点")
        return

    # 2. 获取已占用的节点
    busy_nodes = _get_busy_nodes(namespace)

    # 3. 计算空闲节点
    free_nodes = [n for n in all_nodes if n["name"] not in busy_nodes]

    # 4. 显示总览
    _print_node_overview(all_nodes, busy_nodes, free_nodes)
    console.print()

    if not free_nodes:
        print_warning("没有空闲的 GPU 节点可供占用")
        return

    # 5. 按实例类型分组空闲节点
    free_by_type: Dict[str, List[Dict]] = {}
    for n in free_nodes:
        itype = n["instance_type"]
        free_by_type.setdefault(itype, []).append(n)

    total_free_gpus = sum(n["gpu_count"] for n in free_nodes)
    console.print(f"[bold green]空闲节点: {len(free_nodes)} 个 ({total_free_gpus} 张 GPU)[/bold green]")
    if len(free_by_type) > 1:
        for itype, nodes in sorted(free_by_type.items()):
            profile = _get_instance_profile(itype)
            console.print(f"  [cyan]{itype}[/cyan]: {len(nodes)} 个 ({profile['description']})")
    console.print()

    # 6. 按实例类型分批：同一批次内只包含相同类型的节点
    batch_plan = []       # 每个元素: (batch_size, instance_type)
    for itype in sorted(free_by_type.keys()):
        type_free = len(free_by_type[itype])
        remaining = type_free
        while remaining > 0:
            batch = min(DEFAULT_BATCH_SIZE, remaining)
            batch_plan.append((batch, itype))
            remaining -= batch

    # 7. 为每个批次随机生成任务名，避免重名
    date_str = datetime.now().strftime("%m%d")
    existing_names = _get_existing_job_names(namespace)
    job_names = _generate_random_job_names(len(batch_plan), date_str, existing_names)

    # 8. 展示占卡计划
    _print_occupy_plan(batch_plan, job_names)
    console.print()

    # 9. 让用户选择要提交的批次
    total_nodes = sum(b[0] for b in batch_plan)
    choices = []
    choices.append({"name": f"全部提交 ({total_nodes} 节点, {len(batch_plan)} 个任务)", "value": "all"})
    for i, (batch_size, itype) in enumerate(batch_plan):
        profile = _get_instance_profile(itype)
        gpus = batch_size * profile["gpus"]
        choices.append({
            "name": f"仅第 {i+1} 批: {job_names[i]} ({batch_size} 节点, {gpus} GPU, {profile['description']})",
            "value": str(i),
        })
    choices.append({"name": "取消", "value": "cancel"})

    selected = inquirer.select(
        message="主人，请选择占卡方案",
        choices=choices,
        pointer="❯",
    ).execute()

    if selected == "cancel":
        print_warning("已取消占卡操作")
        return

    if selected != "all":
        batch_idx = int(selected)
        batch_plan = [batch_plan[batch_idx]]
        job_names = [job_names[batch_idx]]

    # 10. 生成 YAML 并确认
    yaml_files = _generate_occupy_yamls(batch_plan, namespace, job_names)

    console.print()
    console.print(Panel(
        "\n".join([f"  {os.path.basename(f)}" for f in yaml_files]),
        title="📄 将生成以下占卡任务 YAML",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not confirm("确认提交占卡任务?"):
        for f in yaml_files:
            if os.path.exists(f):
                os.remove(f)
        print_warning("已取消，YAML 文件已清理")
        return

    # 11. 逐个提交
    for yaml_file in yaml_files:
        job_name = os.path.basename(yaml_file).replace(".yaml", "")
        print_info(f"正在提交: {job_name}")
        success, message = apply_yaml(yaml_file, namespace)
        if success:
            print_success(f"已提交: {message}")
        else:
            print_error(f"提交失败: {message}")

    console.print()
    total_gpus = sum(b[0] * _get_instance_profile(b[1])["gpus"] for b in batch_plan)
    total_submit_nodes = sum(b[0] for b in batch_plan)
    print_success(f"占卡任务全部提交完成! 共 {total_submit_nodes} 节点, {total_gpus} 张 GPU")
    console.print("[dim]提示: 使用 '集群概况总览' 或 '监控 Pods 状态' 查看占卡任务启动情况[/dim]")


def _auto_occupy(namespace: str) -> int:
    """非交互式自动占卡：检测空闲节点并全部占满。返回本次占用的节点数（0 表示无空闲）。"""
    all_nodes = _get_gpu_nodes(namespace)
    if not all_nodes:
        return 0

    busy_nodes = _get_busy_nodes(namespace)
    free_nodes = [n for n in all_nodes if n["name"] not in busy_nodes]

    if not free_nodes:
        return 0

    # 按实例类型分组
    free_by_type: Dict[str, List[Dict]] = {}
    for n in free_nodes:
        free_by_type.setdefault(n["instance_type"], []).append(n)

    # 按实例类型分批
    batch_plan = []  # (batch_size, instance_type)
    for itype in sorted(free_by_type.keys()):
        remaining = len(free_by_type[itype])
        while remaining > 0:
            batch = min(DEFAULT_BATCH_SIZE, remaining)
            batch_plan.append((batch, itype))
            remaining -= batch

    # 生成随机任务名
    date_str = datetime.now().strftime("%m%d")
    existing_names = _get_existing_job_names(namespace)
    job_names = _generate_random_job_names(len(batch_plan), date_str, existing_names)

    # 生成 YAML
    yaml_files = _generate_occupy_yamls(batch_plan, namespace, job_names)

    # 逐个提交
    success_count = 0
    for yaml_file in yaml_files:
        job_name = os.path.basename(yaml_file).replace(".yaml", "")
        success, message = apply_yaml(yaml_file, namespace)
        if success:
            print_success(f"  ✅ 已提交: {job_name}")
            success_count += 1
        else:
            print_error(f"  ❌ 提交失败 {job_name}: {message}")

    total_nodes = sum(b[0] for b in batch_plan)
    return total_nodes if success_count > 0 else 0


def _auto_patrol(namespace: str):
    """自动巡逻模式：定时检测空闲 GPU 节点，发现空闲则自动占卡"""
    # 让用户选择巡逻间隔
    interval_choice = inquirer.select(
        message="选择巡逻间隔",
        choices=[
            {"name": "1 分钟", "value": 60},
            {"name": "3 分钟", "value": 180},
            {"name": "5 分钟 (推荐)", "value": 300},
            {"name": "10 分钟", "value": 600},
            {"name": "自定义...", "value": "custom"},
        ],
        default=300,
        pointer="❯",
    ).execute()

    if interval_choice == "custom":
        interval = int(inquirer.number(
            message="输入巡逻间隔（秒）",
            min_allowed=30,
            max_allowed=3600,
            default=300,
        ).execute())
    else:
        interval = interval_choice

    console.print()
    console.print(Panel(
        f"[bold cyan]自动巡逻模式已启动[/bold cyan]\n\n"
        f"  巡逻间隔: [bold]{interval}[/bold] 秒 ({interval // 60} 分 {interval % 60} 秒)\n"
        f"  检测到空闲节点时将自动提交占卡任务\n"
        f"  按 [bold yellow]Ctrl+C[/bold yellow] 停止巡逻",
        title="👁️ 自动巡逻",
        border_style="cyan",
        padding=(1, 2),
    ))
    console.print()

    round_num = 0
    total_occupied = 0

    try:
        while True:
            round_num += 1
            now = datetime.now().strftime("%H:%M:%S")
            console.print(f"[dim]── 第 {round_num} 轮巡逻 ({now}) ──[/dim]")

            try:
                all_nodes = _get_gpu_nodes(namespace)
                busy_nodes = _get_busy_nodes(namespace)
                free_count = len([n for n in all_nodes if n["name"] not in busy_nodes]) if all_nodes else 0
                total = len(all_nodes) if all_nodes else 0

                if free_count > 0:
                    console.print(f"  🔍 发现 [bold green]{free_count}[/bold green] 个空闲节点 (共 {total} 个)，正在自动占卡...")
                    occupied = _auto_occupy(namespace)
                    if occupied > 0:
                        total_occupied += occupied
                        print_success(f"  本轮占用 {occupied} 节点，累计占用 {total_occupied} 节点")
                    else:
                        print_warning("  占卡提交失败或节点已被抢占")
                else:
                    console.print(f"  ✅ 全部 {total} 节点已占满，无需操作")
            except Exception as e:
                print_error(f"  巡逻异常: {e}")

            console.print(f"  [dim]下次巡逻: {interval} 秒后...[/dim]")
            console.print()
            time.sleep(interval)

    except KeyboardInterrupt:
        console.print()
        console.print(Panel(
            f"[bold]巡逻已停止[/bold]\n\n"
            f"  总巡逻轮数: {round_num}\n"
            f"  累计占用节点: {total_occupied}",
            title="👁️ 巡逻报告",
            border_style="yellow",
            padding=(0, 2),
        ))


def _get_gpu_nodes(namespace: str) -> List[Dict]:
    """获取所有带 nvidia.com/gpu 资源的 GPU 节点，自动识别实例类型"""
    rc, stdout, stderr = run_kubectl(
        ["get", "nodes", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return []

    try:
        data = json.loads(stdout)
        nodes = []
        for item in data.get("items", []):
            metadata = item.get("metadata", {})
            labels = metadata.get("labels", {})
            status = item.get("status", {})

            # GPU 容量 — 跳过没有 GPU 的节点
            capacity = status.get("capacity", {})
            gpu_count = int(capacity.get("nvidia.com/gpu", 0))
            if gpu_count == 0:
                continue

            # 实例类型：优先使用标准 label，回退到 beta label
            instance_type = (
                labels.get("node.kubernetes.io/instance-type")
                or labels.get("beta.kubernetes.io/instance-type")
                or "unknown"
            )

            conditions = status.get("conditions", [])
            ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            )

            profile = _get_instance_profile(instance_type)

            nodes.append({
                "name": metadata.get("name", ""),
                "ready": ready,
                "gpu_count": gpu_count,
                "status": "Ready" if ready else "NotReady",
                "instance_type": instance_type,
                "gpu_model": profile["gpu_model"],
                "efa_count": profile["efa"],
                "description": profile["description"],
            })
        return nodes
    except (json.JSONDecodeError, KeyError):
        return []


def _get_busy_nodes(namespace: str) -> set:
    """获取已有 Pod 占用的节点名集合"""
    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return set()

    try:
        data = json.loads(stdout)
        busy = set()
        for item in data.get("items", []):
            phase = item.get("status", {}).get("phase", "")
            # 只关心 Running / Pending 的 Pod（它们占用了节点资源）
            if phase in ("Running", "Pending", "ContainerCreating"):
                node_name = item.get("spec", {}).get("nodeName", "")
                if node_name:
                    busy.add(node_name)
                # Pending 的 Pod 可能还没有 nodeName，但仍占用资源配额
        return busy
    except (json.JSONDecodeError, KeyError):
        return set()


def _print_node_overview(all_nodes: list, busy_nodes: set, free_nodes: list):
    """打印节点总览表"""
    table = Table(title="🖥️  GPU 节点总览", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("节点名称", style="cyan", min_width=40)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("GPU", justify="center", width=10)
    table.add_column("节点状态", justify="center", width=12)
    table.add_column("占用状态", justify="center", width=12)

    free_names = set(n["name"] for n in free_nodes)

    for i, node in enumerate(sorted(all_nodes, key=lambda x: (x["instance_type"], x["name"])), 1):
        is_busy = node["name"] in busy_nodes
        occupy_status = "[red]已占用[/red]" if is_busy else "[green]空闲[/green]"
        node_status = colorize_status(node["status"])
        gpu_info = f"{node['gpu_count']}×{node['gpu_model']}"
        table.add_row(
            str(i),
            node["name"],
            node["instance_type"].replace("ml.", ""),
            gpu_info,
            node_status,
            occupy_status,
        )

    console.print(table)

    # 按实例类型统计
    from collections import Counter
    type_counter = Counter()
    free_type_counter = Counter()
    for n in all_nodes:
        type_counter[n["instance_type"]] += 1
    for n in free_nodes:
        free_type_counter[n["instance_type"]] += 1

    total = len(all_nodes)
    free = len(free_nodes)
    busy = total - free

    if total > 0:
        bar_width = 40
        f_len = max(1, round(free / total * bar_width)) if free else 0
        b_len = bar_width - f_len
        bar = f"[green]{'█' * f_len}[/green][red]{'█' * b_len}[/red]"
        console.print(f"  空闲 [green]{free}[/green] / 已占用 [red]{busy}[/red] / 总计 {total}")
        console.print(f"  {bar}")

        # 按类型显示空闲详情
        if len(type_counter) > 1:
            console.print()
            console.print("  [bold]按实例类型:[/bold]")
            for itype in sorted(type_counter.keys()):
                t_total = type_counter[itype]
                t_free = free_type_counter.get(itype, 0)
                profile = _get_instance_profile(itype)
                status_color = "green" if t_free > 0 else "dim"
                console.print(
                    f"    [{status_color}]{itype}[/{status_color}]: "
                    f"空闲 {t_free}/{t_total} ({profile['description']})"
                )


def _get_existing_job_names(namespace: str) -> set:
    """查询集群中已有的所有 PyTorchJob 名称"""
    rc, stdout, stderr = run_kubectl(
        ["get", "pytorchjobs", "-o", "jsonpath={.items[*].metadata.name}"],
        namespace,
        timeout=15,
    )
    if rc != 0 or not stdout.strip():
        return set()
    return set(stdout.strip().split())


def _generate_random_job_names(count: int, date_str: str, existing_names: set) -> List[str]:
    """为每个占卡任务随机生成不重复的任务名，格式: run-{model}-{task}-{date}-{index}"""
    # 生成所有可能的 (model, task) 组合
    combos = []
    for m in MODEL_NAMES:
        for t in TASK_TYPES:
            model = m.lower().replace(".", "").replace("_", "-")
            task = t.lower().replace("_", "-")
            combos.append((model, task))
    random.shuffle(combos)

    names = []
    combo_idx = 0
    for _ in range(count):
        # 找一个不冲突的名字
        found = False
        attempts = 0
        while attempts < len(combos) * 100:
            model, task = combos[combo_idx % len(combos)]
            combo_idx += 1
            # 找到该前缀下的最大编号
            prefix = f"run-{model}-{task}-{date_str}"
            max_idx = 0
            for existing in existing_names | set(names):
                m = re.match(rf"^{re.escape(prefix)}-(\d+)$", existing)
                if m:
                    max_idx = max(max_idx, int(m.group(1)))
            job_name = f"{prefix}-{max_idx + 1:02d}"
            if job_name not in existing_names and job_name not in names:
                names.append(job_name)
                found = True
                break
            attempts += 1
        if not found:
            # fallback
            fallback_name = f"run-qwen3-retool-{date_str}-{random.randint(50, 99):02d}"
            names.append(fallback_name)

    return names


def _print_occupy_plan(batch_plan: list, job_names: list):
    """打印占卡计划。batch_plan 元素为 (batch_size, instance_type)"""
    table = Table(title="📋 占卡计划", show_lines=False, border_style="cyan")
    table.add_column("批次", style="bold", width=6)
    table.add_column("任务名", style="cyan", min_width=35)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("Master", justify="center", width=8)
    table.add_column("Worker", justify="center", width=8)
    table.add_column("GPU 数", justify="center", width=8)

    total_gpus = 0
    for i, (batch_size, itype) in enumerate(batch_plan):
        worker_count = batch_size - 1
        profile = _get_instance_profile(itype)
        gpus = batch_size * profile["gpus"]
        total_gpus += gpus
        table.add_row(
            f"#{i+1}",
            job_names[i],
            itype.replace("ml.", ""),
            str(batch_size),
            "1",
            str(worker_count),
            str(gpus),
        )

    total_nodes = sum(b[0] for b in batch_plan)
    table.add_row(
        "[bold]合计[/bold]",
        f"[bold]{len(batch_plan)} 个任务[/bold]",
        "",
        f"[bold]{total_nodes}[/bold]",
        f"[bold]{len(batch_plan)}[/bold]",
        f"[bold]{total_nodes - len(batch_plan)}[/bold]",
        f"[bold]{total_gpus}[/bold]",
    )

    console.print(table)


def _generate_occupy_yamls(batch_plan: list, namespace: str, job_names: list) -> List[str]:
    """根据批次计划生成占卡 YAML 文件。batch_plan 元素为 (batch_size, instance_type)"""
    # 确保输出目录存在
    os.makedirs(OCCUPY_YAML_DIR, exist_ok=True)

    yaml_files = []

    for i, (batch_size, itype) in enumerate(batch_plan):
        worker_count = batch_size - 1  # 1 个 Master + N 个 Worker
        job_name = job_names[i]
        yaml_content = _build_occupy_yaml(job_name, namespace, worker_count, itype)
        
        output_path = os.path.join(OCCUPY_YAML_DIR, f"{job_name}.yaml")
        with open(output_path, "w") as f:
            f.write(yaml_content)
        yaml_files.append(output_path)

    return yaml_files


def _build_occupy_yaml(job_name: str, namespace: str, worker_replicas: int, instance_type: str) -> str:
    """构建占卡 PyTorchJob YAML 内容，根据实例类型动态调整资源配置"""
    profile = _get_instance_profile(instance_type)
    gpu_count = profile["gpus"]
    efa_count = profile["efa"]

    # 公共的启动命令
    occupy_cmd = """echo "=== GPU Occupy - {role} ==="
                  echo "RANK: $RANK"
                  echo "WORLD_SIZE: $WORLD_SIZE"
                  echo "Instance Type: """ + instance_type + """"
                  hostname -I
                  nvidia-smi

                  # 清理 PyTorchJob 注入的分布式环境变量
                  unset MASTER_ADDR MASTER_PORT WORLD_SIZE RANK LOCAL_RANK
                  unset GROUP_RANK ROLE_RANK LOCAL_WORLD_SIZE ROLE_WORLD_SIZE

                  source /root/miniconda3/etc/profile.d/conda.sh
                  conda activate agent-lightning
                  export PATH=$CONDA_PREFIX/bin:$PATH

                  echo "Python: $(which python3)"
                  echo "CUDA available: $(python3 -c 'import torch; print(torch.cuda.is_available())')"
                  echo "GPU count: $(python3 -c 'import torch; print(torch.cuda.device_count())')"

                  python3 /fsx/gpu_stress.py \\
                    --matrix-size 4096 \\
                    --duty-cycle 0.80 \\
                    --mem-fraction 0.75 \\
                    --duration 168

                  EXIT_CODE=$?
                  echo "gpu_stress.py exited with code: $EXIT_CODE"
                  if [ $EXIT_CODE -ne 0 ]; then
                    echo "[ERROR] gpu_stress.py failed!"
                  fi
                  sleep 365d"""

    master_cmd = occupy_cmd.format(role="Master")
    worker_cmd = occupy_cmd.format(role="Worker")

    # 公共的 volumeMounts
    volume_mounts = [
        {"name": "shmem", "mountPath": "/dev/shm"},
        {"name": "local", "mountPath": "/local"},
        {"name": "inst-nvme", "mountPath": "/ckpt-path"},
        {"name": "local-cache", "mountPath": "/root/.cache"},
        {"name": "fsx-storage", "mountPath": "/fsx", "subPath": "youtu-agent/zhijianzhou"},
    ]

    # 公共的 volumes
    volumes = [
        {"name": "shmem", "hostPath": {"path": "/dev/shm"}},
        {"name": "local", "hostPath": {"path": "/mnt/k8s-disks/0"}},
        {"name": "local-cache", "hostPath": {"path": "/opt/dlami/nvme/.cache"}},
        {"name": "inst-nvme", "hostPath": {"path": "/opt/dlami/nvme/checkpoints/"}},
        {"name": "fsx-storage", "persistentVolumeClaim": {"claimName": "fsx-claim"}},
    ]

    # 公共环境变量
    env = [
        {"name": "FI_PROVIDER", "value": "efa"},
        {"name": "FI_EFA_USE_DEVICE_RDMA", "value": "1"},
        {"name": "PATH", "value": "/root/miniconda3/envs/agent-lightning/bin:/root/miniconda3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
    ]

    # 根据实例类型动态配置资源请求
    resources = {
        "requests": {"nvidia.com/gpu": gpu_count, "vpc.amazonaws.com/efa": efa_count},
        "limits": {"nvidia.com/gpu": gpu_count, "vpc.amazonaws.com/efa": efa_count},
    }

    image = "054486717055.dkr.ecr.ap-southeast-3.amazonaws.com/youtu-agent:agent-lightning-0.2.2-1218-aws"

    # 构建 Master spec
    master_container = {
        "name": "pytorch",
        "image": image,
        "imagePullPolicy": "Always",
        "ports": [
            {"containerPort": 6379, "name": "gcs-server"},
            {"containerPort": 8265, "name": "dashboard"},
            {"containerPort": 10001, "name": "client"},
            {"containerPort": 8000, "name": "serve"},
            {"containerPort": 8080, "name": "metrics"},
        ],
        "resources": resources,
        "env": env,
        "command": ["bash", "-c"],
        "args": [master_cmd],
        "volumeMounts": volume_mounts,
    }

    # 构建 Worker spec
    worker_container = {
        "name": "pytorch",
        "image": image,
        "imagePullPolicy": "Always",
        "resources": resources,
        "env": env,
        "command": ["bash", "-c"],
        "args": [worker_cmd],
        "volumeMounts": volume_mounts,
    }

    # 根据实例类型设置 nodeSelector
    node_selector = {"node.kubernetes.io/instance-type": instance_type}

    pytorchjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
        },
        "spec": {
            "nprocPerNode": str(gpu_count),
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "restartPolicy": "OnFailure",
                    "template": {
                        "spec": {
                            "nodeSelector": node_selector,
                            "containers": [master_container],
                            "volumes": volumes,
                        }
                    },
                },
            },
        },
    }

    # 只在有 Worker 时添加 Worker spec
    if worker_replicas > 0:
        pytorchjob["spec"]["pytorchReplicaSpecs"]["Worker"] = {
            "replicas": worker_replicas,
            "restartPolicy": "OnFailure",
            "template": {
                "spec": {
                    "nodeSelector": node_selector,
                    "containers": [worker_container],
                    "volumes": volumes,
                }
            },
        }

    return yaml.dump(pytorchjob, default_flow_style=False, sort_keys=False, allow_unicode=True)
