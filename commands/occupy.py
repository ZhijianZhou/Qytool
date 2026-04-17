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
    colorize_status, ESC_KEYBINDING,
)

# 占卡任务名前缀（新格式: run-{model}-{task}-{date}-{index}）
OCCUPY_PREFIX = "run-"
# 匹配占卡任务名的正则: run-{name segments}-{MMDD}-{NN}
OCCUPY_NAME_PATTERN = re.compile(r"^run-(?:[a-z0-9]+-)+\d{4}-\d{2}$")
# 以下默认值可通过全局配置覆盖 (occupy_task_types / occupy_model_names / occupy_batch_size)
TASK_TYPES = ["retool", "search", "swebench"]
MODEL_NAMES = ["qwen3", "qwen25"]
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
# 占卡 YAML 存放目录 — 优先使用用户 home 下的 .raytool/occupy-jobs，避免权限问题
_RAYTOOL_HOME = os.path.join(os.path.expanduser("~"), ".raytool")
OCCUPY_YAML_DIR = os.path.join(_RAYTOOL_HOME, "occupy-jobs")
# 占卡 YAML 模板路径（仍基于源码位置）
OCCUPY_TEMPLATE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "ray-job", "zzj-gpu-occupy.yaml")
# 守护进程 PID 文件和日志文件
GUARD_PID_FILE = os.path.join(_RAYTOOL_HOME, "guard.pid")
GUARD_LOG_FILE = os.path.join(_RAYTOOL_HOME, "guard.log")


def _apply_config(config: dict) -> None:
    """从全局配置覆盖模块级默认值"""
    global TASK_TYPES, MODEL_NAMES, DEFAULT_BATCH_SIZE
    if config.get("occupy_task_types"):
        TASK_TYPES = config["occupy_task_types"]
    if config.get("occupy_model_names"):
        MODEL_NAMES = config["occupy_model_names"]
    if config.get("occupy_batch_size"):
        DEFAULT_BATCH_SIZE = config["occupy_batch_size"]


def occupy_gpus(namespace: str, config: dict = None, custom_name: str = None, custom_gpus: int = None):
    """GPU 占卡交互式入口 — 先选择操作类型。
    如果指定了 custom_name 或 custom_gpus，则直接进入提交流程。
    """
    config = config or {}
    _apply_config(config)

    # CLI 直接指定了参数，跳过菜单直接提交
    if custom_name is not None or custom_gpus is not None:
        _submit_occupy_jobs(namespace, config, custom_name=custom_name, custom_gpus=custom_gpus)
        return

    action = inquirer.select(
        message="主人，请选择占卡操作",
        choices=[
            {"name": "🚀 提交新的占卡任务", "value": "submit"},
            {"name": "🗑️  删除已有占卡任务", "value": "delete"},
            {"name": "🛡️  智能守护 (Pending自动让卡 + 空闲自动补占)", "value": "guard"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if action == "cancel" or action is None:
        return
    elif action == "delete":
        _delete_occupy_jobs(namespace)
        return
    elif action == "guard":
        _auto_guard(namespace, config)
        return

    # ── 提交新占卡任务 ──
    _submit_occupy_jobs(namespace, config)


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

        # 提取 AZ 信息（从 Master 的 nodeSelector 中获取）
        node_selector = master_spec.get("template", {}).get("spec", {}).get("nodeSelector", {})
        az = node_selector.get("topology.kubernetes.io/zone", "")
        az_short = az.split("-")[-1] if az and "-" in az else (az or "?")

        job_info = {
            "name": name,
            "creation": creation[:19].replace("T", " ") if creation else "-",
            "nodes": total_nodes,
            "gpus": total_gpus,
            "az": az_short,
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
        table.add_column("AZ", justify="center", width=6)
        table.add_column("节点数", justify="center", width=8)
        table.add_column("GPU 数", justify="center", width=8)
        table.add_column("创建时间", width=22)

        for i, job in enumerate(occupy_jobs, 1):
            table.add_row(
                str(i),
                job["name"],
                job["az"],
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
                "name": f"  {job['name']}  ({job['az']}区, {job['nodes']}节点, {job['gpus']}GPU, {job['creation']})",
                "value": job["name"],
            })

    if other_jobs:
        all_choices.append({"name": f"--- 其他任务 ({len(other_jobs)} 个) ---", "value": "__separator_other__", "enabled": False})
        for job in sorted(other_jobs, key=lambda x: x["name"]):
            all_choices.append({
                "name": f"  {job['name']}  ({job['az']}区, {job['nodes']}节点, {job['gpus']}GPU, {job['creation']})",
                "value": job["name"],
            })

    all_choices.append({"name": "❌ 取消", "value": "__cancel__"})

    selected = inquirer.checkbox(
        message="请选择要删除的任务 (空格选中, 回车确认)",
        choices=all_choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
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
                console.print(f"  [bold red]✖[/bold red] {name}  ({j['az']}区, {j['nodes']}节点, {j['gpus']}GPU)")
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


def _submit_occupy_jobs(namespace: str, config: dict = None, custom_name: str = None, custom_gpus: int = None):
    """提交新的占卡任务。
    custom_name: 用户指定的任务名称 (如 glm5, qwen3-sft)
    custom_gpus: 用户指定的 GPU 数量 (如 256)
    """
    config = config or {}
    print_info("正在查询集群 GPU 节点信息...")
    console.print()

    # 1. 获取所有 GPU 节点
    all_nodes = _get_gpu_nodes(namespace)
    if not all_nodes:
        print_error("未找到任何 GPU 节点")
        return

    # 2. 获取已占用的节点
    busy_nodes = _get_busy_nodes(namespace)

    # 3. 计算空闲节点（排除已占用 + cordon 禁止调度的节点）
    free_nodes = [n for n in all_nodes if n["name"] not in busy_nodes and not n.get("unschedulable", False)]
    # 统计 cordon 节点
    cordoned_nodes = [n for n in all_nodes if n.get("unschedulable", False)]

    # 4. 显示总览
    _print_node_overview(all_nodes, busy_nodes, free_nodes)
    console.print()

    if cordoned_nodes:
        console.print(f"[bold yellow]🛡️  已禁止调度 (cordon): {len(cordoned_nodes)} 个节点 (占卡将跳过)[/bold yellow]")
        for n in cordoned_nodes:
            console.print(f"[dim]    🚫 {n['name']}[/dim]")
        console.print()

    if not free_nodes:
        print_warning("没有空闲的 GPU 节点可供占用")
        return

    # 5. 按 AZ + 实例类型分组空闲节点
    free_by_az_type: Dict[Tuple[str, str], List[Dict]] = {}
    for n in free_nodes:
        key = (n.get("az", "unknown"), n["instance_type"])
        free_by_az_type.setdefault(key, []).append(n)

    total_free_gpus = sum(n["gpu_count"] for n in free_nodes)
    console.print(f"[bold green]空闲节点: {len(free_nodes)} 个 ({total_free_gpus} 张 GPU)[/bold green]")
    if len(free_by_az_type) > 1:
        for (az, itype), nodes in sorted(free_by_az_type.items()):
            profile = _get_instance_profile(itype)
            az_short = az.split("-")[-1] if "-" in az else az
            console.print(f"  [cyan]{az_short} / {itype}[/cyan]: {len(nodes)} 个 ({profile['description']})")
    console.print()

    # ── 6. 输入任务名称 ──
    if custom_name is None:
        name_input = inquirer.text(
            message="请输入任务名称 (例如: glm5, qwen3-sft, retool-v2)",
            default="",
        ).execute().strip()
        if not name_input:
            print_warning("任务名称不能为空，已取消")
            return
    else:
        name_input = custom_name

    # K8s 名称合规处理: 只保留小写字母、数字、连字符
    task_base = name_input.lower().replace("_", "-").replace(".", "-")
    task_base = re.sub(r"[^a-z0-9-]", "", task_base)
    task_base = re.sub(r"-+", "-", task_base).strip("-")
    if task_base.startswith("run-"):
        task_base = task_base[4:]
    if not task_base:
        task_base = "occupy"

    # ── 7. 输入 GPU 数量 ──
    if custom_gpus is None:
        gpus_input = inquirer.number(
            message=f"请输入需要占用的 GPU 数量 (最大可用: {total_free_gpus})",
            min_allowed=1,
            max_allowed=total_free_gpus,
            default=total_free_gpus,
        ).execute()
        if gpus_input is None:
            print_warning("已取消")
            return
        custom_gpus = int(gpus_input)
    else:
        if custom_gpus > total_free_gpus:
            print_warning(f"请求 {custom_gpus} 张 GPU，但仅有 {total_free_gpus} 张可用，将使用最大可用量")
            custom_gpus = total_free_gpus

    # ── 8. 按 AZ + 实例类型分配节点，直到满足 GPU 需求 ──
    remaining_gpus = custom_gpus
    batch_plan = []       # 每个元素: (batch_size, instance_type, az)

    for (az, itype) in sorted(free_by_az_type.keys()):
        if remaining_gpus <= 0:
            break
        group_nodes = free_by_az_type[(az, itype)]
        profile = _get_instance_profile(itype)
        gpus_per_node = profile["gpus"]

        max_from_group = len(group_nodes)
        needed_from_group = min(max_from_group, (remaining_gpus + gpus_per_node - 1) // gpus_per_node)
        remaining_gpus -= needed_from_group * gpus_per_node

        while needed_from_group > 0:
            batch = min(DEFAULT_BATCH_SIZE, needed_from_group)
            batch_plan.append((batch, itype, az))
            needed_from_group -= batch

    if not batch_plan:
        print_warning("无法分配足够的节点")
        return

    # ── 9. 生成任务名: run-{name}-{MMDD}-{NN} ──
    date_str = datetime.now().strftime("%m%d")
    existing_names = _get_existing_job_names(namespace)

    job_names = []
    for _ in range(len(batch_plan)):
        idx = 1
        while True:
            name = f"run-{task_base}-{date_str}-{idx:02d}"
            if name not in existing_names and name not in job_names:
                job_names.append(name)
                break
            idx += 1

    # ── 10. 展示占卡计划 ──
    _print_occupy_plan(batch_plan, job_names)
    console.print()

    total_planned_gpus = sum(b[0] * _get_instance_profile(b[1])["gpus"] for b in batch_plan)
    total_submit_nodes = sum(b[0] for b in batch_plan)
    console.print(
        f"[bold]任务名称: [cyan]run-{task_base}-{date_str}-*[/cyan]  |  "
        f"共 {len(batch_plan)} 个任务, {total_submit_nodes} 节点, {total_planned_gpus} 张 GPU[/bold]"
    )
    console.print()

    # ── 11. 确认并提交 ──
    if not confirm("确认提交占卡任务?"):
        print_warning("已取消占卡操作")
        return

    yaml_files = _generate_occupy_yamls(batch_plan, namespace, job_names, config)

    for yaml_file in yaml_files:
        job_name = os.path.basename(yaml_file).replace(".yaml", "")
        print_info(f"正在提交: {job_name}")
        success, message = apply_yaml(yaml_file, namespace)
        if success:
            print_success(f"已提交: {message}")
        else:
            print_error(f"提交失败: {message}")

    console.print()
    print_success(f"占卡任务全部提交完成! 共 {total_submit_nodes} 节点, {total_planned_gpus} 张 GPU")
    console.print("[dim]提示: 使用 '集群概况总览' 或 '监控 Pods 状态' 查看占卡任务启动情况[/dim]")


def _auto_occupy(namespace: str, config: dict = None) -> int:
    """非交互式自动占卡：检测空闲节点并全部占满。返回本次占用的节点数（0 表示无空闲）。"""
    config = config or {}
    all_nodes = _get_gpu_nodes(namespace)
    if not all_nodes:
        return 0

    busy_nodes = _get_busy_nodes(namespace)
    free_nodes = [n for n in all_nodes if n["name"] not in busy_nodes and not n.get("unschedulable", False)]

    if not free_nodes:
        return 0

    # 按 AZ + 实例类型分组
    free_by_az_type: Dict[Tuple[str, str], List[Dict]] = {}
    for n in free_nodes:
        key = (n.get("az", "unknown"), n["instance_type"])
        free_by_az_type.setdefault(key, []).append(n)

    # 按 AZ + 实例类型分批
    batch_plan = []  # (batch_size, instance_type, az)
    for (az, itype) in sorted(free_by_az_type.keys()):
        remaining = len(free_by_az_type[(az, itype)])
        while remaining > 0:
            batch = min(DEFAULT_BATCH_SIZE, remaining)
            batch_plan.append((batch, itype, az))
            remaining -= batch

    # 生成随机任务名
    date_str = datetime.now().strftime("%m%d")
    existing_names = _get_existing_job_names(namespace)
    job_names = _generate_random_job_names(len(batch_plan), date_str, existing_names)

    # 生成 YAML
    yaml_files = _generate_occupy_yamls(batch_plan, namespace, job_names, config)

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

            # 是否被 cordon（禁止调度）
            spec = item.get("spec", {})
            unschedulable = spec.get("unschedulable", False)

            # 可用区 (AZ)
            az = (
                labels.get("topology.kubernetes.io/zone")
                or labels.get("failure-domain.beta.kubernetes.io/zone")
                or "unknown"
            )

            nodes.append({
                "name": metadata.get("name", ""),
                "ready": ready,
                "gpu_count": gpu_count,
                "status": "SchedulingDisabled" if unschedulable else ("Ready" if ready else "NotReady"),
                "instance_type": instance_type,
                "az": az,
                "gpu_model": profile["gpu_model"],
                "efa_count": profile["efa"],
                "description": profile["description"],
                "unschedulable": unschedulable,
            })
        return nodes
    except (json.JSONDecodeError, KeyError):
        return []


def _pod_requests_gpu(pod: dict) -> bool:
    """判断 Pod 是否请求了 GPU 资源（检查所有容器和 initContainers）"""
    spec = pod.get("spec", {})
    for container_key in ("containers", "initContainers"):
        for c in spec.get(container_key, []):
            gpu_req = int(
                c.get("resources", {}).get("requests", {}).get("nvidia.com/gpu", 0)
            )
            if gpu_req > 0:
                return True
    return False


def _get_busy_nodes(namespace: str) -> set:
    """获取已有 Pod 占用的节点名集合（仅计算请求了 GPU 的 Pod，排除 image-prewarmer 等无 GPU Pod）"""
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
            if phase not in ("Running", "Pending", "ContainerCreating"):
                continue
            # 跳过不请求 GPU 的 Pod（如 image-prewarmer DaemonSet）
            if not _pod_requests_gpu(item):
                continue
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
    table.add_column("AZ", justify="center", width=8)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("GPU", justify="center", width=10)
    table.add_column("节点状态", justify="center", width=12)
    table.add_column("占用状态", justify="center", width=12)

    free_names = set(n["name"] for n in free_nodes)

    for i, node in enumerate(sorted(all_nodes, key=lambda x: (x.get("az", ""), x["instance_type"], x["name"])), 1):
        is_busy = node["name"] in busy_nodes
        is_cordoned = node.get("unschedulable", False)
        if is_cordoned:
            occupy_status = "[yellow]🛡️ 禁止调度[/yellow]"
        elif is_busy:
            occupy_status = "[red]已占用[/red]"
        else:
            occupy_status = "[green]空闲[/green]"
        node_status = colorize_status(node["status"])
        gpu_info = f"{node['gpu_count']}×{node['gpu_model']}"
        az_short = node.get("az", "?").split("-")[-1] if node.get("az") else "?"
        table.add_row(
            str(i),
            node["name"],
            az_short,
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
    az_type_counter = Counter()
    free_az_type_counter = Counter()
    for n in all_nodes:
        type_counter[n["instance_type"]] += 1
        az_type_counter[(n.get("az", "?"), n["instance_type"])] += 1
    for n in free_nodes:
        free_type_counter[n["instance_type"]] += 1
        free_az_type_counter[(n.get("az", "?"), n["instance_type"])] += 1

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

        # 按 AZ + 实例类型 显示空闲详情
        console.print()
        console.print("  [bold]按 AZ + 实例类型:[/bold]")
        for (az, itype) in sorted(az_type_counter.keys()):
            t_total = az_type_counter[(az, itype)]
            t_free = free_az_type_counter.get((az, itype), 0)
            profile = _get_instance_profile(itype)
            status_color = "green" if t_free > 0 else "dim"
            az_short = az.split("-")[-1] if "-" in az else az
            console.print(
                f"    [{status_color}]{az_short} / {itype}[/{status_color}]: "
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
    """打印占卡计划。batch_plan 元素为 (batch_size, instance_type, az)"""
    table = Table(title="📋 占卡计划", show_lines=False, border_style="cyan")
    table.add_column("批次", style="bold", width=6)
    table.add_column("任务名", style="cyan", min_width=35)
    table.add_column("AZ", justify="center", width=8)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("Master", justify="center", width=8)
    table.add_column("Worker", justify="center", width=8)
    table.add_column("GPU 数", justify="center", width=8)

    total_gpus = 0
    for i, (batch_size, itype, az) in enumerate(batch_plan):
        worker_count = batch_size - 1
        profile = _get_instance_profile(itype)
        gpus = batch_size * profile["gpus"]
        total_gpus += gpus
        az_short = az.split("-")[-1] if "-" in az else az
        table.add_row(
            f"#{i+1}",
            job_names[i],
            az_short,
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
        "",
        f"[bold]{total_nodes}[/bold]",
        f"[bold]{len(batch_plan)}[/bold]",
        f"[bold]{total_nodes - len(batch_plan)}[/bold]",
        f"[bold]{total_gpus}[/bold]",
    )

    console.print(table)


def _generate_occupy_yamls(batch_plan: list, namespace: str, job_names: list, config: dict = None) -> List[str]:
    """根据批次计划生成占卡 YAML 文件。batch_plan 元素为 (batch_size, instance_type, az)"""
    config = config or {}
    os.makedirs(OCCUPY_YAML_DIR, exist_ok=True)

    yaml_files = []

    for i, (batch_size, itype, az) in enumerate(batch_plan):
        worker_count = batch_size - 1  # 1 个 Master + N 个 Worker
        job_name = job_names[i]
        yaml_content = _build_occupy_yaml(job_name, namespace, worker_count, itype, az, config)
        
        output_path = os.path.join(OCCUPY_YAML_DIR, f"{job_name}.yaml")
        with open(output_path, "w") as f:
            f.write(yaml_content)
        yaml_files.append(output_path)

    return yaml_files


def _build_occupy_yaml(job_name: str, namespace: str, worker_replicas: int, instance_type: str, az: str = "", config: dict = None) -> str:
    """构建占卡 PyTorchJob YAML 内容，根据实例类型和 AZ 动态调整资源配置"""
    config = config or {}
    profile = _get_instance_profile(instance_type)
    gpu_count = profile["gpus"]
    efa_count = profile["efa"]

    conda_env = config.get("occupy_conda_env", "agent-lightning")

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
                  conda activate """ + conda_env + """
                  export PATH=$CONDA_PREFIX/bin:$PATH

                  echo "Python: $(which python3)"
                  echo "CUDA available: $(python3 -c 'import torch; print(torch.cuda.is_available())')"
                  echo "GPU count: $(python3 -c 'import torch; print(torch.cuda.device_count())')"

                  cat > /tmp/gpu_stress.py << 'STRESS_EOF'
import argparse, time, torch, torch.cuda

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--matrix-size", type=int, default=4096)
    p.add_argument("--duty-cycle", type=float, default=0.80)
    p.add_argument("--mem-fraction", type=float, default=0.75)
    p.add_argument("--duration", type=float, default=168,
                   help="hours to run")
    args = p.parse_args()

    n_gpus = torch.cuda.device_count()
    assert n_gpus > 0, "No GPUs found"
    print(f"[gpu_stress] {n_gpus} GPU(s), matrix={args.matrix_size}, "
          f"duty={args.duty_cycle}, mem={args.mem_fraction}, "
          f"duration={args.duration}h")

    bufs = []
    for d in range(n_gpus):
        torch.cuda.set_device(d)
        free, total = torch.cuda.mem_get_info(d)
        alloc = int(total * args.mem_fraction)
        elems = alloc // 4  # float32
        bufs.append(torch.empty(elems, dtype=torch.float32, device=f"cuda:{d}"))
        print(f"  GPU {d}: allocated {alloc/1e9:.1f} GB / {total/1e9:.1f} GB")

    mats = []
    for d in range(n_gpus):
        torch.cuda.set_device(d)
        m = args.matrix_size
        mats.append((torch.randn(m, m, device=f"cuda:{d}"),
                      torch.randn(m, m, device=f"cuda:{d}")))

    deadline = time.time() + args.duration * 3600
    cycle = 1.0
    work_t = cycle * args.duty_cycle
    sleep_t = cycle - work_t
    iters = 0
    while time.time() < deadline:
        t0 = time.time()
        while time.time() - t0 < work_t:
            for a, b in mats:
                torch.mm(a, b)
        for d in range(n_gpus):
            torch.cuda.synchronize(d)
        if sleep_t > 0:
            time.sleep(sleep_t)
        iters += 1
        if iters % 60 == 0:
            elapsed_h = (time.time() - (deadline - args.duration*3600)) / 3600
            print(f"[gpu_stress] running {elapsed_h:.1f}h / {args.duration}h")

    print("[gpu_stress] finished")

if __name__ == "__main__":
    main()
STRESS_EOF

                  python3 /tmp/gpu_stress.py \\
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

    pvc_name = config.get("occupy_pvc_name", "fsx-claim")
    fsx_subpath = config.get("occupy_fsx_subpath", "youtu-agent/zhijianzhou")
    host_local = config.get("occupy_host_local", "/mnt/k8s-disks/0")
    host_cache = config.get("occupy_host_cache", "/opt/dlami/nvme/.cache")
    host_ckpt = config.get("occupy_host_checkpoints", "/opt/dlami/nvme/checkpoints/")
    cpu_req = config.get("occupy_cpu_request", 64)

    # 公共的 volumeMounts
    volume_mounts = [
        {"name": "shmem", "mountPath": "/dev/shm"},
        {"name": "local", "mountPath": "/local"},
        {"name": "inst-nvme", "mountPath": "/ckpt-path"},
        {"name": "local-cache", "mountPath": "/root/.cache"},
        {"name": "fsx-storage", "mountPath": "/fsx", "subPath": fsx_subpath},
    ]

    # 公共的 volumes
    volumes = [
        {"name": "shmem", "hostPath": {"path": "/dev/shm"}},
        {"name": "local", "hostPath": {"path": host_local}},
        {"name": "local-cache", "hostPath": {"path": host_cache}},
        {"name": "inst-nvme", "hostPath": {"path": host_ckpt}},
        {"name": "fsx-storage", "persistentVolumeClaim": {"claimName": pvc_name}},
    ]

    # 公共环境变量
    env = [
        {"name": "PATH", "value": f"/root/miniconda3/envs/{conda_env}/bin:/root/miniconda3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
    ]

    # 根据实例类型动态配置资源请求
    resources = {
        "requests": {"cpu": cpu_req, "nvidia.com/gpu": gpu_count},
        "limits": {"cpu": cpu_req, "nvidia.com/gpu": gpu_count},
    }

    image = config.get("occupy_image", "054486717055.dkr.ecr.ap-southeast-3.amazonaws.com/youtu-agent:slime0401.h200")

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

    # 根据实例类型和 AZ 设置 nodeSelector
    node_selector = {"node.kubernetes.io/instance-type": instance_type}
    if az and az != "unknown":
        node_selector["topology.kubernetes.io/zone"] = az

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


# ──────────────────────── 智能守护模式 ────────────────────────

def _get_pending_non_occupy_pods(namespace: str) -> List[Dict]:
    """
    获取所有 Pending 状态的非占卡 Pod。
    返回 [{name, job_name, gpu_request, creation, instance_type, az}]
    """
    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return []

    try:
        data = json.loads(stdout)
        pending_pods = []
        for item in data.get("items", []):
            phase = item.get("status", {}).get("phase", "")
            if phase != "Pending":
                continue

            metadata = item.get("metadata", {})
            pod_name = metadata.get("name", "")
            labels = metadata.get("labels", {})

            # 推断 job 名称
            job_name = (
                labels.get("training.kubeflow.org/job-name", "")
                or labels.get("ray.io/cluster", "")
                or labels.get("app.kubernetes.io/instance", "")
                or ""
            )
            if not job_name:
                # 从 pod 名称推断
                parts = pod_name.split("-")
                for idx, part in enumerate(parts):
                    if part in ("master", "worker", "head"):
                        job_name = "-".join(parts[:idx])
                        break
                if not job_name and len(parts) > 2:
                    job_name = "-".join(parts[:-2])

            # 跳过占卡任务自身的 Pending Pod
            if OCCUPY_NAME_PATTERN.match(job_name):
                continue

            # 提取 GPU 请求数
            containers = item.get("spec", {}).get("containers", [])
            gpu_request = 0
            for c in containers:
                gpu_request += int(
                    c.get("resources", {}).get("requests", {}).get("nvidia.com/gpu", 0)
                )

            # 提取 nodeSelector 中的实例类型和 AZ
            node_selector = item.get("spec", {}).get("nodeSelector", {})
            instance_type = node_selector.get("node.kubernetes.io/instance-type", "")
            az = node_selector.get("topology.kubernetes.io/zone", "")

            creation = metadata.get("creationTimestamp", "")

            pending_pods.append({
                "name": pod_name,
                "job_name": job_name,
                "gpu_request": gpu_request,
                "instance_type": instance_type,
                "az": az,
                "creation": creation,
            })
        return pending_pods
    except (json.JSONDecodeError, KeyError):
        return []


def _get_occupy_job_details(namespace: str) -> List[Dict]:
    """
    获取所有占卡 PyTorchJob 的详细信息。
    返回 [{name, nodes, gpus, gpu_per_node, az, az_full, instance_type, creation}]
    """
    rc, stdout, stderr = run_kubectl(
        ["get", "pytorchjobs", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return []

    try:
        data = json.loads(stdout)
        jobs = []
        for item in data.get("items", []):
            name = item.get("metadata", {}).get("name", "")
            if not OCCUPY_NAME_PATTERN.match(name):
                continue

            creation = item.get("metadata", {}).get("creationTimestamp", "")
            master_spec = item.get("spec", {}).get("pytorchReplicaSpecs", {}).get("Master", {})
            worker_spec = item.get("spec", {}).get("pytorchReplicaSpecs", {}).get("Worker", {})
            master_replicas = master_spec.get("replicas", 1)
            worker_replicas = worker_spec.get("replicas", 0)
            total_nodes = master_replicas + worker_replicas

            try:
                gpu_per_node = int(
                    master_spec.get("template", {}).get("spec", {}).get("containers", [{}])[0]
                    .get("resources", {}).get("requests", {}).get("nvidia.com/gpu", 8)
                )
            except (IndexError, ValueError):
                gpu_per_node = 8

            node_selector = master_spec.get("template", {}).get("spec", {}).get("nodeSelector", {})
            az_full = node_selector.get("topology.kubernetes.io/zone", "")
            az_short = az_full.split("-")[-1] if az_full and "-" in az_full else (az_full or "?")
            instance_type = node_selector.get("node.kubernetes.io/instance-type", "")

            jobs.append({
                "name": name,
                "nodes": total_nodes,
                "gpus": total_nodes * gpu_per_node,
                "gpu_per_node": gpu_per_node,
                "az": az_short,
                "az_full": az_full,
                "instance_type": instance_type,
                "creation": creation,
            })
        return jobs
    except (json.JSONDecodeError, KeyError):
        return []


def _auto_yield_once(namespace: str) -> Tuple[int, int]:
    """
    执行一次智能让卡检测：
    1. 查找 Pending 的非占卡 Pod
    2. 按 AZ + 实例类型匹配需要释放的占卡任务
    3. 删除匹配的占卡任务（优先删除节点数少的）

    返回 (释放的占卡任务数, 释放的节点数)
    """
    pending_pods = _get_pending_non_occupy_pods(namespace)
    if not pending_pods:
        return 0, 0

    occupy_jobs = _get_occupy_job_details(namespace)
    if not occupy_jobs:
        return 0, 0

    # 按 job_name 分组 pending pods，计算每个训练任务需要的节点数
    from collections import defaultdict
    pending_by_job: Dict[str, Dict] = {}
    for pod in pending_pods:
        jn = pod["job_name"]
        if jn not in pending_by_job:
            pending_by_job[jn] = {
                "job_name": jn,
                "pending_count": 0,
                "gpu_per_pod": pod["gpu_request"],
                "instance_type": pod["instance_type"],
                "az": pod["az"],
            }
        pending_by_job[jn]["pending_count"] += 1

    # 确定需要释放的占卡任务
    to_delete = []
    remaining_occupy = list(occupy_jobs)  # 可被释放的占卡任务池

    for jn, info in pending_by_job.items():
        needed_nodes = info["pending_count"]
        if needed_nodes == 0:
            continue

        az_full_needed = info["az"]  # 训练任务要求的 AZ（完整形式）
        itype_needed = info["instance_type"]  # 训练任务要求的实例类型

        # 从占卡任务中找匹配的（优先匹配同 AZ + 同实例类型，然后同实例类型）
        # 按节点数升序排列，优先删小的，减少浪费
        candidates = sorted(remaining_occupy, key=lambda x: x["nodes"])

        freed_nodes = 0
        for oj in candidates:
            if freed_nodes >= needed_nodes:
                break

            # 匹配条件：如果训练任务指定了 AZ/实例类型，占卡任务必须一致
            if itype_needed and oj["instance_type"] and oj["instance_type"] != itype_needed:
                continue
            if az_full_needed and oj["az_full"] and oj["az_full"] != az_full_needed:
                continue

            to_delete.append(oj)
            freed_nodes += oj["nodes"]
            remaining_occupy.remove(oj)

    if not to_delete:
        # 有 Pending Pod 但没有匹配的占卡任务可释放
        return 0, 0

    # 执行删除
    deleted_jobs = 0
    deleted_nodes = 0
    for oj in to_delete:
        console.print(
            f"  🔄 正在释放占卡任务: [cyan]{oj['name']}[/cyan] "
            f"({oj['az']}区, {oj['nodes']}节点, {oj['gpus']}GPU)"
        )
        rc, stdout, stderr = run_kubectl(
            ["delete", "pytorchjob", oj["name"], "--ignore-not-found=true"],
            namespace,
        )
        if rc == 0:
            print_success(f"  已释放: {oj['name']}")
            deleted_jobs += 1
            deleted_nodes += oj["nodes"]
            # 清理对应的 YAML 文件
            yaml_path = os.path.join(OCCUPY_YAML_DIR, f"{oj['name']}.yaml")
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
        else:
            print_error(f"  释放失败 {oj['name']}: {stderr.strip()}")

    return deleted_jobs, deleted_nodes


def _auto_guard(namespace: str, config: dict = None):
    """
    智能守护入口：提供 启动(前台/后台) / 停止 / 查看状态 子菜单。
    """
    config = config or {}
    # 检查是否已有后台守护在运行
    running_pid = _get_guard_pid()

    choices = []
    if running_pid:
        choices.append({"name": f"🟢 守护进程运行中 (PID: {running_pid})", "value": "__info__", "enabled": False})
        choices.append({"name": "📋 查看守护日志 (tail)", "value": "log"})
        choices.append({"name": "🛑 停止后台守护", "value": "stop"})
        choices.append({"name": "🚀 前台启动新守护 (会先停止已有的)", "value": "foreground"})
    else:
        choices.append({"name": "🛡️  后台启动守护 (推荐，可关闭终端)", "value": "background"})
        choices.append({"name": "🖥️  前台启动守护 (Ctrl+C 停止)", "value": "foreground"})
    choices.append({"name": "❌ 返回", "value": "cancel"})

    action = inquirer.select(
        message="主人，请选择守护操作",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if action == "cancel" or action == "__info__" or action is None:
        return
    elif action == "stop":
        _stop_guard_daemon()
        return
    elif action == "log":
        _tail_guard_log()
        return
    elif action == "background":
        _start_guard_config_and_launch(namespace, background=True, config=config)
        return
    elif action == "foreground":
        if running_pid:
            _stop_guard_daemon()
        _start_guard_config_and_launch(namespace, background=False, config=config)
        return


def _start_guard_config_and_launch(namespace: str, background: bool, config: dict = None):
    """交互式配置守护参数，然后启动（前台或后台）"""
    interval_choice = inquirer.select(
        message="选择守护间隔",
        choices=[
            {"name": "30 秒 (推荐)", "value": 30},
            {"name": "1 分钟", "value": 60},
            {"name": "3 分钟", "value": 180},
            {"name": "5 分钟", "value": 300},
            {"name": "自定义...", "value": "custom"},
        ],
        default=30,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if interval_choice is None:
        return

    if interval_choice == "custom":
        interval = int(inquirer.number(
            message="输入守护间隔（秒）",
            min_allowed=10,
            max_allowed=3600,
            default=30,
        ).execute())
    else:
        interval = interval_choice

    auto_refill = inquirer.select(
        message="空闲节点自动补占?",
        choices=[
            {"name": "是 — 让卡后如有空闲节点自动重新占卡 (推荐)", "value": True},
            {"name": "否 — 仅让卡，不自动补占", "value": False},
        ],
        default=True,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if auto_refill is None:
        return

    if background:
        _start_guard_daemon(namespace, interval, auto_refill, config)
    else:
        _guard_foreground(namespace, interval, auto_refill, config)


def _start_guard_daemon(namespace: str, interval: int, auto_refill: bool, config: dict = None):
    """以后台守护进程方式启动智能守护"""
    os.makedirs(_RAYTOOL_HOME, exist_ok=True)

    pid = os.fork()
    if pid > 0:
        # 父进程：记录子进程 PID 并返回
        with open(GUARD_PID_FILE, "w") as f:
            f.write(str(pid))
        console.print()
        console.print(Panel(
            f"[bold green]智能守护已在后台启动[/bold green]\n\n"
            f"  PID: [bold]{pid}[/bold]\n"
            f"  守护间隔: [bold]{interval}[/bold] 秒\n"
            f"  自动补占: [bold]{'开启' if auto_refill else '关闭'}[/bold]\n"
            f"  日志文件: [cyan]{GUARD_LOG_FILE}[/cyan]\n"
            f"  PID 文件: [cyan]{GUARD_PID_FILE}[/cyan]\n\n"
            f"  [dim]查看日志: tail -f {GUARD_LOG_FILE}[/dim]\n"
            f"  [dim]停止守护: 回到此菜单选择「停止后台守护」[/dim]",
            title="🛡️ 智能守护 (后台)",
            border_style="green",
            padding=(1, 2),
        ))
        return

    # ── 子进程：守护进程 ──
    try:
        # 创建新会话，脱离终端
        os.setsid()

        # 重定向 stdout/stderr 到日志文件
        log_fd = os.open(GUARD_LOG_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.dup2(log_fd, 1)  # stdout
        os.dup2(log_fd, 2)  # stderr
        # 关闭 stdin
        devnull = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull, 0)

        _guard_loop(namespace, interval, auto_refill, config)
    except Exception as e:
        print(f"[FATAL] 守护进程异常退出: {e}")
    finally:
        _cleanup_pid_file()
        os._exit(0)


def _guard_foreground(namespace: str, interval: int, auto_refill: bool, config: dict = None):
    """前台模式运行智能守护（直接输出到终端）"""
    console.print()
    console.print(Panel(
        f"[bold cyan]智能守护模式已启动 (前台)[/bold cyan]\n\n"
        f"  守护间隔: [bold]{interval}[/bold] 秒\n"
        f"  自动补占: [bold]{'开启' if auto_refill else '关闭'}[/bold]\n\n"
        f"  [bold yellow]策略:[/bold yellow]\n"
        f"    1. 检测到正式训练任务 Pending → 自动删除占卡任务腾出资源\n"
        f"    2. {'检测到空闲节点 → 自动提交占卡任务补位' if auto_refill else '(仅让卡，不自动补占)'}\n\n"
        f"  按 [bold yellow]Ctrl+C[/bold yellow] 停止守护",
        title="🛡️ 智能守护 (前台)",
        border_style="cyan",
        padding=(1, 2),
    ))
    console.print()

    try:
        _guard_loop(namespace, interval, auto_refill, config)
    except KeyboardInterrupt:
        pass


def _guard_loop(namespace: str, interval: int, auto_refill: bool, config: dict = None):
    """
    智能守护核心循环（前台/后台共用）。
    每轮：
      1. 检测 Pending 训练任务 → 自动删除占卡任务腾位
      2. 检测空闲节点 → 自动补占
    """
    round_num = 0
    total_yielded_jobs = 0
    total_yielded_nodes = 0
    total_occupied_nodes = 0

    def _log(msg: str):
        """统一输出（前台用 rich console，后台用 print 写日志）"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 后台模式 stdout 已重定向到文件，print 即写日志
        # 前台模式也可用 print，但 rich 标记会变乱，所以统一用 print + 纯文本
        import sys
        if sys.stdout.isatty():
            console.print(msg)
        else:
            # 去掉 rich 标记
            import re as _re
            clean = _re.sub(r"\[/?[a-z_ ]+\]", "", msg)
            print(f"[{now}] {clean}", flush=True)

    try:
        while True:
            round_num += 1
            now = datetime.now().strftime("%H:%M:%S")
            _log(f"[dim]── 第 {round_num} 轮守护 ({now}) ──[/dim]")

            try:
                # ── 阶段1: 让卡 ──
                pending_pods = _get_pending_non_occupy_pods(namespace)
                if pending_pods:
                    pending_jobs = {}
                    for p in pending_pods:
                        jn = p["job_name"]
                        if jn not in pending_jobs:
                            pending_jobs[jn] = 0
                        pending_jobs[jn] += 1

                    pending_summary = ", ".join(f"{jn}({cnt})" for jn, cnt in pending_jobs.items())
                    _log(
                        f"  ⏳ 发现 [bold yellow]{len(pending_pods)}[/bold yellow] 个 Pending Pod "
                        f"({len(pending_jobs)} 个任务: {pending_summary})"
                    )

                    yielded_jobs, yielded_nodes = _auto_yield_once(namespace)
                    if yielded_jobs > 0:
                        total_yielded_jobs += yielded_jobs
                        total_yielded_nodes += yielded_nodes
                        _log(
                            f"  ✅ 本轮让卡: 释放 {yielded_jobs} 个占卡任务, {yielded_nodes} 节点 "
                            f"(累计释放 {total_yielded_jobs} 任务, {total_yielded_nodes} 节点)"
                        )
                        if yielded_nodes > 0:
                            _log(f"  [dim]等待 10 秒让调度器分配资源...[/dim]")
                            time.sleep(10)
                    else:
                        _log(f"  [dim]无匹配的占卡任务可释放 (可能实例类型/AZ 不匹配)[/dim]")
                else:
                    _log(f"  ✅ 无 Pending 的训练任务")

                # ── 阶段2: 补占 ──
                if auto_refill:
                    all_nodes = _get_gpu_nodes(namespace)
                    busy_nodes = _get_busy_nodes(namespace)
                    free_count = len([
                        n for n in all_nodes
                        if n["name"] not in busy_nodes and not n.get("unschedulable", False)
                    ]) if all_nodes else 0

                    if free_count > 0:
                        _log(
                            f"  🔍 发现 [bold green]{free_count}[/bold green] 个空闲节点，正在自动补占..."
                        )
                        occupied = _auto_occupy(namespace, config)
                        if occupied > 0:
                            total_occupied_nodes += occupied
                            _log(
                                f"  ✅ 本轮补占 {occupied} 节点 (累计补占 {total_occupied_nodes} 节点)"
                            )
                        else:
                            _log(f"  ⚠️ 补占提交失败或节点已被抢占")
                    else:
                        total = len(all_nodes) if all_nodes else 0
                        _log(f"  ✅ 全部 {total} 节点已占满")

            except Exception as e:
                _log(f"  ❌ 守护异常: {e}")

            _log(f"  [dim]下次守护: {interval} 秒后...[/dim]")
            time.sleep(interval)

    except KeyboardInterrupt:
        pass

    # 输出守护报告
    report = (
        f"守护已停止 | 总轮数: {round_num} | "
        f"累计让卡: {total_yielded_jobs} 任务/{total_yielded_nodes} 节点 | "
        f"累计补占: {total_occupied_nodes} 节点"
    )
    import sys
    if sys.stdout.isatty():
        console.print()
        console.print(Panel(
            f"[bold]守护已停止[/bold]\n\n"
            f"  总守护轮数: {round_num}\n"
            f"  累计让卡: {total_yielded_jobs} 个任务, {total_yielded_nodes} 节点\n"
            f"  累计补占: {total_occupied_nodes} 节点",
            title="🛡️ 守护报告",
            border_style="yellow",
            padding=(0, 2),
        ))
    else:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {report}", flush=True)


# ── 守护进程管理辅助函数 ──

def _get_guard_pid() -> int:
    """读取守护进程 PID，返回 PID (进程存活) 或 0 (未运行)"""
    if not os.path.exists(GUARD_PID_FILE):
        return 0
    try:
        with open(GUARD_PID_FILE, "r") as f:
            pid = int(f.read().strip())
        # 检查进程是否存活
        os.kill(pid, 0)
        return pid
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        # 进程不存在或 PID 无效，清理残留文件
        _cleanup_pid_file()
        return 0


def _cleanup_pid_file():
    """清理 PID 文件"""
    try:
        if os.path.exists(GUARD_PID_FILE):
            os.remove(GUARD_PID_FILE)
    except OSError:
        pass


def _stop_guard_daemon():
    """停止后台守护进程"""
    pid = _get_guard_pid()
    if not pid:
        print_warning("当前没有运行中的守护进程")
        return

    console.print(f"[cyan]正在停止守护进程 (PID: {pid})...[/cyan]")
    try:
        os.kill(pid, signal.SIGTERM)
        # 等待进程退出
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            # 超时则强制 kill
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        _cleanup_pid_file()
        print_success(f"守护进程已停止 (PID: {pid})")

        # 显示最后几行日志
        if os.path.exists(GUARD_LOG_FILE):
            console.print(f"\n[dim]最近日志 ({GUARD_LOG_FILE}):[/dim]")
            try:
                from rich.markup import escape as _esc
                with open(GUARD_LOG_FILE, "r") as f:
                    lines = f.readlines()
                for line in lines[-10:]:
                    console.print(f"[dim]  {_esc(line.rstrip())}[/dim]")
            except Exception:
                pass

    except ProcessLookupError:
        _cleanup_pid_file()
        print_success(f"守护进程已不存在 (PID: {pid})")
    except PermissionError:
        print_error(f"无权限停止进程 {pid}")


def _tail_guard_log():
    """查看守护日志（最近 50 行 + 实时 tail）"""
    if not os.path.exists(GUARD_LOG_FILE):
        print_warning(f"日志文件不存在: {GUARD_LOG_FILE}")
        return

    console.print(f"[cyan]日志文件: {GUARD_LOG_FILE}[/cyan]")
    console.print(f"[dim]按 Ctrl+C 退出查看[/dim]\n")

    import subprocess
    try:
        subprocess.run(["tail", "-n", "50", "-f", GUARD_LOG_FILE])
    except KeyboardInterrupt:
        console.print("\n[dim]已退出日志查看[/dim]")
    except FileNotFoundError:
        # tail 命令不存在，回退到读文件
        try:
            from rich.markup import escape as _esc
            with open(GUARD_LOG_FILE, "r") as f:
                lines = f.readlines()
            for line in lines[-50:]:
                console.print(f"  {_esc(line.rstrip())}")
        except Exception as e:
            print_error(f"读取日志失败: {e}")
