"""功能: 查看所有任务列表（包括 Running / Pending / Failed 等全部状态）"""
import json
from datetime import datetime, timezone
from typing import List, Dict, Tuple

from rich.table import Table
from rich.panel import Panel
from InquirerPy import inquirer

from raytool.utils.kube import run_kubectl, get_pods, group_pods_by_job, get_pod_role
from raytool.utils.ui import (
    console, colorize_status, print_warning, print_info, print_error,
)


def list_jobs(namespace: str):
    """列出所有任务（按 PyTorchJob + Pod 全量展示）"""
    print_info("正在查询所有任务...")
    console.print()

    # ── 1. 获取所有 PyTorchJob ──
    pytorchjobs = _get_pytorchjobs(namespace)
    # ── 2. 获取所有 Pod ──
    pods = get_pods(namespace)

    if not pytorchjobs and not pods:
        print_warning("当前没有任何任务或 Pod")
        return

    # ── 3. 按状态分类 PyTorchJob ──
    running_jobs = []
    pending_jobs = []
    other_jobs = []

    for job in pytorchjobs:
        status = job["status"]
        if status in ("Running", "Succeeded"):
            running_jobs.append(job)
        elif status in ("Pending", "Creating", "Restarting"):
            pending_jobs.append(job)
        else:
            other_jobs.append(job)

    # ── 4. 状态摘要 ──
    total = len(pytorchjobs)
    _print_status_summary(pytorchjobs)
    console.print()

    # ── 5. 展示任务表格 ──
    if pytorchjobs:
        _print_all_jobs_table(pytorchjobs, pods, namespace)
    else:
        # 无 PyTorchJob，按 Pod 分组展示
        jobs = group_pods_by_job(pods)
        _print_pod_groups_table(jobs)

    console.print()

    # ── 6. Pending 任务特别提示 ──
    if pending_jobs:
        _print_pending_alerts(pending_jobs, pods)

    # ── 7. 交互操作 ──
    if not pytorchjobs:
        return

    action = inquirer.select(
        message="需要进一步操作吗？",
        choices=[
            {"name": "🔍 查看某个任务的 Pod 详情", "value": "detail"},
            {"name": "🩺 诊断 Pending 任务", "value": "diagnose"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
    ).execute()

    if action == "cancel":
        return
    elif action == "detail":
        _show_job_pod_detail(pytorchjobs, pods, namespace)
    elif action == "diagnose":
        _diagnose_pending(pending_jobs, pods, namespace)


def _get_pytorchjobs(namespace: str) -> List[Dict]:
    """获取所有 PyTorchJob，解析基本信息"""
    rc, stdout, stderr = run_kubectl(
        ["get", "pytorchjobs", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return []

    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, KeyError):
        return []

    jobs = []
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        status_block = item.get("status", {})

        name = metadata.get("name", "")
        creation = metadata.get("creationTimestamp", "")

        # 副本数
        replica_specs = spec.get("pytorchReplicaSpecs", {})
        master_replicas = replica_specs.get("Master", {}).get("replicas", 1)
        worker_replicas = replica_specs.get("Worker", {}).get("replicas", 0)
        total_nodes = master_replicas + worker_replicas

        # GPU 数（从容器资源请求中获取）
        try:
            gpu_per_node = int(
                replica_specs.get("Master", {}).get("template", {}).get("spec", {})
                .get("containers", [{}])[0]
                .get("resources", {}).get("requests", {}).get("nvidia.com/gpu", 0)
            )
        except (IndexError, ValueError):
            gpu_per_node = 0
        total_gpus = total_nodes * gpu_per_node

        # 实例类型
        try:
            instance_type = (
                replica_specs.get("Master", {}).get("template", {}).get("spec", {})
                .get("nodeSelector", {}).get("node.kubernetes.io/instance-type", "-")
            )
        except (KeyError, AttributeError):
            instance_type = "-"

        # 状态推断
        conditions = status_block.get("conditions", [])
        job_status = _infer_job_status(conditions, status_block)

        # 运行时间
        age = _calc_age_from_timestamp(creation)

        jobs.append({
            "name": name,
            "status": job_status,
            "creation": creation,
            "age": age,
            "master_replicas": master_replicas,
            "worker_replicas": worker_replicas,
            "total_nodes": total_nodes,
            "gpu_per_node": gpu_per_node,
            "total_gpus": total_gpus,
            "instance_type": instance_type,
        })

    return sorted(jobs, key=lambda j: j["creation"], reverse=True)


def _infer_job_status(conditions: list, status_block: dict) -> str:
    """从 PyTorchJob 的 conditions 推断当前状态"""
    # 优先看 conditions（最新的 condition 在最后）
    for cond in reversed(conditions):
        cond_type = cond.get("type", "")
        cond_status = cond.get("status", "")
        reason = cond.get("reason", "")
        if cond_type == "Succeeded" and cond_status == "True":
            return "Succeeded"
        if cond_type == "Failed" and cond_status == "True":
            return "Failed"
        if cond_type == "Running" and cond_status == "True":
            return "Running"
        if cond_type == "Created" and cond_status == "True":
            return "Pending"
        if cond_type == "Restarting" and cond_status == "True":
            return "Restarting"

    # 回退到 replicaStatuses
    replica_statuses = status_block.get("replicaStatuses", {})
    has_active = False
    for role_status in replica_statuses.values():
        if role_status.get("active", 0) > 0:
            has_active = True
    if has_active:
        return "Running"

    return "Pending"


def _calc_age_from_timestamp(ts: str) -> str:
    """从 ISO 时间戳计算 age 字符串"""
    if not ts:
        return "-"
    try:
        created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - created
        total_sec = int(delta.total_seconds())
        if total_sec < 60:
            return f"{total_sec}s"
        elif total_sec < 3600:
            return f"{total_sec // 60}m"
        elif total_sec < 86400:
            h = total_sec // 3600
            m = (total_sec % 3600) // 60
            return f"{h}h{m}m"
        else:
            d = total_sec // 86400
            h = (total_sec % 86400) // 3600
            return f"{d}d{h}h"
    except (ValueError, TypeError):
        return "-"


def _print_status_summary(pytorchjobs: list):
    """打印状态摘要面板"""
    from collections import Counter
    status_counts = Counter(j["status"] for j in pytorchjobs)
    total = len(pytorchjobs)
    total_gpus = sum(j["total_gpus"] for j in pytorchjobs)

    parts = []
    for status in ["Running", "Pending", "Failed", "Succeeded", "Restarting", "Creating"]:
        cnt = status_counts.get(status, 0)
        if cnt > 0:
            parts.append(f"{colorize_status(status)}: {cnt}")

    summary_line = " | ".join(parts) if parts else "无任务"
    console.print(Panel(
        f"  任务总数: [bold]{total}[/bold]  |  GPU 总计: [bold]{total_gpus}[/bold] 张\n"
        f"  {summary_line}",
        title="📊 任务状态摘要",
        border_style="cyan",
        padding=(0, 2),
    ))


def _print_all_jobs_table(pytorchjobs: list, pods: list, namespace: str):
    """打印全量 PyTorchJob 任务表格"""
    # 将 Pod 按任务名分组（用于统计实际 Pod 状态）
    pod_by_job = {}
    for p in pods:
        # PyTorchJob 的 Pod 名格式: {job-name}-{role}-{index}
        pname = p["name"]
        for job in pytorchjobs:
            jname = job["name"]
            if pname.startswith(jname + "-"):
                pod_by_job.setdefault(jname, []).append(p)
                break

    table = Table(title="📋 全部任务列表", show_lines=True, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("任务名称", style="bold cyan", min_width=30)
    table.add_column("状态", justify="center", width=12)
    table.add_column("节点", justify="center", width=6)
    table.add_column("GPU", justify="center", width=8)
    table.add_column("实例类型", style="magenta", min_width=16)
    table.add_column("Pod 状态", min_width=24)
    table.add_column("运行时间", justify="center", width=10)

    for i, job in enumerate(pytorchjobs, 1):
        # 统计该任务下 Pod 的各状态数
        job_pods = pod_by_job.get(job["name"], [])
        pod_status_str = _summarize_pod_statuses(job_pods, job["total_nodes"])

        instance_display = job["instance_type"].replace("ml.", "") if job["instance_type"] != "-" else "-"
        gpu_display = f"{job['total_gpus']}" if job["total_gpus"] > 0 else "-"

        table.add_row(
            str(i),
            job["name"],
            colorize_status(job["status"]),
            str(job["total_nodes"]),
            gpu_display,
            instance_display,
            pod_status_str,
            job["age"],
        )

    console.print(table)


def _summarize_pod_statuses(job_pods: list, expected_nodes: int) -> str:
    """将 Pod 状态汇总为简洁字符串，如 '4 Running' 或 '2 Running, 2 Pending'"""
    if not job_pods:
        return f"[dim]0/{expected_nodes} Pod[/dim]"

    from collections import Counter
    counts = Counter(p["status"] for p in job_pods)
    parts = []
    for status in ["Running", "Pending", "ContainerCreating", "Failed", "Error", "CrashLoopBackOff", "Succeeded", "Unknown"]:
        cnt = counts.get(status, 0)
        if cnt > 0:
            parts.append(f"{cnt} {colorize_status(status)}")

    total_pods = len(job_pods)
    result = ", ".join(parts)
    if total_pods < expected_nodes:
        result += f" [dim]({total_pods}/{expected_nodes})[/dim]"
    return result


def _print_pod_groups_table(jobs: dict):
    """无 PyTorchJob 时按 Pod 分组展示（兜底）"""
    table = Table(title="📋 Pod 分组列表", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("任务名称", style="bold cyan", min_width=25)
    table.add_column("状态", justify="center", width=12)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("Master/Head", justify="center", width=12)
    table.add_column("Worker", justify="center", width=8)

    for i, (job_name, pods) in enumerate(sorted(jobs.items()), 1):
        statuses = set(p["status"] for p in pods)
        if "Running" in statuses:
            status = "Running"
        elif "Pending" in statuses:
            status = "Pending"
        elif "Failed" in statuses:
            status = "Failed"
        else:
            status = list(statuses)[0] if statuses else "Unknown"

        head_count = sum(1 for p in pods if get_pod_role(p) in ("Head", "Master"))
        worker_count = sum(1 for p in pods if get_pod_role(p) == "Worker")

        table.add_row(
            str(i),
            job_name,
            colorize_status(status),
            str(len(pods)),
            str(head_count),
            str(worker_count),
        )

    console.print(table)


def _print_pending_alerts(pending_jobs: list, pods: list):
    """Pending 任务告警"""
    console.print(Panel(
        f"[bold yellow]⚠️  有 {len(pending_jobs)} 个任务处于 Pending 状态[/bold yellow]\n\n"
        + "\n".join(
            f"  [yellow]•[/yellow] {j['name']}  ({j['total_nodes']} 节点, {j['total_gpus']} GPU, 等待 {j['age']})"
            for j in pending_jobs
        )
        + "\n\n[dim]  提示: 选择「🩺 诊断 Pending 任务」查看详细原因[/dim]",
        title="⚠️  Pending 告警",
        border_style="yellow",
        padding=(0, 2),
    ))
    console.print()


def _show_job_pod_detail(pytorchjobs: list, pods: list, namespace: str):
    """选择某个任务，展示其下所有 Pod 的详细状态"""
    choices = []
    for job in pytorchjobs:
        label = f"{job['name']}  ({colorize_status(job['status'])}, {job['total_nodes']}节点, {job['age']})"
        choices.append({"name": label, "value": job["name"]})
    choices.append({"name": "❌ 返回", "value": "__cancel__"})

    selected = inquirer.select(
        message="选择要查看的任务",
        choices=choices,
        pointer="❯",
    ).execute()

    if selected == "__cancel__":
        return

    # 过滤该任务的 Pod
    job_pods = [p for p in pods if p["name"].startswith(selected + "-")]

    if not job_pods:
        print_warning(f"任务 {selected} 下没有找到 Pod")
        return

    console.print()
    table = Table(title=f"📋 {selected} — Pod 详情", show_lines=True, border_style="cyan")
    table.add_column("#", style="dim", width=4)
    table.add_column("Pod 名称", style="cyan", min_width=40)
    table.add_column("角色", justify="center", width=8)
    table.add_column("状态", justify="center", width=18)
    table.add_column("READY", justify="center", width=8)
    table.add_column("重启", justify="center", width=6)
    table.add_column("创建时间", width=22)

    for i, pod in enumerate(sorted(job_pods, key=lambda p: p["name"]), 1):
        role = pod.get("role", "Unknown")
        # Master/Worker 推断
        if role == "Unknown":
            if "-master-" in pod["name"]:
                role = "Master"
            elif "-worker-" in pod["name"]:
                role = "Worker"

        table.add_row(
            str(i),
            pod["name"],
            role,
            colorize_status(pod["status"]),
            pod["ready"],
            str(pod["restarts"]),
            pod["creation"][:19].replace("T", " ") if pod["creation"] else "-",
        )

    console.print(table)


def _diagnose_pending(pending_jobs: list, pods: list, namespace: str):
    """诊断 Pending 任务：获取事件信息展示原因"""
    if not pending_jobs:
        print_warning("当前没有 Pending 的任务")
        return

    # 选择要诊断的任务
    if len(pending_jobs) == 1:
        target = pending_jobs[0]
    else:
        choices = []
        for job in pending_jobs:
            choices.append({
                "name": f"{job['name']}  ({job['total_nodes']}节点, 等待 {job['age']})",
                "value": job["name"],
            })
        choices.append({"name": "📋 诊断全部", "value": "__all__"})
        choices.append({"name": "❌ 返回", "value": "__cancel__"})

        selected = inquirer.select(
            message="选择要诊断的任务",
            choices=choices,
            pointer="❯",
        ).execute()

        if selected == "__cancel__":
            return

        if selected == "__all__":
            for job in pending_jobs:
                _diagnose_single_job(job, pods, namespace)
            return

        target = next((j for j in pending_jobs if j["name"] == selected), None)
        if not target:
            return

    _diagnose_single_job(target, pods, namespace)


def _diagnose_single_job(job: dict, pods: list, namespace: str):
    """诊断单个 Pending 任务"""
    console.print()
    console.print(f"[bold cyan]🩺 诊断: {job['name']}[/bold cyan]")
    console.print(f"  状态: {colorize_status(job['status'])}  |  节点: {job['total_nodes']}  |  GPU: {job['total_gpus']}  |  等待: {job['age']}")
    console.print()

    # 找到该任务的一个 Pending Pod，查看事件
    job_pods = [p for p in pods if p["name"].startswith(job["name"] + "-") and p["status"] == "Pending"]

    if not job_pods:
        # 没有 Pending Pod，可能 Pod 还没创建出来
        print_warning("未找到 Pending Pod，任务可能还未创建 Pod")
        # 查看 PyTorchJob 事件
        print_info(f"正在获取 PyTorchJob 事件...")
        rc, stdout, stderr = run_kubectl(
            ["describe", "pytorchjob", job["name"]],
            namespace,
            timeout=15,
        )
        if rc == 0:
            # 提取 Events 部分
            _print_events_section(stdout, job["name"])
        else:
            print_error(f"获取事件失败: {stderr.strip()}")
        return

    # 取第一个 Pending Pod 查看事件
    target_pod = job_pods[0]["name"]
    print_info(f"正在获取 Pod 事件: {target_pod}")

    rc, stdout, stderr = run_kubectl(
        ["describe", "pod", target_pod],
        namespace,
        timeout=15,
    )
    if rc != 0:
        print_error(f"获取 Pod 信息失败: {stderr.strip()}")
        return

    _print_events_section(stdout, target_pod)


def _print_events_section(describe_output: str, resource_name: str):
    """从 kubectl describe 输出中提取并展示 Events 部分"""
    lines = describe_output.split("\n")
    events_start = -1
    for i, line in enumerate(lines):
        if line.strip().startswith("Events:"):
            events_start = i
            break

    if events_start == -1:
        print_warning("未找到事件信息")
        return

    events_lines = lines[events_start:]

    # 解析事件，高亮 Warning
    formatted = []
    for line in events_lines:
        stripped = line.strip()
        if stripped.startswith("Warning") or "Warning" in stripped:
            formatted.append(f"  [bold yellow]{stripped}[/bold yellow]")
        elif stripped.startswith("Normal"):
            formatted.append(f"  [dim]{stripped}[/dim]")
        elif stripped.startswith("Events:"):
            formatted.append(f"  [bold]{stripped}[/bold]")
        elif stripped.startswith("Type") and "Reason" in stripped:
            formatted.append(f"  [bold]{stripped}[/bold]")
        elif stripped.startswith("----"):
            formatted.append(f"  [dim]{stripped}[/dim]")
        else:
            formatted.append(f"  {stripped}")

    console.print(Panel(
        "\n".join(formatted),
        title=f"📋 {resource_name} 事件",
        border_style="yellow",
        padding=(0, 1),
    ))
    console.print()
