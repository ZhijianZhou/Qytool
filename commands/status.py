"""功能10: 集群概况总览"""
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from raytool.utils.kube import (
    get_pods, get_running_pods, group_pods_by_job, get_pod_role, run_kubectl,
)
from raytool.utils.ui import (
    console, colorize_status, print_info, print_error, print_warning, print_success,
    confirm,
)
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns

# 占卡任务名匹配正则 (与 occupy.py 保持一致)
_OCCUPY_NAME_PATTERN = re.compile(r"^run-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}$")


def cluster_status(namespace: str, config: dict = None, current_user: str = None):
    """显示集群概况总览: Pod 统计、任务列表、资源使用、异常告警"""
    print_info(f"正在获取 {namespace} 命名空间的集群概况...")
    console.print()

    # ── 获取所有 Pod ──
    all_pods = get_pods(namespace)
    if not all_pods:
        print_warning("当前命名空间下没有任何 Pod")
        return

    # ── 1. Pod 状态统计 ──
    _print_pod_summary(all_pods)
    console.print()

    # ── 2. GPU 卡数总览 ──
    _print_gpu_card_summary(namespace, all_pods)
    console.print()

    # ── 3. 任务概览表 ──
    jobs = group_pods_by_job(all_pods)
    _print_jobs_overview(jobs)
    console.print()

    # ── 4. 资源使用情况 (如果 metrics-server 可用) ──
    _print_resource_usage(namespace, all_pods)
    console.print()

    # ── 5. 异常 Pod 告警 ──
    _print_alerts(all_pods)

    # ── 6. RayCluster / RayJob CRD 状态 ──
    _print_crd_status(namespace)

    # ── 7. Succeeded Pods 清理建议 ──
    _check_and_cleanup_succeeded_pods(all_pods, namespace, config, current_user)


def _print_pod_summary(pods: list):
    """打印 Pod 状态统计面板"""
    status_counts = {}
    for pod in pods:
        s = pod["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    total = len(pods)
    running = status_counts.get("Running", 0)
    pending = status_counts.get("Pending", 0) + status_counts.get("ContainerCreating", 0)
    failed = status_counts.get("Failed", 0) + status_counts.get("Error", 0) + status_counts.get("CrashLoopBackOff", 0)
    other = total - running - pending - failed

    # 构建统计面板
    lines = []
    lines.append(f"[bold]总计:[/bold] {total} 个 Pod")
    lines.append(f"  [green]Running:[/green]  {running}")
    lines.append(f"  [yellow]Pending:[/yellow]  {pending}")
    lines.append(f"  [red]Failed:[/red]   {failed}")
    if other > 0:
        lines.append(f"  [dim]Other:[/dim]    {other}")

    # 状态条
    if total > 0:
        bar_width = 40
        r_len = max(1, round(running / total * bar_width)) if running else 0
        p_len = max(1, round(pending / total * bar_width)) if pending else 0
        f_len = max(1, round(failed / total * bar_width)) if failed else 0
        o_len = bar_width - r_len - p_len - f_len
        if o_len < 0:
            o_len = 0
        bar = f"[green]{'█' * r_len}[/green][yellow]{'█' * p_len}[/yellow][red]{'█' * f_len}[/red][dim]{'░' * o_len}[/dim]"
        lines.append(f"\n  {bar}")

    panel_content = "\n".join(lines)
    console.print(Panel(panel_content, title="📊 Pod 状态统计", border_style="cyan", padding=(0, 2)))


def _get_all_ns_gpu_usage(node_gpu_map: dict, current_namespace: str) -> tuple:
    """查询所有命名空间的 GPU 占用情况，返回其他命名空间在 GPU 节点上的使用统计。
    返回: (ns_gpu_usage, pod_details)
        ns_gpu_usage: {namespace: gpu_count}
        pod_details:  [{namespace, pod_name, node_name, gpu_count, phase}]
    """
    try:
        cmd = [
            "kubectl", "get", "pods", "--all-namespaces", "-o", "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode != 0:
            return {}, []

        data = json.loads(result.stdout)
        ns_gpu_usage = {}  # {namespace: gpu_count}
        pod_details = []

        for item in data.get("items", []):
            phase = item.get("status", {}).get("phase", "")
            if phase not in ("Running", "Pending", "ContainerCreating"):
                continue

            pod_ns = item.get("metadata", {}).get("namespace", "")
            if pod_ns == current_namespace:
                continue

            node_name = item.get("spec", {}).get("nodeName", "")
            if node_name not in node_gpu_map:
                continue

            pod_gpu = 0
            for container in item.get("spec", {}).get("containers", []):
                pod_gpu += int(
                    container.get("resources", {})
                    .get("requests", {})
                    .get("nvidia.com/gpu", 0)
                )
            if pod_gpu > 0:
                ns_gpu_usage[pod_ns] = ns_gpu_usage.get(pod_ns, 0) + pod_gpu
                pod_details.append({
                    "namespace": pod_ns,
                    "pod_name": item.get("metadata", {}).get("name", ""),
                    "node_name": node_name,
                    "gpu_count": pod_gpu,
                    "phase": phase,
                })

        return ns_gpu_usage, pod_details
    except Exception:
        return {}, []


def _print_gpu_card_summary(namespace: str, all_pods: list):
    """打印 GPU 卡数总览面板: 总卡数、占卡卡数、运行任务卡数、其他空间占用、空闲卡数"""
    # ── 1. 获取所有 GPU 节点的总卡数 ──
    rc, stdout, stderr = run_kubectl(
        ["get", "nodes", "-o", "json"],
        namespace,
        timeout=15,
    )
    if rc != 0:
        return

    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, KeyError):
        return

    # 建立节点 → GPU 数量的映射
    node_gpu_map = {}  # {node_name: gpu_count}
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        status_block = item.get("status", {})
        capacity = status_block.get("capacity", {})
        gpu_count = int(capacity.get("nvidia.com/gpu", 0))
        if gpu_count > 0:
            node_gpu_map[metadata.get("name", "")] = gpu_count

    total_gpus = sum(node_gpu_map.values())
    if total_gpus == 0:
        return

    # ── 2. 获取当前命名空间 Pod 的 JSON 数据来判断 GPU 占用 ──
    rc2, stdout2, stderr2 = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace,
        timeout=15,
    )

    occupy_gpus = 0    # 占卡任务使用的 GPU 数
    running_gpus = 0   # 正常运行任务使用的 GPU 数
    occupy_jobs = set()
    running_jobs = set()
    our_busy_nodes = set()  # 当前命名空间有 GPU Pod 的节点

    if rc2 == 0:
        try:
            pod_data = json.loads(stdout2)
            for item in pod_data.get("items", []):
                phase = item.get("status", {}).get("phase", "")
                if phase not in ("Running", "Pending", "ContainerCreating"):
                    continue

                # 计算该 Pod 请求的 GPU 数
                pod_gpu = 0
                spec = item.get("spec", {})
                for container in spec.get("containers", []):
                    pod_gpu += int(
                        container.get("resources", {})
                        .get("requests", {})
                        .get("nvidia.com/gpu", 0)
                    )
                if pod_gpu == 0:
                    continue

                node_name = spec.get("nodeName", "")
                if node_name and node_name in node_gpu_map:
                    our_busy_nodes.add(node_name)

                # 推断任务名
                labels = item.get("metadata", {}).get("labels", {})
                job_name = (
                    labels.get("training.kubeflow.org/job-name", "")
                    or labels.get("ray.io/cluster", "")
                    or labels.get("ray.io/job-name", "")
                    or labels.get("app.kubernetes.io/instance", "")
                    or ""
                )

                # 判断是占卡任务还是正常任务
                if job_name and _OCCUPY_NAME_PATTERN.match(job_name):
                    occupy_gpus += pod_gpu
                    occupy_jobs.add(job_name)
                else:
                    running_gpus += pod_gpu
                    if job_name:
                        running_jobs.add(job_name)
        except (json.JSONDecodeError, KeyError):
            pass

    # ── 3. 查询其他命名空间在 GPU 节点上的占用 ──
    other_ns_usage, other_ns_pods = _get_all_ns_gpu_usage(node_gpu_map, namespace)
    other_ns_gpus = sum(other_ns_usage.values())

    free_gpus = total_gpus - occupy_gpus - running_gpus - other_ns_gpus
    if free_gpus < 0:
        free_gpus = 0

    # ── 4. 构建面板 ──
    lines = []
    lines.append(f"[bold]GPU 总卡数:[/bold]     [bold cyan]{total_gpus}[/bold cyan] 张  ({len(node_gpu_map)} 个 GPU 节点)")
    lines.append(f"[bold]运行任务:[/bold]      [bold green]{running_gpus}[/bold green] 张  ({len(running_jobs)} 个任务)")
    lines.append(f"[bold]占卡任务:[/bold]      [bold yellow]{occupy_gpus}[/bold yellow] 张  ({len(occupy_jobs)} 个占卡)")
    if other_ns_gpus > 0:
        lines.append(f"[bold]其他空间:[/bold]      [bold magenta]{other_ns_gpus}[/bold magenta] 张  ({len(other_ns_usage)} 个命名空间)")
        for ns, cnt in sorted(other_ns_usage.items(), key=lambda x: -x[1]):
            lines.append(f"  [dim]  └─ {ns}:[/dim] [magenta]{cnt}[/magenta] 张")
    else:
        lines.append(f"[bold]其他空间:[/bold]      [dim]0 张[/dim]")
    lines.append(f"[bold]空闲:[/bold]          [bold]{free_gpus}[/bold] 张")

    # GPU 使用率条
    bar_width = 40
    if total_gpus > 0:
        r_len = max(1, round(running_gpus / total_gpus * bar_width)) if running_gpus else 0
        o_len = max(1, round(occupy_gpus / total_gpus * bar_width)) if occupy_gpus else 0
        x_len = max(1, round(other_ns_gpus / total_gpus * bar_width)) if other_ns_gpus else 0
        f_len = bar_width - r_len - o_len - x_len
        if f_len < 0:
            f_len = 0
        bar = (
            f"[green]{'█' * r_len}[/green]"
            f"[yellow]{'█' * o_len}[/yellow]"
            f"[magenta]{'█' * x_len}[/magenta]"
            f"[dim]{'░' * f_len}[/dim]"
        )
        lines.append(f"\n  {bar}")
        legend = f"  [green]█[/green] 运行任务  [yellow]█[/yellow] 占卡"
        if other_ns_gpus > 0:
            legend += f"  [magenta]█[/magenta] 其他空间"
        legend += f"  [dim]░[/dim] 空闲"
        lines.append(legend)

    panel_content = "\n".join(lines)
    console.print(Panel(panel_content, title="🎮 GPU 卡数总览", border_style="cyan", padding=(0, 2)))

    # ── 5. 其他空间 Pod 详情表 ──
    if other_ns_pods:
        table = Table(
            title="🔍 其他命名空间 GPU 占用详情",
            show_lines=False,
            border_style="dim",
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("命名空间", style="magenta", min_width=20)
        table.add_column("Pod 名称", style="cyan", min_width=40)
        table.add_column("节点", style="dim", min_width=30)
        table.add_column("GPU", justify="center", width=6)
        table.add_column("状态", justify="center", width=12)

        for i, pod in enumerate(
            sorted(other_ns_pods, key=lambda x: (x["namespace"], x["pod_name"])), 1
        ):
            table.add_row(
                str(i),
                pod["namespace"],
                pod["pod_name"],
                pod["node_name"],
                str(pod["gpu_count"]),
                colorize_status(pod["phase"]),
            )

        console.print(table)

    # ── 6. 当前空间空闲节点列表 (含 GPU 利用率) ──
    other_busy_nodes = set(p["node_name"] for p in other_ns_pods)
    free_nodes = []
    for node_name, gpu_count in sorted(node_gpu_map.items()):
        if node_name not in our_busy_nodes:
            used_by_other = node_name in other_busy_nodes
            free_nodes.append((node_name, gpu_count, used_by_other))

    if free_nodes:
        # 并行查询空闲节点的 GPU 利用率
        node_gpu_util = {}
        print_info(f"正在查询 {len(free_nodes)} 个空闲节点的 GPU 利用率...")
        with ThreadPoolExecutor(max_workers=min(8, len(free_nodes))) as executor:
            futures = {
                executor.submit(_query_gpu_for_node, n): n
                for n, _, _ in free_nodes
            }
            for future in as_completed(futures):
                result = future.result()
                node_gpu_util[result["node"]] = result

        truly_free = [(n, g) for n, g, used in free_nodes if not used]
        other_used = [(n, g) for n, g, used in free_nodes if used]

        # 用表格展示空闲节点 + GPU 利用率
        table = Table(
            show_lines=False,
            border_style="dim",
            padding=(0, 1),
            show_header=True,
        )
        table.add_column("状态", width=4, justify="center")
        table.add_column("节点名称", style="cyan", min_width=30)
        table.add_column("GPU 数", justify="center", width=7)
        table.add_column("GPU 利用率", justify="right", width=14)
        table.add_column("GPU 显存", justify="right", width=22)

        def _format_node_gpu(node_name: str) -> tuple:
            info = node_gpu_util.get(node_name)
            if not info or not info["gpus"]:
                err = info["error"] if info and info["error"] else ""
                return ("[dim]N/A[/dim]", f"[dim]{err[:18]}[/dim]" if err else "[dim]-[/dim]")
            gpus = info["gpus"]
            avg_util = sum(g["util"] for g in gpus) / len(gpus)
            total_mem_used = sum(g["mem_used"] for g in gpus)
            total_mem_total = sum(g["mem_total"] for g in gpus)
            mem_pct = total_mem_used / total_mem_total * 100 if total_mem_total > 0 else 0
            if avg_util >= 80:
                util_str = f"[green]{avg_util:.0f}%[/green]"
            elif avg_util >= 30:
                util_str = f"[yellow]{avg_util:.0f}%[/yellow]"
            else:
                util_str = f"[red]{avg_util:.0f}%[/red]"
            mem_used_gb = total_mem_used / 1024
            mem_total_gb = total_mem_total / 1024
            if mem_pct >= 80:
                gmem_str = f"[yellow]{mem_used_gb:.0f}[/yellow]/{mem_total_gb:.0f}GB ({mem_pct:.0f}%)"
            else:
                gmem_str = f"{mem_used_gb:.0f}/{mem_total_gb:.0f}GB ({mem_pct:.0f}%)"
            return (util_str, gmem_str)

        if truly_free:
            truly_free_gpus = sum(g for _, g in truly_free)
            table.add_row(
                "", f"[green bold]完全空闲: {len(truly_free)} 个节点, {truly_free_gpus} 张 GPU[/green bold]",
                "", "", "",
            )
            for node_name, gpu_count in truly_free:
                util_str, gmem_str = _format_node_gpu(node_name)
                table.add_row("[green]✅[/green]", node_name, str(gpu_count), util_str, gmem_str)

        if other_used:
            other_used_gpus = sum(g for _, g in other_used)
            table.add_row(
                "", f"[magenta bold]被其他空间占用: {len(other_used)} 个节点, {other_used_gpus} 张 GPU[/magenta bold]",
                "", "", "",
            )
            for node_name, gpu_count in other_used:
                util_str, gmem_str = _format_node_gpu(node_name)
                table.add_row("[magenta]⚠️[/magenta]", node_name, str(gpu_count), util_str, gmem_str)

        console.print(Panel(
            table,
            title=f"🟢 本空间空闲节点 ({len(free_nodes)} 个节点, {sum(g for _, g, _ in free_nodes)} 张 GPU)",
            border_style="green",
            padding=(0, 1),
        ))


def _print_jobs_overview(jobs: dict):
    """打印任务概览表"""
    if not jobs:
        return

    table = Table(title="📋 任务概览", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("任务名称", style="bold cyan", min_width=25)
    table.add_column("状态", justify="center", width=18)
    table.add_column("Head", justify="center", width=6)
    table.add_column("Worker", justify="center", width=8)
    table.add_column("Ready", justify="center", width=10)
    table.add_column("重启", justify="center", width=6)
    table.add_column("运行时间", width=14)

    for i, (job_name, pods) in enumerate(sorted(jobs.items()), 1):
        # 状态汇总
        statuses = set(p["status"] for p in pods)
        if all(s == "Running" for s in statuses):
            status = "Running"
        elif "Failed" in statuses or "Error" in statuses or "CrashLoopBackOff" in statuses:
            status = "Failed"
        elif "Pending" in statuses or "ContainerCreating" in statuses:
            status = "Pending"
        else:
            status = list(statuses)[0] if statuses else "Unknown"

        head_count = sum(1 for p in pods if get_pod_role(p) == "Head")
        worker_count = sum(1 for p in pods if get_pod_role(p) == "Worker")

        # Ready 计数
        ready_parts = [p["ready"].split("/") for p in pods]
        total_ready = sum(int(r[0]) for r in ready_parts if len(r) == 2)
        total_containers = sum(int(r[1]) for r in ready_parts if len(r) == 2)

        # 总重启次数
        total_restarts = sum(p["restarts"] for p in pods)

        # 运行时间 (取最早的 Pod 创建时间)
        age = _calc_age(pods)

        table.add_row(
            str(i),
            job_name,
            colorize_status(status),
            str(head_count),
            str(worker_count),
            f"{total_ready}/{total_containers}",
            str(total_restarts) if total_restarts == 0 else f"[yellow]{total_restarts}[/yellow]",
            age,
        )

    console.print(table)


def _parse_nvidia_smi_output(stdout: str) -> list:
    """解析 nvidia-smi CSV 输出，返回 GPU 信息列表"""
    gpus = []
    for line in stdout.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 4:
            try:
                gpus.append({
                    "index": parts[0],
                    "util": int(parts[1]),
                    "mem_used": int(parts[2]),
                    "mem_total": int(parts[3]),
                })
            except (ValueError, IndexError):
                continue
    return gpus


def _query_gpu_for_pod(pod_name: str, namespace: str) -> dict:
    """在单个 Pod 内执行 nvidia-smi 获取 GPU 信息"""
    cmd = [
        "kubectl", "exec", pod_name, "-n", namespace, "--",
        "nvidia-smi",
        "--query-gpu=index,utilization.gpu,memory.used,memory.total",
        "--format=csv,noheader,nounits",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return {"pod": pod_name, "gpus": [], "error": result.stderr.strip()}
        return {"pod": pod_name, "gpus": _parse_nvidia_smi_output(result.stdout), "error": ""}
    except subprocess.TimeoutExpired:
        return {"pod": pod_name, "gpus": [], "error": "timeout"}
    except Exception as e:
        return {"pod": pod_name, "gpus": [], "error": str(e)}


def _query_gpu_for_node(node_name: str) -> dict:
    """通过节点上的 DaemonSet Pod (如 nvidia-device-plugin) 执行 nvidia-smi 获取 GPU 利用率。
    会依次尝试常见的 GPU 相关 DaemonSet Pod。
    """
    result_stub = {"node": node_name, "gpus": [], "error": ""}

    # 找到该节点上所有 Running 的 Pod
    try:
        cmd = [
            "kubectl", "get", "pods", "--all-namespaces",
            "--field-selector", f"spec.nodeName={node_name},status.phase=Running",
            "-o", "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            result_stub["error"] = "无法获取节点 Pod"
            return result_stub

        data = json.loads(result.stdout)
        items = data.get("items", [])
        if not items:
            result_stub["error"] = "节点无 Running Pod"
            return result_stub
    except Exception as e:
        result_stub["error"] = str(e)
        return result_stub

    # 优先尝试 GPU 相关的 DaemonSet Pod (nvidia-device-plugin, dcgm-exporter 等)
    gpu_daemon_keywords = ["nvidia", "dcgm", "gpu"]
    candidate_pods = []
    other_pods = []
    for item in items:
        pod_name = item.get("metadata", {}).get("name", "")
        pod_ns = item.get("metadata", {}).get("namespace", "")
        owner_refs = item.get("metadata", {}).get("ownerReferences", [])
        is_daemonset = any(ref.get("kind") == "DaemonSet" for ref in owner_refs)

        if is_daemonset and any(kw in pod_name.lower() for kw in gpu_daemon_keywords):
            candidate_pods.append((pod_name, pod_ns))
        elif is_daemonset:
            other_pods.append((pod_name, pod_ns))

    # 非 DaemonSet Pod 作为最后备选
    for item in items:
        pod_name = item.get("metadata", {}).get("name", "")
        pod_ns = item.get("metadata", {}).get("namespace", "")
        pair = (pod_name, pod_ns)
        if pair not in candidate_pods and pair not in other_pods:
            other_pods.append(pair)

    # 依次尝试 exec nvidia-smi
    for pod_name, pod_ns in candidate_pods + other_pods:
        try:
            exec_cmd = [
                "kubectl", "exec", pod_name, "-n", pod_ns, "--",
                "nvidia-smi",
                "--query-gpu=index,utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ]
            exec_result = subprocess.run(exec_cmd, capture_output=True, text=True, timeout=10)
            if exec_result.returncode == 0 and exec_result.stdout.strip():
                gpus = _parse_nvidia_smi_output(exec_result.stdout)
                if gpus:
                    return {"node": node_name, "gpus": gpus, "error": ""}
        except (subprocess.TimeoutExpired, Exception):
            continue

    result_stub["error"] = "nvidia-smi 不可用"
    return result_stub


def _print_resource_usage(namespace: str, pods: list):
    """尝试获取并展示资源使用情况 (需要 metrics-server)"""
    rc, stdout, stderr = run_kubectl(
        ["top", "pods", "--no-headers"],
        namespace,
        timeout=10,
    )

    # 解析 CPU/内存 metrics
    cpu_mem_map = {}
    if rc == 0:
        for line in stdout.strip().splitlines():
            parts = line.split()
            if len(parts) >= 3:
                cpu_mem_map[parts[0]] = {"cpu": parts[1], "mem": parts[2]}

    # 收集 Running Pod 名称用于 GPU 查询
    running_pods = [p["name"] for p in pods if p["status"] == "Running"]

    # 并行查询每个 Pod 的 GPU 利用率
    gpu_map = {}
    if running_pods:
        print_info(f"正在查询 {len(running_pods)} 个 Pod 的 GPU 利用率...")
        with ThreadPoolExecutor(max_workers=min(16, len(running_pods))) as executor:
            futures = {
                executor.submit(_query_gpu_for_pod, pod_name, namespace): pod_name
                for pod_name in running_pods
            }
            for future in as_completed(futures):
                result = future.result()
                gpu_map[result["pod"]] = result

    if not cpu_mem_map and not gpu_map:
        return

    # 构建表格
    table = Table(title="💻 资源使用情况", show_lines=False, border_style="dim")
    table.add_column("Pod 名称", style="cyan", min_width=30)
    table.add_column("CPU", justify="right", width=12)
    table.add_column("内存", justify="right", width=12)
    table.add_column("GPU 利用率", justify="right", width=14)
    table.add_column("GPU 显存", justify="right", width=18)

    all_pod_names = sorted(set(list(cpu_mem_map.keys()) + list(gpu_map.keys())))
    for pod_name in all_pod_names:
        cm = cpu_mem_map.get(pod_name, {})
        cpu = cm.get("cpu", "-")
        mem = cm.get("mem", "-")

        # CPU 高亮
        cpu_style = ""
        cpu_end = ""
        if cpu != "-" and cpu.endswith("m"):
            try:
                if int(cpu[:-1]) > 4000:
                    cpu_style = "[red]"
                    cpu_end = "[/red]"
            except ValueError:
                pass

        # GPU 信息
        gpu_info = gpu_map.get(pod_name)
        if gpu_info and gpu_info["gpus"]:
            gpus = gpu_info["gpus"]
            # 平均利用率
            avg_util = sum(g["util"] for g in gpus) / len(gpus)
            total_mem_used = sum(g["mem_used"] for g in gpus)
            total_mem_total = sum(g["mem_total"] for g in gpus)
            mem_pct = total_mem_used / total_mem_total * 100 if total_mem_total > 0 else 0

            # 利用率颜色
            if avg_util >= 80:
                util_str = f"[green]{avg_util:.0f}%[/green] ({len(gpus)}卡)"
            elif avg_util >= 30:
                util_str = f"[yellow]{avg_util:.0f}%[/yellow] ({len(gpus)}卡)"
            else:
                util_str = f"[red]{avg_util:.0f}%[/red] ({len(gpus)}卡)"

            # 显存
            mem_used_gb = total_mem_used / 1024
            mem_total_gb = total_mem_total / 1024
            if mem_pct >= 80:
                gmem_str = f"[yellow]{mem_used_gb:.0f}[/yellow]/{mem_total_gb:.0f}GB ({mem_pct:.0f}%)"
            else:
                gmem_str = f"{mem_used_gb:.0f}/{mem_total_gb:.0f}GB ({mem_pct:.0f}%)"
        elif gpu_info and gpu_info["error"]:
            util_str = "[dim]N/A[/dim]"
            gmem_str = f"[dim]{gpu_info['error'][:20]}[/dim]"
        else:
            util_str = "-"
            gmem_str = "-"

        table.add_row(
            pod_name,
            f"{cpu_style}{cpu}{cpu_end}",
            mem,
            util_str,
            gmem_str,
        )

    console.print(table)


def _print_alerts(pods: list):
    """检查并打印异常 Pod 告警"""
    alerts = []

    for pod in pods:
        name = pod["name"]
        status = pod["status"]

        # 异常状态
        if status in ("Failed", "Error", "CrashLoopBackOff", "ImagePullBackOff"):
            alerts.append(f"[red]  ❌ {name}: 状态异常 ({status})[/red]")

        # 高重启次数
        if pod["restarts"] > 5:
            alerts.append(f"[yellow]  ⚠️  {name}: 重启次数过多 ({pod['restarts']}次)[/yellow]")

        # 容器未就绪
        ready_parts = pod["ready"].split("/")
        if len(ready_parts) == 2:
            ready, total = int(ready_parts[0]), int(ready_parts[1])
            if ready < total and status == "Running":
                alerts.append(f"[yellow]  ⚠️  {name}: 容器未全部就绪 ({pod['ready']})[/yellow]")

        # Pending 超过 5 分钟
        if status in ("Pending", "ContainerCreating") and pod["creation"]:
            try:
                created = datetime.fromisoformat(pod["creation"].replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                age_minutes = (now - created).total_seconds() / 60
                if age_minutes > 5:
                    alerts.append(f"[yellow]  ⚠️  {name}: Pending 已超过 {int(age_minutes)} 分钟[/yellow]")
            except (ValueError, TypeError):
                pass

    if alerts:
        alert_content = "\n".join(alerts)
        console.print(Panel(alert_content, title="🚨 异常告警", border_style="red", padding=(0, 1)))
    else:
        console.print(Panel(
            "[green]  ✅ 所有 Pod 状态正常，未发现异常[/green]",
            title="🚨 异常告警",
            border_style="green",
            padding=(0, 1),
        ))


def _print_crd_status(namespace: str):
    """获取 RayCluster / RayJob CRD 状态"""
    console.print()

    # 尝试获取 RayCluster
    rc, stdout, stderr = run_kubectl(
        ["get", "rayclusters", "-o", "json"],
        namespace,
        timeout=10,
    )

    has_crd = False

    if rc == 0:
        try:
            data = json.loads(stdout)
            items = data.get("items", [])
            if items:
                has_crd = True
                table = Table(title="🔷 RayCluster 资源", show_lines=False, border_style="dim")
                table.add_column("名称", style="bold cyan", min_width=25)
                table.add_column("状态", justify="center", width=12)
                table.add_column("Head 副本", justify="center", width=10)
                table.add_column("Worker 副本", justify="center", width=12)
                table.add_column("创建时间", width=22)

                for item in items:
                    meta = item.get("metadata", {})
                    spec = item.get("spec", {})
                    status = item.get("status", {})

                    name = meta.get("name", "-")
                    state = status.get("state", status.get("phase", "Unknown"))

                    # Head
                    head_spec = spec.get("headGroupSpec", {})
                    head_replicas = 1  # Head 默认 1

                    # Worker
                    worker_groups = spec.get("workerGroupSpecs", [])
                    worker_replicas = sum(wg.get("replicas", 0) for wg in worker_groups)

                    creation = meta.get("creationTimestamp", "-")
                    if creation and creation != "-":
                        creation = creation[:19].replace("T", " ")

                    table.add_row(
                        name,
                        colorize_status(state),
                        str(head_replicas),
                        str(worker_replicas),
                        creation,
                    )

                console.print(table)
        except (json.JSONDecodeError, KeyError):
            pass

    # 尝试获取 RayJob
    rc2, stdout2, stderr2 = run_kubectl(
        ["get", "rayjobs", "-o", "json"],
        namespace,
        timeout=10,
    )

    if rc2 == 0:
        try:
            data2 = json.loads(stdout2)
            items2 = data2.get("items", [])
            if items2:
                has_crd = True
                table2 = Table(title="🔶 RayJob 资源", show_lines=False, border_style="dim")
                table2.add_column("名称", style="bold cyan", min_width=25)
                table2.add_column("状态", justify="center", width=12)
                table2.add_column("入口", min_width=20)
                table2.add_column("创建时间", width=22)

                for item in items2:
                    meta = item.get("metadata", {})
                    status = item.get("status", {})

                    name = meta.get("name", "-")
                    state = status.get("jobStatus", status.get("jobDeploymentStatus", "Unknown"))
                    entrypoint = item.get("spec", {}).get("entrypoint", "-")
                    if len(entrypoint) > 40:
                        entrypoint = entrypoint[:37] + "..."

                    creation = meta.get("creationTimestamp", "-")
                    if creation and creation != "-":
                        creation = creation[:19].replace("T", " ")

                    table2.add_row(name, colorize_status(state), entrypoint, creation)

                console.print(table2)
        except (json.JSONDecodeError, KeyError):
            pass

    if not has_crd:
        console.print("[dim]未检测到 RayCluster / RayJob CRD 资源[/dim]")


def _calc_age(pods: list) -> str:
    """计算任务运行时间（取最早 Pod 的创建时间）"""
    earliest = None
    for pod in pods:
        ts = pod.get("creation", "")
        if not ts:
            continue
        try:
            created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if earliest is None or created < earliest:
                earliest = created
        except (ValueError, TypeError):
            continue

    if earliest is None:
        return "-"

    now = datetime.now(timezone.utc)
    delta = now - earliest
    total_seconds = int(delta.total_seconds())

    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        return f"{total_seconds // 60}m"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        return f"{hours}h{mins}m"
    else:
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        return f"{days}d{hours}h"


def _check_and_cleanup_succeeded_pods(all_pods: list, namespace: str,
                                       config: dict = None, current_user: str = None):
    """检测 Succeeded 状态的 Pods，提供一键清理功能。
    清理前会保存日志到对应用户目录。
    """
    succeeded_pods = [p for p in all_pods if p["status"] == "Succeeded"]
    if not succeeded_pods:
        return

    # 按任务分组展示
    from raytool.utils.kube import group_pods_by_job
    succeeded_jobs = group_pods_by_job(succeeded_pods)

    # 加载任务归属数据
    job_owner_map = {}
    store = None
    if config:
        try:
            from raytool.utils.user_store import UserStore
            store = UserStore(config["data_dir"])
            owners_data = store._load_job_owners()
            for job_name, info in owners_data.items():
                job_owner_map[job_name] = info.get("owner", "")
        except Exception:
            pass

    console.print()
    # 构建 Succeeded Pod 摘要
    lines = []
    total_succeeded = len(succeeded_pods)
    for job_name, pods in sorted(succeeded_jobs.items()):
        owner = job_owner_map.get(job_name, "")
        owner_tag = f" [green]({owner})[/green]" if owner else ""
        lines.append(f"  [cyan]{job_name}[/cyan]{owner_tag}: {len(pods)} 个 Pod")

    console.print(Panel(
        f"[bold yellow]🧹 发现 {total_succeeded} 个 Succeeded 状态的 Pod 可以清理[/bold yellow]\n\n"
        + "\n".join(lines)
        + "\n\n[dim]  这些 Pod 已运行完成，删除后可释放资源[/dim]",
        title="🧹 Succeeded Pods 清理",
        border_style="yellow",
        padding=(0, 2),
    ))
    console.print()

    # 询问是否清理
    if not confirm("是否清理这些 Succeeded Pods? (清理前会自动保存日志)"):
        return

    # 保存日志到对应用户目录
    if config:
        try:
            from raytool.utils.job_logs import JobLogSaver
            from raytool.utils.audit import AuditLogger
            saver = JobLogSaver(config["data_dir"])
            audit = AuditLogger(config["data_dir"])

            for job_name, pods in succeeded_jobs.items():
                # 确定日志保存的目标用户: 优先使用任务归属用户，否则用当前用户
                owner = job_owner_map.get(job_name, "") or current_user or "unknown"
                print_info(f"正在保存 {job_name} 的日志 (用户: {owner})...")

                saved = saver.save_job_logs(job_name, namespace, owner)
                if saved:
                    print_success(f"已保存 {len(saved)} 个日志文件到 {owner} 目录")
                else:
                    # Succeeded Pods 可能已无法获取日志，尝试直接获取
                    _save_succeeded_pod_logs(saver, pods, job_name, namespace, owner)

                # 记录审计日志
                audit.log(
                    current_user or owner,
                    "cleanup_succeeded",
                    f"{job_name} ({len(pods)} pods, logs saved to {owner})",
                )
        except Exception as e:
            print_warning(f"日志保存出错（不影响清理操作）: {e}")

    # 执行删除
    print_info("正在删除 Succeeded Pods...")
    deleted_count = 0
    failed_count = 0
    for pod in succeeded_pods:
        pod_name = pod["name"]
        rc, stdout, stderr = run_kubectl(
            ["delete", "pod", pod_name, "--grace-period=0"],
            namespace,
            timeout=15,
        )
        if rc == 0:
            deleted_count += 1
        else:
            failed_count += 1
            print_warning(f"删除 {pod_name} 失败: {stderr.strip()}")

    console.print()
    if deleted_count > 0:
        print_success(f"已清理 {deleted_count} 个 Succeeded Pod")
    if failed_count > 0:
        print_warning(f"{failed_count} 个 Pod 删除失败")

    # 清理任务归属记录中已不存在的任务
    if store:
        for job_name in succeeded_jobs:
            try:
                store.remove_job_owner(job_name)
            except Exception:
                pass


def _save_succeeded_pod_logs(saver, pods: list, job_name: str,
                              namespace: str, username: str):
    """针对 Succeeded Pods 尝试保存日志（它们可能不在 Running 列表中）。"""
    from pathlib import Path
    saved_count = 0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    user_dir = saver._user_dir(username)

    for pod in pods:
        pod_name = pod["name"]
        containers = pod.get("containers", [])
        target_containers = containers if containers else [None]

        for container in target_containers:
            try:
                args = ["logs", pod_name]
                if container:
                    args += ["-c", container]
                args.append("--timestamps=true")

                rc, stdout, stderr = run_kubectl(args, namespace, timeout=120)
                if rc == 0 and stdout.strip():
                    safe_pod = pod_name.replace("/", "_")
                    container_part = f"_{container.replace('/', '_')}" if container else ""
                    filename = f"{job_name}_{safe_pod}{container_part}_{timestamp}.log"
                    filepath = user_dir / filename

                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(f"# Job: {job_name}\n")
                        f.write(f"# Pod: {pod_name}\n")
                        if container:
                            f.write(f"# Container: {container}\n")
                        f.write(f"# Namespace: {namespace}\n")
                        f.write(f"# User: {username}\n")
                        f.write(f"# Status: Succeeded (auto-cleanup)\n")
                        f.write(f"# Saved at: {datetime.now().isoformat()}\n")
                        f.write(f"# {'=' * 60}\n\n")
                        f.write(stdout)

                    saved_count += 1
            except Exception:
                pass

    if saved_count > 0:
        print_success(f"已保存 {saved_count} 个日志文件到 {username} 目录")
