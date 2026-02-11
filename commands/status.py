"""功能10: 集群概况总览"""
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from raytool.utils.kube import (
    get_pods, get_running_pods, group_pods_by_job, get_pod_role, run_kubectl,
)
from raytool.utils.ui import (
    console, colorize_status, print_info, print_error, print_warning,
)
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns


def cluster_status(namespace: str):
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

    # ── 2. 任务概览表 ──
    jobs = group_pods_by_job(all_pods)
    _print_jobs_overview(jobs)
    console.print()

    # ── 3. 资源使用情况 (如果 metrics-server 可用) ──
    _print_resource_usage(namespace, all_pods)
    console.print()

    # ── 4. 异常 Pod 告警 ──
    _print_alerts(all_pods)

    # ── 5. RayCluster / RayJob CRD 状态 ──
    _print_crd_status(namespace)


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
        gpus = []
        for line in result.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 4:
                gpus.append({
                    "index": parts[0],
                    "util": int(parts[1]),
                    "mem_used": int(parts[2]),
                    "mem_total": int(parts[3]),
                })
        return {"pod": pod_name, "gpus": gpus, "error": ""}
    except subprocess.TimeoutExpired:
        return {"pod": pod_name, "gpus": [], "error": "timeout"}
    except Exception as e:
        return {"pod": pod_name, "gpus": [], "error": str(e)}


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
