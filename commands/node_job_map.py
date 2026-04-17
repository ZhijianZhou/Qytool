"""功能: 节点与任务双向查询 — 查看节点对应的 Job / Job 对应的节点"""
import json
from typing import List, Dict, Tuple

from rich.table import Table
from rich.panel import Panel
from InquirerPy import inquirer

from raytool.utils.kube import run_kubectl, get_pods
from raytool.utils.ui import (
    console, colorize_status, print_warning, print_info, print_error, ESC_KEYBINDING,
)


def node_job_map(namespace: str):
    """节点与任务双向查询入口"""
    action = inquirer.select(
        message="主人，请选择查询方式",
        choices=[
            {"name": "🔎 搜索查询 (输入节点名或Job名关键词)", "value": "search"},
            {"name": "🔍 节点 → 查看该节点上运行的 Job", "value": "node2job"},
            {"name": "🔍 Job → 查看该 Job 使用的节点", "value": "job2node"},
            {"name": "📋 展示全量节点-Job 映射表", "value": "full_map"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if action == "cancel" or action is None:
        return
    elif action == "search":
        _search_query(namespace)
    elif action == "node2job":
        _node_to_job(namespace)
    elif action == "job2node":
        _job_to_node(namespace)
    elif action == "full_map":
        _full_mapping_table(namespace)


# ──────────────────────── 搜索查询 ────────────────────────


def _search_query(namespace: str):
    """通过关键词搜索节点名或 Job 名，展示匹配结果"""
    keyword = inquirer.text(
        message="主人，请输入搜索关键词 (节点名或 Job 名的部分内容)",
    ).execute()

    if not keyword or not keyword.strip():
        print_warning("未输入关键词")
        return

    keyword = keyword.strip().lower()
    print_info(f"正在搜索 \"{keyword}\" ...")

    node_to_pods, job_to_nodes = _get_node_job_mapping(namespace)

    if not node_to_pods:
        print_warning("未找到任何映射数据")
        return

    # 匹配节点
    matched_nodes = {
        node: pods for node, pods in node_to_pods.items()
        if keyword in node.lower()
    }
    # 匹配 Job
    matched_jobs = {
        job: nodes for job, nodes in job_to_nodes.items()
        if keyword in job.lower()
    }

    if not matched_nodes and not matched_jobs:
        print_warning(f"未找到包含 \"{keyword}\" 的节点或 Job")
        console.print()
        # 提示可能的匹配
        all_nodes = sorted(node_to_pods.keys())
        all_jobs = sorted(job_to_nodes.keys())
        console.print("[dim]  可用节点:[/dim]")
        for n in all_nodes[:10]:
            console.print(f"[dim]    • {n}[/dim]")
        if len(all_nodes) > 10:
            console.print(f"[dim]    ... 共 {len(all_nodes)} 个节点[/dim]")
        console.print("[dim]  可用 Job:[/dim]")
        for j in all_jobs[:10]:
            console.print(f"[dim]    • {j}[/dim]")
        if len(all_jobs) > 10:
            console.print(f"[dim]    ... 共 {len(all_jobs)} 个 Job[/dim]")
        return

    console.print()

    # 展示匹配到的节点 → Job
    if matched_nodes:
        console.print(Panel(
            f"  匹配到 [bold]{len(matched_nodes)}[/bold] 个节点",
            title="🖥️  节点匹配结果",
            border_style="green",
            padding=(0, 2),
        ))
        for node_name, pods in sorted(matched_nodes.items()):
            jobs = {}
            for p in pods:
                jobs.setdefault(p["job_name"], []).append(p)

            table = Table(
                title=f"  节点: [bold]{node_name}[/bold]",
                show_lines=False,
                border_style="cyan",
                padding=(0, 1),
            )
            table.add_column("Job 名称", style="bold cyan", min_width=30)
            table.add_column("Pod 名称", style="dim", min_width=40)
            table.add_column("角色", justify="center", width=8)
            table.add_column("状态", justify="center", width=12)

            for job_name, job_pods in sorted(jobs.items()):
                for p in sorted(job_pods, key=lambda x: x["pod_name"]):
                    table.add_row(
                        job_name,
                        p["pod_name"],
                        p["role"],
                        colorize_status(p["status"]),
                    )
            console.print(table)
            console.print()

    # 展示匹配到的 Job → 节点
    if matched_jobs:
        console.print(Panel(
            f"  匹配到 [bold]{len(matched_jobs)}[/bold] 个 Job",
            title="📋 Job 匹配结果",
            border_style="green",
            padding=(0, 2),
        ))
        for job_name, nodes in sorted(matched_jobs.items()):
            unique_nodes = sorted(set(n["node_name"] for n in nodes))

            table = Table(
                title=f"  Job: [bold]{job_name}[/bold]  ({len(unique_nodes)} 个节点)",
                show_lines=False,
                border_style="cyan",
                padding=(0, 1),
            )
            table.add_column("节点名称", style="bold", min_width=35)
            table.add_column("Pod 名称", style="dim", min_width=40)
            table.add_column("角色", justify="center", width=8)
            table.add_column("状态", justify="center", width=12)

            for n in sorted(nodes, key=lambda x: x["node_name"]):
                table.add_row(
                    n["node_name"],
                    n["pod_name"],
                    n["role"],
                    colorize_status(n["status"]),
                )
            console.print(table)
            console.print()


# ──────────────────────── 核心数据获取 ────────────────────────


def _get_node_job_mapping(namespace: str) -> Tuple[Dict[str, List[Dict]], Dict[str, List[Dict]]]:
    """
    构建双向映射:
      node_to_pods: {node_name: [{pod_name, job_name, role, status}]}
      job_to_nodes: {job_name: [{node_name, pod_name, role, status}]}
    """
    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace,
        timeout=30,
    )
    if rc != 0:
        print_error(f"获取 Pod 信息失败: {stderr.strip()}")
        return {}, {}

    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, KeyError):
        print_error("解析 Pod 信息失败")
        return {}, {}

    node_to_pods: Dict[str, List[Dict]] = {}
    job_to_nodes: Dict[str, List[Dict]] = {}

    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        status = item.get("status", {})

        pod_name = metadata.get("name", "")
        node_name = spec.get("nodeName", "")
        phase = status.get("phase", "Unknown")
        labels = metadata.get("labels", {})

        # 跳过没有分配节点的 Pod
        if not node_name:
            continue

        # 推断 Job 名称
        job_name = _infer_job_name_from_pod(pod_name, labels)

        # 推断角色
        role = _infer_role(pod_name, labels)

        entry_for_node = {
            "pod_name": pod_name,
            "job_name": job_name,
            "role": role,
            "status": phase,
        }
        entry_for_job = {
            "node_name": node_name,
            "pod_name": pod_name,
            "role": role,
            "status": phase,
        }

        node_to_pods.setdefault(node_name, []).append(entry_for_node)
        job_to_nodes.setdefault(job_name, []).append(entry_for_job)

    return node_to_pods, job_to_nodes


def _infer_job_name_from_pod(pod_name: str, labels: dict) -> str:
    """从 Pod 信息推断所属 Job 名称"""
    # 优先使用 label
    job_name = (
        labels.get("ray.io/cluster", "")
        or labels.get("ray.io/job-name", "")
        or labels.get("app.kubernetes.io/instance", "")
        or labels.get("training.kubeflow.org/job-name", "")
    )
    if job_name:
        return job_name

    # 通过名称推断: 去掉 -master-N / -worker-N / -head-N 后缀
    parts = pod_name.split("-")
    for i, part in enumerate(parts):
        if part in ("head", "worker", "master"):
            return "-".join(parts[:i])
    # 兜底: 去掉最后两段
    if len(parts) > 2:
        return "-".join(parts[:-2])
    return pod_name


def _infer_role(pod_name: str, labels: dict) -> str:
    """推断 Pod 角色"""
    role = labels.get("ray.io/node-type", "").capitalize()
    if role:
        return role
    if "-head-" in pod_name or pod_name.endswith("-head"):
        return "Head"
    if "-master-" in pod_name or pod_name.endswith("-master"):
        return "Master"
    if "-worker-" in pod_name or pod_name.endswith("-worker"):
        return "Worker"
    return "-"


# ──────────────────────── 节点 → Job ────────────────────────


def _node_to_job(namespace: str):
    """查询指定节点上运行的所有 Job"""
    print_info("正在查询节点与 Job 映射...")

    node_to_pods, _ = _get_node_job_mapping(namespace)

    if not node_to_pods:
        print_warning("未找到任何节点与 Pod 的映射")
        return

    # 让用户选择节点
    node_names = sorted(node_to_pods.keys())
    choices = []
    for node in node_names:
        pods = node_to_pods[node]
        job_names = sorted(set(p["job_name"] for p in pods))
        jobs_str = ", ".join(job_names) if len(job_names) <= 3 else f"{', '.join(job_names[:3])} +{len(job_names)-3}"
        choices.append({
            "name": f"{node}  →  [{len(pods)} Pod, Job: {jobs_str}]",
            "value": node,
        })
    choices.append({"name": "📋 查看全部节点", "value": "__all__"})
    choices.append({"name": "❌ 返回", "value": "__cancel__"})

    selected = inquirer.select(
        message="主人，请选择节点",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if selected == "__cancel__" or selected is None:
        return

    if selected == "__all__":
        _print_node_to_job_table(node_to_pods)
        return

    # 展示单个节点详情
    _print_single_node_detail(selected, node_to_pods[selected])


def _print_single_node_detail(node_name: str, pods: List[Dict]):
    """打印单个节点上的 Job 详情"""
    console.print()

    # 按 Job 分组
    jobs = {}
    for p in pods:
        jobs.setdefault(p["job_name"], []).append(p)

    table = Table(
        title=f"🖥️  节点 {node_name} 上的任务",
        show_lines=True,
        border_style="cyan",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Job 名称", style="bold cyan", min_width=30)
    table.add_column("Pod 名称", style="dim", min_width=40)
    table.add_column("角色", justify="center", width=8)
    table.add_column("状态", justify="center", width=12)

    idx = 1
    for job_name, job_pods in sorted(jobs.items()):
        for p in sorted(job_pods, key=lambda x: x["pod_name"]):
            table.add_row(
                str(idx),
                job_name,
                p["pod_name"],
                p["role"],
                colorize_status(p["status"]),
            )
            idx += 1

    console.print(table)
    console.print(f"\n[dim]  共 {len(jobs)} 个 Job, {len(pods)} 个 Pod[/dim]")


def _print_node_to_job_table(node_to_pods: Dict[str, List[Dict]]):
    """打印全部节点 → Job 映射表"""
    console.print()
    table = Table(
        title="🖥️  全部节点 → Job 映射",
        show_lines=True,
        border_style="cyan",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("节点名称", style="bold", min_width=35)
    table.add_column("Job 名称", style="cyan", min_width=30)
    table.add_column("Pod 数", justify="center", width=8)
    table.add_column("角色", justify="center", width=15)
    table.add_column("状态", justify="center", width=12)

    idx = 1
    for node_name in sorted(node_to_pods.keys()):
        pods = node_to_pods[node_name]
        # 按 Job 分组
        jobs = {}
        for p in pods:
            jobs.setdefault(p["job_name"], []).append(p)

        for job_name, job_pods in sorted(jobs.items()):
            roles = ", ".join(sorted(set(p["role"] for p in job_pods)))
            statuses = set(p["status"] for p in job_pods)
            status = "Running" if "Running" in statuses else list(statuses)[0]
            table.add_row(
                str(idx),
                node_name,
                job_name,
                str(len(job_pods)),
                roles,
                colorize_status(status),
            )
            idx += 1

    console.print(table)


# ──────────────────────── Job → 节点 ────────────────────────


def _job_to_node(namespace: str):
    """查询指定 Job 使用的所有节点"""
    print_info("正在查询 Job 与节点映射...")

    _, job_to_nodes = _get_node_job_mapping(namespace)

    if not job_to_nodes:
        print_warning("未找到任何 Job 与节点的映射")
        return

    # 让用户选择 Job
    job_names = sorted(job_to_nodes.keys())
    choices = []
    for job in job_names:
        nodes = job_to_nodes[job]
        node_names = sorted(set(n["node_name"] for n in nodes))
        node_count = len(node_names)
        choices.append({
            "name": f"{job}  →  [{node_count} 个节点]",
            "value": job,
        })
    choices.append({"name": "📋 查看全部 Job", "value": "__all__"})
    choices.append({"name": "❌ 返回", "value": "__cancel__"})

    selected = inquirer.select(
        message="主人，请选择 Job",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if selected == "__cancel__" or selected is None:
        return

    if selected == "__all__":
        _print_job_to_node_table(job_to_nodes)
        return

    # 展示单个 Job 详情
    _print_single_job_detail(selected, job_to_nodes[selected])


def _print_single_job_detail(job_name: str, nodes: List[Dict]):
    """打印单个 Job 使用的节点详情"""
    console.print()

    table = Table(
        title=f"📋 Job {job_name} 使用的节点",
        show_lines=True,
        border_style="cyan",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("节点名称", style="bold", min_width=35)
    table.add_column("Pod 名称", style="dim", min_width=40)
    table.add_column("角色", justify="center", width=8)
    table.add_column("状态", justify="center", width=12)

    for i, n in enumerate(sorted(nodes, key=lambda x: x["node_name"]), 1):
        table.add_row(
            str(i),
            n["node_name"],
            n["pod_name"],
            n["role"],
            colorize_status(n["status"]),
        )

    console.print(table)

    unique_nodes = sorted(set(n["node_name"] for n in nodes))
    console.print(f"\n[dim]  共 {len(unique_nodes)} 个节点, {len(nodes)} 个 Pod[/dim]")


def _print_job_to_node_table(job_to_nodes: Dict[str, List[Dict]]):
    """打印全部 Job → 节点映射表"""
    console.print()
    table = Table(
        title="📋 全部 Job → 节点映射",
        show_lines=True,
        border_style="cyan",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Job 名称", style="bold cyan", min_width=30)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("节点列表", min_width=45)
    table.add_column("角色分布", justify="center", width=15)
    table.add_column("状态", justify="center", width=12)

    idx = 1
    for job_name in sorted(job_to_nodes.keys()):
        nodes = job_to_nodes[job_name]
        unique_nodes = sorted(set(n["node_name"] for n in nodes))
        roles = ", ".join(sorted(set(n["role"] for n in nodes)))
        statuses = set(n["status"] for n in nodes)
        status = "Running" if "Running" in statuses else list(statuses)[0]

        # 节点列表展示
        if len(unique_nodes) <= 4:
            node_display = "\n".join(unique_nodes)
        else:
            node_display = "\n".join(unique_nodes[:4]) + f"\n... +{len(unique_nodes)-4} 更多"

        table.add_row(
            str(idx),
            job_name,
            str(len(unique_nodes)),
            node_display,
            roles,
            colorize_status(status),
        )
        idx += 1

    console.print(table)


# ──────────────────────── 全量映射 ────────────────────────


def _full_mapping_table(namespace: str):
    """展示全量节点-Job 双向映射"""
    print_info("正在查询全量映射...")

    node_to_pods, job_to_nodes = _get_node_job_mapping(namespace)

    if not node_to_pods:
        print_warning("未找到任何映射关系")
        return

    # 汇总统计
    total_nodes = len(node_to_pods)
    total_jobs = len(job_to_nodes)
    total_pods = sum(len(v) for v in node_to_pods.values())

    console.print(Panel(
        f"  节点总数: [bold]{total_nodes}[/bold]  |  Job 总数: [bold]{total_jobs}[/bold]  |  Pod 总数: [bold]{total_pods}[/bold]",
        title="📊 映射概要",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    # 打印两个表
    _print_job_to_node_table(job_to_nodes)
    console.print()
    _print_node_to_job_table(node_to_pods)
