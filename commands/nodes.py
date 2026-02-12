"""功能: 节点信息查看 — 按 AZ / 实例类型查看节点与 Pod 分布"""
from collections import Counter, defaultdict

from InquirerPy import inquirer
from rich.table import Table
from rich.panel import Panel

from raytool.utils.kube import get_nodes_info, get_pod_node_mapping
from raytool.utils.ui import console, print_info, print_warning, colorize_status


def nodes_info(namespace: str):
    """节点信息查看入口"""
    action = inquirer.select(
        message="请选择查看方式",
        choices=[
            {"name": "📊 节点总览 (按 AZ + 实例类型分组)", "value": "overview"},
            {"name": "🔍 按区域/类型筛选 Pod", "value": "filter"},
            {"name": "📋 节点详细列表", "value": "detail"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
    ).execute()

    if action == "cancel":
        return
    elif action == "overview":
        _show_overview(namespace)
    elif action == "filter":
        _filter_pods_by_az_type(namespace)
    elif action == "detail":
        _show_detail_list(namespace)


def _show_overview(namespace: str):
    """按 AZ + 实例类型分组的节点总览，显示每个节点上的 Pod 和 Job"""
    print_info("正在获取节点和 Pod 信息...")

    nodes = get_nodes_info(namespace)
    pod_map = get_pod_node_mapping(namespace)

    if not nodes:
        print_warning("未获取到节点信息")
        return

    # 构建节点到 Pod 的映射
    node_pods = defaultdict(list)
    for pod_name, info in pod_map.items():
        if info["node_name"]:
            node_pods[info["node_name"]].append({
                "pod_name": pod_name,
                **info,
            })

    # 按 AZ + 实例类型分组
    groups = defaultdict(list)
    for node in nodes:
        key = (node["az"], node["instance_type"])
        groups[key].append(node)

    for (az, itype), group_nodes in sorted(groups.items()):
        az_short = az.split("-")[-1] if "-" in az else az
        gpu_total = sum(n["gpu_count"] for n in group_nodes)

        table = Table(
            title=f"🌐 {az_short} / {itype} ({len(group_nodes)} 节点, {gpu_total} GPU)",
            show_lines=True,
            border_style="cyan",
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("节点名称", style="cyan", min_width=35)
        table.add_column("GPU", justify="center", width=6)
        table.add_column("状态", justify="center", width=14)
        table.add_column("Pod / Job", min_width=50)

        for i, node in enumerate(sorted(group_nodes, key=lambda x: x["name"]), 1):
            pods_on_node = node_pods.get(node["name"], [])
            if pods_on_node:
                pod_lines = []
                for p in sorted(pods_on_node, key=lambda x: x["job_name"]):
                    status_icon = "🟢" if p["status"] == "Running" else "🟡" if p["status"] == "Pending" else "🔴"
                    pod_lines.append(
                        f"{status_icon} {p['pod_name']}\n"
                        f"   Job: {p['job_name']}  Role: {p['role']}"
                    )
                pod_info = "\n".join(pod_lines)
            else:
                pod_info = "[dim]空闲[/dim]"

            table.add_row(
                str(i),
                node["name"],
                str(node["gpu_count"]),
                colorize_status(node["status"]),
                pod_info,
            )

        console.print(table)
        console.print()


def _filter_pods_by_az_type(namespace: str):
    """交互式选择 AZ + 实例类型组合，查看该组合下所有节点的 Pod 详情"""
    print_info("正在获取节点信息...")

    nodes = get_nodes_info(namespace)
    pod_map = get_pod_node_mapping(namespace)

    if not nodes:
        print_warning("未获取到节点信息")
        return

    # 构建节点到 Pod 的映射
    node_pods = defaultdict(list)
    for pod_name, info in pod_map.items():
        if info["node_name"]:
            node_pods[info["node_name"]].append({
                "pod_name": pod_name,
                **info,
            })

    # 统计各 AZ + 类型组合
    groups = defaultdict(list)
    for node in nodes:
        key = (node["az"], node["instance_type"])
        groups[key].append(node)

    # 构建选择列表
    choices = []
    for (az, itype), group_nodes in sorted(groups.items()):
        az_short = az.split("-")[-1] if "-" in az else az
        gpu_total = sum(n["gpu_count"] for n in group_nodes)
        busy_count = sum(1 for n in group_nodes if node_pods.get(n["name"]))
        free_count = len(group_nodes) - busy_count
        choices.append({
            "name": f"{az_short} / {itype}: {len(group_nodes)} 节点 ({gpu_total} GPU, 空闲 {free_count})",
            "value": (az, itype),
        })
    choices.append({"name": "❌ 返回", "value": "cancel"})

    selected = inquirer.select(
        message="选择要查看的 AZ + 实例类型组合",
        choices=choices,
        pointer="❯",
    ).execute()

    if selected == "cancel":
        return

    az, itype = selected
    az_short = az.split("-")[-1] if "-" in az else az
    target_nodes = groups[(az, itype)]

    # 展示该组合下的所有 Pod
    table = Table(
        title=f"🔍 {az_short} / {itype} — Pod 详情",
        show_lines=True,
        border_style="cyan",
    )
    table.add_column("节点名称", style="cyan", min_width=35)
    table.add_column("Pod 名称", min_width=40)
    table.add_column("Job", style="yellow", min_width=25)
    table.add_column("角色", justify="center", width=8)
    table.add_column("状态", justify="center", width=10)

    for node in sorted(target_nodes, key=lambda x: x["name"]):
        pods_on_node = node_pods.get(node["name"], [])
        if pods_on_node:
            for p in sorted(pods_on_node, key=lambda x: x["job_name"]):
                table.add_row(
                    node["name"],
                    p["pod_name"],
                    p["job_name"],
                    p["role"],
                    colorize_status(p["status"]),
                )
        else:
            table.add_row(
                node["name"],
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[dim]-[/dim]",
                "[green]空闲[/green]",
            )

    console.print(table)


def _show_detail_list(namespace: str):
    """展示完整节点列表并做多维度统计"""
    print_info("正在获取节点信息...")

    nodes = get_nodes_info(namespace)
    pod_map = get_pod_node_mapping(namespace)

    if not nodes:
        print_warning("未获取到节点信息")
        return

    # 忙碌节点集合
    busy_nodes = set()
    for pod_name, info in pod_map.items():
        if info["node_name"] and info["status"] in ("Running", "Pending"):
            busy_nodes.add(info["node_name"])

    # 节点详细列表
    table = Table(title="📋 节点详细列表", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("节点名称", style="cyan", min_width=40)
    table.add_column("AZ", justify="center", width=8)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("GPU", justify="center", width=6)
    table.add_column("状态", justify="center", width=14)
    table.add_column("占用", justify="center", width=8)

    for i, node in enumerate(sorted(nodes, key=lambda x: (x["az"], x["instance_type"], x["name"])), 1):
        is_busy = node["name"] in busy_nodes
        az_short = node["az"].split("-")[-1] if "-" in node["az"] else node["az"]
        if node["unschedulable"]:
            occupy = "[yellow]禁调[/yellow]"
        elif is_busy:
            occupy = "[red]占用[/red]"
        else:
            occupy = "[green]空闲[/green]"

        table.add_row(
            str(i),
            node["name"],
            az_short,
            node["instance_type"].replace("ml.", ""),
            str(node["gpu_count"]),
            colorize_status(node["status"]),
            occupy,
        )

    console.print(table)
    console.print()

    # ── 多维度统计 ──
    az_counter = Counter()
    type_counter = Counter()
    az_type_counter = Counter()
    free_az_counter = Counter()
    free_type_counter = Counter()
    free_az_type_counter = Counter()

    for n in nodes:
        az_counter[n["az"]] += 1
        type_counter[n["instance_type"]] += 1
        az_type_counter[(n["az"], n["instance_type"])] += 1
        if n["name"] not in busy_nodes and not n["unschedulable"]:
            free_az_counter[n["az"]] += 1
            free_type_counter[n["instance_type"]] += 1
            free_az_type_counter[(n["az"], n["instance_type"])] += 1

    # 按 AZ 统计
    console.print("[bold]📍 按可用区 (AZ) 统计:[/bold]")
    for az in sorted(az_counter.keys()):
        total = az_counter[az]
        free = free_az_counter.get(az, 0)
        az_short = az.split("-")[-1] if "-" in az else az
        color = "green" if free > 0 else "dim"
        console.print(f"  [{color}]{az_short}[/{color}]: 空闲 {free}/{total}")
    console.print()

    # 按实例类型统计
    console.print("[bold]🏷️  按实例类型统计:[/bold]")
    for itype in sorted(type_counter.keys()):
        total = type_counter[itype]
        free = free_type_counter.get(itype, 0)
        color = "green" if free > 0 else "dim"
        console.print(f"  [{color}]{itype}[/{color}]: 空闲 {free}/{total}")
    console.print()

    # 按 AZ + 类型统计
    console.print("[bold]🌐 按 AZ + 实例类型统计:[/bold]")
    for (az, itype) in sorted(az_type_counter.keys()):
        total = az_type_counter[(az, itype)]
        free = free_az_type_counter.get((az, itype), 0)
        az_short = az.split("-")[-1] if "-" in az else az
        color = "green" if free > 0 else "dim"
        console.print(f"  [{color}]{az_short} / {itype}[/{color}]: 空闲 {free}/{total}")
