"""功能: 节点调度管理 — cordon/uncordon 禁止/恢复节点调度"""
import json
import subprocess
from typing import List, Dict

from rich.table import Table
from rich.panel import Panel
from InquirerPy import inquirer

from raytool.utils.kube import run_kubectl
from raytool.utils.ui import (
    console, colorize_status, confirm,
    print_success, print_error, print_warning, print_info, ESC_KEYBINDING,
)


def manage_cordon(namespace: str):
    """节点调度管理入口"""
    action = inquirer.select(
        message="主人，请选择操作",
        choices=[
            {"name": "📊 查看所有节点调度状态", "value": "status"},
            {"name": "🚫 禁止调度 (cordon) — 防止新任务调度到该节点", "value": "cordon"},
            {"name": "✅ 恢复调度 (uncordon) — 允许新任务调度到该节点", "value": "uncordon"},
            {"name": "❌ 返回", "value": "cancel"},
        ],
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if action == "cancel" or action is None:
        return
    elif action == "status":
        _show_node_status(namespace)
    elif action == "cordon":
        _cordon_nodes(namespace)
    elif action == "uncordon":
        _uncordon_nodes(namespace)


# ──────────────────────── 获取节点信息 ────────────────────────


def _get_all_nodes() -> List[Dict]:
    """获取所有节点信息（不限 namespace，节点是集群级资源）"""
    cmd = ["kubectl", "get", "nodes", "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return []

    nodes = []
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        labels = metadata.get("labels", {})

        name = metadata.get("name", "")
        conditions = status.get("conditions", [])

        # Ready 状态
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in conditions
        )

        # 是否被 cordon（unschedulable）
        unschedulable = spec.get("unschedulable", False)

        # GPU 信息
        capacity = status.get("capacity", {})
        gpu_count = int(capacity.get("nvidia.com/gpu", 0))

        # 实例类型
        instance_type = (
            labels.get("node.kubernetes.io/instance-type")
            or labels.get("beta.kubernetes.io/instance-type")
            or "-"
        )

        # 综合状态
        if unschedulable:
            node_status = "SchedulingDisabled"
        elif ready:
            node_status = "Ready"
        else:
            node_status = "NotReady"

        nodes.append({
            "name": name,
            "ready": ready,
            "unschedulable": unschedulable,
            "status": node_status,
            "gpu_count": gpu_count,
            "instance_type": instance_type,
        })

    return sorted(nodes, key=lambda n: n["name"])


# ──────────────────────── 查看状态 ────────────────────────


def _show_node_status(namespace: str):
    """展示所有节点的调度状态"""
    print_info("正在查询节点状态...")

    nodes = _get_all_nodes()
    if not nodes:
        print_warning("未找到任何节点")
        return

    # 统计
    total = len(nodes)
    gpu_nodes = [n for n in nodes if n["gpu_count"] > 0]
    cordoned = [n for n in nodes if n["unschedulable"]]
    cordoned_gpu = [n for n in gpu_nodes if n["unschedulable"]]

    console.print(Panel(
        f"  节点总数: [bold]{total}[/bold]  |  GPU 节点: [bold]{len(gpu_nodes)}[/bold]  |  "
        f"已禁止调度: [bold red]{len(cordoned)}[/bold red]  |  GPU 已禁止: [bold red]{len(cordoned_gpu)}[/bold red]",
        title="📊 节点调度概要",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    # 只展示 GPU 节点
    table = Table(title="🖥️  GPU 节点调度状态", show_lines=True, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("节点名称", style="bold", min_width=35)
    table.add_column("GPU", justify="center", width=6)
    table.add_column("实例类型", style="magenta", min_width=18)
    table.add_column("调度状态", justify="center", width=20)

    for i, node in enumerate(gpu_nodes, 1):
        if node["unschedulable"]:
            sched_status = "[bold red]🚫 SchedulingDisabled[/bold red]"
        elif node["ready"]:
            sched_status = "[green]✅ 可调度[/green]"
        else:
            sched_status = "[yellow]⚠️  NotReady[/yellow]"

        instance_display = node["instance_type"].replace("ml.", "") if node["instance_type"] != "-" else "-"

        table.add_row(
            str(i),
            node["name"],
            str(node["gpu_count"]),
            instance_display,
            sched_status,
        )

    console.print(table)


# ──────────────────────── Cordon 禁止调度 ────────────────────────


def _cordon_nodes(namespace: str):
    """选择节点并禁止调度"""
    print_info("正在查询节点...")

    nodes = _get_all_nodes()
    gpu_nodes = [n for n in nodes if n["gpu_count"] > 0]

    # 筛选可被 cordon 的节点（当前可调度的）
    schedulable_nodes = [n for n in gpu_nodes if not n["unschedulable"] and n["ready"]]

    if not schedulable_nodes:
        print_warning("没有可禁止调度的节点（所有 GPU 节点均已禁止或 NotReady）")
        return

    # 已禁止的节点提示
    cordoned = [n for n in gpu_nodes if n["unschedulable"]]
    if cordoned:
        console.print(f"[dim]  已禁止调度的节点 ({len(cordoned)}):[/dim]")
        for n in cordoned:
            console.print(f"[dim]    🚫 {n['name']}[/dim]")
        console.print()

    choices = []
    for n in schedulable_nodes:
        instance_display = n["instance_type"].replace("ml.", "") if n["instance_type"] != "-" else "-"
        choices.append({
            "name": f"{n['name']}  ({n['gpu_count']} GPU, {instance_display})",
            "value": n["name"],
        })

    selected = inquirer.checkbox(
        message="主人，请选择要禁止调度的节点 (空格选择, 回车确认)",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if not selected:
        print_warning("未选择任何节点")
        return

    console.print()
    console.print("[bold yellow]即将禁止以下节点的调度:[/bold yellow]")
    for name in selected:
        console.print(f"  🚫 {name}")
    console.print()
    console.print("[dim]  说明: cordon 只禁止新 Pod 调度，已有 Pod 不受影响[/dim]")
    console.print()

    if not confirm("确认禁止调度?"):
        print_warning("已取消")
        return

    console.print()
    for name in selected:
        cmd = ["kubectl", "cordon", name]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print_success(f"{name} 已禁止调度")
            else:
                print_error(f"{name} 操作失败: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            print_error(f"{name} 操作超时")


# ──────────────────────── Uncordon 恢复调度 ────────────────────────


def _uncordon_nodes(namespace: str):
    """选择节点并恢复调度"""
    print_info("正在查询节点...")

    nodes = _get_all_nodes()
    gpu_nodes = [n for n in nodes if n["gpu_count"] > 0]

    # 筛选已 cordon 的节点
    cordoned_nodes = [n for n in gpu_nodes if n["unschedulable"]]

    if not cordoned_nodes:
        print_warning("没有需要恢复调度的节点（所有 GPU 节点均可调度）")
        return

    choices = []
    for n in cordoned_nodes:
        instance_display = n["instance_type"].replace("ml.", "") if n["instance_type"] != "-" else "-"
        choices.append({
            "name": f"🚫 {n['name']}  ({n['gpu_count']} GPU, {instance_display})",
            "value": n["name"],
        })

    selected = inquirer.checkbox(
        message="主人，请选择要恢复调度的节点 (空格选择, 回车确认)",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if not selected:
        print_warning("未选择任何节点")
        return

    console.print()
    console.print("[bold green]即将恢复以下节点的调度:[/bold green]")
    for name in selected:
        console.print(f"  ✅ {name}")
    console.print()

    if not confirm("确认恢复调度?"):
        print_warning("已取消")
        return

    console.print()
    for name in selected:
        cmd = ["kubectl", "uncordon", name]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print_success(f"{name} 已恢复调度")
            else:
                print_error(f"{name} 操作失败: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            print_error(f"{name} 操作超时")
