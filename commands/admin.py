"""管理员模式: 查看并删除任意 PyTorchJob"""
import json

from InquirerPy import inquirer
from rich.table import Table

from raytool.utils.kube import run_kubectl
from raytool.utils.ui import (
    console,
    confirm_with_input,
    print_success,
    print_error,
    print_warning,
    print_info,
    ESC_KEYBINDING,
)


def admin_mode(namespace: str):
    """管理员模式入口 —— 列出所有 PyTorchJob 并支持多选删除"""

    print_info("🔑 管理员模式 — 正在查询所有 PyTorchJob...")
    console.print()

    # ── 获取所有 PyTorchJob ──
    rc, stdout, stderr = run_kubectl(
        ["get", "pytorchjobs", "-o", "json"],
        namespace,
        timeout=30,
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
        print_warning("当前命名空间中没有任何 PyTorchJob")
        return

    # ── 解析每个 Job 的元信息 ──
    jobs = []
    for item in items:
        metadata = item.get("metadata", {})
        name = metadata.get("name", "")
        creation = metadata.get("creationTimestamp", "")

        # 状态
        conditions = item.get("status", {}).get("conditions", [])
        phase = "Unknown"
        for cond in reversed(conditions):
            ctype = cond.get("type", "")
            if ctype in ("Succeeded", "Failed", "Running", "Created"):
                phase = ctype
                break

        # 副本数 & GPU
        replica_specs = item.get("spec", {}).get("pytorchReplicaSpecs", {})
        master_spec = replica_specs.get("Master", {})
        worker_spec = replica_specs.get("Worker", {})
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
        total_gpus = total_nodes * gpu_per_node

        # AZ
        node_selector = master_spec.get("template", {}).get("spec", {}).get("nodeSelector", {})
        az = node_selector.get("topology.kubernetes.io/zone", "")
        az_short = az.split("-")[-1] if az and "-" in az else (az or "?")

        jobs.append({
            "name": name,
            "creation": creation[:19].replace("T", " ") if creation else "-",
            "nodes": total_nodes,
            "gpus": total_gpus,
            "az": az_short,
            "phase": phase,
        })

    # ── 表格展示 ──
    table = Table(title="🔑 所有 PyTorchJob", show_lines=False, border_style="magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("任务名称", style="bold cyan", min_width=30)
    table.add_column("状态", justify="center", width=10)
    table.add_column("AZ", justify="center", width=6)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("GPU 数", justify="center", width=8)
    table.add_column("创建时间", width=22)

    phase_style = {
        "Running": "green",
        "Created": "yellow",
        "Succeeded": "blue",
        "Failed": "red",
    }
    for i, job in enumerate(sorted(jobs, key=lambda j: j["name"]), 1):
        color = phase_style.get(job["phase"], "white")
        table.add_row(
            str(i),
            job["name"],
            f"[{color}]{job['phase']}[/{color}]",
            job["az"],
            str(job["nodes"]),
            str(job["gpus"]),
            job["creation"],
        )

    console.print(table)
    console.print()
    console.print(f"[dim]共 {len(jobs)} 个 PyTorchJob[/dim]")
    console.print()

    # ── 询问是否删除 ──
    action = inquirer.select(
        message="请选择操作",
        choices=[
            {"name": "🗑️  选择任务删除", "value": "delete"},
            {"name": "↩️  返回主菜单", "value": "back"},
        ],
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if action == "back" or action is None:
        return

    # ── 多选删除 ──
    choices = []
    total_gpus_all = sum(j["gpus"] for j in jobs)
    choices.append({
        "name": f"⚡ 全选所有任务 ({len(jobs)} 个, {total_gpus_all} GPU)",
        "value": "__all__",
    })
    for job in sorted(jobs, key=lambda j: j["name"]):
        color = phase_style.get(job["phase"], "white")
        choices.append({
            "name": f"  {job['name']}  ({job['phase']}, {job['az']}区, {job['nodes']}节点, {job['gpus']}GPU, {job['creation']})",
            "value": job["name"],
        })
    choices.append({"name": "❌ 取消", "value": "__cancel__"})

    selected = inquirer.checkbox(
        message="请选择要删除的任务 (空格选中, 回车确认)",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if not selected or "__cancel__" in selected:
        print_warning("已取消")
        return

    # 处理全选
    if "__all__" in selected:
        selected = [j["name"] for j in jobs]
    else:
        selected = [s for s in selected if not s.startswith("__")]

    if not selected:
        print_warning("未选择任何任务")
        return

    # ── 确认删除 ──
    total_del_gpus = 0
    console.print()
    console.print("[bold yellow]⚠️  即将删除以下 PyTorchJob:[/bold yellow]")
    for name in selected:
        for j in jobs:
            if j["name"] == name:
                total_del_gpus += j["gpus"]
                console.print(
                    f"  [bold red]✖[/bold red] {name}  "
                    f"({j['phase']}, {j['az']}区, {j['nodes']}节点, {j['gpus']}GPU)"
                )
                break
        else:
            console.print(f"  [bold red]✖[/bold red] {name}")

    console.print()
    console.print(f"[bold]共 {len(selected)} 个任务, {total_del_gpus} 张 GPU 将被释放[/bold]")
    console.print()

    if not confirm_with_input("确认删除? 请输入 'yes'"):
        print_warning("已取消删除")
        return

    # ── 执行删除 ──
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
        else:
            print_error(f"删除失败 {name}: {stderr.strip()}")
            fail_count += 1

    console.print()
    if fail_count == 0:
        print_success(f"全部删除完成! 共删除 {success_count} 个任务, 释放 {total_del_gpus} 张 GPU")
    else:
        print_warning(f"删除完成: {success_count} 成功, {fail_count} 失败")
