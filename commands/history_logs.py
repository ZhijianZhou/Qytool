"""历史任务日志查看

分级浏览：任务列表 → 节点(Pod)列表 → 日志内容。
每个用户只能查看自己的历史日志。
"""
from typing import Any

from raytool.utils.ui import console, print_info, print_warning, print_error, ESC_KEYBINDING
from raytool.utils.job_logs import JobLogSaver


def view_history_logs(config: dict[str, Any], current_user: str):
    """查看历史任务日志（只能查看自己的）。"""
    from InquirerPy import inquirer
    from rich.table import Table
    from rich.markup import escape

    saver = JobLogSaver(config["data_dir"])

    # ── 第一级：任务列表 ──
    while True:
        jobs = saver.list_user_jobs(current_user)

        if not jobs:
            print_info("暂无历史日志")
            return

        # 显示任务列表
        table = Table(title=f"📦 {current_user} 的历史任务", show_lines=False, border_style="cyan")
        table.add_column("#", style="dim", width=4)
        table.add_column("任务名", style="bold")
        table.add_column("节点数", style="cyan", justify="center", width=6)
        table.add_column("保存时间", style="dim")
        table.add_column("总大小", style="dim", justify="right")

        for i, job in enumerate(jobs, 1):
            table.add_row(
                str(i),
                escape(job["job_name"]),
                str(len(job["log_files"])),
                job["saved_at"],
                _format_size(job["total_size"]),
            )

        console.print(table)

        # 选择任务
        choices = [
            {"name": f"{i}. {job['job_name']} [{len(job['log_files'])}个节点] ({job['saved_at']})", "value": i - 1}
            for i, job in enumerate(jobs, 1)
        ]
        choices.append({"name": "⬅️  返回主菜单", "value": -1})

        selected_job = inquirer.fuzzy(
            message="主人，选择要查看的任务:",
            choices=choices,
            pointer="❯",
            border=True,
            keybindings=ESC_KEYBINDING,
        ).execute()

        if selected_job is None or selected_job == -1:
            return

        # ── 第二级：节点(Pod)列表 ──
        job_entry = jobs[selected_job]
        _view_job_pods(saver, job_entry)


def _view_job_pods(saver: JobLogSaver, job_entry: dict):
    """查看某个任务的节点列表。"""
    from InquirerPy import inquirer
    from rich.table import Table
    from rich.markup import escape

    log_files = job_entry["log_files"]

    while True:
        # 显示节点列表
        table = Table(
            title=f"📦 {job_entry['job_name']} — 节点列表",
            show_lines=False, border_style="cyan",
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Pod", style="bold")
        table.add_column("Container", style="cyan")
        table.add_column("大小", style="dim", justify="right")

        for i, log in enumerate(log_files, 1):
            pod_display = log.get("pod_name") or log["filename"]
            container_display = log.get("container", "-")
            table.add_row(
                str(i),
                escape(pod_display),
                escape(container_display),
                _format_size(log["size"]),
            )

        console.print(table)

        # 选择节点
        choices = []
        for i, log in enumerate(log_files, 1):
            pod_display = log.get("pod_name") or log["filename"]
            container_display = log.get("container", "")
            label = f"{i}. {pod_display}"
            if container_display:
                label += f" [{container_display}]"
            label += f" ({_format_size(log['size'])})"
            choices.append({"name": label, "value": i - 1})

        choices.append({"name": "⬅️  返回任务列表", "value": -1})

        selected_pod = inquirer.fuzzy(
            message="主人，选择要查看的节点日志:",
            choices=choices,
            pointer="❯",
            border=True,
            keybindings=ESC_KEYBINDING,
        ).execute()

        if selected_pod is None or selected_pod == -1:
            return

        # ── 第三级：查看日志内容 ──
        log_entry = log_files[selected_pod]
        _view_log_content(saver, log_entry)


def _view_log_content(saver: JobLogSaver, log_entry: dict):
    """查看单个日志文件内容。"""
    from InquirerPy import inquirer
    from rich.markup import escape

    content = saver.read_log(log_entry["path"])
    if content is None:
        print_error("无法读取日志文件")
        return

    total_lines = content.count("\n")
    pod_display = log_entry.get("pod_name") or log_entry["filename"]

    while True:
        mode = inquirer.select(
            message=f"[{pod_display}] 日志共 {total_lines} 行，查看模式:",
            choices=[
                {"name": "最后 100 行", "value": 100},
                {"name": "最后 500 行", "value": 500},
                {"name": "最后 1000 行", "value": 1000},
                {"name": "全部", "value": 0},
                {"name": "⬅️  返回节点列表", "value": -1},
            ],
            pointer="❯",
            keybindings=ESC_KEYBINDING,
        ).execute()

        if mode is None or mode == -1:
            return

        console.print()
        lines = content.splitlines()
        if mode > 0:
            lines = lines[-mode:]

        for line in lines:
            escaped = escape(line)
            if "ERROR" in line or "error" in line:
                console.print(f"[red]{escaped}[/red]")
            elif "WARNING" in line or "warning" in line or "WARN" in line:
                console.print(f"[yellow]{escaped}[/yellow]")
            else:
                console.print(escaped)

        console.print()


def _format_size(size_bytes: int) -> str:
    """格式化文件大小。"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
