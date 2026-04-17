"""交互式 UI 组件封装"""
from typing import List, Dict, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from InquirerPy import inquirer
from InquirerPy.separator import Separator

console = Console()

# ESC 键绑定：按 ESC 跳过当前选择（返回 None），统一用于所有交互式选择器
# 注意：由于终端转义序列机制，ESC 键响应会有约 1 秒延迟，这是正常现象
ESC_KEYBINDING = {"skip": [{"key": "escape"}]}

# 状态颜色映射
STATUS_COLORS = {
    "Running": "green",
    "Succeeded": "blue",
    "Completed": "dim",
    "Pending": "yellow",
    "ContainerCreating": "yellow",
    "Init": "yellow",
    "Failed": "red",
    "Error": "red",
    "CrashLoopBackOff": "red",
    "ImagePullBackOff": "red",
    "Terminating": "magenta",
    "Unknown": "dim",
}


def print_banner():
    """打印工具横幅"""
    banner = Text()
    banner.append("🚀 RayTool v2.0\n", style="bold cyan")
    banner.append("   主人的 Ray 集群任务管理工具", style="dim")
    console.print(Panel(banner, border_style="cyan", padding=(0, 2)))


def colorize_status(status: str) -> str:
    """为状态添加 rich 颜色标记"""
    color = STATUS_COLORS.get(status, "white")
    return f"[{color}]{status}[/{color}]"


def print_pods_table(pods: List[Dict], title: str = "Pods 状态"):
    """打印 Pod 表格"""
    table = Table(title=title, show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("名称", style="cyan", min_width=30)
    table.add_column("READY", justify="center", width=8)
    table.add_column("状态", justify="center", width=18)
    table.add_column("重启", justify="center", width=6)
    table.add_column("创建时间", width=22)

    for i, pod in enumerate(pods, 1):
        status_display = colorize_status(pod["status"])
        table.add_row(
            str(i),
            pod["name"],
            pod["ready"],
            status_display,
            str(pod["restarts"]),
            pod["creation"][:19].replace("T", " ") if pod["creation"] else "-",
        )

    console.print(table)


def print_jobs_table(jobs: Dict[str, List[Dict]]):
    """打印任务分组表格"""
    table = Table(title="运行中的任务", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("任务名称", style="bold cyan", min_width=25)
    table.add_column("状态", justify="center", width=12)
    table.add_column("节点数", justify="center", width=8)
    table.add_column("Head", justify="center", width=6)
    table.add_column("Worker", justify="center", width=8)

    for i, (job_name, pods) in enumerate(sorted(jobs.items()), 1):
        from raytool.utils.kube import get_pod_role
        statuses = set(p["status"] for p in pods)
        status = "Running" if "Running" in statuses else list(statuses)[0] if statuses else "Unknown"
        head_count = sum(1 for p in pods if get_pod_role(p) == "Head")
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


def select_job(jobs: Dict[str, List[Dict]], message: str = "请选择任务") -> Optional[str]:
    """交互式选择一个任务，返回任务名。选择返回时返回 None"""
    if not jobs:
        console.print("[yellow]⚠️  主人，当前没有运行中的任务[/yellow]")
        return None

    choices = []
    for job_name, pods in sorted(jobs.items()):
        from raytool.utils.kube import get_pod_role
        head_count = sum(1 for p in pods if get_pod_role(p) == "Head")
        worker_count = sum(1 for p in pods if get_pod_role(p) == "Worker")
        label = f"{job_name}  ({len(pods)}节点: {head_count}H + {worker_count}W)"
        choices.append({"name": label, "value": job_name})
    choices.append({"name": "↩️  返回上一级", "value": None})

    result = inquirer.select(
        message=f"主人，{message}",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()
    return result


def select_jobs_multi(jobs: Dict[str, List[Dict]], message: str = "请选择任务 (空格多选)") -> List[str]:
    """交互式多选任务，返回任务名列表"""
    if not jobs:
        console.print("[yellow]⚠️  主人，当前没有运行中的任务[/yellow]")
        return []

    choices = []
    for job_name, pods in sorted(jobs.items()):
        from raytool.utils.kube import get_pod_role
        head_count = sum(1 for p in pods if get_pod_role(p) == "Head")
        worker_count = sum(1 for p in pods if get_pod_role(p) == "Worker")
        label = f"{job_name}  ({len(pods)}节点: {head_count}H + {worker_count}W)"
        choices.append({"name": label, "value": job_name})

    result = inquirer.checkbox(
        message=f"主人，{message}",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()
    return result


def select_pod(pods: List[Dict], message: str = "请选择节点") -> Optional[Dict]:
    """交互式选择一个 Pod，返回 Pod 字典。选择返回时返回 None"""
    if not pods:
        console.print("[yellow]⚠️  主人，该任务下没有 Pod[/yellow]")
        return None

    choices = []
    for pod in pods:
        role = pod.get("role", "Unknown")
        label = f"{pod['name']}  ({role}, {pod['status']})"
        choices.append({"name": label, "value": pod})
    choices.append({"name": "↩️  返回上一级", "value": None})

    result = inquirer.select(
        message=f"主人，{message}",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()
    return result


def select_container(containers: List[str], message: str = "请选择容器") -> Optional[str]:
    """交互式选择容器。选择返回时返回 None"""
    if not containers:
        return None
    if len(containers) == 1:
        return containers[0]

    choices = list(containers) + ["↩️  返回上一级"]
    result = inquirer.select(
        message=f"主人，{message}",
        choices=choices,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()
    if result == "↩️  返回上一级":
        return None
    return result


def confirm(message: str = "确认操作?", default: bool = False) -> bool:
    """确认操作"""
    return inquirer.confirm(message=f"主人，{message}", default=default).execute()


def confirm_with_input(message: str = "请输入 'yes' 确认") -> bool:
    """需要输入 yes 的强确认"""
    result = inquirer.text(message=f"主人，{message}").execute()
    return result.strip().lower() == "yes"


def print_success(msg: str):
    from rich.markup import escape
    console.print(f"[green]✅ 主人，{escape(msg)}[/green]")


def print_error(msg: str):
    from rich.markup import escape
    console.print(f"[red]❌ 主人，{escape(msg)}[/red]")


def print_warning(msg: str):
    from rich.markup import escape
    console.print(f"[yellow]⚠️  主人，{escape(msg)}[/yellow]")


def print_info(msg: str):
    from rich.markup import escape
    console.print(f"[cyan]ℹ️  主人，{escape(msg)}[/cyan]")


def browse_yaml_dir(yaml_dir: str, message: str = "请选择 YAML 文件") -> Optional[str]:
    """交互式浏览 YAML 目录，支持进入子目录和返回上级。

    返回选中的 YAML 文件绝对路径，或以下特殊值:
    - None: 按 ESC 或选择返回
    - "__manual__": 用户选择手动输入路径
    """
    import os
    import glob

    current_dir = os.path.abspath(yaml_dir)
    root_dir = current_dir  # 记录根目录，防止浏览到根目录之上

    while True:
        if not os.path.isdir(current_dir):
            print_warning(f"目录 {current_dir} 不存在")
            return None

        choices = []

        # 子目录
        subdirs = sorted([
            d for d in os.listdir(current_dir)
            if os.path.isdir(os.path.join(current_dir, d)) and not d.startswith(".")
        ])
        for d in subdirs:
            # 统计子目录下的 YAML 文件数（含递归）
            sub_path = os.path.join(current_dir, d)
            yaml_count = len(glob.glob(os.path.join(sub_path, "**", "*.yaml"), recursive=True)) + \
                         len(glob.glob(os.path.join(sub_path, "**", "*.yml"), recursive=True))
            count_hint = f" ({yaml_count} 个YAML)" if yaml_count > 0 else " (空)"
            choices.append({"name": f"📂 {d}/{count_hint}", "value": ("dir", sub_path)})

        # 当前目录的 YAML 文件
        yaml_files = sorted(
            glob.glob(os.path.join(current_dir, "*.yaml")) +
            glob.glob(os.path.join(current_dir, "*.yml"))
        )
        for f in yaml_files:
            choices.append({"name": f"📄 {os.path.basename(f)}", "value": ("file", f)})

        if not choices:
            print_warning(f"目录 {current_dir} 中没有 YAML 文件或子目录")

        # 始终添加功能选项
        choices.append({"name": "📁 手动输入路径...", "value": ("action", "__manual__")})
        # 如果不在根目录，显示返回上级
        if os.path.abspath(current_dir) != os.path.abspath(root_dir):
            choices.append({"name": "⬆️  返回上级目录", "value": ("action", "__parent__")})
        choices.append({"name": "↩️  返回", "value": ("action", "__cancel__")})

        # 显示当前路径提示
        rel_path = os.path.relpath(current_dir, root_dir)
        if rel_path == ".":
            path_hint = os.path.basename(current_dir) + "/"
        else:
            path_hint = os.path.basename(root_dir) + "/" + rel_path + "/"

        selected = inquirer.select(
            message=f"{message} [{path_hint}]",
            choices=choices,
            pointer="❯",
            keybindings=ESC_KEYBINDING,
        ).execute()

        if selected is None:
            # ESC pressed
            if os.path.abspath(current_dir) != os.path.abspath(root_dir):
                # 在子目录中按 ESC → 返回上级
                current_dir = os.path.dirname(current_dir)
                continue
            return None

        action_type, value = selected

        if action_type == "dir":
            current_dir = value
            continue
        elif action_type == "file":
            return value
        elif action_type == "action":
            if value == "__parent__":
                current_dir = os.path.dirname(current_dir)
                continue
            elif value == "__cancel__":
                return None
            elif value == "__manual__":
                return "__manual__"

