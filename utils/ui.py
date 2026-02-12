"""交互式 UI 组件封装"""
from typing import List, Dict, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from InquirerPy import inquirer
from InquirerPy.separator import Separator

console = Console()

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
    banner.append("🚀 RayTool v1.0\n", style="bold cyan")
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
    console.print(f"[green]✅ 主人，{msg}[/green]")


def print_error(msg: str):
    console.print(f"[red]❌ 主人，{msg}[/red]")


def print_warning(msg: str):
    console.print(f"[yellow]⚠️  主人，{msg}[/yellow]")


def print_info(msg: str):
    console.print(f"[cyan]ℹ️  主人，{msg}[/cyan]")

