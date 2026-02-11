#!/bin/bash
# ═══════════════════════════════════════════════
#  🚀 RayTool 一键安装脚本
#  使用方法：复制粘贴到母机终端执行
# ═══════════════════════════════════════════════
set -e

echo ""
echo "╭──────────────────────────────────╮"
echo "│   🚀 RayTool 安装向导            │"
echo "│   主人，欢迎使用！                │"
echo "╰──────────────────────────────────╯"
echo ""

# ──────────── 1. 交互式配置 ────────────

# 安装目录
read -p "📁 主人，请指定安装目录 [默认: ~/raytool]: " INSTALL_DIR
INSTALL_DIR="${INSTALL_DIR:-$HOME/raytool}"
INSTALL_DIR=$(eval echo "$INSTALL_DIR")  # 展开 ~

# 命名空间
read -p "🏷️  请指定 K8s 命名空间 [默认: ray-system]: " NAMESPACE
NAMESPACE="${NAMESPACE:-ray-system}"

# YAML 目录
read -p "📂 请指定 YAML 任务文件目录 [默认: ~/ray-jobs/]: " YAML_DIR
YAML_DIR="${YAML_DIR:-~/ray-jobs/}"

# 默认日志行数
read -p "📜 默认查看日志行数 [默认: 100]: " LOG_LINES
LOG_LINES="${LOG_LINES:-100}"

# 默认 Shell
read -p "🖥️  容器默认 Shell [默认: /bin/bash]: " DEFAULT_SHELL
DEFAULT_SHELL="${DEFAULT_SHELL:-/bin/bash}"

# 命令别名
read -p "⌨️  命令行别名 [默认: raytool]: " CMD_ALIAS
CMD_ALIAS="${CMD_ALIAS:-raytool}"

echo ""
echo "────────────────────────────────────"
echo "  安装目录:   $INSTALL_DIR"
echo "  命名空间:   $NAMESPACE"
echo "  YAML 目录:  $YAML_DIR"
echo "  日志行数:   $LOG_LINES"
echo "  默认 Shell: $DEFAULT_SHELL"
echo "  命令别名:   $CMD_ALIAS"
echo "────────────────────────────────────"
echo ""
read -p "主人，确认以上配置? [Y/n]: " CONFIRM
CONFIRM="${CONFIRM:-Y}"
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "已取消安装。"
    exit 0
fi

echo ""
echo "🚀 主人，正在创建项目文件..."

# ──────────── 2. 创建目录 ────────────

mkdir -p "$INSTALL_DIR/commands" "$INSTALL_DIR/utils"

# ──────────── 3. 生成配置文件 ────────────

cat > ~/.raytool.yaml << CONFEOF
namespace: $NAMESPACE
yaml_dir: $YAML_DIR
default_log_lines: $LOG_LINES
default_shell: $DEFAULT_SHELL
CONFEOF

echo "  ✅ 配置文件: ~/.raytool.yaml"

# ──────────── 4. 写入代码文件 ────────────

cat > "$INSTALL_DIR/utils/__init__.py" << 'FILEEOF'

FILEEOF

echo "  ✅ utils/__init__.py"

cat > "$INSTALL_DIR/utils/config.py" << 'FILEEOF'
"""配置文件读取模块"""
import os
import yaml

DEFAULT_CONFIG = {
    "namespace": "ray-system",
    "yaml_dir": "~/ray-jobs/",
    "default_log_lines": 100,
    "default_shell": "/bin/bash",
}

CONFIG_PATH = os.path.expanduser("~/.raytool.yaml")


def load_config() -> dict:
    """加载配置文件，不存在则返回默认配置"""
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                user_config = yaml.safe_load(f) or {}
            config.update(user_config)
        except Exception:
            pass
    # 展开路径中的 ~
    if "yaml_dir" in config:
        config["yaml_dir"] = os.path.expanduser(config["yaml_dir"])
    return config

FILEEOF

echo "  ✅ utils/config.py"

cat > "$INSTALL_DIR/utils/kube.py" << 'FILEEOF'
"""kubectl 命令调用封装"""
import subprocess
import sys
import json
from typing import List, Dict, Optional, Tuple
from collections import defaultdict


def run_kubectl(args: List[str], namespace: str, capture: bool = True, timeout: int = 30) -> Tuple[int, str, str]:
    """
    执行 kubectl 命令
    返回 (returncode, stdout, stderr)
    """
    cmd = ["kubectl"] + args + ["-n", namespace]
    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result.returncode, result.stdout, result.stderr
        else:
            # 交互模式（如 exec -it），直接继承终端
            result = subprocess.run(cmd, timeout=None)
            return result.returncode, "", ""
    except subprocess.TimeoutExpired:
        return 1, "", "命令执行超时"
    except FileNotFoundError:
        return 1, "", "未找到 kubectl 命令，请确认已安装并在 PATH 中"


def run_kubectl_stream(args: List[str], namespace: str):
    """
    以流式方式执行 kubectl 命令（用于 watch / logs -f）
    返回 Popen 对象
    """
    cmd = ["kubectl"] + args + ["-n", namespace]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return proc
    except FileNotFoundError:
        return None


def get_pods(namespace: str) -> List[Dict]:
    """获取所有 Pod 信息，返回字典列表"""
    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace
    )
    if rc != 0:
        return []
    try:
        data = json.loads(stdout)
        pods = []
        for item in data.get("items", []):
            metadata = item.get("metadata", {})
            status = item.get("status", {})
            # 容器列表
            containers = [c["name"] for c in item.get("spec", {}).get("containers", [])]
            # ready 计数
            container_statuses = status.get("containerStatuses", [])
            ready_count = sum(1 for cs in container_statuses if cs.get("ready", False))
            total_count = len(container_statuses) if container_statuses else len(containers)
            # 重启次数
            restarts = sum(cs.get("restartCount", 0) for cs in container_statuses)

            pods.append({
                "name": metadata.get("name", ""),
                "namespace": metadata.get("namespace", ""),
                "status": status.get("phase", "Unknown"),
                "ready": f"{ready_count}/{total_count}",
                "restarts": restarts,
                "creation": metadata.get("creationTimestamp", ""),
                "containers": containers,
                "labels": metadata.get("labels", {}),
            })
        return pods
    except (json.JSONDecodeError, KeyError):
        return []


def get_running_pods(namespace: str) -> List[Dict]:
    """获取所有 Running 状态的 Pod"""
    pods = get_pods(namespace)
    return [p for p in pods if p["status"] == "Running"]


def group_pods_by_job(pods: List[Dict]) -> Dict[str, List[Dict]]:
    """
    按任务名称对 Pod 分组
    通过 Pod 名称前缀推断任务名：去掉最后的 -head-N / -worker-N 部分
    也支持通过 label 分组（如果有 ray.io/cluster 等标签）
    """
    groups = defaultdict(list)
    for pod in pods:
        name = pod["name"]
        # 优先使用 label 中的集群名
        labels = pod.get("labels", {})
        job_name = (
            labels.get("ray.io/cluster", "")
            or labels.get("ray.io/job-name", "")
            or labels.get("app.kubernetes.io/instance", "")
        )
        if not job_name:
            # 回退：通过名称推断，去掉 -head-N / -worker-N / -raycluster-XXXXX 后缀
            job_name = _infer_job_name(name)
        groups[job_name].append(pod)
    return dict(groups)


def _infer_job_name(pod_name: str) -> str:
    """从 Pod 名称推断任务名"""
    parts = pod_name.split("-")
    # 尝试找到 head / worker 关键字的位置
    for i, part in enumerate(parts):
        if part in ("head", "worker"):
            return "-".join(parts[:i])
    # 找不到的话，去掉最后两段（通常是 hash-xxxxx）
    if len(parts) > 2:
        return "-".join(parts[:-2])
    return pod_name


def get_pod_role(pod: Dict) -> str:
    """判断 Pod 角色: Head / Worker / Unknown"""
    name = pod["name"]
    labels = pod.get("labels", {})
    # 通过 label 判断
    role = labels.get("ray.io/node-type", "").capitalize()
    if role:
        return role
    # 通过名称判断
    if "-head-" in name or name.endswith("-head"):
        return "Head"
    elif "-worker-" in name or name.endswith("-worker"):
        return "Worker"
    return "Unknown"


def delete_pods(pod_names: List[str], namespace: str) -> List[Tuple[str, bool, str]]:
    """批量删除 Pod，返回 [(pod_name, success, message)]"""
    results = []
    for name in pod_names:
        rc, stdout, stderr = run_kubectl(["delete", "pod", name, "--grace-period=30"], namespace)
        if rc == 0:
            results.append((name, True, "已删除"))
        else:
            results.append((name, False, stderr.strip()))
    return results


def exec_into_pod(pod_name: str, namespace: str, container: Optional[str] = None, shell: str = "/bin/bash"):
    """进入 Pod 容器终端"""
    args = ["exec", "-it", pod_name, "-n", namespace]
    if container:
        args += ["-c", container]
    args += ["--", shell]
    cmd = ["kubectl"] + args
    result = subprocess.run(cmd)
    # 如果 bash 不存在，回退到 sh
    if result.returncode != 0 and shell == "/bin/bash":
        args_sh = ["exec", "-it", pod_name, "-n", namespace]
        if container:
            args_sh += ["-c", container]
        args_sh += ["--", "/bin/sh"]
        cmd_sh = ["kubectl"] + args_sh
        subprocess.run(cmd_sh)


def apply_yaml(yaml_path: str, namespace: str) -> Tuple[bool, str]:
    """应用 YAML 文件"""
    rc, stdout, stderr = run_kubectl(["apply", "-f", yaml_path], namespace)
    if rc == 0:
        return True, stdout.strip()
    return False, stderr.strip()

FILEEOF

echo "  ✅ utils/kube.py"

cat > "$INSTALL_DIR/utils/ui.py" << 'FILEEOF'
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
        from utils.kube import get_pod_role
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
    """交互式选择一个任务，返回任务名"""
    if not jobs:
        console.print("[yellow]⚠️  主人，当前没有运行中的任务[/yellow]")
        return None

    choices = []
    for job_name, pods in sorted(jobs.items()):
        from utils.kube import get_pod_role
        head_count = sum(1 for p in pods if get_pod_role(p) == "Head")
        worker_count = sum(1 for p in pods if get_pod_role(p) == "Worker")
        label = f"{job_name}  ({len(pods)}节点: {head_count}H + {worker_count}W)"
        choices.append({"name": label, "value": job_name})

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
        from utils.kube import get_pod_role
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
    """交互式选择一个 Pod，返回 Pod 字典"""
    if not pods:
        console.print("[yellow]⚠️  主人，该任务下没有 Pod[/yellow]")
        return None

    from utils.kube import get_pod_role
    choices = []
    for pod in pods:
        role = get_pod_role(pod)
        label = f"{pod['name']}  ({role}, {pod['status']})"
        choices.append({"name": label, "value": pod})

    result = inquirer.select(
        message=f"主人，{message}",
        choices=choices,
        pointer="❯",
    ).execute()
    return result


def select_container(containers: List[str], message: str = "请选择容器") -> Optional[str]:
    """交互式选择容器"""
    if not containers:
        return None
    if len(containers) == 1:
        return containers[0]

    result = inquirer.select(
        message=f"主人，{message}",
        choices=containers,
        pointer="❯",
    ).execute()
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

FILEEOF

echo "  ✅ utils/ui.py"

cat > "$INSTALL_DIR/commands/__init__.py" << 'FILEEOF'

FILEEOF

echo "  ✅ commands/__init__.py"

cat > "$INSTALL_DIR/commands/watch.py" << 'FILEEOF'
"""功能1: 监控 Pods 状态 (kubectl get pods -n ray-system -w)"""
import sys
import signal
from utils.kube import run_kubectl_stream
from utils.ui import console, print_info, print_error, colorize_status, STATUS_COLORS


def watch_pods(namespace: str):
    """实时监控 Pod 状态变化"""
    print_info(f"正在监控 {namespace} 命名空间的 Pods... (Ctrl+C 退出)")
    console.print()

    proc = run_kubectl_stream(["get", "pods", "-w"], namespace)
    if proc is None:
        print_error("无法执行 kubectl 命令")
        return

    try:
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            # 为状态关键字上色
            colored = line
            for status, color in STATUS_COLORS.items():
                if status in colored:
                    colored = colored.replace(status, f"[{color}]{status}[/{color}]")
                    break
            console.print(colored)
    except KeyboardInterrupt:
        console.print("\n[dim]主人，已退出监控模式[/dim]")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()

FILEEOF

echo "  ✅ commands/watch.py"

cat > "$INSTALL_DIR/commands/list_jobs.py" << 'FILEEOF'
"""功能2: 查看运行中任务列表"""
from utils.kube import get_pods, group_pods_by_job
from utils.ui import console, print_jobs_table, print_pods_table, print_warning


def list_jobs(namespace: str):
    """列出所有运行中的任务（按 Pod 名称前缀分组）"""
    pods = get_pods(namespace)
    if not pods:
        print_warning("当前没有任何 Pod")
        return

    # 先打印所有 Pod 概览
    print_pods_table(pods, title=f"所有 Pods ({namespace})")
    console.print()

    # 按任务分组
    running_pods = [p for p in pods if p["status"] == "Running"]
    if not running_pods:
        print_warning("当前没有 Running 状态的任务")
        return

    jobs = group_pods_by_job(running_pods)
    print_jobs_table(jobs)

FILEEOF

echo "  ✅ commands/list_jobs.py"

cat > "$INSTALL_DIR/commands/logs.py" << 'FILEEOF'
"""功能3: 查看任务日志"""
from InquirerPy import inquirer
from utils.kube import get_running_pods, group_pods_by_job, run_kubectl, run_kubectl_stream
from utils.ui import (
    console, select_job, select_pod, select_container,
    print_info, print_error, print_warning, STATUS_COLORS,
)


def view_logs(namespace: str, default_lines: int = 100):
    """交互式查看任务日志"""
    # 步骤1: 获取运行中的任务
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    job_name = select_job(jobs, message="请选择要查看日志的任务")
    if not job_name:
        return

    # 步骤2: 选择节点
    job_pods = jobs[job_name]
    pod = select_pod(job_pods, message="请选择节点")
    if not pod:
        return

    # 步骤3: 选择容器（如果有多个）
    container = select_container(pod["containers"], message="请选择容器")

    # 步骤4: 选择日志模式
    mode = inquirer.select(
        message="主人，请选择日志查看模式",
        choices=[
            {"name": f"最后 {default_lines} 行", "value": ("tail", default_lines)},
            {"name": "最后 500 行", "value": ("tail", 500)},
            {"name": "最后 1000 行", "value": ("tail", 1000)},
            {"name": "实时追踪 (follow)", "value": ("follow", None)},
            {"name": "全部日志", "value": ("all", None)},
        ],
        pointer="❯",
    ).execute()

    mode_type, mode_value = mode

    # 构建 kubectl logs 参数
    args = ["logs", pod["name"]]
    if container:
        args += ["-c", container]

    if mode_type == "tail":
        args += [f"--tail={mode_value}"]
    elif mode_type == "follow":
        args += ["-f", "--tail=100"]

    pod_display = pod["name"]
    if container:
        pod_display += f" ({container})"

    if mode_type == "follow":
        # 流式追踪
        print_info(f"正在追踪 {pod_display} 的日志... (Ctrl+C 退出)")
        console.print()
        proc = run_kubectl_stream(args, namespace)
        if proc is None:
            print_error("无法执行 kubectl 命令")
            return
        try:
            for line in proc.stdout:
                # 简单的日志着色: ERROR 红色, WARNING 黄色
                line = line.rstrip()
                if "ERROR" in line or "error" in line:
                    console.print(f"[red]{line}[/red]")
                elif "WARNING" in line or "warning" in line or "WARN" in line:
                    console.print(f"[yellow]{line}[/yellow]")
                else:
                    console.print(line)
        except KeyboardInterrupt:
            console.print("\n[dim]主人，已退出日志追踪[/dim]")
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
    else:
        # 一次性输出
        print_info(f"正在获取 {pod_display} 的日志...")
        console.print()
        rc, stdout, stderr = run_kubectl(args, namespace, timeout=60)
        if rc != 0:
            print_error(f"获取日志失败: {stderr}")
            return
        if not stdout.strip():
            print_warning("日志为空")
            return
        # 输出并着色
        for line in stdout.splitlines():
            if "ERROR" in line or "error" in line:
                console.print(f"[red]{line}[/red]")
            elif "WARNING" in line or "warning" in line or "WARN" in line:
                console.print(f"[yellow]{line}[/yellow]")
            else:
                console.print(line)

FILEEOF

echo "  ✅ commands/logs.py"

cat > "$INSTALL_DIR/commands/submit.py" << 'FILEEOF'
"""功能4: 提交新任务"""
import os
import glob
import yaml
from InquirerPy import inquirer
from utils.kube import apply_yaml
from utils.ui import (
    console, confirm, print_success, print_error, print_warning, print_info,
)
from rich.panel import Panel
from rich.syntax import Syntax


def submit_job(namespace: str, yaml_dir: str = "~/ray-jobs/", yaml_path: str = None):
    """提交新任务"""
    yaml_dir = os.path.expanduser(yaml_dir)

    if yaml_path:
        # 直接通过参数传入路径
        _apply_and_report(yaml_path, namespace)
        return

    # 交互式选择
    choices = []

    # 扫描默认目录
    if os.path.isdir(yaml_dir):
        yaml_files = sorted(glob.glob(os.path.join(yaml_dir, "*.yaml")) + glob.glob(os.path.join(yaml_dir, "*.yml")))
        for f in yaml_files:
            choices.append({"name": os.path.basename(f), "value": f})

    if choices:
        choices.append({"name": "📁 手动输入路径...", "value": "__manual__"})
    else:
        if os.path.isdir(yaml_dir):
            print_warning(f"目录 {yaml_dir} 中没有找到 YAML 文件")
        else:
            print_warning(f"默认 YAML 目录 {yaml_dir} 不存在")
        choices.append({"name": "📁 手动输入路径...", "value": "__manual__"})

    selected = inquirer.select(
        message="主人，请选择 YAML 文件",
        choices=choices,
        pointer="❯",
    ).execute()

    if selected == "__manual__":
        selected = inquirer.filepath(
            message="主人，请输入 YAML 文件路径",
            validate=lambda x: os.path.isfile(x),
            invalid_message="文件不存在",
        ).execute()

    if not os.path.isfile(selected):
        print_error(f"文件不存在: {selected}")
        return

    # 预览 YAML
    _preview_yaml(selected)

    # 确认提交
    if not confirm("确认提交?"):
        print_warning("已取消提交")
        return

    _apply_and_report(selected, namespace)


def _preview_yaml(yaml_path: str):
    """预览 YAML 文件关键信息"""
    try:
        with open(yaml_path, "r") as f:
            content = f.read()
        data = yaml.safe_load(content)

        info_lines = []
        info_lines.append(f"[bold]文件:[/bold] {os.path.basename(yaml_path)}")

        if isinstance(data, dict):
            kind = data.get("kind", "Unknown")
            info_lines.append(f"[bold]类型:[/bold] {kind}")

            metadata = data.get("metadata", {})
            info_lines.append(f"[bold]名称:[/bold] {metadata.get('name', '-')}")

            # 尝试提取镜像信息
            images = _extract_images(data)
            if images:
                info_lines.append(f"[bold]镜像:[/bold] {', '.join(images[:3])}")

            # 尝试提取副本数
            replicas = data.get("spec", {}).get("replicas", None)
            if replicas:
                info_lines.append(f"[bold]副本:[/bold] {replicas}")

        panel_content = "\n".join(info_lines)
        console.print(Panel(panel_content, title="📄 任务预览", border_style="cyan"))

    except Exception as e:
        print_warning(f"无法解析 YAML 预览: {e}")
        # 仍然显示文件内容
        try:
            with open(yaml_path, "r") as f:
                content = f.read()
            syntax = Syntax(content[:2000], "yaml", theme="monokai", line_numbers=True)
            console.print(syntax)
        except Exception:
            pass


def _extract_images(data: dict) -> list:
    """递归提取 YAML 中的镜像信息"""
    images = []
    if isinstance(data, dict):
        if "image" in data and isinstance(data["image"], str):
            images.append(data["image"])
        for v in data.values():
            images.extend(_extract_images(v))
    elif isinstance(data, list):
        for item in data:
            images.extend(_extract_images(item))
    return list(dict.fromkeys(images))  # 去重保序


def _apply_and_report(yaml_path: str, namespace: str):
    """执行 apply 并报告结果"""
    print_info(f"正在提交: {os.path.basename(yaml_path)}")
    success, message = apply_yaml(yaml_path, namespace)
    if success:
        print_success(f"任务已提交: {message}")
        console.print("[dim]提示: 可使用 [1] 监控 Pods 状态 查看任务启动情况[/dim]")
    else:
        print_error(f"提交失败: {message}")

FILEEOF

echo "  ✅ commands/submit.py"

cat > "$INSTALL_DIR/commands/delete.py" << 'FILEEOF'
"""功能5: 删除任务"""
import os
import glob
import yaml
import InquirerPy
from utils.kube import run_kubectl, get_running_pods, group_pods_by_job
from utils.ui import (
    console, select_jobs_multi, confirm, confirm_with_input,
    print_success, print_error, print_warning, print_info,
)


def get_job_names_from_yaml(yaml_path: str) -> list:
    """从 YAML 文件中提取 job 名称"""
    try:
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load_all(f)
            job_names = []
            for doc in data:
                if doc and isinstance(doc, dict):
                    kind = doc.get('kind', '')
                    if 'PyTorchJob' in kind or 'RayCluster' in kind or 'Job' in kind:
                        name = doc.get('metadata', {}).get('name', '')
                        if name:
                            job_names.append((kind, name))
            return job_names
    except Exception as e:
        print_warning(f"解析 YAML 失败: {e}")
        return []


def _delete_pytorchjob(job_name: str, namespace: str) -> tuple:
    """删除 PyTorchJob 资源"""
    rc, stdout, stderr = run_kubectl(
        ["delete", "pytorchjob", job_name, "--ignore-not-found=true"],
        namespace
    )
    if rc == 0:
        return True, "已删除"
    return False, stderr.strip()


def _delete_yaml(yaml_path: str, namespace: str) -> tuple:
    """完全模拟 kubectl delete -f <yaml>"""
    if not os.path.isfile(yaml_path):
        return False, f"文件不存在: {yaml_path}"

    # 预览 YAML 中的资源
    job_infos = get_job_names_from_yaml(yaml_path)
    if not job_infos:
        return False, "YAML 中未找到 PyTorchJob 或 RayCluster 资源"

    console.print(f"[bold]文件:[/bold] {os.path.basename(yaml_path)}")
    for kind, name in job_infos:
        console.print(f"  [bold]- {kind}[/bold]: {name}")

    if not confirm("确认删除这些资源?"):
        print_warning("已取消")
        return False, "已取消"

    # 直接执行 kubectl delete -f
    print_info(f"执行: kubectl delete -f {yaml_path}")
    rc, stdout, stderr = run_kubectl(["delete", "-f", yaml_path], namespace)
    if rc == 0:
        return True, stdout.strip()
    return False, stderr.strip()


def delete_jobs(namespace: str, yaml_path: str = None):
    """交互式删除任务 - 默认使用 kubectl delete -f"""
    from utils.config import load_config
    config = load_config()
    yaml_dir = config.get("yaml_dir", "ray-job")

    # 如果指定了 YAML 文件，直接通过 YAML 删除
    if yaml_path:
        _delete_yaml(yaml_path, namespace)
        return

    # 扫描配置目录下的 YAML 文件
    yaml_files = []
    if os.path.isdir(yaml_dir):
        yaml_files = sorted(glob.glob(os.path.join(yaml_dir, "*.yaml")) +
                           glob.glob(os.path.join(yaml_dir, "*.yml")))

    if not yaml_files:
        print_warning(f"目录 {yaml_dir} 中没有找到 YAML 文件")
        print_info("将使用 kubectl delete pytorchjob 方式删除...")
        _delete_by_running_jobs(namespace)
        return

    # 默认使用 YAML 文件方式删除（模拟 kubectl delete -f）
    file_choices = [{"name": os.path.basename(f), "value": f} for f in yaml_files]
    file_choices.append({"name": "📁 手动输入路径...", "value": "__manual__"})
    file_choices.append({"name": "❌ 取消", "value": "__cancel__"})

    selected = InquirerPy.inquirer.select(
        message="请选择 YAML 文件删除 (kubectl delete -f)",
        choices=file_choices,
        pointer="❯",
    ).execute()

    if selected == "__cancel__":
        print_warning("已取消")
        return

    if selected == "__manual__":
        selected = InquirerPy.inquirer.filepath(
            message="请输入 YAML 文件路径",
            validate=lambda x: os.path.isfile(x),
            invalid_message="文件不存在",
        ).execute()

    _delete_yaml(selected, namespace)


def _delete_by_running_jobs(namespace: str):
    """从运行中的任务中选择删除"""
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    if not jobs:
        print_warning("当前没有运行中的任务")
        return

    # 多选任务
    selected = select_jobs_multi(jobs, message="请选择要删除的任务 (空格多选, 回车确认)")
    if not selected:
        print_warning("未选择任何任务")
        return

    console.print()
    console.print("[bold yellow]⚠️  即将删除以下任务:[/bold yellow]")
    for job_name in selected:
        console.print(f"  [bold]- {job_name}[/bold]")
    console.print()

    # 强确认
    if not confirm_with_input("确认删除? 请输入 'yes'"):
        print_warning("已取消删除")
        return

    console.print()
    for job_name in selected:
        success, msg = _delete_pytorchjob(job_name, namespace)
        if success:
            print_success(f"已删除: {job_name}")
        else:
            print_error(f"删除失败 {job_name}: {msg}")

FILEEOF

echo "  ✅ commands/delete.py"

cat > "$INSTALL_DIR/commands/shell.py" << 'FILEEOF'
"""功能6: 进入容器终端"""
from utils.kube import get_running_pods, group_pods_by_job, exec_into_pod
from utils.ui import (
    console, select_job, select_pod, select_container,
    print_info, print_error, print_warning,
)


def shell_into_pod(namespace: str, default_shell: str = "/bin/bash"):
    """交互式选择并进入容器终端"""
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    # 步骤1: 选择任务
    job_name = select_job(jobs, message="请选择任务")
    if not job_name:
        return

    # 步骤2: 选择节点
    job_pods = jobs[job_name]
    pod = select_pod(job_pods, message="请选择要进入的节点")
    if not pod:
        return

    # 步骤3: 选择容器
    container = select_container(pod["containers"], message="请选择容器")

    # 步骤4: 进入容器
    pod_display = pod["name"]
    if container:
        pod_display += f" ({container})"

    console.print(f"\n[bold cyan]🖥️  主人，正在连接 {pod_display} ...[/bold cyan]")
    console.print(f"[dim]Shell: {default_shell} | 输入 exit 退出容器[/dim]\n")

    exec_into_pod(
        pod_name=pod["name"],
        namespace=namespace,
        container=container,
        shell=default_shell,
    )

    console.print(f"\n[dim]主人，已退出容器 {pod['name']}[/dim]")

FILEEOF

echo "  ✅ commands/shell.py"

cat > "$INSTALL_DIR/raytool.py" << 'FILEEOF'
#!/usr/bin/env python3
"""
RayTool — Ray 集群任务管理命令行工具
用法:
    python raytool.py          # 交互式主菜单
    python raytool.py watch    # 直接执行子命令
"""
import sys
import os

# 将项目根目录加入 path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import click
from utils.config import load_config
from utils.ui import console, print_banner, print_error

# 加载配置
config = load_config()


# ──────────────────────── click 命令组 ────────────────────────

@click.group(invoke_without_command=True)
@click.option("--namespace", "-n", default=None, help="覆盖默认命名空间")
@click.option("--kubeconfig", default=None, help="指定 kubeconfig 路径")
@click.pass_context
def cli(ctx, namespace, kubeconfig):
    """🚀 RayTool — Ray 集群任务管理工具"""
    ctx.ensure_object(dict)
    ctx.obj["namespace"] = namespace or config["namespace"]

    if kubeconfig:
        os.environ["KUBECONFIG"] = kubeconfig

    # 没有子命令时进入交互式主菜单
    if ctx.invoked_subcommand is None:
        interactive_menu(ctx.obj["namespace"])


@cli.command("watch")
@click.pass_context
def cmd_watch(ctx):
    """📋 监控 Pods 状态"""
    from commands.watch import watch_pods
    watch_pods(ctx.obj["namespace"])


@cli.command("list")
@click.pass_context
def cmd_list(ctx):
    """📃 查看运行中任务列表"""
    from commands.list_jobs import list_jobs
    list_jobs(ctx.obj["namespace"])


@cli.command("logs")
@click.argument("job_name", required=False)
@click.argument("pod_name", required=False)
@click.pass_context
def cmd_logs(ctx, job_name, pod_name):
    """📜 查看任务日志"""
    from commands.logs import view_logs
    view_logs(ctx.obj["namespace"], config["default_log_lines"])


@cli.command("submit")
@click.argument("yaml_path", required=False)
@click.pass_context
def cmd_submit(ctx, yaml_path):
    """🚀 提交新任务"""
    from commands.submit import submit_job
    submit_job(ctx.obj["namespace"], config["yaml_dir"], yaml_path)


@cli.command("delete")
@click.argument("yaml_path", required=False)
@click.pass_context
def cmd_delete(ctx, yaml_path):
    """🗑️  删除任务"""
    from commands.delete import delete_jobs
    delete_jobs(ctx.obj["namespace"], yaml_path)


@cli.command("exec")
@click.argument("job_name", required=False)
@click.argument("pod_name", required=False)
@click.pass_context
def cmd_exec(ctx, job_name, pod_name):
    """🖥️  进入容器终端"""
    from commands.shell import shell_into_pod
    shell_into_pod(ctx.obj["namespace"], config["default_shell"])


# ──────────────────────── 交互式主菜单 ────────────────────────

def interactive_menu(namespace: str):
    """交互式主菜单循环"""
    from InquirerPy import inquirer

    while True:
        console.clear()
        print_banner()
        console.print(f"[dim]命名空间: {namespace}[/dim]\n")

        try:
            action = inquirer.select(
                message="主人，请选择操作",
                choices=[
                    {"name": "📋 监控 Pods 状态", "value": "watch"},
                    {"name": "📃 查看运行中任务列表", "value": "list"},
                    {"name": "📜 查看任务日志", "value": "logs"},
                    {"name": "🚀 提交新任务", "value": "submit"},
                    {"name": "🗑️  删除任务", "value": "delete"},
                    {"name": "🖥️  进入容器终端", "value": "exec"},
                    {"name": "❌ 退出", "value": "quit"},
                ],
                pointer="❯",
            ).execute()
        except (KeyboardInterrupt, EOFError):
            _exit_gracefully()
            return

        if action == "quit":
            _exit_gracefully()
            return

        console.print()

        try:
            if action == "watch":
                from commands.watch import watch_pods
                watch_pods(namespace)
            elif action == "list":
                from commands.list_jobs import list_jobs
                list_jobs(namespace)
            elif action == "logs":
                from commands.logs import view_logs
                view_logs(namespace, config["default_log_lines"])
            elif action == "submit":
                from commands.submit import submit_job
                submit_job(namespace, config["yaml_dir"])
            elif action == "delete":
                from commands.delete import delete_jobs
                delete_jobs(namespace)
            elif action == "exec":
                from commands.shell import shell_into_pod
                shell_into_pod(namespace, config["default_shell"])
        except KeyboardInterrupt:
            console.print("\n[dim]操作已中断[/dim]")
        except Exception as e:
            print_error(f"执行出错: {e}")

        # 操作完成后等待用户按键返回主菜单
        console.print()
        try:
            inquirer.text(message="主人，按回车键返回主菜单...").execute()
        except (KeyboardInterrupt, EOFError):
            _exit_gracefully()
            return


def _exit_gracefully():
    console.print("\n[cyan]👋 主人再见！[/cyan]")


# ──────────────────────── 入口 ────────────────────────

if __name__ == "__main__":
    cli()

FILEEOF

echo "  ✅ raytool.py"

cat > "$INSTALL_DIR/requirements.txt" << 'FILEEOF'
rich>=13.0
InquirerPy>=0.3.4
click>=8.0
pyyaml>=6.0

FILEEOF

echo "  ✅ requirements.txt"

cat > "$INSTALL_DIR/README.md" << 'FILEEOF'
# 🚀 RayTool

Ray 集群任务管理命令行工具，封装 kubectl 常用操作为交互式菜单。

## 安装

```bash
pip install -r requirements.txt
```

要求：Python 3.8+，kubectl 已安装并配置好集群访问权限。

## 使用

```bash
# 交互式主菜单
python raytool.py

# 直接执行子命令
python raytool.py watch              # 监控 Pods 状态
python raytool.py list               # 查看运行中任务
python raytool.py logs               # 查看任务日志
python raytool.py submit job.yaml    # 提交任务
python raytool.py delete             # 删除任务
python raytool.py exec               # 进入容器终端

# 全局选项
python raytool.py -n my-namespace list   # 指定命名空间
python raytool.py --kubeconfig ~/.kube/other-config list
```

## 配置文件

创建 `~/.raytool.yaml` 自定义默认参数：

```yaml
namespace: ray-system
yaml_dir: ~/ray-jobs/
default_log_lines: 100
default_shell: /bin/bash
```

## 快捷方式（可选）

```bash
# 添加 alias
echo 'alias raytool="python3 /path/to/raytool/raytool.py"' >> ~/.bashrc
source ~/.bashrc

# 之后直接使用
raytool
raytool list
raytool logs
```

FILEEOF

echo "  ✅ README.md"


chmod +x "$INSTALL_DIR/raytool.py"

# ──────────── 5. 添加命令行别名 ────────────

ALIAS_LINE="alias $CMD_ALIAS='python3 $INSTALL_DIR/raytool.py'"

# 检测 shell 配置文件
if [ -f "$HOME/.zshrc" ]; then
    SHELL_RC="$HOME/.zshrc"
elif [ -f "$HOME/.bashrc" ]; then
    SHELL_RC="$HOME/.bashrc"
else
    SHELL_RC="$HOME/.bashrc"
fi

# 去掉旧的 alias（如果存在）
if grep -q "alias $CMD_ALIAS=" "$SHELL_RC" 2>/dev/null; then
    sed -i "/alias $CMD_ALIAS=/d" "$SHELL_RC"
fi

echo "" >> "$SHELL_RC"
echo "# RayTool 命令别名" >> "$SHELL_RC"
echo "$ALIAS_LINE" >> "$SHELL_RC"

echo "  ✅ 别名已添加到 $SHELL_RC"

# 立即生效
eval "$ALIAS_LINE"

# ──────────── 6. 完成 ────────────

echo ""
echo "╭──────────────────────────────────╮"
echo "│   ✅ 安装完成！                   │"
echo "╰──────────────────────────────────╯"
echo ""
echo "📁 项目文件："
find "$INSTALL_DIR" -type f | sort
echo ""
echo "📦 主人，请先安装 Python 依赖："
echo "   pip install -r $INSTALL_DIR/requirements.txt"
echo ""
echo "🚀 然后直接使用："
echo "   $CMD_ALIAS              # 交互式主菜单"
echo "   $CMD_ALIAS list         # 查看任务列表"
echo "   $CMD_ALIAS watch        # 监控 Pods"
echo "   $CMD_ALIAS logs         # 查看日志"
echo "   $CMD_ALIAS exec         # 进入容器"
echo ""
echo "💡 如果 '$CMD_ALIAS' 命令未生效，请执行: source $SHELL_RC"
echo ""
