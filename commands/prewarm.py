"""功能: 镜像预热 — 引导用户管理 prewarm 目录下的 YAML 并执行 apply/delete/查看状态"""
import os
import glob
import re
from typing import Any

from InquirerPy import inquirer
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from raytool.utils.kube import run_kubectl, apply_yaml
from raytool.utils.ui import (
    console, confirm, print_success, print_error, print_warning, print_info,
    colorize_status, ESC_KEYBINDING,
)

# 以下默认值可通过全局配置覆盖 (namespace / pause_image / instance_types)
PREWARM_NAMESPACE = "ray-system"
PAUSE_IMAGE = "gcr.io/google_containers/pause:3.2"


def _get_prewarm_dir(config: dict[str, Any]) -> str:
    """从配置获取 prewarm 目录的绝对路径。
    优先使用配置中的 prewarm_dir，否则回退到 yaml_dir 同级的 prewarm/ 目录。
    """
    prewarm_dir = config.get("prewarm_dir", "")
    if prewarm_dir:
        return prewarm_dir

    # 回退: yaml_dir 同级的 prewarm/
    yaml_dir = config.get("yaml_dir", "")
    if yaml_dir:
        return os.path.join(os.path.dirname(yaml_dir), "prewarm")

    # 最终回退: 当前工作目录
    return os.path.join(os.getcwd(), "prewarm")


def _ensure_prewarm_dir(prewarm_dir: str) -> bool:
    """确保 prewarm 目录存在，不存在则引导用户创建。返回 True 表示目录就绪。"""
    if os.path.isdir(prewarm_dir):
        return True

    console.print(Panel(
        f"[bold yellow]镜像预热目录不存在[/bold yellow]\n\n"
        f"  路径: [cyan]{prewarm_dir}[/cyan]\n\n"
        f"  镜像预热功能需要一个 [bold]prewarm[/bold] 目录来存放预热 YAML 文件。\n"
        f"  请将 DaemonSet 等预热 YAML 文件放入该目录中。",
        title="📁 镜像预热",
        border_style="yellow",
        padding=(1, 2),
    ))
    console.print()

    if not confirm(f"是否创建目录 {prewarm_dir} ?", default=True):
        print_warning("已取消")
        return False

    try:
        os.makedirs(prewarm_dir, exist_ok=True)
        print_success(f"目录已创建: {prewarm_dir}")
        console.print(f"[dim]请将镜像预热 YAML 文件（如 image_prewarmer.yaml）放入该目录[/dim]")
        return True
    except OSError as e:
        print_error(f"创建目录失败: {e}")
        return False


def _scan_yaml_files(prewarm_dir: str) -> list:
    """扫描 prewarm 目录下的所有 YAML 文件"""
    files = sorted(
        glob.glob(os.path.join(prewarm_dir, "*.yaml"))
        + glob.glob(os.path.join(prewarm_dir, "*.yml"))
    )
    return files


def _preview_yaml(yaml_path: str):
    """预览 YAML 文件内容"""
    try:
        with open(yaml_path, "r") as f:
            content = f.read()
        syntax = Syntax(content[:3000], "yaml", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, title=f"📄 {os.path.basename(yaml_path)}", border_style="cyan"))
    except Exception as e:
        print_error(f"无法读取文件: {e}")


def _apply_prewarm(yaml_path: str, namespace: str):
    """应用预热 YAML (kubectl apply -f)"""
    print_info(f"正在应用: kubectl apply -f {os.path.basename(yaml_path)}")
    rc, stdout, stderr = run_kubectl(["apply", "-f", yaml_path], namespace)
    if rc == 0:
        print_success(f"预热任务已提交: {stdout.strip()}")
        console.print("[dim]提示: 选择 '查看预热 Pod 状态' 可监控预热进度[/dim]")
    else:
        print_error(f"提交失败: {stderr.strip()}")


def _delete_prewarm(yaml_path: str, namespace: str):
    """删除预热 YAML (kubectl delete -f)"""
    print_info(f"正在删除: kubectl delete -f {os.path.basename(yaml_path)}")
    rc, stdout, stderr = run_kubectl(["delete", "-f", yaml_path], namespace)
    if rc == 0:
        print_success(f"预热资源已删除: {stdout.strip()}")
    else:
        print_error(f"删除失败: {stderr.strip()}")


def _check_prewarm_pods(namespace: str):
    """查看预热相关的 Pod 状态 (ray-system namespace)"""
    print_info(f"正在查询 {namespace} 命名空间下的 Pod 状态...")
    console.print()

    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace,
        timeout=15,
    )

    if rc != 0:
        print_error(f"查询失败: {stderr.strip()}")
        return

    import json
    try:
        data = json.loads(stdout)
        items = data.get("items", [])
    except (json.JSONDecodeError, KeyError):
        print_error("解析 Pod 列表失败")
        return

    if not items:
        print_warning(f"命名空间 {namespace} 下没有 Pod")
        return

    table = Table(title=f"📋 {namespace} Pod 状态", show_lines=False, border_style="dim")
    table.add_column("#", style="dim", width=4)
    table.add_column("名称", style="cyan", min_width=40)
    table.add_column("READY", justify="center", width=8)
    table.add_column("状态", justify="center", width=18)
    table.add_column("节点", style="dim", min_width=30)
    table.add_column("创建时间", width=22)

    for i, item in enumerate(items, 1):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        name = metadata.get("name", "")
        phase = status.get("phase", "Unknown")

        container_statuses = status.get("containerStatuses", [])
        containers = [c["name"] for c in spec.get("containers", [])]
        ready_count = sum(1 for cs in container_statuses if cs.get("ready", False))
        total_count = len(container_statuses) if container_statuses else len(containers)
        ready_str = f"{ready_count}/{total_count}"

        node_name = spec.get("nodeName", "-")
        creation = metadata.get("creationTimestamp", "")
        creation_display = creation[:19].replace("T", " ") if creation else "-"

        table.add_row(str(i), name, ready_str, colorize_status(phase), node_name, creation_display)

    console.print(table)
    console.print(f"\n[dim]共 {len(items)} 个 Pod[/dim]")


# 以下默认值可通过全局配置覆盖 (instance_types)
DEFAULT_INSTANCE_TYPE = "ml.p5en.48xlarge"
INSTANCE_TYPE_CHOICES = [
    "ml.p5en.48xlarge",
    "ml.p5e.48xlarge",
    "ml.p4de.24xlarge",
    "ml.p4d.24xlarge",
]


def _sanitize_k8s_name(name: str) -> str:
    """将字符串转为合法的 Kubernetes 资源名称（小写字母、数字、连字符）。"""
    name = name.lower()
    name = re.sub(r'[^a-z0-9-]', '-', name)
    name = re.sub(r'-+', '-', name)
    name = name.strip('-')
    # Kubernetes 名称最长 253 字符
    return name[:253]


def _generate_prewarm_yaml(image: str, instance_type: str = DEFAULT_INSTANCE_TYPE) -> str:
    """根据镜像地址生成 DaemonSet YAML 内容。"""
    # 从镜像地址提取短名作为 DaemonSet 名
    image_short = image.split("/")[-1]  # e.g. youtu-agent:agent-sandbox-v2
    ds_name = _sanitize_k8s_name(f"prewarm-{image_short}")

    yaml_content = f"""apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {ds_name}
  namespace: {PREWARM_NAMESPACE}
  labels:
    app: {ds_name}
    raytool/prewarm: "true"
spec:
  selector:
    matchLabels:
      app: {ds_name}
  template:
    metadata:
      labels:
        app: {ds_name}
        raytool/prewarm: "true"
    spec:
      nodeSelector:
        node.kubernetes.io/instance-type: {instance_type}
      initContainers:
      - name: prewarm
        image: {image}
        command: ["sh", "-c", "echo 'Image prewarmed: {image}' && exit 0"]
      containers:
      - name: pause
        image: {PAUSE_IMAGE}
        resources:
          requests:
            cpu: 10m
            memory: 10Mi
"""
    return yaml_content


def _quick_prewarm_by_image(prewarm_dir: str):
    """输入镜像名快速预热：生成 DaemonSet YAML → 预览 → apply → 保存到 prewarm 目录。"""
    console.print()
    console.print(Panel(
        "  输入完整的镜像地址，将自动生成 DaemonSet 在所有目标节点上拉取该镜像。\n"
        "  [dim]示例: <account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>[/dim]",
        title="🔥 快速镜像预热",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    # 输入镜像地址
    image = inquirer.text(
        message="主人，请输入镜像地址",
        validate=lambda x: len(x.strip()) > 0,
        invalid_message="镜像地址不能为空",
    ).execute().strip()

    if not image:
        print_warning("已取消")
        return

    # 选择实例类型
    instance_type = inquirer.select(
        message="选择目标节点实例类型",
        choices=[{"name": t, "value": t} for t in INSTANCE_TYPE_CHOICES]
                + [{"name": "✏️  自定义输入...", "value": "__custom__"}],
        default=DEFAULT_INSTANCE_TYPE,
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if instance_type is None:
        print_warning("已取消")
        return

    if instance_type == "__custom__":
        instance_type = inquirer.text(
            message="请输入实例类型",
            default=DEFAULT_INSTANCE_TYPE,
        ).execute().strip()

    # 生成 YAML
    yaml_content = _generate_prewarm_yaml(image, instance_type)

    # 预览
    console.print()
    syntax = Syntax(yaml_content, "yaml", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="📄 预热 DaemonSet 预览", border_style="cyan"))
    console.print()

    if not confirm("确认应用该预热配置?"):
        print_warning("已取消")
        return

    # 写入临时文件并 apply
    image_short = image.split("/")[-1]
    filename = _sanitize_k8s_name(f"prewarm-{image_short}") + ".yaml"
    yaml_path = os.path.join(prewarm_dir, filename)

    try:
        os.makedirs(prewarm_dir, exist_ok=True)
        with open(yaml_path, "w") as f:
            f.write(yaml_content)
        print_success(f"YAML 已保存: {yaml_path}")
    except OSError as e:
        print_error(f"保存 YAML 失败: {e}")
        return

    # 执行 apply
    print_info(f"正在应用: kubectl apply -f {filename}")
    rc, stdout, stderr = run_kubectl(["apply", "-f", yaml_path], PREWARM_NAMESPACE)
    if rc == 0:
        print_success(f"预热任务已提交: {stdout.strip()}")
        console.print("[dim]提示: 选择 '查看预热 Pod 状态' 可监控预热进度[/dim]")
        console.print(f"[dim]清理命令: kubectl delete -f {yaml_path} -n {PREWARM_NAMESPACE}[/dim]")
    else:
        print_error(f"提交失败: {stderr.strip()}")


def prewarm_images(namespace: str, config: dict[str, Any] = None):
    """镜像预热交互式入口"""
    if config is None:
        config = {}

    global PREWARM_NAMESPACE, PAUSE_IMAGE, INSTANCE_TYPE_CHOICES, DEFAULT_INSTANCE_TYPE
    PREWARM_NAMESPACE = config.get("namespace", PREWARM_NAMESPACE)
    PAUSE_IMAGE = config.get("pause_image", PAUSE_IMAGE)
    if config.get("instance_types"):
        INSTANCE_TYPE_CHOICES = config["instance_types"]
        DEFAULT_INSTANCE_TYPE = INSTANCE_TYPE_CHOICES[0]

    prewarm_dir = _get_prewarm_dir(config)

    # 确保目录存在
    if not _ensure_prewarm_dir(prewarm_dir):
        return

    while True:
        # 扫描 YAML 文件
        yaml_files = _scan_yaml_files(prewarm_dir)

        console.print()
        console.print(Panel(
            f"  目录: [cyan]{prewarm_dir}[/cyan]\n"
            f"  YAML 文件: [bold]{len(yaml_files)}[/bold] 个\n"
            f"  预热命名空间: [cyan]{PREWARM_NAMESPACE}[/cyan]",
            title="🔥 镜像预热",
            border_style="cyan",
            padding=(0, 2),
        ))
        console.print()

        if not yaml_files:
            print_warning(f"目录 {prewarm_dir} 中没有现有的 YAML 文件")
            console.print(f"[dim]可使用「输入镜像名快速预热」直接创建[/dim]")
            # 没有 YAML 文件时仍可使用快速预热和查看状态
            action = inquirer.select(
                message="主人，请选择操作",
                choices=[
                    {"name": "🔥 输入镜像名快速预热", "value": "quick"},
                    {"name": "👁️  查看预热 Pod 状态", "value": "status"},
                    {"name": "↩️  返回上一级", "value": "back"},
                ],
                pointer="❯",
                keybindings=ESC_KEYBINDING,
            ).execute()

            if action == "quick":
                _quick_prewarm_by_image(prewarm_dir)
                continue
            elif action == "status":
                _check_prewarm_pods(PREWARM_NAMESPACE)
                continue
            else:
                # "back" 或 ESC (None)
                return

        # 选择操作
        action = inquirer.select(
            message="主人，请选择预热操作",
            choices=[
                {"name": "🔥 输入镜像名快速预热", "value": "quick"},
                {"name": "🚀 应用预热 YAML (kubectl apply -f)", "value": "apply"},
                {"name": "🗑️  删除预热资源 (kubectl delete -f)", "value": "delete"},
                {"name": "👁️  查看预热 Pod 状态 (kubectl get pods)", "value": "status"},
                {"name": "📄 预览 YAML 文件内容", "value": "preview"},
                {"name": "↩️  返回上一级", "value": "back"},
            ],
            pointer="❯",
            keybindings=ESC_KEYBINDING,
        ).execute()

        if action == "back" or action is None:
            return

        if action == "quick":
            _quick_prewarm_by_image(prewarm_dir)
            continue

        if action == "status":
            _check_prewarm_pods(PREWARM_NAMESPACE)
            continue

        # 选择 YAML 文件
        file_choices = [{"name": os.path.basename(f), "value": f} for f in yaml_files]
        file_choices.append({"name": "↩️  返回", "value": "__cancel__"})

        if action == "apply":
            selected = inquirer.select(
                message="主人，请选择要应用的 YAML 文件",
                choices=file_choices,
                pointer="❯",
                keybindings=ESC_KEYBINDING,
            ).execute()

            if selected == "__cancel__" or selected is None:
                continue

            _preview_yaml(selected)
            console.print()
            if confirm("确认应用该预热文件?"):
                _apply_prewarm(selected, PREWARM_NAMESPACE)

        elif action == "delete":
            selected = inquirer.select(
                message="主人，请选择要删除的预热 YAML",
                choices=file_choices,
                pointer="❯",
                keybindings=ESC_KEYBINDING,
            ).execute()

            if selected == "__cancel__" or selected is None:
                continue

            _preview_yaml(selected)
            console.print()
            if confirm("确认删除该预热资源?"):
                _delete_prewarm(selected, PREWARM_NAMESPACE)

        elif action == "preview":
            selected = inquirer.select(
                message="主人，请选择要预览的 YAML 文件",
                choices=file_choices,
                pointer="❯",
                keybindings=ESC_KEYBINDING,
            ).execute()

            if selected == "__cancel__" or selected is None:
                continue

            _preview_yaml(selected)
