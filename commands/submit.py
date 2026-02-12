"""功能4: 提交新任务"""
import os
import glob
import yaml
from InquirerPy import inquirer
from raytool.utils.kube import apply_yaml
from raytool.utils.ui import (
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
    choices.append({"name": "↩️  返回上一级", "value": "__cancel__"})

    selected = inquirer.select(
        message="主人，请选择 YAML 文件",
        choices=choices,
        pointer="❯",
    ).execute()

    if selected == "__cancel__":
        return

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

