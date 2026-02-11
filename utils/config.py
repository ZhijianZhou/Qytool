"""配置文件读取模块"""
import os
import yaml
from raytool.utils.ui import console

DEFAULT_CONFIG = {
    "namespace": "ray-system",
    "yaml_dir": "./ray-job",
    "default_log_lines": 100,
    "default_shell": "/bin/bash",
}

# 按优先级查找配置文件
CONFIG_PATHS = [
    os.environ.get("RAYTOOL_CONFIG"),           # 1. 环境变量
    os.path.join(os.getcwd(), ".raytoolconfig"), # 2. 当前目录 .raytoolconfig
    os.path.expanduser("~/.raytoolconfig"),      # 3. 用户目录 .raytoolconfig
]


def _find_config_file() -> str:
    """查找配置文件，返回找到的路径或 None"""
    for path in CONFIG_PATHS:
        if path and os.path.exists(path):
            return path
    return None


def _create_default_config(config_path: str, config: dict = None):
    """创建配置文件"""
    if config is None:
        config = DEFAULT_CONFIG.copy()
        config["yaml_dir"] = os.path.join(os.getcwd(), "ray-job")
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)
    return config


def _interactive_init() -> tuple:
    """交互式引导用户创建配置，返回 (config_path, config_dict)"""
    from InquirerPy import inquirer
    from rich.panel import Panel

    console.print()
    console.print(Panel(
        "[bold cyan]欢迎使用 RayTool![/bold cyan]\n\n"
        "  首次使用，让我们快速完成初始化配置。\n"
        "  所有选项都有默认值，直接回车即可跳过。",
        title="🔧 初始化配置",
        border_style="cyan",
        padding=(1, 2),
    ))
    console.print()

    # 1. 配置文件存放位置
    default_config_path = os.path.join(os.getcwd(), ".raytoolconfig")
    config_path = inquirer.select(
        message="配置文件存放位置",
        choices=[
            {"name": f"当前目录 ({default_config_path})", "value": default_config_path},
            {"name": f"用户目录 ({os.path.expanduser('~/.raytoolconfig')})", "value": os.path.expanduser("~/.raytoolconfig")},
        ],
        default=default_config_path,
        pointer="❯",
    ).execute()

    # 2. Kubernetes 命名空间
    namespace = inquirer.text(
        message="Kubernetes 命名空间",
        default=DEFAULT_CONFIG["namespace"],
    ).execute().strip()
    if not namespace:
        namespace = DEFAULT_CONFIG["namespace"]

    # 3. YAML 任务文件目录
    default_yaml_dir = os.path.join(os.getcwd(), "ray-job")
    yaml_dir = inquirer.text(
        message="YAML 任务文件目录",
        default=default_yaml_dir,
    ).execute().strip()
    if not yaml_dir:
        yaml_dir = default_yaml_dir

    # 4. 默认日志行数
    log_lines = inquirer.number(
        message="默认查看日志行数",
        default=DEFAULT_CONFIG["default_log_lines"],
        min_allowed=10,
        max_allowed=10000,
    ).execute()
    log_lines = int(log_lines)

    # 5. 默认 shell
    default_shell = inquirer.select(
        message="默认容器 Shell",
        choices=[
            {"name": "/bin/bash", "value": "/bin/bash"},
            {"name": "/bin/sh", "value": "/bin/sh"},
            {"name": "/bin/zsh", "value": "/bin/zsh"},
        ],
        default="/bin/bash",
        pointer="❯",
    ).execute()

    config = {
        "namespace": namespace,
        "yaml_dir": yaml_dir,
        "default_log_lines": log_lines,
        "default_shell": default_shell,
    }

    # 显示配置摘要
    console.print()
    summary = "\n".join([f"  [cyan]{k}[/cyan]: [bold]{v}[/bold]" for k, v in config.items()])
    console.print(Panel(
        summary,
        title="📋 配置摘要",
        border_style="green",
        padding=(0, 2),
    ))
    console.print()

    # 写入文件
    _create_default_config(config_path, config)
    console.print(f"[green]✅ 配置已保存: {config_path}[/green]")
    console.print("[dim]后续可直接编辑该文件修改配置[/dim]")
    console.print()

    return config_path, config


def load_config(ask_if_missing: bool = True) -> dict:
    """加载配置文件"""
    config = DEFAULT_CONFIG.copy()

    # 查找配置文件
    config_path = _find_config_file()

    if config_path:
        try:
            with open(config_path, "r") as f:
                user_config = yaml.safe_load(f) or {}
            config.update(user_config)
        except Exception as e:
            print(f"⚠️  读取配置文件失败: {e}")
    elif ask_if_missing:
        # 交互式引导创建配置
        _, config = _interactive_init()
    else:
        # 不引导时使用默认值
        pass

    # 展开路径中的 ~
    if "yaml_dir" in config:
        config["yaml_dir"] = os.path.expanduser(config["yaml_dir"])
    if "kubeconfig" in config:
        config["kubeconfig"] = os.path.expanduser(config["kubeconfig"])

    return config

