"""配置文件读取模块

配置加载优先级（从高到低）:
    1. RAYTOOL_CONFIG 环境变量指定的文件
    2. 共享数据目录中的全局配置: {data_dir}/raytool_global_config.yaml
    3. 当前目录的 .raytoolconfig（本地覆盖，向后兼容）
    4. 用户目录的 ~/.raytoolconfig（向后兼容）
    5. 内置 DEFAULT_CONFIG 默认值

设计理念:
    - 安装时全局配置写入共享 data_dir，所有用户共享
    - 个人偏好（yaml_dir / prewarm_dir 等）跟随用户，存储在 raytool_users.json
    - .raytoolconfig 仅作为可选的本地覆盖，不再是必需品
"""
import logging
import os
from typing import Optional

import yaml

from raytool.utils.ui import console

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "namespace": "ray-system",
    "yaml_dir": "",
    "prewarm_dir": "",
    "default_log_lines": 100,
    "default_shell": "/bin/bash",
    "data_dir": "/mnt/fsx-c/youtu-agent/youtu",
    "admin_password": "3.1415926",
    # occupy
    "occupy_image": "054486717055.dkr.ecr.ap-southeast-3.amazonaws.com/youtu-agent:slime0401.h200",
    "occupy_conda_env": "agent-lightning",
    "occupy_pvc_name": "fsx-claim",
    "occupy_fsx_subpath": "youtu-agent/zhijianzhou",
    "occupy_cpu_request": 64,
    "occupy_batch_size": 4,
    "occupy_task_types": ["retool", "search", "swebench"],
    "occupy_model_names": ["qwen3", "qwen25"],
    "occupy_host_local": "/mnt/k8s-disks/0",
    "occupy_host_cache": "/opt/dlami/nvme/.cache",
    "occupy_host_checkpoints": "/opt/dlami/nvme/checkpoints/",
    # prewarm
    "pause_image": "gcr.io/google_containers/pause:3.2",
    "instance_types": [
        "ml.p5en.48xlarge",
        "ml.p5e.48xlarge",
        "ml.p5.48xlarge",
        "ml.p4d.24xlarge",
        "ml.p4de.24xlarge",
        "ml.g5.48xlarge",
    ],
    # port-forward
    "default_remote_port": 8265,
}

# 全局配置文件名（存放在 data_dir 中）
GLOBAL_CONFIG_FILENAME = "raytool_global_config.yaml"


def _get_global_config_path(data_dir: str) -> str:
    """获取共享目录中全局配置文件的路径。

    Args:
        data_dir: 共享数据目录路径。

    Returns:
        全局配置文件的完整路径。
    """
    return os.path.join(data_dir, GLOBAL_CONFIG_FILENAME)


def _find_config_file() -> Optional[str]:
    """按优先级查找配置文件，返回找到的路径或 None。

    搜索顺序:
        1. RAYTOOL_CONFIG 环境变量
        2. {data_dir}/raytool_global_config.yaml（共享全局配置）
        3. $(pwd)/.raytoolconfig（本地覆盖）
        4. ~/.raytoolconfig（用户目录，向后兼容）
    """
    search_paths = [
        os.environ.get("RAYTOOL_CONFIG"),
        _get_global_config_path(DEFAULT_CONFIG["data_dir"]),
        os.path.join(os.getcwd(), ".raytoolconfig"),
        os.path.expanduser("~/.raytoolconfig"),
    ]
    for path in search_paths:
        if path and os.path.exists(path):
            return path
    return None


def _save_config(config_path: str, config: dict) -> None:
    """将配置写入 YAML 文件。

    Args:
        config_path: 配置文件路径。
        config: 配置字典。
    """
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, indent=2, allow_unicode=True)


def _interactive_init() -> tuple[str, dict]:
    """交互式引导用户创建全局配置。

    配置文件默认写入共享数据目录 {data_dir}/raytool_global_config.yaml，
    这样安装一次后所有用户都可以直接使用，无需每人维护一份 .raytoolconfig。

    Returns:
        (config_path, config_dict) 元组。
    """
    from InquirerPy import inquirer
    from rich.panel import Panel

    console.print()
    console.print(Panel(
        "[bold cyan]欢迎使用 RayTool![/bold cyan]\n\n"
        "  首次使用，让我们快速完成初始化配置。\n"
        "  配置将保存到共享目录，团队成员无需重复配置。\n"
        "  所有选项都有默认值，直接回车即可跳过。",
        title="🔧 初始化配置",
        border_style="cyan",
        padding=(1, 2),
    ))
    console.print()

    # 1. 数据存储目录（决定全局配置文件的存放位置）
    data_dir = inquirer.text(
        message="共享数据目录 (团队共享的存储路径)",
        default=DEFAULT_CONFIG["data_dir"],
    ).execute().strip()
    if not data_dir:
        data_dir = DEFAULT_CONFIG["data_dir"]
    data_dir = os.path.expanduser(data_dir)

    # 2. Kubernetes 命名空间
    namespace = inquirer.text(
        message="Kubernetes 命名空间",
        default=DEFAULT_CONFIG["namespace"],
    ).execute().strip()
    if not namespace:
        namespace = DEFAULT_CONFIG["namespace"]

    # 3. 默认日志行数
    log_lines = inquirer.number(
        message="默认查看日志行数",
        default=DEFAULT_CONFIG["default_log_lines"],
        min_allowed=10,
        max_allowed=10000,
    ).execute()
    log_lines = int(log_lines)

    # 4. 默认 shell
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
        "yaml_dir": "",
        "prewarm_dir": "",
        "default_log_lines": log_lines,
        "default_shell": default_shell,
        "data_dir": data_dir,
        "admin_password": DEFAULT_CONFIG["admin_password"],
    }

    # 全局配置文件路径
    config_path = _get_global_config_path(data_dir)

    # 显示配置摘要
    console.print()
    display_items = {k: v for k, v in config.items() if k != "admin_password"}
    summary = "\n".join([
        f"  [cyan]{k}[/cyan]: [bold]{v}[/bold]" for k, v in display_items.items()
    ])
    summary += f"\n\n  [dim]配置文件: {config_path}[/dim]"
    console.print(Panel(
        summary,
        title="📋 配置摘要",
        border_style="green",
        padding=(0, 2),
    ))
    console.print()

    # 确保数据目录存在并写入配置
    try:
        os.makedirs(data_dir, exist_ok=True)
        _save_config(config_path, config)
        from rich.markup import escape as _esc
        console.print(f"[green]✅ 全局配置已保存: {_esc(str(config_path))}[/green]")
        console.print("[dim]团队成员无需重复配置，直接登录即可使用[/dim]")
    except (PermissionError, OSError) as e:
        # 共享目录不可写，data_dir 和配置文件都回退到本地
        logger.warning(f"无法写入共享目录 {data_dir}: {e}")
        local_data_dir = os.path.expanduser("~/.raytool_data")
        os.makedirs(local_data_dir, exist_ok=True)
        config["data_dir"] = local_data_dir
        config_path = os.path.expanduser("~/.raytoolconfig")
        _save_config(config_path, config)
        from rich.markup import escape as _esc
        console.print(f"[yellow]⚠️  共享目录不可用 ({data_dir})，已回退到本地[/yellow]")
        console.print(f"[green]✅ 数据目录: {local_data_dir}[/green]")
        console.print(f"[green]✅ 配置已保存: {_esc(str(config_path))}[/green]")
        console.print("[dim]后续如果共享目录可用，可重新运行 bash install.sh 迁移配置[/dim]")

    console.print("[dim]个人偏好 (yaml_dir / prewarm_dir) 登录后在「用户管理 → 个人设置」中配置[/dim]")
    console.print()

    return config_path, config


def load_config(ask_if_missing: bool = True) -> dict:
    """加载配置文件。

    加载优先级: 环境变量 > 全局配置 > 本地 .raytoolconfig > 内置默认值。
    如果找不到任何配置且 ask_if_missing=True，则触发交互式初始化。

    Args:
        ask_if_missing: 找不到配置文件时是否交互式引导创建。

    Returns:
        合并后的配置字典。
    """
    config = DEFAULT_CONFIG.copy()

    # 查找配置文件
    config_path = _find_config_file()

    if config_path:
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                file_config = yaml.safe_load(f) or {}
            config.update(file_config)
            logger.debug(f"已加载配置: {config_path}")
        except Exception as e:
            logger.warning(f"读取配置文件失败: {config_path}: {e}")

        # 如果加载的是全局配置，再检查是否有本地 .raytoolconfig 覆盖
        global_path = _get_global_config_path(config.get("data_dir", DEFAULT_CONFIG["data_dir"]))
        if config_path == global_path:
            local_overrides = [
                os.path.join(os.getcwd(), ".raytoolconfig"),
                os.path.expanduser("~/.raytoolconfig"),
            ]
            for local_path in local_overrides:
                if os.path.exists(local_path):
                    try:
                        with open(local_path, "r", encoding="utf-8") as f:
                            local_config = yaml.safe_load(f) or {}
                        config.update(local_config)
                        logger.debug(f"已加载本地覆盖配置: {local_path}")
                    except Exception as e:
                        logger.warning(f"读取本地覆盖配置失败: {local_path}: {e}")
                    break  # 只加载第一个找到的本地覆盖
    elif ask_if_missing:
        # 交互式引导创建全局配置
        _, config = _interactive_init()
    else:
        # 不引导时使用默认值
        pass

    # 展开路径中的 ~
    for key in ("yaml_dir", "prewarm_dir", "kubeconfig", "data_dir"):
        if key in config and config[key]:
            config[key] = os.path.expanduser(config[key])

    # 验证 data_dir 可达性：尝试创建，失败时自动回退到本地
    data_dir = config.get("data_dir", "")
    if data_dir and not os.path.isdir(data_dir):
        try:
            os.makedirs(data_dir, exist_ok=True)
        except (PermissionError, OSError) as e:
            logger.warning(f"共享数据目录不可用 {data_dir}: {e}，回退到本地目录")
            local_data_dir = os.path.expanduser("~/.raytool_data")
            os.makedirs(local_data_dir, exist_ok=True)
            config["data_dir"] = local_data_dir
            console.print(
                f"\n[yellow]⚠️  共享数据目录不可用: {data_dir}[/yellow]\n"
                f"[green]   已自动回退到本地: {local_data_dir}[/green]\n"
                f"[dim]   如需恢复共享目录，请重新运行 bash install.sh[/dim]\n"
            )

    return config

