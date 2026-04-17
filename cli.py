#!/usr/bin/env python3
"""
RayTool — Ray 集群任务管理命令行工具
用法:
    python -m raytool         # 交互式主菜单
    python -m raytool watch    # 直接执行子命令
"""
import os

import click
from raytool.utils.ui import console, print_banner, print_error, print_success, ESC_KEYBINDING

# 延迟加载配置，避免 import 阶段触发交互式引导
_config = None

def _get_config():
    global _config
    if _config is None:
        from raytool.utils.config import load_config
        _config = load_config()
    return _config


# ──────────────────────── click 命令组 ────────────────────────

@click.group(invoke_without_command=True)
@click.option("--namespace", "-n", default=None, help="覆盖默认命名空间")
@click.option("--kubeconfig", default=None, help="指定 kubeconfig 路径")
@click.option("--user", "-u", default=None, help="快速登录用户名，如: raytool -u dexterzhou")
@click.pass_context
def cli(ctx, namespace, kubeconfig, user):
    """🚀 RayTool — Ray 集群任务管理工具"""
    config = _get_config()
    ctx.ensure_object(dict)
    ctx.obj["namespace"] = namespace or config["namespace"]
    ctx.obj["config"] = config

    if kubeconfig:
        os.environ["KUBECONFIG"] = kubeconfig

    # 没有子命令时进入交互式主菜单
    if ctx.invoked_subcommand is None:
        interactive_menu(ctx.obj["namespace"], config, quick_user=user)


@cli.command("watch")
@click.pass_context
def cmd_watch(ctx):
    """📋 监控 Pods 状态"""
    from raytool.commands.watch import watch_pods
    watch_pods(ctx.obj["namespace"])


@cli.command("list")
@click.pass_context
def cmd_list(ctx):
    """📃 查看所有任务列表 (含 Pending/Failed)"""
    from raytool.commands.list_jobs import list_jobs
    list_jobs(ctx.obj["namespace"], config=ctx.obj["config"])


@cli.command("status")
@click.pass_context
def cmd_status(ctx):
    """📊 集群概况总览"""
    from raytool.commands.status import cluster_status
    cluster_status(ctx.obj["namespace"], config=ctx.obj["config"])


@cli.command("logs")
@click.argument("job_name", required=False)
@click.argument("pod_name", required=False)
@click.pass_context
def cmd_logs(ctx, job_name, pod_name):
    """📜 查看任务日志"""
    from raytool.commands.logs import view_logs
    config = ctx.obj["config"]
    view_logs(ctx.obj["namespace"], config["default_log_lines"], job_name=job_name, pod_name=pod_name)


@cli.command("submit")
@click.argument("yaml_path", required=False)
@click.option("--user", "-u", default=None, help="操作用户名")
@click.pass_context
def cmd_submit(ctx, yaml_path, user):
    """🚀 提交新任务"""
    from raytool.commands.submit import submit_job
    config = ctx.obj["config"]
    submit_job(ctx.obj["namespace"], config["yaml_dir"], yaml_path, current_user=user, config=config)


@cli.command("delete")
@click.argument("yaml_path", required=False)
@click.option("--user", "-u", default=None, help="操作用户名")
@click.pass_context
def cmd_delete(ctx, yaml_path, user):
    """🗑️  删除任务"""
    from raytool.commands.delete import delete_jobs
    config = ctx.obj["config"]
    delete_jobs(ctx.obj["namespace"], yaml_path, current_user=user, config=config)


@cli.command("exec")
@click.argument("pod_name", required=False)
@click.pass_context
def cmd_exec(ctx, pod_name):
    """🖥️  进入容器终端"""
    from raytool.commands.shell import shell_into_pod
    config = ctx.obj["config"]
    shell_into_pod(ctx.obj["namespace"], config["default_shell"], pod_name=pod_name)


@cli.command("describe")
@click.argument("pod_name", required=False)
@click.pass_context
def cmd_describe(ctx, pod_name):
    """📋 查看任务/Pod详细信息"""
    from raytool.commands.describe import describe_job
    describe_job(ctx.obj["namespace"], pod_name=pod_name)


@cli.command("port-forward")
@click.argument("local_port", type=int, required=False)
@click.argument("remote_port", type=int, default=8265, required=False)
@click.pass_context
def cmd_port_forward(ctx, local_port, remote_port):
    """🔌 端口转发 (访问 Ray Dashboard)"""
    from raytool.commands.port_forward import port_forward
    config = ctx.obj["config"]
    if remote_port == 8265:
        remote_port = config.get("default_remote_port", 8265)
    port_forward(ctx.obj["namespace"], local_port, remote_port)


@cli.command("scale")
@click.argument("worker_count", type=int, required=False)
@click.pass_context
def cmd_scale(ctx, worker_count):
    """📏 扩缩容 Ray 集群"""
    from raytool.commands.scale import scale_job
    scale_job(ctx.obj["namespace"])


@cli.command("occupy")
@click.option("--name", "-N", default=None, help="占卡任务名称 (如: glm5, qwen3-sft)")
@click.option("--gpus", "-g", type=int, default=None, help="指定占用 GPU 数量 (如: 256)")
@click.pass_context
def cmd_occupy(ctx, name, gpus):
    """🔥 GPU 占卡 (查询空闲节点并提交占卡任务)"""
    from raytool.commands.occupy import occupy_gpus
    occupy_gpus(ctx.obj["namespace"], config=ctx.obj["config"], custom_name=name, custom_gpus=gpus)


@cli.command("map")
@click.pass_context
def cmd_map(ctx):
    """🗺️  节点-Job 双向查询 (节点→Job / Job→节点)"""
    from raytool.commands.node_job_map import node_job_map
    node_job_map(ctx.obj["namespace"])


@cli.command("cordon")
@click.pass_context
def cmd_cordon(ctx):
    """🛡️  节点调度管理 (cordon/uncordon 禁止/恢复调度)"""
    from raytool.commands.cordon import manage_cordon
    manage_cordon(ctx.obj["namespace"])


@cli.command("nodes")
@click.pass_context
def cmd_nodes(ctx):
    """🌐 节点信息 (按 AZ/实例类型查看节点与 Pod 分布)"""
    from raytool.commands.nodes import nodes_info
    nodes_info(ctx.obj["namespace"])


@cli.command("prewarm")
@click.pass_context
def cmd_prewarm(ctx):
    """🔥 镜像预热 (管理 prewarm 目录下的预热 YAML)"""
    from raytool.commands.prewarm import prewarm_images
    prewarm_images(ctx.obj["namespace"], ctx.obj["config"])


@cli.command("admin")
@click.pass_context
def cmd_admin(ctx):
    """🔑 管理员模式 (查看/删除任意 PyTorchJob)"""
    from raytool.commands.admin import admin_mode
    admin_mode(ctx.obj["namespace"])


# ──────────────────────── 菜单选择辅助 ────────────────────────

def _select_menu(message, choices):
    """主菜单选择"""
    from InquirerPy import inquirer

    return inquirer.fuzzy(
        message=f"主人，{message}" if not message.startswith("主人") else message,
        choices=choices,
        pointer="❯",
        border=True,
        info=False,
        prompt="🔍 ",
        match_exact=False,
        max_height="70%",
        keybindings=ESC_KEYBINDING,
    ).execute()


def _wait_for_enter():
    """按回车返回主菜单"""
    from InquirerPy import inquirer
    inquirer.text(message="主人，按回车键返回主菜单...").execute()


# ──────────────────────── Ray-Job 目录检查 ────────────────────────

def _ensure_yaml_dir(config, store, current_user):
    """检查 ray-job 目录是否设置，未设置则引导用户配置。返回 (config, yaml_dir)。"""
    import os
    from InquirerPy import inquirer
    from raytool.utils.ui import print_warning

    yaml_dir = config.get("yaml_dir", "")
    if yaml_dir:
        expanded = os.path.expanduser(yaml_dir)
        if os.path.isdir(expanded):
            return config, yaml_dir

    # 需要设置
    console.print()
    print_warning("尚未设置 Ray-Job YAML 目录，请先设置你的任务文件目录")
    console.print("[dim]该目录用于存放你的 Ray 任务 YAML 文件，提交任务时会从中选择[/dim]\n")

    while True:
        new_dir = inquirer.text(
            message="主人，请输入你的 Ray-Job YAML 目录路径:",
            default="",
        ).execute().strip()

        if not new_dir:
            print_warning("目录路径不能为空，请重新输入")
            continue

        expanded = os.path.expanduser(new_dir)
        if not os.path.isdir(expanded):
            create = inquirer.confirm(
                message=f"目录 {expanded} 不存在，是否创建？",
                default=True,
            ).execute()
            if create:
                try:
                    os.makedirs(expanded, exist_ok=True)
                    print_success(f"目录已创建: {expanded}")
                except OSError as e:
                    print_error(f"创建目录失败: {e}")
                    continue
            else:
                continue

        # 保存到用户偏好
        store.update_user_config(current_user, "yaml_dir", new_dir)
        config["yaml_dir"] = new_dir
        print_success(f"Ray-Job 目录已设置: {new_dir}")
        return config, new_dir


# ──────────────────────── 用户登录 ────────────────────────

def _login(store):
    """用户登录：从预置列表中选择用户名。"""
    from InquirerPy import inquirer

    console.print("\n[bold]请登录[/bold]")

    preset_users = store.preset_users
    preset_usernames = {u for u, _ in preset_users}

    users_data = store.list_users()
    choices = []
    for username, display_name in preset_users:
        if username in users_data:
            choices.append({"name": f"{username} ({display_name})", "value": username})
    for username, info in users_data.items():
        if username not in preset_usernames:
            choices.append({"name": f"{username} ({info.get('display_name', '')})", "value": username})

    if not choices:
        console.print("[red]无可用用户[/red]")
        return None

    try:
        username = inquirer.fuzzy(
            message="选择你的用户名:",
            choices=choices,
            pointer="❯",
            border=True,
            keybindings=ESC_KEYBINDING,
        ).execute()
    except KeyboardInterrupt:
        return None

    if not username:
        return None

    display_name = store.get_display_name(username)
    print_success(f"欢迎, {display_name} ({username})！")
    return username


# ──────────────────────── 交互式主菜单 ────────────────────────

def interactive_menu(namespace: str, config: dict = None, quick_user: str = None):
    """交互式主菜单循环"""
    from InquirerPy import inquirer
    from raytool.utils.user_store import UserStore
    from raytool.utils.audit import AuditLogger

    if config is None:
        config = _get_config()

    store = UserStore(config["data_dir"])
    audit = AuditLogger(config["data_dir"])

    # ── 登录 ──
    console.clear()
    print_banner()

    if quick_user:
        # 快速登录：验证用户是否存在
        user_info = store.get_user(quick_user)
        if user_info:
            current_user = quick_user
            display_name = store.get_display_name(current_user)
            print_success(f"快速登录: {display_name} ({current_user})")
        else:
            print_error(f"用户 '{quick_user}' 不存在，请从列表中选择")
            current_user = _login(store)
    else:
        current_user = _login(store)

    if not current_user:
        console.print("[dim]再见！[/dim]")
        return

    audit.log(current_user, "login", "interactive_menu")

    # 加载用户个性化配置覆盖全局配置
    user_yaml_dir = store.get_user_config(current_user, "yaml_dir")
    if user_yaml_dir:
        config["yaml_dir"] = user_yaml_dir
    user_prewarm_dir = store.get_user_config(current_user, "prewarm_dir")
    if user_prewarm_dir:
        config["prewarm_dir"] = user_prewarm_dir
    user_log_lines = store.get_user_config(current_user, "default_log_lines")
    if user_log_lines:
        config["default_log_lines"] = int(user_log_lines)

    # 检查 ray-job 目录是否已设置
    config, _ = _ensure_yaml_dir(config, store, current_user)

    while True:
        console.clear()
        print_banner()
        display_name = store.get_display_name(current_user)
        is_admin = store.is_admin(current_user)
        role_tag = " [magenta](管理员)[/magenta]" if is_admin else ""
        console.print(f"[dim]当前用户: [cyan]{current_user}[/cyan] ({display_name}){role_tag}[/dim]")
        console.print(f"[dim]命名空间: {namespace}[/dim]")
        console.print(f"[dim]Ray-Job 目录: {config['yaml_dir']}[/dim]\n")

        try:
            action = _select_menu(
                message="主人，请选择操作 (↑↓选择 / 输入数字或关键词搜索)",
                choices=[
                    {"name": " 1. 📊 集群概况总览", "value": "status"},
                    {"name": " 2. 📋 监控 Pods 状态", "value": "watch"},
                    {"name": " 3. 📃 查看所有任务列表", "value": "list"},
                    {"name": " 4. 📜 查看任务日志", "value": "logs"},
                    {"name": " 5. 🚀 提交新任务", "value": "submit"},
                    {"name": " 6. 🗑️  删除任务", "value": "delete"},
                    {"name": " 7. 🖥️  进入容器终端", "value": "exec"},
                    {"name": " 8. 📋 查看任务详情", "value": "describe"},
                    {"name": " 9. 📏 扩缩容集群", "value": "scale"},
                    {"name": "10. 🔌 端口转发 (Dashboard)", "value": "port-forward"},
                    {"name": "11. 🔥 GPU 占卡", "value": "occupy"},
                    {"name": "12. 🗺️  节点-Job 映射查询", "value": "map"},
                    {"name": "13. 🛡️  节点调度管理 (禁止/恢复调度)", "value": "cordon"},
                    {"name": "14. 🌐 节点信息 (AZ/实例类型/Pod分布)", "value": "nodes"},
                    {"name": "15. 🔥 镜像预热 (管理预热YAML)", "value": "prewarm"},
                    {"name": "16. 🔑 管理员模式 (删除任意PyTorchJob)", "value": "admin"},
                    {"name": "17. 📦 历史任务日志 (已删除任务的日志)", "value": "history_logs"},
                    {"name": "18. 👤 用户管理", "value": "user"},
                    {"name": "19. 🔄 切换用户", "value": "switch"},
                    {"name": " 0. ❌ 退出", "value": "quit"},
                ],
            )
        except (KeyboardInterrupt, EOFError):
            _exit_gracefully()
            return

        if action == "quit":
            _exit_gracefully()
            return

        # ESC 键在主菜单 → 视为退出
        if action is None:
            _exit_gracefully()
            return

        if action == "switch":
            new_user = _login(store)
            if new_user:
                audit.log(current_user, "switch_user", f"{current_user} -> {new_user}")
                current_user = new_user
                # 重新加载用户偏好
                user_yaml_dir = store.get_user_config(current_user, "yaml_dir")
                if user_yaml_dir:
                    config["yaml_dir"] = user_yaml_dir
                else:
                    config["yaml_dir"] = ""
                user_prewarm_dir = store.get_user_config(current_user, "prewarm_dir")
                if user_prewarm_dir:
                    config["prewarm_dir"] = user_prewarm_dir
                else:
                    config["prewarm_dir"] = ""
                user_log_lines = store.get_user_config(current_user, "default_log_lines")
                if user_log_lines:
                    config["default_log_lines"] = int(user_log_lines)
                # 检查 ray-job 目录
                config, _ = _ensure_yaml_dir(config, store, current_user)
            continue

        console.print()

        try:
            if action == "status":
                from raytool.commands.status import cluster_status
                cluster_status(namespace, config=config, current_user=current_user)
            elif action == "watch":
                from raytool.commands.watch import watch_pods
                watch_pods(namespace)
            elif action == "list":
                from raytool.commands.list_jobs import list_jobs
                list_jobs(namespace, config=config)
            elif action == "logs":
                from raytool.commands.logs import view_logs
                view_logs(namespace, config["default_log_lines"])
            elif action == "submit":
                from raytool.commands.submit import submit_job
                submit_job(namespace, config["yaml_dir"], current_user=current_user, config=config)
            elif action == "delete":
                from raytool.commands.delete import delete_jobs
                delete_jobs(namespace, current_user=current_user, config=config)
            elif action == "exec":
                from raytool.commands.shell import shell_into_pod
                shell_into_pod(namespace, config["default_shell"])
            elif action == "describe":
                from raytool.commands.describe import describe_job
                describe_job(namespace)
            elif action == "port-forward":
                from raytool.commands.port_forward import port_forward
                port_forward(namespace, remote_port=config.get("default_remote_port", 8265))
            elif action == "scale":
                from raytool.commands.scale import scale_job
                scale_job(namespace)
            elif action == "occupy":
                from raytool.commands.occupy import occupy_gpus
                occupy_gpus(namespace, config=config)
            elif action == "map":
                from raytool.commands.node_job_map import node_job_map
                node_job_map(namespace)
            elif action == "cordon":
                from raytool.commands.cordon import manage_cordon
                manage_cordon(namespace)
            elif action == "nodes":
                from raytool.commands.nodes import nodes_info
                nodes_info(namespace)
            elif action == "prewarm":
                from raytool.commands.prewarm import prewarm_images
                prewarm_images(namespace, config)
            elif action == "admin":
                from raytool.commands.admin import admin_mode
                admin_mode(namespace)
            elif action == "history_logs":
                from raytool.commands.history_logs import view_history_logs
                view_history_logs(config, current_user)
            elif action == "user":
                from raytool.commands.user import user_cmd
                user_cmd(config, current_user)
        except KeyboardInterrupt:
            console.print("\n[dim]操作已中断[/dim]")
        except Exception as e:
            print_error(f"执行出错: {e}")

        # 操作完成后显示毒鸡汤 + 等待用户按键返回主菜单（也带屏保超时）
        console.print()
        try:
            from raytool.utils.fun import show_fortune
            show_fortune()
        except Exception:
            pass
        console.print()
        try:
            _wait_for_enter()
        except (KeyboardInterrupt, EOFError):
            _exit_gracefully()
            return


def _exit_gracefully():
    console.print("\n[cyan]👋 主人再见！[/cyan]")


# ──────────────────────── 入口 ────────────────────────

if __name__ == "__main__":
    cli()
