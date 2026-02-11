#!/usr/bin/env python3
"""
RayTool — Ray 集群任务管理命令行工具
用法:
    python -m raytool         # 交互式主菜单
    python -m raytool watch    # 直接执行子命令
"""
import sys
import os

import click
from raytool.utils.ui import console, print_banner, print_error

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
@click.pass_context
def cli(ctx, namespace, kubeconfig):
    """🚀 RayTool — Ray 集群任务管理工具"""
    config = _get_config()
    ctx.ensure_object(dict)
    ctx.obj["namespace"] = namespace or config["namespace"]
    ctx.obj["config"] = config

    if kubeconfig:
        os.environ["KUBECONFIG"] = kubeconfig

    # 没有子命令时进入交互式主菜单
    if ctx.invoked_subcommand is None:
        interactive_menu(ctx.obj["namespace"], config)


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
    list_jobs(ctx.obj["namespace"])


@cli.command("status")
@click.pass_context
def cmd_status(ctx):
    """📊 集群概况总览"""
    from raytool.commands.status import cluster_status
    cluster_status(ctx.obj["namespace"])


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
@click.pass_context
def cmd_submit(ctx, yaml_path):
    """🚀 提交新任务"""
    from raytool.commands.submit import submit_job
    config = ctx.obj["config"]
    submit_job(ctx.obj["namespace"], config["yaml_dir"], yaml_path)


@cli.command("delete")
@click.argument("yaml_path", required=False)
@click.pass_context
def cmd_delete(ctx, yaml_path):
    """🗑️  删除任务"""
    from raytool.commands.delete import delete_jobs
    delete_jobs(ctx.obj["namespace"], yaml_path)


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
    port_forward(ctx.obj["namespace"], local_port, remote_port)


@cli.command("scale")
@click.argument("worker_count", type=int, required=False)
@click.pass_context
def cmd_scale(ctx, worker_count):
    """📏 扩缩容 Ray 集群"""
    from raytool.commands.scale import scale_job
    scale_job(ctx.obj["namespace"])


@cli.command("occupy")
@click.pass_context
def cmd_occupy(ctx):
    """🔥 GPU 占卡 (查询空闲节点并提交占卡任务)"""
    from raytool.commands.occupy import occupy_gpus
    occupy_gpus(ctx.obj["namespace"])


# ──────────────────────── 交互式主菜单 ────────────────────────

def interactive_menu(namespace: str, config: dict = None):
    """交互式主菜单循环"""
    from InquirerPy import inquirer

    if config is None:
        config = _get_config()

    while True:
        console.clear()
        print_banner()
        console.print(f"[dim]命名空间: {namespace}[/dim]\n")

        try:
            action = inquirer.select(
                message="主人，请选择操作",
                choices=[
                    {"name": "📊 集群概况总览", "value": "status"},
                    {"name": "📋 监控 Pods 状态", "value": "watch"},
                    {"name": "📃 查看所有任务列表", "value": "list"},
                    {"name": "📜 查看任务日志", "value": "logs"},
                    {"name": "🚀 提交新任务", "value": "submit"},
                    {"name": "🗑️  删除任务", "value": "delete"},
                    {"name": "🖥️  进入容器终端", "value": "exec"},
                    {"name": "📋 查看任务详情", "value": "describe"},
                    {"name": "📏 扩缩容集群", "value": "scale"},
                    {"name": "🔌 端口转发 (Dashboard)", "value": "port-forward"},
                    {"name": "🔥 GPU 占卡", "value": "occupy"},
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
            if action == "status":
                from raytool.commands.status import cluster_status
                cluster_status(namespace)
            elif action == "watch":
                from raytool.commands.watch import watch_pods
                watch_pods(namespace)
            elif action == "list":
                from raytool.commands.list_jobs import list_jobs
                list_jobs(namespace)
            elif action == "logs":
                from raytool.commands.logs import view_logs
                view_logs(namespace, config["default_log_lines"])
            elif action == "submit":
                from raytool.commands.submit import submit_job
                submit_job(namespace, config["yaml_dir"])
            elif action == "delete":
                from raytool.commands.delete import delete_jobs
                delete_jobs(namespace)
            elif action == "exec":
                from raytool.commands.shell import shell_into_pod
                shell_into_pod(namespace, config["default_shell"])
            elif action == "describe":
                from raytool.commands.describe import describe_job
                describe_job(namespace)
            elif action == "port-forward":
                from raytool.commands.port_forward import port_forward
                port_forward(namespace)
            elif action == "scale":
                from raytool.commands.scale import scale_job
                scale_job(namespace)
            elif action == "occupy":
                from raytool.commands.occupy import occupy_gpus
                occupy_gpus(namespace)
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
