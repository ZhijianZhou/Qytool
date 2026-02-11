"""功能8: 端口转发 (访问 Ray Dashboard)"""
import subprocess
import signal
from InquirerPy import inquirer
from raytool.utils.kube import get_running_pods, group_pods_by_job, run_kubectl
from raytool.utils.ui import (
    console, select_job, select_pod, print_info, print_error, print_warning,
)


def port_forward(namespace: str, local_port: int = None, remote_port: int = 8265):
    """
    端口转发到 Ray Dashboard

    默认 Ray Dashboard 端口:
    - Ray Dashboard: 8265
    - Ray Head: 8000
    """
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    if not jobs:
        print_warning("当前没有运行中的任务")
        return

    # 选择任务
    job_name = select_job(jobs, message="请选择任务")
    if not job_name:
        return

    job_pods = jobs[job_name]

    # 找到 Head 节点
    head_pods = [p for p in job_pods if p.get("role") == "Head"]
    if not head_pods:
        # 尝试通过名称找 head
        head_pods = [p for p in job_pods if "head" in p["name"].lower()]

    if len(head_pods) == 1:
        selected_pod = head_pods[0]
    elif len(head_pods) > 1:
        pod = select_pod(head_pods, message="请选择 Head 节点")
        if not pod:
            return
        selected_pod = pod
    else:
        # 没有 head，让用户选择
        pod = select_pod(job_pods, message="请选择节点")
        if not pod:
            return
        selected_pod = pod

    pod_name = selected_pod["name"]

    # 确定本地端口
    if local_port is None:
        port_choices = [
            {"name": "Ray Dashboard (8265)", "value": 8265},
            {"name": "Ray Head API (8000)", "value": 8000},
            {"name": "自定义端口...", "value": "custom"},
        ]
        local_port = inquirer.select(
            message="请选择本地端口",
            choices=port_choices,
            pointer="❯",
        ).execute()

        if local_port == "custom":
            local_port = inquirer.number(
                message="请输入本地端口号",
                min_valid=1,
                max_valid=65535,
                default=8265,
            ).execute()

    # 执行端口转发
    _start_port_forward(pod_name, namespace, local_port, remote_port)


def _start_port_forward(pod_name: str, namespace: str, local_port: int, remote_port: int):
    """启动端口转发"""
    cmd = [
        "kubectl", "port-forward",
        f"pod/{pod_name}",
        "-n", namespace,
        f"{local_port}:{remote_port}"
    ]

    console.print()
    print_info(f"正在启动端口转发: localhost:{local_port} -> {pod_name}:{remote_port}")
    console.print("[dim]按 Ctrl+C 停止端口转发[/dim]\n")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # 等待用户中断
        proc.wait()
    except KeyboardInterrupt:
        print_info("正在停止端口转发...")
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
        console.print("[dim]端口转发已停止[/dim]")
    except FileNotFoundError:
        print_error("未找到 kubectl 命令")
