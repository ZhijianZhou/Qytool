"""功能6: 进入容器终端"""
from raytool.utils.kube import get_running_pods, group_pods_by_job, exec_into_pod
from raytool.utils.ui import (
    console, select_job, select_pod, select_container,
    print_info, print_error, print_warning,
)


def shell_into_pod(namespace: str, default_shell: str = "/bin/bash", pod_name: str = None):
    """交互式选择并进入容器终端"""
    pods = get_running_pods(namespace)

    # 如果指定了 pod_name，直接定位
    if pod_name:
        target_pod = next((p for p in pods if p["name"] == pod_name), None)
        if not target_pod:
            print_error(f"未找到 Pod: {pod_name}")
            return
        pod = target_pod
    else:
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

