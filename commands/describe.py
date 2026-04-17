"""功能7: 查看任务/Pod详细信息"""
from InquirerPy import inquirer
from rich.markup import escape
from raytool.utils.kube import get_pods, group_pods_by_job, run_kubectl, get_pod_role
from raytool.utils.ui import (
    console, select_job, select_pod, print_info, print_error, print_warning,
)
from rich.table import Table
from rich.panel import Panel
from rich.text import Text


def describe_job(namespace: str, pod_name: str = None):
    """交互式查看任务/Pod详细信息"""
    # 如果指定了 pod_name，直接查看
    if pod_name:
        _display_pod_details({"name": pod_name}, namespace)
        return

    pods = get_pods(namespace)
    if not pods:
        print_warning("当前没有任何 Pod")
        return

    # 先按任务分组
    running_pods = [p for p in pods if p["status"] == "Running"]
    all_pods = group_pods_by_job(pods)

    # 合并 Running 和非 Running 的 pods
    jobs = {}
    for job_name, job_pods in all_pods.items():
        jobs[job_name] = job_pods

    # 选择要查看的任务
    job_name = select_job(jobs, message="请选择要查看详情的任务")
    if not job_name:
        return

    job_pods = jobs[job_name]

    # 选择要查看的 Pod
    pod = select_pod(job_pods, message="请选择要查看的节点")
    if not pod:
        return

    _display_pod_details(pod, namespace)


def _display_pod_details(pod: dict, namespace: str):
    """显示 Pod 详细信息"""
    pod_name = pod["name"]
    console.clear()

    # 获取 Pod 详情
    rc, stdout, stderr = run_kubectl(
        ["describe", "pod", pod_name],
        namespace,
        timeout=30
    )

    if rc != 0:
        print_error(f"获取详情失败: {stderr}")
        return

    console.print(Panel(
        Text(stdout, style="dim"),
        title=f"📋 Pod 详情: {escape(pod_name)}",
        border_style="cyan",
        padding=(0, 1)
    ))


def describe_pod_yaml(namespace: str, pod_name: str = None):
    """直接查看 Pod YAML 配置"""
    if not pod_name:
        pods = get_pods(namespace)
        if not pods:
            print_warning("当前没有任何 Pod")
            return

        pod = select_pod(pods, message="请选择要查看 YAML 的 Pod")
        if not pod:
            return
        pod_name = pod["name"]

    rc, stdout, stderr = run_kubectl(
        ["get", "pod", pod_name, "-o", "yaml"],
        namespace,
        timeout=30
    )

    if rc != 0:
        print_error(f"获取 YAML 失败: {stderr}")
        return

    from rich.syntax import Syntax
    syntax = Syntax(stdout, "yaml", theme="monokai", line_numbers=True)
    console.print(Panel(
        syntax,
        title=f"📄 YAML: {escape(pod_name)}",
        border_style="cyan"
    ))
