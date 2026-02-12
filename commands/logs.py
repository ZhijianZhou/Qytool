"""功能3: 查看任务日志"""
from InquirerPy import inquirer
from raytool.utils.kube import get_running_pods, group_pods_by_job, run_kubectl, run_kubectl_stream
from raytool.utils.ui import (
    console, select_job, select_pod, select_container,
    print_info, print_error, print_warning, STATUS_COLORS,
)


def view_logs(namespace: str, default_lines: int = 100, job_name: str = None, pod_name: str = None):
    """交互式查看任务日志"""
    # 步骤1: 获取运行中的任务
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    # 如果指定了 pod_name，直接定位
    if pod_name:
        target_pod = next((p for p in pods if p["name"] == pod_name), None)
        if not target_pod:
            print_error(f"未找到 Pod: {pod_name}")
            return
        pod = target_pod
        container = select_container(pod["containers"], message="请选择容器")
        if container is None and len(pod["containers"]) > 1:
            return
    else:
        # 如果指定了 job_name，跳过任务选择
        if job_name and job_name in jobs:
            selected_job = job_name
        else:
            selected_job = select_job(jobs, message="请选择要查看日志的任务")
        if not selected_job:
            return

        # 步骤2: 选择节点
        job_pods = jobs[selected_job]
        pod = select_pod(job_pods, message="请选择节点")
        if not pod:
            return

        # 步骤3: 选择容器（如果有多个）
        container = select_container(pod["containers"], message="请选择容器")
        if container is None and len(pod["containers"]) > 1:
            return

    # 步骤4: 选择日志模式
    mode = inquirer.select(
        message="主人，请选择日志查看模式",
        choices=[
            {"name": f"最后 {default_lines} 行", "value": ("tail", default_lines)},
            {"name": "最后 500 行", "value": ("tail", 500)},
            {"name": "最后 1000 行", "value": ("tail", 1000)},
            {"name": "实时追踪 (follow)", "value": ("follow", None)},
            {"name": "全部日志", "value": ("all", None)},
            {"name": "↩️  返回上一级", "value": ("cancel", None)},
        ],
        pointer="❯",
    ).execute()

    mode_type, mode_value = mode
    if mode_type == "cancel":
        return

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

