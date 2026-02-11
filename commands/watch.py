"""功能1: 监控 Pods 状态 (kubectl get pods -n ray-system -w)"""
import sys
import signal
from raytool.utils.kube import run_kubectl_stream
from raytool.utils.ui import console, print_info, print_error, colorize_status, STATUS_COLORS


def watch_pods(namespace: str):
    """实时监控 Pod 状态变化"""
    print_info(f"正在监控 {namespace} 命名空间的 Pods... (Ctrl+C 退出)")
    console.print()

    proc = run_kubectl_stream(["get", "pods", "-w"], namespace)
    if proc is None:
        print_error("无法执行 kubectl 命令")
        return

    try:
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            # 为状态关键字上色
            colored = line
            for status, color in STATUS_COLORS.items():
                if status in colored:
                    colored = colored.replace(status, f"[{color}]{status}[/{color}]")
                    break
            console.print(colored)
    except KeyboardInterrupt:
        console.print("\n[dim]主人，已退出监控模式[/dim]")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()

