"""kubectl 命令调用封装"""
import subprocess
import sys
import json
from typing import List, Dict, Optional, Tuple
from collections import defaultdict


def run_kubectl(args: List[str], namespace: str, capture: bool = True, timeout: int = 30) -> Tuple[int, str, str]:
    """
    执行 kubectl 命令
    返回 (returncode, stdout, stderr)
    """
    cmd = ["kubectl"] + args + ["-n", namespace]
    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result.returncode, result.stdout, result.stderr
        else:
            # 交互模式（如 exec -it），直接继承终端
            result = subprocess.run(cmd, timeout=None)
            return result.returncode, "", ""
    except subprocess.TimeoutExpired:
        return 1, "", "命令执行超时"
    except FileNotFoundError:
        return 1, "", "未找到 kubectl 命令，请确认已安装并在 PATH 中"


def run_kubectl_stream(args: List[str], namespace: str):
    """
    以流式方式执行 kubectl 命令（用于 watch / logs -f）
    返回 Popen 对象
    """
    cmd = ["kubectl"] + args + ["-n", namespace]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return proc
    except FileNotFoundError:
        return None


def get_pods(namespace: str) -> List[Dict]:
    """获取所有 Pod 信息，返回字典列表"""
    rc, stdout, stderr = run_kubectl(
        ["get", "pods", "-o", "json"],
        namespace
    )
    if rc != 0:
        return []
    try:
        data = json.loads(stdout)
        pods = []
        for item in data.get("items", []):
            metadata = item.get("metadata", {})
            status = item.get("status", {})
            # 容器列表
            containers = [c["name"] for c in item.get("spec", {}).get("containers", [])]
            # ready 计数
            container_statuses = status.get("containerStatuses", [])
            ready_count = sum(1 for cs in container_statuses if cs.get("ready", False))
            total_count = len(container_statuses) if container_statuses else len(containers)
            # 重启次数
            restarts = sum(cs.get("restartCount", 0) for cs in container_statuses)
            # 角色
            role = _get_pod_role_name(item)

            pods.append({
                "name": metadata.get("name", ""),
                "namespace": metadata.get("namespace", ""),
                "status": status.get("phase", "Unknown"),
                "ready": f"{ready_count}/{total_count}",
                "restarts": restarts,
                "creation": metadata.get("creationTimestamp", ""),
                "containers": containers,
                "labels": metadata.get("labels", {}),
                "role": role,
            })
        return pods
    except (json.JSONDecodeError, KeyError):
        return []


def _get_pod_role_name(item: dict) -> str:
    """从 Pod 信息中提取角色名"""
    labels = item.get("metadata", {}).get("labels", {})
    name = item.get("metadata", {}).get("name", "")

    # 优先使用 label
    role = labels.get("ray.io/node-type", "").capitalize()
    if role:
        return role

    # 通过名称判断
    if "-head-" in name or name.endswith("-head"):
        return "Head"
    elif "-worker-" in name or name.endswith("-worker"):
        return "Worker"
    return "Unknown"


def get_running_pods(namespace: str) -> List[Dict]:
    """获取所有 Running 状态的 Pod"""
    pods = get_pods(namespace)
    return [p for p in pods if p["status"] == "Running"]


def group_pods_by_job(pods: List[Dict]) -> Dict[str, List[Dict]]:
    """
    按任务名称对 Pod 分组
    通过 Pod 名称前缀推断任务名：去掉最后的 -head-N / -worker-N 部分
    也支持通过 label 分组（如果有 ray.io/cluster 等标签）
    """
    groups = defaultdict(list)
    for pod in pods:
        name = pod["name"]
        # 优先使用 label 中的集群名
        labels = pod.get("labels", {})
        job_name = (
            labels.get("ray.io/cluster", "")
            or labels.get("ray.io/job-name", "")
            or labels.get("app.kubernetes.io/instance", "")
        )
        if not job_name:
            # 回退：通过名称推断，去掉 -head-N / -worker-N / -raycluster-XXXXX 后缀
            job_name = _infer_job_name(name)
        groups[job_name].append(pod)
    return dict(groups)


def _infer_job_name(pod_name: str) -> str:
    """从 Pod 名称推断任务名"""
    parts = pod_name.split("-")
    # 尝试找到 head / worker 关键字的位置
    for i, part in enumerate(parts):
        if part in ("head", "worker"):
            return "-".join(parts[:i])
    # 找不到的话，去掉最后两段（通常是 hash-xxxxx）
    if len(parts) > 2:
        return "-".join(parts[:-2])
    return pod_name


def get_pod_role(pod: Dict) -> str:
    """判断 Pod 角色: Head / Worker / Unknown"""
    name = pod["name"]
    labels = pod.get("labels", {})
    # 通过 label 判断
    role = labels.get("ray.io/node-type", "").capitalize()
    if role:
        return role
    # 通过名称判断
    if "-head-" in name or name.endswith("-head"):
        return "Head"
    elif "-worker-" in name or name.endswith("-worker"):
        return "Worker"
    return "Unknown"


def delete_pods(pod_names: List[str], namespace: str) -> List[Tuple[str, bool, str]]:
    """批量删除 Pod，返回 [(pod_name, success, message)]"""
    results = []
    for name in pod_names:
        rc, stdout, stderr = run_kubectl(["delete", "pod", name, "--grace-period=30"], namespace)
        if rc == 0:
            results.append((name, True, "已删除"))
        else:
            results.append((name, False, stderr.strip()))
    return results


def exec_into_pod(pod_name: str, namespace: str, container: Optional[str] = None, shell: str = "/bin/bash"):
    """进入 Pod 容器终端"""
    args = ["exec", "-it", pod_name]
    if container:
        args += ["-c", container]
    args += ["--", shell]
    cmd = ["kubectl"] + args + ["-n", namespace]
    result = subprocess.run(cmd)
    # 如果 bash 不存在，回退到 sh
    if result.returncode != 0 and shell == "/bin/bash":
        args_sh = ["exec", "-it", pod_name]
        if container:
            args_sh += ["-c", container]
        args_sh += ["--", "/bin/sh"]
        cmd_sh = ["kubectl"] + args_sh + ["-n", namespace]
        subprocess.run(cmd_sh)


def apply_yaml(yaml_path: str, namespace: str) -> Tuple[bool, str]:
    """应用 YAML 文件"""
    rc, stdout, stderr = run_kubectl(["apply", "-f", yaml_path], namespace)
    if rc == 0:
        return True, stdout.strip()
    return False, stderr.strip()

