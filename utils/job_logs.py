"""任务日志持久化模块

在任务删除前自动保存日志到共享目录，确保任务删除后日志仍然可查。
日志按用户/任务名组织存储。

目录结构:
    {data_dir}/raytool_logs/
        {username}/
            {job_name}_{timestamp}.log
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from raytool.utils.kube import run_kubectl, get_running_pods, group_pods_by_job

logger = logging.getLogger(__name__)


class JobLogSaver:
    """任务日志持久化保存。"""

    def __init__(self, data_dir: str) -> None:
        self._logs_dir = Path(data_dir) / "raytool_logs"
        self._logs_dir.mkdir(parents=True, exist_ok=True)

    def _user_dir(self, username: str) -> Path:
        """获取用户日志目录。"""
        d = self._logs_dir / username
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_job_logs(self, job_name: str, namespace: str, username: str) -> list[str]:
        """保存任务所有 Pod 的日志。返回保存的文件路径列表。"""
        saved_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 获取该任务的所有 Pod
        pods = get_running_pods(namespace)
        jobs = group_pods_by_job(pods)
        job_pods = jobs.get(job_name, [])

        if not job_pods:
            # 尝试直接用 job_name 匹配 pod（可能任务名不完全匹配）
            job_pods = [p for p in pods if job_name in p["name"]]

        if not job_pods:
            logger.warning(f"未找到任务 {job_name} 的 Pod，跳过日志保存")
            return saved_files

        user_dir = self._user_dir(username)

        for pod in job_pods:
            pod_name = pod["name"]
            containers = pod.get("containers", [])

            # 每个容器的日志都保存（带时间戳，不限行数）
            for container in containers:
                try:
                    args = ["logs", pod_name, "-c", container, "--timestamps=true"]
                    rc, stdout, stderr = run_kubectl(args, namespace, timeout=120)

                    if rc != 0 or not stdout.strip():
                        # 尝试不指定容器
                        args = ["logs", pod_name, "--timestamps=true"]
                        rc, stdout, stderr = run_kubectl(args, namespace, timeout=120)

                    if rc == 0 and stdout.strip():
                        # 构建文件名: job_pod_container_timestamp.log
                        safe_pod = pod_name.replace("/", "_")
                        safe_container = container.replace("/", "_")
                        filename = f"{job_name}_{safe_pod}_{safe_container}_{timestamp}.log"
                        filepath = user_dir / filename

                        # 写入日志 + 元信息头
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(f"# Job: {job_name}\n")
                            f.write(f"# Pod: {pod_name}\n")
                            f.write(f"# Container: {container}\n")
                            f.write(f"# Namespace: {namespace}\n")
                            f.write(f"# User: {username}\n")
                            f.write(f"# Saved at: {datetime.now().isoformat()}\n")
                            f.write(f"# {'=' * 60}\n\n")
                            f.write(stdout)

                        saved_files.append(str(filepath))
                except Exception as e:
                    logger.warning(f"保存 {pod_name}/{container} 日志失败: {e}")

            # 如果没有容器列表，尝试直接获取 Pod 日志
            if not containers:
                try:
                    args = ["logs", pod_name, "--timestamps=true"]
                    rc, stdout, stderr = run_kubectl(args, namespace, timeout=120)
                    if rc == 0 and stdout.strip():
                        filename = f"{job_name}_{pod_name}_{timestamp}.log"
                        filepath = user_dir / filename
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(f"# Job: {job_name}\n")
                            f.write(f"# Pod: {pod_name}\n")
                            f.write(f"# Namespace: {namespace}\n")
                            f.write(f"# User: {username}\n")
                            f.write(f"# Saved at: {datetime.now().isoformat()}\n")
                            f.write(f"# {'=' * 60}\n\n")
                            f.write(stdout)
                        saved_files.append(str(filepath))
                except Exception as e:
                    logger.warning(f"保存 {pod_name} 日志失败: {e}")

        return saved_files

    def list_user_logs(self, username: str) -> list[dict]:
        """列出用户保存的所有历史日志文件。"""
        user_dir = self._logs_dir / username
        if not user_dir.exists():
            return []

        logs = []
        for f in sorted(user_dir.glob("*.log"), reverse=True):
            stat = f.stat()
            meta = self._parse_log_meta(f)
            logs.append({
                "path": str(f),
                "filename": f.name,
                "job_name": meta.get("job", ""),
                "pod_name": meta.get("pod", ""),
                "container": meta.get("container", ""),
                "saved_at_raw": meta.get("saved_at", ""),
                "size": stat.st_size,
                "saved_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            })
        return logs

    def list_user_jobs(self, username: str) -> list[dict]:
        """列出用户的历史任务（按任务分组），返回任务列表。
        每个任务包含: job_name, saved_at, total_size, log_files(list)。
        """
        all_logs = self.list_user_logs(username)
        if not all_logs:
            return []

        # 按 (job_name, saved_at 的日期时间前缀) 分组
        # 同一次删除操作的所有 Pod 日志时间戳一致
        from collections import OrderedDict
        jobs: OrderedDict[str, dict] = OrderedDict()

        for log in all_logs:
            job_name = log["job_name"] or log["filename"]
            # 用 job_name + saved_at 作为 key，区分同一任务多次删除
            # 从文件名中提取时间戳部分 (最后的 _YYYYMMDD_HHMMSS.log)
            fname = log["filename"]
            ts_key = ""
            parts = fname.rsplit("_", 2)
            if len(parts) >= 3 and parts[-1].endswith(".log"):
                ts_key = f"{parts[-2]}_{parts[-1].replace('.log', '')}"

            group_key = f"{job_name}||{ts_key}"

            if group_key not in jobs:
                jobs[group_key] = {
                    "job_name": job_name,
                    "saved_at": log["saved_at"],
                    "total_size": 0,
                    "log_files": [],
                }
            jobs[group_key]["total_size"] += log["size"]
            jobs[group_key]["log_files"].append(log)

        return list(jobs.values())

    def read_log(self, filepath: str) -> Optional[str]:
        """读取日志内容。"""
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.error(f"读取日志失败: {e}")
            return None

    @staticmethod
    def _parse_log_meta(filepath: Path) -> dict:
        """从日志文件头部解析元信息。"""
        meta = {}
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.startswith("# "):
                        break
                    if line.startswith("# Job: "):
                        meta["job"] = line[7:].strip()
                    elif line.startswith("# Pod: "):
                        meta["pod"] = line[7:].strip()
                    elif line.startswith("# Container: "):
                        meta["container"] = line[13:].strip()
                    elif line.startswith("# Saved at: "):
                        meta["saved_at"] = line[12:].strip()
        except Exception:
            pass
        return meta
