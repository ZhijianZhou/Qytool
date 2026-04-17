"""操作审计日志模块

以 append-only 文本文件记录所有关键操作。
格式: {timestamp} | {user} | {action} | {target}
"""

import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

AUDIT_FILE = "raytool_audit.log"


class AuditLogger:
    """审计日志记录器。"""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._log_file = self._data_dir / AUDIT_FILE

    def log(self, username: str, action: str, target: str) -> None:
        """记录一条审计日志。

        Args:
            username: 操作用户名。
            action: 操作类型（如 submit / delete / login / switch_user 等）。
            target: 操作目标（如任务名、YAML 文件名等）。
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} | {username} | {action} | {target}\n"
        try:
            with open(self._log_file, "a", encoding="utf-8") as f:
                f.write(line)
        except OSError as e:
            logger.warning(f"写入审计日志失败: {e}")

    def get_recent(self, count: int = 50) -> list[str]:
        """获取最近的审计日志条目。

        Args:
            count: 返回条目数。

        Returns:
            日志行列表（最新在前）。
        """
        if not self._log_file.exists():
            return []
        try:
            with open(self._log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            return [line.rstrip() for line in lines[-count:]][::-1]
        except OSError:
            return []

    def get_user_logs(self, username: str, count: int = 50) -> list[str]:
        """获取指定用户的审计日志。

        Args:
            username: 用户名。
            count: 返回条目数。

        Returns:
            日志行列表（最新在前）。
        """
        if not self._log_file.exists():
            return []
        try:
            with open(self._log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            user_lines = [
                line.rstrip() for line in lines
                if f"| {username} |" in line
            ]
            return user_lines[-count:][::-1]
        except OSError:
            return []
