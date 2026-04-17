"""用户数据存储模块

用户数据以 JSON 文件存储在共享目录中，支持多台机器共享。
用户名必须在预置列表中，登录只需选择用户名。

预置用户列表从 {data_dir}/.raytoolconfig/preset_users.yaml 加载，
文件不存在时自动从内置默认列表生成。
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

USERS_FILE = "raytool_users.json"
LOCK_TIMEOUT = 10

PRESET_USERS_FILENAME = "preset_users.yaml"
RAYTOOLCONFIG_DIR = ".raytoolconfig"

_DEFAULT_PRESET_USERS: list[tuple[str, str]] = []


def _load_preset_users_from_file(data_dir: str) -> list[tuple[str, str]]:
    """从 {data_dir}/.raytoolconfig/preset_users.yaml 加载预置用户列表。

    文件不存在时自动创建并写入默认列表。
    文件格式:
        users:
          - username: tristanli
            display_name: 李珂
          - username: arthurtan
            display_name: 谭晓宇
    """
    config_dir = Path(data_dir) / RAYTOOLCONFIG_DIR
    preset_file = config_dir / PRESET_USERS_FILENAME

    if not preset_file.exists():
        try:
            config_dir.mkdir(parents=True, exist_ok=True)
            entries = [
                {"username": u, "display_name": n} for u, n in _DEFAULT_PRESET_USERS
            ]
            with open(preset_file, "w", encoding="utf-8") as f:
                yaml.dump(
                    {"users": entries},
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                )
            logger.info(f"已生成预置用户配置: {preset_file}")
        except OSError as e:
            logger.warning(f"无法写入预置用户配置 {preset_file}: {e}，使用内置默认列表")
            return list(_DEFAULT_PRESET_USERS)

    try:
        with open(preset_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        users_list = data.get("users", [])
        result: list[tuple[str, str]] = []
        for item in users_list:
            username = item.get("username", "")
            display_name = item.get("display_name", username)
            if username:
                result.append((username, display_name))
        if not result:
            logger.warning(f"预置用户配置为空: {preset_file}，使用内置默认列表")
            return list(_DEFAULT_PRESET_USERS)
        return result
    except (yaml.YAMLError, OSError) as e:
        logger.warning(f"读取预置用户配置失败 {preset_file}: {e}，使用内置默认列表")
        return list(_DEFAULT_PRESET_USERS)


# 向后兼容: 模块级变量，首次 UserStore 初始化后被更新
PRESET_USERS: list[tuple[str, str]] = list(_DEFAULT_PRESET_USERS)
PRESET_USER_MAP: dict[str, str] = {u: n for u, n in PRESET_USERS}


def is_valid_username(username: str) -> bool:
    """检查用户名是否在预置列表中。"""
    return username in PRESET_USER_MAP


def get_preset_display_name(username: str) -> str:
    """获取预置用户的中文名。"""
    return PRESET_USER_MAP.get(username, username)


class UserStore:
    """用户数据存储管理。

    数据存储在共享目录的 JSON 文件中，支持多机共享。
    使用简易文件锁防止并发写冲突。
    """

    def __init__(self, data_dir: str) -> None:
        global PRESET_USERS, PRESET_USER_MAP

        self._data_dir = Path(data_dir)
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise SystemExit(
                f"\n❌ 无法创建数据目录: {data_dir}\n"
                f"   权限不足，请检查路径是否正确。\n\n"
                f"   解决方法:\n"
                f"   1. 重新运行安装脚本并指定可用的共享数据目录:\n"
                f"      bash install.sh\n"
                f"   2. 或设置环境变量指向已有的配置文件:\n"
                f"      export RAYTOOL_CONFIG=/path/to/raytool_global_config.yaml\n"
            )
        except OSError as e:
            raise SystemExit(
                f"\n❌ 无法创建数据目录: {data_dir}\n"
                f"   错误: {e}\n\n"
                f"   请重新运行 bash install.sh 并指定可用的共享数据目录。\n"
            )

        PRESET_USERS = _load_preset_users_from_file(data_dir)
        PRESET_USER_MAP = {u: n for u, n in PRESET_USERS}
        self.preset_users = PRESET_USERS
        self.preset_user_map = PRESET_USER_MAP

        self._users_file = self._data_dir / USERS_FILE
        self._lock_file = self._data_dir / f"{USERS_FILE}.lock"
        self._ensure_initial_users()

    def _acquire_lock(self) -> None:
        """获取文件锁（简易实现）。"""
        start = time.time()
        while True:
            try:
                fd = os.open(
                    str(self._lock_file),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return
            except FileExistsError:
                if time.time() - start > LOCK_TIMEOUT:
                    logger.warning("锁超时，强制清除")
                    self._release_lock()
                    continue
                time.sleep(0.1)

    def _release_lock(self) -> None:
        """释放文件锁。"""
        try:
            self._lock_file.unlink(missing_ok=True)
        except OSError:
            pass

    def _load(self) -> dict[str, Any]:
        """加载用户数据。"""
        if not self._users_file.exists():
            return {}
        try:
            with open(self._users_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"读取用户数据失败: {e}")
            return {}

    def _save(self, data: dict[str, Any]) -> None:
        """保存用户数据。"""
        with open(self._users_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _ensure_initial_users(self) -> None:
        """确保初始用户已创建（首次运行时写入）。"""
        if self._users_file.exists():
            return

        self._acquire_lock()
        try:
            if self._users_file.exists():
                return

            users: dict[str, Any] = {}
            now = datetime.now().isoformat()

            for username, display_name in PRESET_USERS:
                users[username] = {
                    "display_name": display_name,
                    "is_admin": False,
                    "registered_at": now,
                }

            self._save(users)
            logger.info(f"已初始化 {len(users)} 个用户")
        finally:
            self._release_lock()

    # ── 任务归属管理 ──

    def get_job_owner_file(self) -> Path:
        """获取任务归属文件路径。"""
        return self._data_dir / "raytool_job_owners.json"

    def _load_job_owners(self) -> dict[str, Any]:
        """加载任务归属数据。"""
        fpath = self.get_job_owner_file()
        if not fpath.exists():
            return {}
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_job_owners(self, data: dict[str, Any]) -> None:
        """保存任务归属数据。"""
        fpath = self.get_job_owner_file()
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def record_job_owner(self, job_name: str, username: str) -> None:
        """记录任务归属。"""
        self._acquire_lock()
        try:
            owners = self._load_job_owners()
            owners[job_name] = {
                "owner": username,
                "submitted_at": datetime.now().isoformat(),
            }
            self._save_job_owners(owners)
        finally:
            self._release_lock()

    def get_job_owner(self, job_name: str) -> Optional[str]:
        """获取任务的归属用户。"""
        owners = self._load_job_owners()
        info = owners.get(job_name)
        if info:
            return info.get("owner")
        return None

    def remove_job_owner(self, job_name: str) -> None:
        """移除任务归属记录。"""
        self._acquire_lock()
        try:
            owners = self._load_job_owners()
            if job_name in owners:
                del owners[job_name]
                self._save_job_owners(owners)
        finally:
            self._release_lock()

    def get_user_jobs(self, username: str) -> list[str]:
        """获取用户提交的所有任务名称。"""
        owners = self._load_job_owners()
        return [name for name, info in owners.items() if info.get("owner") == username]

    # ── 用户 CRUD ──

    def list_users(self) -> dict[str, Any]:
        """获取所有用户数据。"""
        return self._load()

    def get_user(self, username: str) -> Optional[dict[str, Any]]:
        """获取指定用户数据。"""
        users = self._load()
        return users.get(username)

    def set_admin(self, username: str, is_admin: bool = True) -> tuple[bool, str]:
        """设置用户管理员权限。"""
        self._acquire_lock()
        try:
            users = self._load()
            if username not in users:
                return False, f"用户 '{username}' 不存在"
            users[username]["is_admin"] = is_admin
            self._save(users)
            status = "管理员" if is_admin else "普通用户"
            return True, f"用户 '{username}' 已设置为{status}"
        finally:
            self._release_lock()

    def delete_user(self, username: str) -> tuple[bool, str]:
        """删除用户。"""
        self._acquire_lock()
        try:
            users = self._load()
            if username not in users:
                return False, f"用户 '{username}' 不存在"
            del users[username]
            self._save(users)
            return True, f"用户 '{username}' 已删除"
        finally:
            self._release_lock()

    def register(self, username: str, display_name: str = "") -> tuple[bool, str]:
        """注册新用户（管理员操作）。"""
        self._acquire_lock()
        try:
            users = self._load()
            if username in users:
                return False, f"用户名 '{username}' 已存在"
            users[username] = {
                "display_name": display_name or username,
                "is_admin": False,
                "registered_at": datetime.now().isoformat(),
            }
            self._save(users)
            return True, f"用户 '{username}' 注册成功"
        finally:
            self._release_lock()

    def is_admin(self, username: str) -> bool:
        """检查用户是否为管理员。"""
        user = self.get_user(username)
        if user is None:
            return False
        return user.get("is_admin", False)

    def get_display_name(self, username: str) -> str:
        """获取用户显示名称。"""
        user = self.get_user(username)
        if user is None:
            return PRESET_USER_MAP.get(username, username)
        return user.get("display_name", username)

    def update_user_config(self, username: str, key: str, value: Any) -> None:
        """更新用户个性化配置。传 None 表示删除该配置项。"""
        self._acquire_lock()
        try:
            users = self._load()
            if username not in users:
                return
            if "preferences" not in users[username]:
                users[username]["preferences"] = {}
            if value is None:
                users[username]["preferences"].pop(key, None)
            else:
                users[username]["preferences"][key] = value
            self._save(users)
        finally:
            self._release_lock()

    def get_user_config(self, username: str, key: str, default: Any = None) -> Any:
        """获取用户个性化配置。"""
        user = self.get_user(username)
        if user is None:
            return default
        return user.get("preferences", {}).get(key, default)
