"""用户管理命令"""

import logging
from typing import Any

from raytool.utils.ui import (
    console,
    confirm,
    print_error,
    print_info,
    print_success,
    print_warning,
    ESC_KEYBINDING,
)
from raytool.utils.user_store import UserStore
from raytool.utils.audit import AuditLogger

logger = logging.getLogger(__name__)


def user_cmd(config: dict[str, Any], current_user: str) -> None:
    """用户管理入口。"""
    from InquirerPy import inquirer
    from rich.table import Table

    store = UserStore(config["data_dir"])
    audit = AuditLogger(config["data_dir"])
    is_admin = store.is_admin(current_user)

    while True:
        menu = [
            {"name": "📋 查看所有用户", "value": "list"},
            {"name": "⚙️  个人设置 (yaml_dir / 预热目录 / 日志行数)", "value": "preferences"},
            {"name": "📜 查看操作日志", "value": "audit"},
        ]
        if is_admin:
            menu.extend([
                {"name": "📝 添加新用户 (管理员)", "value": "register"},
                {"name": "🔑 设置管理员 (管理员)", "value": "set_admin"},
                {"name": "🗑️  删除用户 (管理员)", "value": "delete"},
            ])
        menu.append({"name": "⬅️  返回", "value": "back"})

        choice = inquirer.fuzzy(
            message="主人，用户管理:",
            choices=menu,
            pointer="❯",
            border=True,
            keybindings=ESC_KEYBINDING,
        ).execute()

        if choice is None or choice == "back":
            break
        elif choice == "list":
            _list_users(store)
        elif choice == "preferences":
            _user_preferences(store, current_user, config)
        elif choice == "audit":
            _view_audit(audit, store, current_user, is_admin)
        elif choice == "register":
            _register(store, audit, current_user)
        elif choice == "set_admin":
            _set_admin(store, config, audit, current_user)
        elif choice == "delete":
            _delete_user(store, audit, current_user)


def _list_users(store: UserStore) -> None:
    """查看所有用户。"""
    from rich.table import Table

    users = store.list_users()
    if not users:
        print_info("暂无注册用户")
        return

    table = Table(title="👥 用户列表", show_lines=False, border_style="cyan")
    table.add_column("#", style="dim", width=4)
    table.add_column("用户名", style="cyan")
    table.add_column("姓名", style="")
    table.add_column("角色", justify="center")
    table.add_column("注册时间", style="dim")

    for i, (name, info) in enumerate(users.items(), 1):
        display_name = info.get("display_name", "")
        role = "🔑 管理员" if info.get("is_admin") else "👤 普通用户"
        registered = info.get("registered_at", "N/A")[:19]
        table.add_row(str(i), name, display_name, role, registered)

    console.print(table)


def _user_preferences(store: UserStore, username: str, config: dict[str, Any]) -> None:
    """个人偏好设置。"""
    import os
    import glob
    from InquirerPy import inquirer

    def _dir_info(path: str) -> tuple[bool, int, str]:
        """检查目录状态，返回 (是否存在, YAML文件数, 状态描述)"""
        expanded = os.path.expanduser(path) if path else ""
        if not expanded or not os.path.isdir(expanded):
            return False, 0, "⚠️ 目录不存在"
        count = len(glob.glob(os.path.join(expanded, "*.yaml"))) + len(glob.glob(os.path.join(expanded, "*.yml")))
        return True, count, "✅"

    def _set_dir_preference(key: str, label: str, current_val: str) -> None:
        """通用的目录偏好设置逻辑"""
        exists, count, _ = _dir_info(current_val)
        print_info(f"当前目录: {current_val}")
        if exists:
            print_info(f"目录下有 {count} 个 YAML 文件")
        new_val = inquirer.text(
            message=f"主人，请输入你的 {label} 目录路径:",
            default=current_val,
        ).execute().strip()
        if new_val:
            new_expanded = os.path.expanduser(new_val)
            if not os.path.isdir(new_expanded):
                if confirm(f"目录 {new_expanded} 不存在，是否创建？"):
                    try:
                        os.makedirs(new_expanded, exist_ok=True)
                        print_success(f"目录已创建: {new_expanded}")
                    except OSError as e:
                        print_error(f"创建目录失败: {e}")
                        return
                else:
                    print_warning("已取消设置")
                    return
            store.update_user_config(username, key, new_val)
            config[key] = new_val
            print_success(f"{label} 目录已更新: {new_val}")

    while True:
        current_yaml_dir = store.get_user_config(username, "yaml_dir", config.get("yaml_dir", "./ray-job"))
        current_prewarm_dir = store.get_user_config(username, "prewarm_dir", config.get("prewarm_dir", ""))
        current_log_lines = store.get_user_config(username, "default_log_lines", config.get("default_log_lines", 100))

        yaml_exists, yaml_count, yaml_status = _dir_info(current_yaml_dir)
        pw_exists, pw_count, pw_status = _dir_info(current_prewarm_dir)
        pw_display = current_prewarm_dir if current_prewarm_dir else "(未设置)"

        menu = [
            {"name": f"📁 Ray-Job 目录: {current_yaml_dir} ({yaml_status}, {yaml_count} 个YAML)", "value": "yaml_dir"},
            {"name": f"🔥 预热镜像目录: {pw_display} ({pw_status}, {pw_count} 个YAML)", "value": "prewarm_dir"},
            {"name": f"📜 默认日志行数: {current_log_lines}", "value": "log_lines"},
            {"name": "🔄 重置为全局默认", "value": "reset"},
            {"name": "⬅️  返回", "value": "back"},
        ]

        choice = inquirer.fuzzy(
            message="主人，个人设置:",
            choices=menu,
            pointer="❯",
            border=True,
            keybindings=ESC_KEYBINDING,
        ).execute()

        if choice is None or choice == "back":
            break
        elif choice == "yaml_dir":
            _set_dir_preference("yaml_dir", "Ray-Job YAML", current_yaml_dir)
        elif choice == "prewarm_dir":
            _set_dir_preference("prewarm_dir", "预热镜像 YAML", current_prewarm_dir)
        elif choice == "log_lines":
            new_val = inquirer.number(
                message="主人，请输入默认日志行数:",
                default=current_log_lines,
                min_allowed=10,
                max_allowed=10000,
            ).execute()
            store.update_user_config(username, "default_log_lines", int(new_val))
            config["default_log_lines"] = int(new_val)
            print_success(f"默认日志行数已更新: {new_val}")
        elif choice == "reset":
            if confirm("确认重置所有个人设置为全局默认值？"):
                store.update_user_config(username, "yaml_dir", None)
                store.update_user_config(username, "prewarm_dir", None)
                store.update_user_config(username, "default_log_lines", None)
                config.pop("prewarm_dir", None)
                print_success("已重置为全局默认配置")


def _view_audit(audit: AuditLogger, store: UserStore, current_user: str, is_admin: bool) -> None:
    """查看操作日志。"""
    from InquirerPy import inquirer
    from rich.table import Table

    if is_admin:
        scope = inquirer.select(
            message="查看范围:",
            choices=[
                {"name": "📋 我的操作日志", "value": "mine"},
                {"name": "📋 所有操作日志", "value": "all"},
            ],
            pointer="❯",
            keybindings=ESC_KEYBINDING,
        ).execute()
    else:
        scope = "mine"

    if scope is None:
        return

    if scope == "mine":
        logs = audit.get_user_logs(current_user, count=30)
    else:
        logs = audit.get_recent(count=50)

    if not logs:
        print_info("暂无操作日志")
        return

    table = Table(title="📜 操作日志", show_lines=False, border_style="dim")
    table.add_column("时间", style="dim", width=20)
    table.add_column("用户", style="cyan", width=15)
    table.add_column("操作", style="yellow", width=15)
    table.add_column("目标", style="")

    for line in logs:
        parts = line.split(" | ", 3)
        if len(parts) == 4:
            table.add_row(*parts)

    console.print(table)


def _register(store: UserStore, audit: AuditLogger, current_user: str) -> None:
    """添加新用户（管理员操作）。"""
    from InquirerPy import inquirer

    username = inquirer.text(
        message="主人，请输入用户名（英文、数字、下划线）:",
    ).execute()
    if not username:
        return
    username = username.strip()

    if not username.replace("_", "").isalnum():
        print_error("用户名仅支持英文字母、数字和下划线")
        return

    display_name = inquirer.text(
        message="主人，请输入中文姓名:",
    ).execute()
    if display_name is None:
        return

    ok, msg = store.register(username, display_name.strip())
    if ok:
        print_success(msg)
        audit.log(current_user, "add_user", username)
    else:
        print_error(msg)


def _set_admin(store: UserStore, config: dict[str, Any], audit: AuditLogger, current_user: str) -> None:
    """设置管理员。"""
    from InquirerPy import inquirer

    password = inquirer.secret(
        message="主人，请输入管理员密码:",
    ).execute()
    if password is None:
        return
    if password != config.get("admin_password", ""):
        print_error("管理员密码错误")
        return

    users = store.list_users()
    if not users:
        print_info("暂无用户")
        return

    choices = [
        {
            "name": f"{name} ({info.get('display_name', '')}) - {'管理员' if info.get('is_admin') else '普通用户'}",
            "value": name,
        }
        for name, info in users.items()
    ]

    username = inquirer.fuzzy(
        message="主人，选择要设置的用户:",
        choices=choices,
        pointer="❯",
        border=True,
        keybindings=ESC_KEYBINDING,
    ).execute()
    if not username:
        return

    user = store.get_user(username)
    if user is None:
        print_error("用户不存在")
        return

    is_admin = not user.get("is_admin", False)
    action = "设为管理员" if is_admin else "取消管理员"
    if confirm(f"确认将 '{username}' {action}？"):
        ok, msg = store.set_admin(username, is_admin)
        if ok:
            print_success(msg)
            audit.log(current_user, "set_admin", f"{username} -> {'admin' if is_admin else 'user'}")
        else:
            print_error(msg)


def _delete_user(store: UserStore, audit: AuditLogger, current_user: str) -> None:
    """删除用户（管理员操作）。"""
    from InquirerPy import inquirer

    users = store.list_users()
    if not users:
        print_info("暂无用户")
        return

    choices = [
        {"name": f"{name} ({info.get('display_name', '')})", "value": name}
        for name, info in users.items()
    ]
    username = inquirer.fuzzy(
        message="主人，选择要删除的用户:",
        choices=choices,
        pointer="❯",
        border=True,
        keybindings=ESC_KEYBINDING,
    ).execute()
    if not username:
        return

    if confirm(f"确认删除用户 '{username}'？此操作不可恢复"):
        ok, msg = store.delete_user(username)
        if ok:
            print_success(msg)
            audit.log(current_user, "delete_user", username)
        else:
            print_error(msg)
