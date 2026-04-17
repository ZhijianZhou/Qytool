"""功能5: 删除任务

权限控制：普通用户只能删除自己提交的任务，管理员可以删除任何任务。
删除前自动保存任务日志到持久化目录。
"""
import os
import yaml
import InquirerPy
from rich.markup import escape
from raytool.utils.kube import run_kubectl, get_running_pods, group_pods_by_job
from raytool.utils.ui import (
    console, select_jobs_multi, confirm, confirm_with_input,
    print_success, print_error, print_warning, print_info,
    ESC_KEYBINDING, browse_yaml_dir,
)


def get_job_names_from_yaml(yaml_path: str) -> list:
    """从 YAML 文件中提取 job 名称"""
    try:
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load_all(f)
            job_names = []
            for doc in data:
                if doc and isinstance(doc, dict):
                    kind = doc.get('kind', '')
                    if 'PyTorchJob' in kind or 'RayCluster' in kind or 'Job' in kind:
                        name = doc.get('metadata', {}).get('name', '')
                        if name:
                            job_names.append((kind, name))
            return job_names
    except Exception as e:
        print_warning(f"解析 YAML 失败: {e}")
        return []


def _get_user_context(current_user, config):
    """获取用户上下文（UserStore, AuditLogger, is_admin）。"""
    store = None
    audit = None
    is_admin = False
    if current_user and config:
        from raytool.utils.user_store import UserStore
        from raytool.utils.audit import AuditLogger
        store = UserStore(config["data_dir"])
        audit = AuditLogger(config["data_dir"])
        is_admin = store.is_admin(current_user)
    return store, audit, is_admin


def _save_logs_before_delete(job_names: list, namespace: str, current_user: str, config: dict):
    """删除前保存任务日志。"""
    if not current_user or not config:
        return
    try:
        from raytool.utils.job_logs import JobLogSaver
        saver = JobLogSaver(config["data_dir"])
        for job_name in job_names:
            print_info(f"正在保存 {job_name} 的日志...")
            saved = saver.save_job_logs(job_name, namespace, current_user)
            if saved:
                print_success(f"已保存 {len(saved)} 个日志文件")
            else:
                print_warning(f"未找到 {job_name} 的日志（可能 Pod 已退出）")
    except Exception as e:
        print_warning(f"日志保存失败（不影响删除操作）: {e}")


def _check_delete_permission(job_name, current_user, store, is_admin):
    """检查用户是否有权删除指定任务。返回 (允许, 原因)。"""
    if not current_user or not store:
        return True, ""
    if is_admin:
        return True, ""
    owner = store.get_job_owner(job_name)
    if owner is None:
        # 无归属记录的任务，允许删除（向后兼容）
        return True, ""
    if owner == current_user:
        return True, ""
    return False, f"任务 '{job_name}' 属于用户 '{owner}'，您无权删除"


def _delete_pytorchjob(job_name: str, namespace: str) -> tuple:
    """删除 PyTorchJob 资源"""
    rc, stdout, stderr = run_kubectl(
        ["delete", "pytorchjob", job_name, "--ignore-not-found=true"],
        namespace
    )
    if rc == 0:
        return True, "已删除"
    return False, stderr.strip()


def _delete_yaml(yaml_path: str, namespace: str, current_user: str = None, config: dict = None) -> tuple:
    """完全模拟 kubectl delete -f <yaml>"""
    if not os.path.isfile(yaml_path):
        return False, f"文件不存在: {yaml_path}"

    store, audit, is_admin = _get_user_context(current_user, config)

    # 预览 YAML 中的资源
    job_infos = get_job_names_from_yaml(yaml_path)
    if not job_infos:
        return False, "YAML 中未找到 PyTorchJob 或 RayCluster 资源"

    # 权限检查
    for kind, name in job_infos:
        allowed, reason = _check_delete_permission(name, current_user, store, is_admin)
        if not allowed:
            print_error(reason)
            return False, reason

    console.print(f"[bold]文件:[/bold] {escape(os.path.basename(yaml_path))}")
    for kind, name in job_infos:
        console.print(f"  [bold]- {escape(kind)}[/bold]: {escape(name)}")

    if not confirm("确认删除这些资源?"):
        print_warning("已取消")
        return False, "已取消"

    # 删除前保存日志
    job_names_to_save = [name for _, name in job_infos]
    _save_logs_before_delete(job_names_to_save, namespace, current_user, config)

    # 直接执行 kubectl delete -f
    print_info(f"执行: kubectl delete -f {yaml_path}")
    rc, stdout, stderr = run_kubectl(["delete", "-f", yaml_path], namespace)
    if rc == 0:
        # 记录审计日志 & 清理归属
        if audit and current_user:
            for kind, name in job_infos:
                audit.log(current_user, "delete", f"{kind}/{name}")
                if store:
                    store.remove_job_owner(name)
        return True, stdout.strip()
    return False, stderr.strip()


def delete_jobs(namespace: str, yaml_path: str = None, current_user: str = None, config: dict = None):
    """交互式删除任务 - 默认使用 kubectl delete -f"""
    from raytool.utils.config import load_config as _load_config
    if config is None:
        config = _load_config()
    yaml_dir = config.get("yaml_dir", "ray-job")

    # 如果指定了 YAML 文件，直接通过 YAML 删除
    if yaml_path:
        _delete_yaml(yaml_path, namespace, current_user, config)
        return

    # 扫描配置目录（支持子目录浏览）
    if not os.path.isdir(yaml_dir):
        print_warning(f"目录 {yaml_dir} 不存在")
        print_info("将使用 kubectl delete pytorchjob 方式删除...")
        _delete_by_running_jobs(namespace, current_user, config)
        return

    selected = browse_yaml_dir(yaml_dir, message="请选择 YAML 文件删除 (kubectl delete -f)")

    if selected is None:
        print_warning("已取消")
        return

    if selected == "__manual__":
        selected = InquirerPy.inquirer.filepath(
            message="请输入 YAML 文件路径",
            validate=lambda x: os.path.isfile(x),
            invalid_message="文件不存在",
        ).execute()

    _delete_yaml(selected, namespace, current_user, config)


def _delete_by_running_jobs(namespace: str, current_user: str = None, config: dict = None):
    """从运行中的任务中选择删除"""
    store, audit, is_admin = _get_user_context(current_user, config)

    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    if not jobs:
        print_warning("当前没有运行中的任务")
        return

    # 多选任务
    selected = select_jobs_multi(jobs, message="请选择要删除的任务 (空格多选, 回车确认)")
    if not selected:
        print_warning("未选择任何任务")
        return

    # 权限检查
    denied = []
    for job_name in selected:
        allowed, reason = _check_delete_permission(job_name, current_user, store, is_admin)
        if not allowed:
            denied.append((job_name, reason))

    if denied:
        for job_name, reason in denied:
            print_error(reason)
        # 从选中列表中移除无权限的任务
        selected = [j for j in selected if j not in [d[0] for d in denied]]
        if not selected:
            print_warning("没有可删除的任务")
            return
        print_info(f"将删除剩余 {len(selected)} 个有权限的任务")

    console.print()
    console.print("[bold yellow]⚠️  即将删除以下任务:[/bold yellow]")
    for job_name in selected:
        console.print(f"  [bold]- {job_name}[/bold]")
    console.print()

    # 强确认
    if not confirm_with_input("确认删除? 请输入 'yes'"):
        print_warning("已取消删除")
        return

    # 删除前保存日志
    _save_logs_before_delete(selected, namespace, current_user, config)

    console.print()
    for job_name in selected:
        success, msg = _delete_pytorchjob(job_name, namespace)
        if success:
            print_success(f"已删除: {job_name}")
            if audit and current_user:
                audit.log(current_user, "delete", f"PyTorchJob/{job_name}")
            if store:
                store.remove_job_owner(job_name)
        else:
            print_error(f"删除失败 {job_name}: {msg}")
