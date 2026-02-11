"""功能5: 删除任务"""
import os
import glob
import yaml
import InquirerPy
from raytool.utils.kube import run_kubectl, get_running_pods, group_pods_by_job
from raytool.utils.ui import (
    console, select_jobs_multi, confirm, confirm_with_input,
    print_success, print_error, print_warning, print_info,
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


def _delete_pytorchjob(job_name: str, namespace: str) -> tuple:
    """删除 PyTorchJob 资源"""
    rc, stdout, stderr = run_kubectl(
        ["delete", "pytorchjob", job_name, "--ignore-not-found=true"],
        namespace
    )
    if rc == 0:
        return True, "已删除"
    return False, stderr.strip()


def _delete_yaml(yaml_path: str, namespace: str) -> tuple:
    """完全模拟 kubectl delete -f <yaml>"""
    if not os.path.isfile(yaml_path):
        return False, f"文件不存在: {yaml_path}"

    # 预览 YAML 中的资源
    job_infos = get_job_names_from_yaml(yaml_path)
    if not job_infos:
        return False, "YAML 中未找到 PyTorchJob 或 RayCluster 资源"

    console.print(f"[bold]文件:[/bold] {os.path.basename(yaml_path)}")
    for kind, name in job_infos:
        console.print(f"  [bold]- {kind}[/bold]: {name}")

    if not confirm("确认删除这些资源?"):
        print_warning("已取消")
        return False, "已取消"

    # 直接执行 kubectl delete -f
    print_info(f"执行: kubectl delete -f {yaml_path}")
    rc, stdout, stderr = run_kubectl(["delete", "-f", yaml_path], namespace)
    if rc == 0:
        return True, stdout.strip()
    return False, stderr.strip()


def delete_jobs(namespace: str, yaml_path: str = None):
    """交互式删除任务 - 默认使用 kubectl delete -f"""
    from raytool.utils.config import load_config
    config = load_config()
    yaml_dir = config.get("yaml_dir", "ray-job")

    # 如果指定了 YAML 文件，直接通过 YAML 删除
    if yaml_path:
        _delete_yaml(yaml_path, namespace)
        return

    # 扫描配置目录下的 YAML 文件
    yaml_files = []
    if os.path.isdir(yaml_dir):
        yaml_files = sorted(glob.glob(os.path.join(yaml_dir, "*.yaml")) +
                           glob.glob(os.path.join(yaml_dir, "*.yml")))

    if not yaml_files:
        print_warning(f"目录 {yaml_dir} 中没有找到 YAML 文件")
        print_info("将使用 kubectl delete pytorchjob 方式删除...")
        _delete_by_running_jobs(namespace)
        return

    # 默认使用 YAML 文件方式删除（模拟 kubectl delete -f）
    file_choices = [{"name": os.path.basename(f), "value": f} for f in yaml_files]
    file_choices.append({"name": "📁 手动输入路径...", "value": "__manual__"})
    file_choices.append({"name": "❌ 取消", "value": "__cancel__"})

    selected = InquirerPy.inquirer.select(
        message="请选择 YAML 文件删除 (kubectl delete -f)",
        choices=file_choices,
        pointer="❯",
    ).execute()

    if selected == "__cancel__":
        print_warning("已取消")
        return

    if selected == "__manual__":
        selected = InquirerPy.inquirer.filepath(
            message="请输入 YAML 文件路径",
            validate=lambda x: os.path.isfile(x),
            invalid_message="文件不存在",
        ).execute()

    _delete_yaml(selected, namespace)


def _delete_by_running_jobs(namespace: str):
    """从运行中的任务中选择删除"""
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

    console.print()
    console.print("[bold yellow]⚠️  即将删除以下任务:[/bold yellow]")
    for job_name in selected:
        console.print(f"  [bold]- {job_name}[/bold]")
    console.print()

    # 强确认
    if not confirm_with_input("确认删除? 请输入 'yes'"):
        print_warning("已取消删除")
        return

    console.print()
    for job_name in selected:
        success, msg = _delete_pytorchjob(job_name, namespace)
        if success:
            print_success(f"已删除: {job_name}")
        else:
            print_error(f"删除失败 {job_name}: {msg}")
