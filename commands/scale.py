"""功能9: 扩缩容 Ray 集群"""
from InquirerPy import inquirer
from raytool.utils.kube import get_running_pods, group_pods_by_job, run_kubectl
from raytool.utils.ui import (
    console, select_job, confirm, print_info, print_error, print_warning, print_success,
    ESC_KEYBINDING,
)


def scale_job(namespace: str):
    """交互式扩缩容 Ray 集群"""
    pods = get_running_pods(namespace)
    jobs = group_pods_by_job(pods)

    if not jobs:
        print_warning("当前没有运行中的任务")
        return

    # 选择要扩缩容的任务
    job_name = select_job(jobs, message="请选择要扩缩容的任务")
    if not job_name:
        return

    job_pods = jobs[job_name]

    # 统计当前 Head 和 Worker 数量
    head_count = sum(1 for p in job_pods if p.get("role") == "Head")
    worker_count = sum(1 for p in job_pods if p.get("role") == "Worker")

    console.print()
    console.print(f"[bold]当前状态:[/bold] Head={head_count}, Worker={worker_count}")
    console.print()

    # 选择扩缩容类型
    scale_type = inquirer.select(
        message="请选择操作类型",
        choices=[
            {"name": "🔼 扩容 (增加 Worker)", "value": "up"},
            {"name": "🔽 缩容 (减少 Worker)", "value": "down"},
            {"name": "↩️  返回上一级", "value": "cancel"},
        ],
        pointer="❯",
        keybindings=ESC_KEYBINDING,
    ).execute()

    if scale_type == "cancel" or scale_type is None:
        return

    # 输入新的 Worker 数量
    if scale_type == "up":
        hint = f"当前 {worker_count} 个 Worker，要增加到多少? (直接回车确认)"
        new_worker_count = inquirer.number(
            message=hint,
            min_valid=head_count + 1,
            max_valid=100,
            default=worker_count + 1,
        ).execute()
    else:
        hint = f"当前 {worker_count} 个 Worker，要减少到多少? (直接回车确认)"
        new_worker_count = inquirer.number(
            message=hint,
            min_valid=0,
            max_valid=worker_count - 1,
            default=max(0, worker_count - 1),
        ).execute()

    # 确认操作
    console.print()
    if not confirm(f"确认将 Worker 从 {worker_count} 调整到 {new_worker_count}?"):
        print_warning("已取消")
        return

    # 执行扩缩容
    _scale_ray_cluster(namespace, job_name, new_worker_count)


def _scale_ray_cluster(namespace: str, job_name: str, worker_count: int):
    """通过 patch RayCluster 或 RayJob 实现扩缩容"""
    # 尝试查找 RayCluster CRD
    rc, stdout, stderr = run_kubectl(
        ["get", "rayclusters", "-o", "json"],
        namespace,
        timeout=15
    )

    scaled = False

    if rc == 0:
        import json
        try:
            data = json.loads(stdout)
            for item in data.get("items", []):
                cluster_name = item.get("metadata", {}).get("name", "")
                if job_name in cluster_name or cluster_name in job_name:
                    # 找到匹配的 RayCluster，尝试扩缩容
                    _patch_raycluster(namespace, cluster_name, worker_count)
                    scaled = True
                    break
        except Exception:
            pass

    if not scaled:
        # 尝试 patch RayJob
        rc, stdout, stderr = run_kubectl(
            ["get", "rayjobs", "-o", "json"],
            namespace,
            timeout=15
        )
        if rc == 0:
            import json
            try:
                data = json.loads(stdout)
                for item in data.get("items", []):
                    job_item_name = item.get("metadata", {}).get("name", "")
                    if job_name in job_item_name or job_item_name in job_name:
                        _patch_rayjob(namespace, job_item_name, worker_count)
                        scaled = True
                        break
            except Exception:
                pass

    if not scaled:
        # 兜底: 提示用户手动操作
        print_warning(f"未找到 {job_name} 对应的 RayCluster/RayJob CRD")
        print_info("请通过以下命令手动扩缩容:")
        console.print(f"  [cyan]kubectl scale raycluster {job_name} --replicas={worker_count} -n {namespace}[/cyan]")


def _patch_raycluster(namespace: str, cluster_name: str, worker_count: int):
    """Patch RayCluster 的 worker group replicas"""
    # 获取 worker group 配置
    rc, stdout, stderr = run_kubectl(
        ["get", "raycluster", cluster_name, "-o", "json"],
        namespace,
        timeout=15
    )

    if rc != 0:
        print_error(f"获取 RayCluster 失败: {stderr}")
        return

    import json
    try:
        data = json.loads(stdout)
        worker_groups = data.get("spec", {}).get("workerGroupSpecs", [])

        if not worker_groups:
            print_warning("RayCluster 没有 workerGroupSpecs 配置")
            return

        # 找到第一个 worker group
        worker_group = worker_groups[0]
        group_name = worker_group.get("groupName", "default")
        current_replicas = worker_group.get("replicas", 0)

        # 执行 scale
        patch_json = f'{{"spec":{{"workerGroupSpecs":[{{"groupName":"{group_name}","replicas":{worker_count}}}]}}}}'
        rc, stdout, stderr = run_kubectl(
            ["patch", "raycluster", cluster_name, "-p", patch_json, "--type=merge"],
            namespace
        )

        if rc == 0:
            print_success(f"已调整 {cluster_name} 的 Worker 数量: {current_replicas} -> {worker_count}")
        else:
            print_error(f"扩缩容失败: {stderr}")
    except Exception as e:
        print_error(f"处理失败: {e}")


def _patch_rayjob(namespace: str, job_name: str, worker_count: int):
    """Patch RayJob 的副本数"""
    # RayJob 通常通过 metadata.annotations 或 spec.rayClusterSpec 控制
    print_warning("RayJob 扩缩容暂未支持，请通过修改 YAML 后重新提交")
    print_info(f"参考: kubectl patch rayjob {job_name} -n {namespace} -p '{{\"spec\":{{\"rayClusterSpec\":{{\"workerGroupSpecs\":[{{\"replicas\":{worker_count}}}]}}}}}}' --type=merge")
