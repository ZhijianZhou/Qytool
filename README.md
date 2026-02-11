# 🚀 RayTool

Ray / PyTorchJob 集群任务管理命令行工具。将 `kubectl` 常用操作封装为交互式菜单和子命令，提供从任务提交、监控、日志查看、扩缩容到 GPU 资源占卡的全流程管理能力。

## ✨ 功能特性

| 命令 | 说明 |
|------|------|
| `raytool` | 交互式主菜单（无需记命令） |
| `raytool status` | 集群概况总览（Pod 统计、资源使用、GPU 利用率、异常告警） |
| `raytool watch` | 实时监控 Pods 状态变化（自动状态着色） |
| `raytool list` | 查看全量任务列表（含 Pending/Failed 诊断） |
| `raytool logs [job] [pod]` | 交互式查看/追踪任务日志（支持 tail / follow / 全量） |
| `raytool submit [yaml]` | 提交新任务（YAML 预览 + 确认） |
| `raytool delete [yaml]` | 删除任务（基于 YAML 或运行中任务多选） |
| `raytool exec [pod]` | 进入容器终端（bash 失败自动回退 sh） |
| `raytool describe [pod]` | 查看 Pod 详细信息 / YAML 配置 |
| `raytool port-forward [local_port] [remote_port]` | 端口转发到 Ray Dashboard (默认 8265) |
| `raytool scale [worker_count]` | 扩缩容 Ray 集群 Worker 节点 |
| `raytool occupy` | GPU 占卡（手动提交 / 删除 / 自动巡逻模式） |

## 📦 安装

### 前置条件

- Python >= 3.9
- `kubectl` 已安装并配置好集群访问权限

### 方式一：pip install（推荐）

```bash
cd /path/to/this/repo
pip install -e .
```

安装后即可全局使用 `raytool` 命令。

### 方式二：仅安装依赖

```bash
pip install -r requirements.txt
python -m raytool
```

## 🚀 快速开始

```bash
# 进入交互式主菜单
raytool

# 直接执行子命令
raytool status                          # 集群概况
raytool watch                           # 监控 Pods
raytool list                            # 任务列表
raytool logs                            # 查看日志
raytool submit job.yaml                 # 提交任务
raytool delete                          # 删除任务
raytool exec                            # 进入容器

# 全局选项
raytool -n my-namespace list            # 指定命名空间
raytool --kubeconfig ~/.kube/other list # 指定 kubeconfig
```

## ⚙️ 配置

首次运行时，工具会交互式引导你创建配置文件。也可以手动创建配置文件，按以下优先级查找：

1. 环境变量 `RAYTOOL_CONFIG` 指定的路径
2. 当前目录 `.raytoolconfig`
3. 用户目录 `~/.raytoolconfig`

配置文件格式（YAML）：

```yaml
namespace: ray-system        # 默认命名空间
yaml_dir: ./ray-job          # YAML 任务文件目录
default_log_lines: 100       # 默认日志行数
default_shell: /bin/bash     # exec 默认 shell
```

## 🔥 GPU 占卡功能

`raytool occupy` 提供三种操作模式：

- **提交占卡任务**：自动检测空闲 GPU 节点，按实例类型分批（支持 H200 / H100 / A100 / A10G 等），动态生成 PyTorchJob 并提交
- **删除占卡任务**：按名称正则匹配，支持全选快捷操作，同时清理生成的 YAML 文件
- **自动巡逻模式**：定时检测空闲 GPU 节点，发现空闲即自动占卡，支持自定义巡逻间隔

支持的 GPU 实例类型：p5en (H200)、p5e (H200)、p5 (H100)、p4d (A100)、p4de (A100-80G)、g5 (A10G)

## 📁 项目结构

```
.
├── pyproject.toml          # 项目构建配置
├── requirements.txt        # Python 依赖
└── raytool/
    ├── __init__.py
    ├── cli.py              # CLI 入口 (click 命令组 + 交互式菜单)
    ├── commands/
    │   ├── watch.py        # 监控 Pods 状态
    │   ├── list_jobs.py    # 任务列表
    │   ├── status.py       # 集群概况
    │   ├── logs.py         # 日志查看
    │   ├── submit.py       # 提交任务
    │   ├── delete.py       # 删除任务
    │   ├── shell.py        # 容器终端
    │   ├── describe.py     # 详细信息
    │   ├── port_forward.py # 端口转发
    │   ├── scale.py        # 扩缩容
    │   └── occupy.py       # GPU 占卡
    └── utils/
        ├── config.py       # 配置管理
        ├── kube.py         # kubectl 封装
        └── ui.py           # Rich + InquirerPy UI 组件
```

## 📄 License

MIT

