#!/bin/bash
# ═══════════════════════════════════════════════
#  RayTool 一键安装脚本
#  使用方法：bash <(curl -s https://raw.githubusercontent.com/ZhijianZhou/Qytool/main/install.sh)
#  或：bash install.sh
# ═══════════════════════════════════════════════
set -e

REPO_URL="https://github.com/ZhijianZhou/Qytool.git"
DEFAULT_INSTALL_DIR="$HOME/Qytool"

echo ""
echo "╭──────────────────────────────────╮"
echo "│   RayTool 安装向导               │"
echo "╰──────────────────────────────────╯"
echo ""

# ──────────── 1. 交互式配置 ────────────

read -p "安装目录 [默认: $DEFAULT_INSTALL_DIR]: " INSTALL_DIR
INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
INSTALL_DIR=$(eval echo "$INSTALL_DIR")

read -p "K8s 命名空间 [默认: ray-system]: " NAMESPACE
NAMESPACE="${NAMESPACE:-ray-system}"

read -p "YAML 任务文件目录 [默认: ~/ray-jobs/]: " YAML_DIR
YAML_DIR="${YAML_DIR:-~/ray-jobs/}"

read -p "默认查看日志行数 [默认: 100]: " LOG_LINES
LOG_LINES="${LOG_LINES:-100}"

read -p "容器默认 Shell [默认: /bin/bash]: " DEFAULT_SHELL
DEFAULT_SHELL="${DEFAULT_SHELL:-/bin/bash}"

read -p "命令行别名 [默认: raytool]: " CMD_ALIAS
CMD_ALIAS="${CMD_ALIAS:-raytool}"

echo ""
echo "────────────────────────────────────"
echo "  安装目录:   $INSTALL_DIR"
echo "  命名空间:   $NAMESPACE"
echo "  YAML 目录:  $YAML_DIR"
echo "  日志行数:   $LOG_LINES"
echo "  默认 Shell: $DEFAULT_SHELL"
echo "  命令别名:   $CMD_ALIAS"
echo "────────────────────────────────────"
echo ""
read -p "确认以上配置? [Y/n]: " CONFIRM
CONFIRM="${CONFIRM:-Y}"
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "已取消安装。"
    exit 0
fi

# ──────────── 2. 克隆/更新仓库 ────────────

echo ""
if [ -d "$INSTALL_DIR/.git" ]; then
    echo "检测到已有安装，正在更新..."
    cd "$INSTALL_DIR"
    git pull origin main
else
    echo "正在克隆仓库..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

# ──────────── 3. 安装 Python 依赖 ────────────

echo ""
echo "正在安装 Python 依赖..."
pip install -e . 2>&1 || pip install --user -e . 2>&1

# ──────────── 4. 生成配置文件 ────────────

cat > ~/.raytoolconfig << CONFEOF
namespace: $NAMESPACE
yaml_dir: $YAML_DIR
default_log_lines: $LOG_LINES
default_shell: $DEFAULT_SHELL
CONFEOF

echo "配置文件: ~/.raytoolconfig"

# ──────────── 5. 创建 YAML 目录 ────────────

YAML_DIR_EXPANDED=$(eval echo "$YAML_DIR")
mkdir -p "$YAML_DIR_EXPANDED"
echo "YAML 目录: $YAML_DIR_EXPANDED"

# ──────────── 6. 验证安装 ────────────

echo ""
if command -v raytool &> /dev/null; then
    echo "raytool 命令已就绪！"
else
    # pip install -e 可能装到 user bin，添加 alias 兜底
    ALIAS_LINE="alias $CMD_ALIAS='python3 -m raytool'"

    if [ -f "$HOME/.zshrc" ]; then
        SHELL_RC="$HOME/.zshrc"
    else
        SHELL_RC="$HOME/.bashrc"
    fi

    # 去掉旧的 alias
    sed -i "/alias $CMD_ALIAS=/d" "$SHELL_RC" 2>/dev/null || true

    echo "" >> "$SHELL_RC"
    echo "# RayTool 命令别名" >> "$SHELL_RC"
    echo "$ALIAS_LINE" >> "$SHELL_RC"
    echo "别名已添加到 $SHELL_RC"
fi

# ──────────── 7. 完成 ────────────

echo ""
echo "╭──────────────────────────────────╮"
echo "│   安装完成！                      │"
echo "╰──────────────────────────────────╯"
echo ""
echo "使用方法："
echo "   raytool              # 交互式主菜单"
echo "   raytool status       # 集群概况"
echo "   raytool list         # 查看任务列表"
echo "   raytool logs         # 查看日志"
echo "   raytool submit       # 提交任务"
echo "   raytool exec         # 进入容器"
echo "   raytool occupy       # GPU 占卡"
echo ""
echo "如果 raytool 命令未生效，请执行: source $SHELL_RC"
echo ""
