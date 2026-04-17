#!/bin/bash
# ═══════════════════════════════════════════════
#  RayTool v2.1 一键安装脚本
#  使用方法：cd /path/to/raytool && bash install.sh
# ═══════════════════════════════════════════════
set -e

# 获取脚本所在目录（即源码目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo "╭──────────────────────────────────╮"
echo "│   RayTool v2.1 安装向导          │"
echo "╰──────────────────────────────────╯"
echo ""

# ──────────── 1. 交互式配置 ────────────

INSTALL_DIR="$SCRIPT_DIR"
echo "安装源码目录: $INSTALL_DIR"
echo ""

read -p "K8s 命名空间 [默认: ray-system]: " NAMESPACE
NAMESPACE="${NAMESPACE:-ray-system}"

read -p "默认查看日志行数 [默认: 100]: " LOG_LINES
LOG_LINES="${LOG_LINES:-100}"

read -p "容器默认 Shell [默认: /bin/bash]: " DEFAULT_SHELL
DEFAULT_SHELL="${DEFAULT_SHELL:-/bin/bash}"

read -p "共享数据目录 [默认: /mnt/fsx-c/youtu-agent/youtu]: " DATA_DIR
DATA_DIR="${DATA_DIR:-/mnt/fsx-c/youtu-agent/youtu}"

read -p "命令行别名 [默认: raytool]: " CMD_ALIAS
CMD_ALIAS="${CMD_ALIAS:-raytool}"

echo ""
echo "────────────────────────────────────"
echo "  源码目录:   $INSTALL_DIR"
echo "  命名空间:   $NAMESPACE"
echo "  日志行数:   $LOG_LINES"
echo "  默认 Shell: $DEFAULT_SHELL"
echo "  数据目录:   $DATA_DIR"
echo "  命令别名:   $CMD_ALIAS"
echo ""
echo "  💡 yaml_dir / prewarm_dir 个人偏好"
echo "     登录后在「用户管理 → 个人设置」中配置"
echo "────────────────────────────────────"
echo ""
read -p "确认以上配置? [Y/n]: " CONFIRM
CONFIRM="${CONFIRM:-Y}"
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "已取消安装。"
    exit 0
fi

# ──────────── 2. 从本目录安装 Python 包 ────────────

echo ""
echo "正在从本地源码安装 Python 依赖..."
cd "$INSTALL_DIR"
pip install -e . 2>&1 || pip install --user -e . 2>&1

# ──────────── 3. 安装趣味工具 (cmatrix + fortune) ────────────

echo ""
echo "正在安装趣味工具 (cmatrix 字符雨 + fortune 毒鸡汤)..."

_install_fun_tools() {
    # 检测包管理器
    if command -v apt-get &> /dev/null; then
        PKG_MGR="apt"
        sudo apt-get update -qq 2>/dev/null
        sudo apt-get install -y -qq cmatrix fortune-mod 2>/dev/null && return 0
    elif command -v dnf &> /dev/null; then
        PKG_MGR="dnf"
        # Amazon Linux / Fedora — fortune-mod 可能在 EPEL
        sudo dnf install -y fortune-mod 2>/dev/null || true
    elif command -v yum &> /dev/null; then
        PKG_MGR="yum"
        sudo yum install -y fortune-mod 2>/dev/null || true
    elif command -v brew &> /dev/null; then
        PKG_MGR="brew"
        brew install cmatrix fortune 2>/dev/null && return 0
    fi

    # cmatrix 大概率不在 dnf/yum 源里，源码编译
    if ! command -v cmatrix &> /dev/null; then
        echo "  源码编译 cmatrix ..."
        # 确保有编译工具和 ncurses
        if command -v dnf &> /dev/null; then
            sudo dnf install -y gcc make cmake ncurses-devel git 2>/dev/null || true
        elif command -v yum &> /dev/null; then
            sudo yum install -y gcc make cmake ncurses-devel git 2>/dev/null || true
        fi

        CMATRIX_TMP="/tmp/cmatrix_build_$$"
        git clone --depth 1 https://github.com/abishekvashok/cmatrix.git "$CMATRIX_TMP" 2>/dev/null
        if [ -d "$CMATRIX_TMP" ]; then
            cd "$CMATRIX_TMP"
            mkdir -p build && cd build
            cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local 2>/dev/null
            make -j"$(nproc)" 2>/dev/null && sudo make install 2>/dev/null
            cd /tmp && rm -rf "$CMATRIX_TMP"
            echo "  cmatrix 编译安装完成"
        else
            echo "  ⚠️  cmatrix 源码克隆失败，跳过（不影响 raytool 使用）"
        fi
    fi

    # fortune — 如果还没装上，也源码编一个简易版
    if ! command -v fortune &> /dev/null; then
        echo "  ⚠️  fortune 未能安装，raytool 将使用内置毒鸡汤兜底"
    fi
}

_install_fun_tools || true

# 回到安装目录
cd "$INSTALL_DIR"

echo ""
if command -v cmatrix &> /dev/null; then
    echo "  ✅ cmatrix 已就绪"
else
    echo "  ⚠️  cmatrix 未安装（不影响使用，退出时字符雨会跳过）"
fi
if command -v fortune &> /dev/null; then
    echo "  ✅ fortune 已就绪"
else
    echo "  ⚠️  fortune 未安装（将使用内置毒鸡汤语录）"
fi

# ──────────── 4. 创建数据目录 & 写入全局配置 ────────────

DATA_DIR_EXPANDED=$(eval echo "$DATA_DIR")

# 尝试创建共享数据目录，失败则回退到本地
if mkdir -p "$DATA_DIR_EXPANDED" 2>/dev/null; then
    echo "数据目录: $DATA_DIR_EXPANDED"
    GLOBAL_CONFIG="$DATA_DIR_EXPANDED/raytool_global_config.yaml"
else
    echo ""
    echo "⚠️  无法创建共享数据目录: $DATA_DIR_EXPANDED"
    echo "   自动回退到本地数据目录..."

    # 回退到 ~/.raytool_data
    DATA_DIR_EXPANDED="$HOME/.raytool_data"
    DATA_DIR="$DATA_DIR_EXPANDED"
    mkdir -p "$DATA_DIR_EXPANDED"
    echo "   数据目录已改为: $DATA_DIR_EXPANDED"

    # 配置文件也放到本地
    GLOBAL_CONFIG="$HOME/.raytoolconfig"
fi

cat > "$GLOBAL_CONFIG" << CONFEOF
namespace: $NAMESPACE
yaml_dir: ''
prewarm_dir: ''
default_log_lines: $LOG_LINES
default_shell: $DEFAULT_SHELL
data_dir: $DATA_DIR

# ── 占卡 (occupy) 配置 ──
occupy_image: '054486717055.dkr.ecr.ap-southeast-3.amazonaws.com/youtu-agent:slime0401.h200'
occupy_conda_env: agent-lightning
occupy_pvc_name: fsx-claim
occupy_fsx_subpath: youtu-agent/zhijianzhou
occupy_cpu_request: 64
occupy_batch_size: 4
occupy_task_types:
  - retool
  - search
  - swebench
occupy_model_names:
  - qwen3
  - qwen25
occupy_host_local: /mnt/k8s-disks/0
occupy_host_cache: /opt/dlami/nvme/.cache
occupy_host_checkpoints: /opt/dlami/nvme/checkpoints/

# ── 预热 (prewarm) 配置 ──
pause_image: 'gcr.io/google_containers/pause:3.2'
instance_types:
  - ml.p5en.48xlarge
  - ml.p5e.48xlarge
  - ml.p5.48xlarge
  - ml.p4d.24xlarge
  - ml.p4de.24xlarge
  - ml.g5.48xlarge

# ── 端口转发配置 ──
default_remote_port: 8265
CONFEOF

echo "全局配置: $GLOBAL_CONFIG"
echo ""

# ──────────── 4.1 生成预置用户配置 ────────────

RAYTOOL_CONFIG_DIR="$DATA_DIR_EXPANDED/.raytoolconfig"
PRESET_USERS_FILE="$RAYTOOL_CONFIG_DIR/preset_users.yaml"

mkdir -p "$RAYTOOL_CONFIG_DIR" 2>/dev/null || true

if [ ! -f "$PRESET_USERS_FILE" ]; then
    cat > "$PRESET_USERS_FILE" << 'USERSEOF'
users:
  - username: Trump
    display_name: 川普
USERSEOF
    echo "预置用户配置: $PRESET_USERS_FILE (新建)"
else
    echo "预置用户配置: $PRESET_USERS_FILE (已存在，保留)"
fi

echo ""
echo "  💡 yaml_dir / prewarm_dir 不再写入全局配置"
echo "     每个用户登录后在「用户管理 → 个人设置」中自行配置"
echo "  💡 预置用户列表可在 $PRESET_USERS_FILE 中编辑"
echo ""

# 清理旧版 ~/.raytoolconfig（如果配置不在这里，才提示清理）
if [ -f "$HOME/.raytoolconfig" ] && [ "$GLOBAL_CONFIG" != "$HOME/.raytoolconfig" ]; then
    echo "⚠️  检测到旧版配置 ~/.raytoolconfig"
    echo "   新版 raytool 已改用共享全局配置: $GLOBAL_CONFIG"
    read -p "   是否删除旧配置? [y/N]: " DEL_OLD
    if [[ "$DEL_OLD" =~ ^[Yy]$ ]]; then
        rm -f "$HOME/.raytoolconfig"
        echo "   已删除 ~/.raytoolconfig"
    else
        echo "   保留旧配置（它会作为本地覆盖生效）"
    fi
    echo ""
fi

# ──────────── 5. 验证安装 ────────────

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

# ──────────── 6. 完成 ────────────

echo ""
echo "╭──────────────────────────────────╮"
echo "│   ✅ RayTool v2.1 安装完成！     │"
echo "╰──────────────────────────────────╯"
echo ""
echo "使用方法："
echo "   raytool              # 交互式主菜单（需先登录）"
echo "   raytool status       # 集群概况"
echo "   raytool list         # 查看任务列表"
echo "   raytool logs         # 查看日志"
echo "   raytool submit       # 提交任务"
echo "   raytool exec         # 进入容器"
echo "   raytool occupy       # GPU 占卡"
echo ""
echo "v2.1 新特性："
echo "   - 全局配置存储在共享目录，安装一次全员可用"
echo "   - 个人偏好随用户走（yaml_dir / prewarm_dir 等）"
echo "   - 不再依赖每人维护 ~/.raytoolconfig"
echo ""
echo "如果 raytool 命令未生效，请执行: source $SHELL_RC"
echo ""
