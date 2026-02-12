#!/bin/bash
# ═══════════════════════════════════════════════
#  RayTool 趣味工具安装脚本
#  安装 cmatrix (黑客帝国字符雨) + fortune (毒鸡汤)
#  使用方法：bash install_fun.sh
# ═══════════════════════════════════════════════

echo ""
echo "╭──────────────────────────────────╮"
echo "│  🎬 趣味工具安装                 │"
echo "│  cmatrix (字符雨) + fortune (鸡汤)│"
echo "╰──────────────────────────────────╯"
echo ""

# ──────────── 检测包管理器 ────────────

if command -v apt-get &> /dev/null; then
    PKG_MGR="apt"
elif command -v dnf &> /dev/null; then
    PKG_MGR="dnf"
elif command -v yum &> /dev/null; then
    PKG_MGR="yum"
elif command -v brew &> /dev/null; then
    PKG_MGR="brew"
else
    PKG_MGR="unknown"
fi

echo "检测到包管理器: $PKG_MGR"
echo ""

# ──────────── 安装 fortune ────────────

echo "────────────────────────────────────"
echo "  📦 安装 fortune (毒鸡汤语录)"
echo "────────────────────────────────────"

if command -v fortune &> /dev/null; then
    echo "  ✅ fortune 已安装: $(which fortune)"
else
    case "$PKG_MGR" in
        apt)
            echo "  → sudo apt-get install -y fortune-mod"
            sudo apt-get update -qq 2>/dev/null
            sudo apt-get install -y fortune-mod
            ;;
        dnf)
            echo "  → sudo dnf install -y fortune-mod"
            sudo dnf install -y fortune-mod 2>/dev/null || {
                echo "  ⚠️  dnf 源中无 fortune-mod，尝试 EPEL..."
                sudo dnf install -y epel-release 2>/dev/null
                sudo dnf install -y fortune-mod 2>/dev/null
            }
            ;;
        yum)
            echo "  → sudo yum install -y fortune-mod"
            sudo yum install -y fortune-mod 2>/dev/null || {
                echo "  ⚠️  yum 源中无 fortune-mod，尝试 EPEL..."
                sudo yum install -y epel-release 2>/dev/null
                sudo yum install -y fortune-mod 2>/dev/null
            }
            ;;
        brew)
            echo "  → brew install fortune"
            brew install fortune
            ;;
        *)
            echo "  ⚠️  未知包管理器，跳过 fortune 安装"
            ;;
    esac

    if command -v fortune &> /dev/null; then
        echo "  ✅ fortune 安装成功!"
    else
        echo "  ⚠️  fortune 安装失败（raytool 会用内置毒鸡汤兜底，不影响使用）"
    fi
fi

echo ""

# ──────────── 安装 cmatrix ────────────

echo "────────────────────────────────────"
echo "  📦 安装 cmatrix (黑客帝国字符雨)"
echo "────────────────────────────────────"

if command -v cmatrix &> /dev/null; then
    echo "  ✅ cmatrix 已安装: $(which cmatrix)"
else
    INSTALLED=false

    # 先尝试包管理器直接装
    case "$PKG_MGR" in
        apt)
            echo "  → sudo apt-get install -y cmatrix"
            sudo apt-get install -y cmatrix 2>/dev/null && INSTALLED=true
            ;;
        brew)
            echo "  → brew install cmatrix"
            brew install cmatrix 2>/dev/null && INSTALLED=true
            ;;
        dnf|yum)
            echo "  → $PKG_MGR 源中通常无 cmatrix，将从源码编译"
            ;;
    esac

    # 包管理器装不上 → 源码编译
    if [ "$INSTALLED" = false ]; then
        echo ""
        echo "  🔧 从源码编译 cmatrix ..."

        # 安装编译依赖
        echo "  → 安装编译依赖 (gcc make cmake ncurses-devel git) ..."
        case "$PKG_MGR" in
            dnf)
                sudo dnf install -y gcc make cmake ncurses-devel git 2>/dev/null
                ;;
            yum)
                sudo yum install -y gcc make cmake ncurses-devel git 2>/dev/null
                ;;
            apt)
                sudo apt-get install -y gcc make cmake libncurses5-dev git 2>/dev/null
                ;;
        esac

        # 克隆并编译
        CMATRIX_TMP="/tmp/cmatrix_build_$$"
        echo "  → git clone cmatrix ..."
        git clone --depth 1 https://github.com/abishekvashok/cmatrix.git "$CMATRIX_TMP" 2>/dev/null

        if [ -d "$CMATRIX_TMP" ]; then
            cd "$CMATRIX_TMP"
            mkdir -p build && cd build
            echo "  → cmake ..."
            cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local 2>/dev/null
            echo "  → make ..."
            make -j"$(nproc)" 2>/dev/null
            echo "  → make install ..."
            sudo make install 2>/dev/null
            cd /tmp && rm -rf "$CMATRIX_TMP"
            INSTALLED=true
        else
            echo "  ❌ 源码克隆失败，请检查网络"
        fi
    fi

    if command -v cmatrix &> /dev/null; then
        echo "  ✅ cmatrix 安装成功!"
    else
        echo "  ⚠️  cmatrix 安装失败（不影响 raytool 使用，退出时字符雨会跳过）"
    fi
fi

# ──────────── 结果总览 ────────────

echo ""
echo "╭──────────────────────────────────╮"
echo "│  安装结果                         │"
echo "╰──────────────────────────────────╯"
echo ""

if command -v cmatrix &> /dev/null; then
    echo "  ✅ cmatrix  — $(which cmatrix)"
else
    echo "  ❌ cmatrix  — 未安装"
fi

if command -v fortune &> /dev/null; then
    echo "  ✅ fortune  — $(which fortune)"
else
    echo "  ❌ fortune  — 未安装（内置兜底可用）"
fi

echo ""
echo "测试命令："
echo "  cmatrix         # 字符雨 (按 q 退出)"
echo "  fortune -s      # 随机短语录"
echo ""
