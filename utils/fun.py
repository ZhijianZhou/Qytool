"""趣味彩蛋模块 — 调用系统 fortune / cmatrix，内置兜底"""
import os
import random
import select as _select
import shutil
import signal
import subprocess
import sys
import termios
import time
import tty

from rich.console import Console

console = Console()

# ──────────────────────── 工具函数 ────────────────────────

def _cmd_exists(cmd: str) -> bool:
    """检查系统命令是否存在"""
    return shutil.which(cmd) is not None


# ──────────────────────── 毒鸡汤 fortune ────────────────────────

# 内置兜底语录（系统没装 fortune 时使用）
_BUILTIN_FORTUNES = [
    "努力不一定成功，但不努力真的好舒服。",
    "条条大路通罗马，但有人就住在罗马。",
    "世上无难事，只要肯放弃。",
    "上帝为你关了一扇门，然后就去睡觉了。",
    "比你优秀的人还在努力，不过跟你没什么关系。",
    "GPU 占不到怎么办？多占几次就习惯了。",
    "训练 Loss 不降了？因为它已经到了它的极限了。",
    "深度学习的深度，不如你的黑眼圈深。",
    "没有什么是一张 A100 解决不了的，如果有，那就八张。",
    "Debug 到凌晨三点，发现是 import 拼错了。",
    "你以为你在炼丹，其实丹在炼你。",
    "模型说：我不想收敛了，我想发散一下。",
    "人生就像 Kubernetes Pod，你不知道什么时候就被 Evicted 了。",
    "别看今天 GPU 满了，明天可能还是满的。",
    "OOM 不可怕，可怕的是你不知道为什么 OOM。",
    "Ray 集群还没起来？没事，人生也经常起不来。",
    "今天的训练炸了没关系，反正昨天也炸了。",
    "deadline 是第一生产力，GPU 是第二生产力。",
    "能用 GPU 解决的问题，都不是问题。问题是你没有 GPU。",
    "不是我不想下班，是 Loss 它不想收敛。",
    "代码能跑就行，别问为什么能跑。",
    "你删掉的那行代码，可能是唯一正确的一行。",
    "重构之前，代码还能跑。重构之后，只有你在跑。",
    "我不是在调参，我是在随机搜索人生的意义。",
    "所有的 Bug 都是 Feature，只要产品经理不知道。",
]


def show_fortune():
    """显示一条 fortune 语录，优先用系统命令，没装则用内置兜底"""
    fortune_text = None

    if _cmd_exists("fortune"):
        try:
            result = subprocess.run(
                ["fortune", "-s"],  # -s 短语录
                capture_output=True, text=True, timeout=3,
            )
            if result.returncode == 0 and result.stdout.strip():
                fortune_text = result.stdout.strip()
        except Exception:
            pass

    if not fortune_text:
        fortune_text = random.choice(_BUILTIN_FORTUNES)

    console.print()
    console.print(f"  [bold yellow]🥠 每日一毒[/bold yellow]  [italic dim]{fortune_text}[/italic dim]")


# ──────────────────────── 黑客帝国字符雨 ────────────────────────

# Python 内置字符雨的字符池
_MATRIX_CHARS = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "ｱｲｳｴｵｶｷｸｹｺｻｼｽｾｿﾀﾁﾂﾃﾄﾅﾆﾇﾈﾉﾊﾋﾌﾍﾎﾏﾐﾑﾒﾓﾔﾕﾖﾗﾘﾙﾚﾛﾜﾝ"
    "@#$%&*<>~"
)


def _python_matrix_rain(duration: float = 3.0):
    """纯 Python 实现的字符雨（cmatrix 不可用时的兜底方案）"""
    try:
        cols, rows = shutil.get_terminal_size((80, 24))
    except Exception:
        cols, rows = 80, 24

    cols = min(cols, 160)
    display_rows = min(rows - 2, 30)

    # 每列的雨滴头位置，-1 表示休眠
    drops = [-1] * cols
    # 字符网格 + 亮度
    grid = [[" "] * cols for _ in range(display_rows)]
    bright = [[0] * cols for _ in range(display_rows)]

    # 隐藏光标
    sys.stdout.write("\033[?25l\033[2J\033[H")
    sys.stdout.flush()

    fps = 14
    frame_time = 1.0 / fps
    total_frames = int(duration * fps)

    try:
        for _ in range(total_frames):
            for c in range(cols):
                # 随机激活新雨滴
                if drops[c] == -1 and random.random() < 0.07:
                    drops[c] = 0

                if drops[c] >= 0:
                    r = drops[c]
                    if r < display_rows:
                        grid[r][c] = random.choice(_MATRIX_CHARS)
                        bright[r][c] = 9
                    drops[c] += 1
                    if drops[c] > display_rows + 10:
                        drops[c] = -1

                # 亮度衰减
                for r in range(display_rows):
                    if bright[r][c] > 0:
                        bright[r][c] -= 1

            # 渲染
            buf = []
            for r in range(display_rows):
                row_buf = []
                for c in range(cols):
                    b = bright[r][c]
                    ch = grid[r][c]
                    if b >= 7:
                        row_buf.append(f"\033[1;97m{ch}\033[0m")
                    elif b >= 4:
                        row_buf.append(f"\033[1;32m{ch}\033[0m")
                    elif b >= 2:
                        row_buf.append(f"\033[32m{ch}\033[0m")
                    elif b >= 1:
                        row_buf.append(f"\033[2;32m{ch}\033[0m")
                    else:
                        row_buf.append(" ")
                buf.append("".join(row_buf))

            sys.stdout.write("\033[H" + "\n".join(buf))
            sys.stdout.flush()
            time.sleep(frame_time)
    except KeyboardInterrupt:
        pass
    finally:
        # 恢复光标 + 清屏
        sys.stdout.write("\033[?25h\033[2J\033[H")
        sys.stdout.flush()


def run_cmatrix(duration: int = 3):
    """
    运行字符雨效果
    优先用系统 cmatrix（通过 os.system 继承终端），没装则用 Python 内置版
    """
    if _cmd_exists("cmatrix"):
        # 用 os.system 直接执行，继承当前 TTY
        # SIGALRM 定时退出
        def _alarm_handler(signum, frame):
            raise SystemExit(0)

        old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        try:
            signal.alarm(duration)
            os.system("cmatrix -b -s -u 2")
        except (SystemExit, KeyboardInterrupt):
            pass
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
            # 清屏恢复
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.flush()
        return True
    else:
        # 没有 cmatrix，用 Python 内置字符雨
        console.print("[dim]  (cmatrix 未安装，使用内置字符雨 — bash install_fun.sh 可安装)[/dim]")
        _python_matrix_rain(duration=duration)
        return True


# ──────────────────────── 按键检测工具 ────────────────────────

def _kbhit(timeout: float = 0) -> bool:
    """非阻塞检测是否有按键输入（Unix only）"""
    try:
        rlist, _, _ = _select.select([sys.stdin], [], [], timeout)
        return bool(rlist)
    except Exception:
        return False


def _flush_stdin():
    """清空 stdin 输入缓冲区，防止屏保退出时的按键泄漏到后续 inquirer"""
    try:
        termios.tcflush(sys.stdin.fileno(), termios.TCIFLUSH)
    except Exception:
        pass
    # 再用 non-blocking select 把残留字符全部读掉
    try:
        while True:
            rlist, _, _ = _select.select([sys.stdin], [], [], 0)
            if not rlist:
                break
            sys.stdin.read(1)
    except Exception:
        pass


# ──────────────────────── 字符雨屏保（无限循环，按任意键退出） ────────────────────────

def screensaver_matrix():
    """
    字符雨屏保模式 — 无限循环直到用户按任意键
    优先用 cmatrix（按 q 退出），没装则用 Python 内置版
    """
    if _cmd_exists("cmatrix"):
        # cmatrix 自带按 q 退出，直接运行不限时
        try:
            os.system("cmatrix -b -s -u 2")
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.flush()
            _flush_stdin()
        return

    # Python 内置字符雨屏保
    try:
        cols, rows = shutil.get_terminal_size((80, 24))
    except Exception:
        cols, rows = 80, 24

    cols = min(cols, 160)
    display_rows = min(rows - 2, 30)

    drops = [-1] * cols
    grid = [[" "] * cols for _ in range(display_rows)]
    bright = [[0] * cols for _ in range(display_rows)]

    # 切换终端为 raw 模式以检测任意按键
    fd = sys.stdin.fileno()
    try:
        old_settings = termios.tcgetattr(fd)
    except Exception:
        old_settings = None

    sys.stdout.write("\033[?25l\033[2J\033[H")
    sys.stdout.flush()

    if old_settings:
        tty.setraw(fd)

    fps = 14
    frame_time = 1.0 / fps

    try:
        while True:
            # 检测按键（非阻塞）
            if _kbhit(0):
                # 读掉按键字符
                try:
                    sys.stdin.read(1)
                except Exception:
                    pass
                break

            for c in range(cols):
                if drops[c] == -1 and random.random() < 0.07:
                    drops[c] = 0
                if drops[c] >= 0:
                    r = drops[c]
                    if r < display_rows:
                        grid[r][c] = random.choice(_MATRIX_CHARS)
                        bright[r][c] = 9
                    drops[c] += 1
                    if drops[c] > display_rows + 10:
                        drops[c] = -1
                for r in range(display_rows):
                    if bright[r][c] > 0:
                        bright[r][c] -= 1

            buf = []
            for r in range(display_rows):
                row_buf = []
                for c in range(cols):
                    b = bright[r][c]
                    ch = grid[r][c]
                    if b >= 7:
                        row_buf.append(f"\033[1;97m{ch}\033[0m")
                    elif b >= 4:
                        row_buf.append(f"\033[1;32m{ch}\033[0m")
                    elif b >= 2:
                        row_buf.append(f"\033[32m{ch}\033[0m")
                    elif b >= 1:
                        row_buf.append(f"\033[2;32m{ch}\033[0m")
                    else:
                        row_buf.append(" ")
                buf.append("".join(row_buf))

            sys.stdout.write("\033[H" + "\n".join(buf))
            sys.stdout.flush()
            time.sleep(frame_time)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        if old_settings:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        sys.stdout.write("\033[?25h\033[2J\033[H")
        sys.stdout.flush()
        _flush_stdin()
