"""dola-pool 配置：全部走环境变量，带默认值。"""
import os
from pathlib import Path


def _load_local_env():
    """Load ignored .env.local for local/tunnel runs; real environment wins."""
    path = Path(__file__).with_name(".env.local")
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_local_env()
HOST = os.getenv("DOLA_HOST", "0.0.0.0")
PORT = int(os.getenv("DOLA_PORT", "8000"))

# 服务对外 API Key（逗号分隔多个；留空 = 不鉴权，仅内网调试用）
API_KEYS = [k.strip() for k in os.getenv("DOLA_API_KEYS", "").split(",") if k.strip()]

# 号池 cookie 文件（一行一个 dola.com cookie）
COOKIES_FILE = os.getenv("DOLA_COOKIES_FILE", "cookies.txt")

# 同时跑的视频任务上限（控制并发，防风控 / 防同号并发）
MAX_CONCURRENCY = int(os.getenv("DOLA_MAX_CONCURRENCY", "3"))

# 全局待处理任务上限（queued + processing），0 = 不限制。
MAX_PENDING_TASKS = int(os.getenv("DOLA_MAX_PENDING_TASKS", "100"))

# 视频生成超时（秒）
VIDEO_TIMEOUT = int(os.getenv("DOLA_VIDEO_TIMEOUT", "300"))

# SQLite 任务库
DB_PATH = os.getenv("DOLA_DB_PATH", "tasks.db")

# 视频下载目录（出片后下载转存，FastAPI 以静态文件方式对外提供）
DOWNLOAD_DIR = os.getenv("DOLA_DOWNLOAD_DIR", "downloads")

# 浏览器显式代理（P0 结论：不能依赖系统代理，Clash 规则变化会把 dola 分流到直连被墙）。
# 必须是指向 JP/KR 出口的代理；留空 = 跟随系统代理（仅调试用）。
PROXY = os.getenv("DOLA_PROXY", "http://127.0.0.1:7890")

# 浏览器是否无头运行（login.py 永远有头）
HEADLESS = os.getenv("DOLA_HEADLESS", "1") == "1"

# 对外返回视频 URL 的基址（FastAPI 静态文件服务）
PUBLIC_BASE = os.getenv("DOLA_PUBLIC_BASE", f"http://127.0.0.1:{PORT}")

# 管理面板密码（留空 = 面板不鉴权，开发模式）
ADMIN_KEY = os.getenv("DOLA_ADMIN_KEY", "")


# Dola 30 秒/无水印 Chromium 扩展（unpacked extension）
EXTENSION_DIR = os.getenv("DOLA_EXTENSION_DIR", "extensions/dola30")
EXTENSION_ENABLED = os.getenv("DOLA_EXTENSION_ENABLED", "1") == "1"

# Dola 每日限流恢复时区；默认按日本日期零点恢复。
LIMIT_RESET_TZ = os.getenv("DOLA_LIMIT_RESET_TZ", "Asia/Tokyo")

# 生成前余额预检的保守最低积分；Dola 当前 2.5/30s 实测成本为 2。
VIDEO_REQUIRED_POINTS = int(os.getenv("DOLA_VIDEO_REQUIRED_POINTS", "2"))


# 公网参考图片下载限制
REFERENCE_IMAGE_MAX_BYTES = int(os.getenv("DOLA_REFERENCE_IMAGE_MAX_BYTES", str(15 * 1024 * 1024)))
REFERENCE_DOWNLOAD_TIMEOUT = int(os.getenv("DOLA_REFERENCE_DOWNLOAD_TIMEOUT", "60"))
REFERENCE_IMAGE_MAX_COUNT = int(os.getenv("DOLA_REFERENCE_IMAGE_MAX_COUNT", "30"))

# 带参考图片的任务给 Dola 更长的异步生成窗口（秒）。
REFERENCE_VIDEO_TIMEOUT = int(os.getenv("DOLA_REFERENCE_VIDEO_TIMEOUT", "900"))
