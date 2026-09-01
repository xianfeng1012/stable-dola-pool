"""patchright persistent context 统一启动：显式代理 + 反检测参数。

代理必须显式传（不依赖系统代理——系统代理规则变化曾把 dola 分流到直连被墙，
见 DEVELOPMENT.md 第 5 节）。所有需要打开 dola 的脚本都走这里。
"""
from pathlib import Path

import config


LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    # ---- 内存优化（2026-08-31，服务器内存紧张无法升级）----
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-component-update",
    "--disable-background-networking",
    "--renderer-process-limit=2",
    "--js-flags=--max-old-space-size=256",
    "--disable-features=Translate,MediaRouter,BackForwardCache",
]


async def launch_account_context(p, account: str, headless: bool = None, use_extension: bool = False):
    """启动 accounts/<account> profile，返回 BrowserContext。调用方负责 close。

    p: async_playwright() 实例
    headless: None = 用 config.HEADLESS
    """
    profile_dir = Path("accounts") / account
    if not profile_dir.exists():
        raise FileNotFoundError(
            f"账号 profile 不存在: {profile_dir}（先跑 python login.py {account}）"
        )
    launch_headless = config.HEADLESS if headless is None else headless
    args = list(LAUNCH_ARGS)
    if use_extension:
        if not config.EXTENSION_ENABLED:
            raise RuntimeError("Dola 扩展已禁用（DOLA_EXTENSION_ENABLED=0）")
        extension_dir = Path(config.EXTENSION_DIR).resolve()
        if not extension_dir.exists():
            raise FileNotFoundError(f"Dola 扩展目录不存在: {extension_dir}")
        # Chrome debugger API 扩展需要有头模式；扩展会拦截 skill/action-bar 响应。
        launch_headless = False
        args.extend([
            f"--disable-extensions-except={extension_dir}",
            f"--load-extension={extension_dir}",
        ])
    kwargs = {
        "headless": launch_headless,
        "args": args,
        "locale": "ja-JP",
        "timezone_id": "Asia/Tokyo",
    }
    if config.PROXY:
        kwargs["proxy"] = {"server": config.PROXY}
    return await p.chromium.launch_persistent_context(str(profile_dir), **kwargs)


def cookie_value(cookies: list, name: str) -> str:
    """从 context.cookies() 结果里取指定 cookie 的值"""
    return next((c["value"] for c in cookies if c["name"] == name and c["value"]), "")

async def check_login_state(account: str) -> bool:
    """无头打开 dola，返回登录态是否有效（sessionid + 输入框存在）。

    供 verify_login.py CLI 与面板「验证」按钮共用。
    """
    from patchright.async_api import async_playwright
    async with async_playwright() as p:
        context = await launch_account_context(p, account)
        try:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto("https://www.dola.com/chat", timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            cookies = await context.cookies("https://www.dola.com")
            if not cookie_value(cookies, "sessionid"):
                return False
            return bool(await page.evaluate(
                """() => !!(document.querySelector('textarea')
                        || document.querySelector('[contenteditable="true"]')
                        || document.querySelector('input[type="text"]'))"""
            ))
        finally:
            await context.close()
