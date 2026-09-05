"""patchright persistent context 统一启动：显式代理 + 反检测参数。

代理必须显式传（不依赖系统代理——系统代理规则变化曾把 dola 分流到直连被墙，
见 DEVELOPMENT.md 第 5 节）。所有需要打开 dola 的脚本都走这里。
"""
from pathlib import Path
from urllib.parse import unquote, urlsplit

import config
import proxy_store


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


def proxy_kwargs_for(account: str | None = None,
                     proxy_info: dict | None = None) -> dict | None:
    """解析某个账号（或 override）应使用的 Playwright proxy 参数。

    账号已绑定代理 → 用绑定代理；否则回落 config.DOLA_PROXY。
    """
    info = proxy_info or (proxy_store.get_account_proxy(account) if account else None)
    if info:
        kwargs = {
            "server": f"{info['protocol']}://{info['host']}:{info['port']}",
        }
        if info.get("username"):
            kwargs["username"] = info["username"]
        if info.get("password"):
            kwargs["password"] = info["password"]
        return kwargs
    raw = (config.PROXY or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "http://" + raw
    parts = urlsplit(raw)
    if not parts.hostname:
        return None
    port = parts.port or (443 if parts.scheme == "https" else 80)
    kwargs = {"server": f"{parts.scheme}://{parts.hostname}:{port}"}
    if parts.username:
        kwargs["username"] = unquote(parts.username)
    if parts.password:
        kwargs["password"] = unquote(parts.password)
    return kwargs


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
    proxy_kwargs = proxy_kwargs_for(account)
    if proxy_kwargs:
        kwargs["proxy"] = proxy_kwargs
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
            if "/security/region-restricted" in page.url:
                return False
            has_chat = bool(await page.evaluate(
                """() => !!(document.querySelector('textarea')
                        || document.querySelector('[contenteditable="true"]')
                        || document.querySelector('input[type="text"]'))"""
            ))
            # 匿名访问时 Dola 页面也有输入框，必须同时确认没有可见的登录入口
            visible_login = bool(await page.evaluate(
                """() => {
                    const terms = ['ログイン', 'ログ イン', 'log in', 'login', 'sign in', '登录', '登入'];
                    return [...document.querySelectorAll('button,a,[role="button"]')].some(n => {
                        const t = (n.innerText || '').trim().toLowerCase();
                        return t && terms.includes(t) && n.offsetParent !== null;
                    });
                }"""
            ))
            return bool(has_chat and not visible_login)
        finally:
            await context.close()
