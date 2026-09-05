"""通过 Cookie 直接导入 dola 账号（替代 Google OAuth 登录）。

接受 Cookie-Editor / EditThisCookie 之类扩展导出的 JSON（对象含 cookies 数组），
或直接传 cookie 对象数组。流程：
1. 在 accounts/<name>/ 创建 persistent profile；
2. 用 add_cookies 写入 dola.com cookie；
3. 打开 https://www.dola.com/chat 验证登录态；
4. 验证通过才保留 profile 并返回成功，否则清理临时 profile 并报错。

与 add_account.py（Google OAuth）产出的 profile 完全同构，
后续浏览器号池（video_worker / browser_pool）可直接使用。
"""
import asyncio
import shutil
import time
from pathlib import Path

from patchright.async_api import async_playwright

import config
from browser import LAUNCH_ARGS, check_login_state


def normalize_cookies(data) -> list[dict]:
    """把扩展导出 JSON / cookie 数组统一成普通 dict 列表。"""
    if isinstance(data, dict):
        data = data.get("cookies") or data.get("cookie") or [data]
    if not isinstance(data, list):
        raise ValueError("cookie 格式无法识别：需要数组，或含 cookies 数组的对象")
    out: list[dict] = []
    for c in data:
        if not isinstance(c, dict) or not c.get("name") or c.get("value") is None:
            raise ValueError("cookie 缺少 name/value 字段")
        out.append(c)
    return out


def to_playwright_cookies(cookies: list[dict]) -> list[dict]:
    """转为 playwright add_cookies 可接受的格式。"""
    out: list[dict] = []
    for c in cookies:
        domain = str(c.get("domain") or "").strip().lstrip(".")
        if not domain:
            raise ValueError("cookie 缺少 domain 字段")
        item = {
            "name": str(c["name"]),
            "value": str(c["value"]),
            "domain": domain,
            "path": str(c.get("path") or "/"),
        }
        # 会话 cookie 不写 expires 时 Chromium 不会落盘到 profile，
        # 因此统一转成“本地 90 天过期”的持久 cookie（服务端有效期不受影响）。
        exp = None
        if not c.get("isSession"):
            exp = c.get("expiresUtc") or c.get("expires")
        if exp is None or not str(exp).strip():
            exp = time.time() + 90 * 86400
        else:
            try:
                exp = float(exp)
                if exp > 1e12:
                    exp = exp / 1000
            except (TypeError, ValueError):
                exp = time.time() + 90 * 86400
        item["expires"] = exp
        if "isHttpOnly" in c:
            item["httpOnly"] = bool(c["isHttpOnly"])
        if "isSecure" in c:
            item["secure"] = bool(c["isSecure"])
        if c.get("sameSite"):
            ss = str(c["sameSite"]).lower()
            item["sameSite"] = {
                "strict": "Strict",
                "lax": "Lax",
                "none": "None",
                "no_restriction": "None",
            }.get(ss, "Lax")
        out.append(item)
    return out


async def import_cookie_account(name: str, cookie_data,
                                require_login: bool = True) -> dict:
    """把 cookie 写入 accounts/<name>/ profile 并验证登录态。

    验证失败会删除刚创建的 profile（避免无效账号进入号池）。
    require_login=False 时只写入 cookie 不验证（供无 JP/KR 代理时先导入，
    之后在面板点“验证”复核）。
    """
    name = (name or "").strip()
    if not name or any(ch in name for ch in "/\\"):
        raise ValueError("账号名不合法")
    profile_dir = Path("accounts") / name
    if profile_dir.exists():
        raise FileExistsError(f"账号已存在: {name}")
    cookies = normalize_cookies(cookie_data)
    if not cookies:
        raise ValueError("cookie 列表为空")
    pw_cookies = to_playwright_cookies(cookies)

    profile_dir.mkdir(parents=True, exist_ok=False)
    session_cookie_present = False
    page_error = ""
    try:
        async with async_playwright() as p:
            kwargs = {
                "headless": config.HEADLESS,
                "args": list(LAUNCH_ARGS),
                "locale": "ja-JP",
                "timezone_id": "Asia/Tokyo",
            }
            if config.PROXY:
                kwargs["proxy"] = {"server": config.PROXY}
            ctx = await p.chromium.launch_persistent_context(
                str(profile_dir), **kwargs
            )
            try:
                page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                await ctx.add_cookies(pw_cookies)
                try:
                    await page.goto(
                        "https://www.dola.com/chat",
                        timeout=60000,
                        wait_until="domcontentloaded",
                    )
                    await page.wait_for_timeout(8000)
                except Exception as exc:
                    page_error = str(exc)[:300]
                stored = await ctx.cookies("https://www.dola.com")
                session_cookie_present = any(
                    c["name"] == "sessionid" and c.get("value")
                    for c in stored
                )
            finally:
                await ctx.close()
    except Exception as exc:
        shutil.rmtree(profile_dir, ignore_errors=True)
        raise RuntimeError(f"写入 cookie profile 失败: {exc}") from exc

    if require_login:
        # 独立再开一次 context 做真实登录态验证（与面板“验证”按钮同逻辑）
        try:
            verified = await check_login_state(name)
        except Exception as exc:
            verified = False
            page_error = page_error or str(exc)[:300]

        if not verified:
            shutil.rmtree(profile_dir, ignore_errors=True)
            reason = page_error or "dola.com 未返回已登录状态"
            raise RuntimeError(
                f"cookie 验证未通过（profile 已清理）: {reason}"
            )
    else:
        verified = False

    return {
        "ok": True,
        "account": name,
        "cookie_count": len(cookies),
        "session_cookie_present": session_cookie_present,
        "profile": str(profile_dir),
        "verified": verified,
    }
