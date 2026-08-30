"""视频生成 worker v2（UI 流 + 滑块求解）——当前主线。

链路：点「创建视频」→ 输入 → 回车（前端自带 a_bogus 签名，协议正确）
     → 若弹 bdcaptcha 滑块：OpenCV 识别缺口 + 拟人轨迹拖动
     → 前端自动重试提交 → 轮询 /im/chain/single 出片 → 下载到 downloads/

为什么走 UI 而不是页面内裸 fetch：裸 fetch 缺 webmssdk 的 a_bogus 签名，
必触发 shark 滑块（710022004）；UI 流由前端自己签名，只需过滑块这一关。

用法：python video_worker_ui.py <账号名> "提示词" [比例] [时长秒]
"""
import asyncio
import json
import random
import re
import sys
import time
from pathlib import Path

import aiohttp
from patchright.async_api import async_playwright

from gap import find_gap_x

import config
from browser import cookie_value, launch_account_context
from dola_client import CREDIT_FAIL_PATTERN, CreditError
from video_worker import POLL_JS, RiskControlError, _download, extract_unwatermarked_url

# Dola 返回的每日上限文案（当前日文 UI 实测：動画生成の 1 日あたりの上限に達しました。）
DAILY_LIMIT_PATTERN = re.compile(
    r"動画生成の\s*1日あたりの上限|每日(?:视频|影片)?生成.*(?:上限|限额|额度)|"
    r"daily.*(?:limit|quota)|(?:limit|quota).*per\s*day",
    re.IGNORECASE,
)


class AccountLimitedError(Exception):
    """当前 Dola 账号达到每日视频生成上限，应立即换下一个账号。"""


class CreditInsufficientError(Exception):
    """生成前已知积分不足，不提交视频，换下一个账号。"""


VIDEO_BTN = "text=動画を作成"          # ja-JP locale 下的入口文案
CAPTCHA_FRAME_KEY = "bdcaptcha.html"   # 字节 verifycenter 滑块 iframe


async def dismiss_popups(page):
    """关闭 Cookie 同意、桌面应用推广等可能拦截点击的弹层。"""
    for sel in ("OK", "同意する", "Accept All", "閉じる", "Close"):
        try:
            loc = page.get_by_text(sel, exact=True).first
            if await loc.count() and await loc.is_visible():
                await loc.click(timeout=1500)
                break
        except Exception:
            continue
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass
    await page.wait_for_timeout(500)



# 生成前只读检查：读取最近会话中的“剩余 N points/ポイント/积分”文案。
# 新账号如果没有历史余额文案，返回 balance=null，交给实际提交流程判断，不误拦截。
BALANCE_JS = r"""
async ({msToken, fp}) => {
  const params = new URLSearchParams({
    version_code: "20800", language: "ja", device_platform: "web",
    doubao_device_platform: "web", aid: "495671", real_aid: "495671",
    pkg_type: "release_version", pc_version: "3.32.62", doubao_pc_version: "3.32.62",
    region: "JP", sys_region: "JP", samantha_web: "1", web_platform: "browser",
    "use-olympus-account": "1", web_tab_id: crypto.randomUUID(),
  });
  if (msToken) params.set("msToken", msToken);
  if (fp) params.set("fp", fp);
  const headers = {
    "Content-Type": "application/json; encoding=utf-8",
    "agw-js-conv": "str", "Accept": "*/*",
  };
  const recent = await fetch("/im/chain/recent_conv?" + params.toString(), {
    method: "POST", headers,
    body: JSON.stringify({
      cmd: 3200,
      uplink_body: {pull_recent_conv_chain_uplink_body: {
        limit: 20, message_count_per_conv: 10, api_version: 1, conv_version: 0,
        direction: 3,
        option: {not_need_message: false, need_complete_conversation: true,
          need_coco_conversation: true, need_coco_bot: true,
          need_pc_pin_chain: true, pc_pin_query_type: 0},
      }},
      sequence_id: crypto.randomUUID(), channel: 2, version: "1",
    }), credentials: "include",
  });
  if (!recent.ok) return {ok: false, texts: []};
  const recentData = await recent.json();
  const body = recentData.downlink_body || {};
  const down = body.pull_recent_conv_chain_downlink_body || {};
  const cells = down.cells || [];
  const ids = cells.map(c => (c.conversation || {}).conversation_id || c.id)
    .filter(Boolean).slice(0, 10);
  if (!ids.length) return {ok: true, texts: []};

  const batch = await fetch("/im/chain/batch_single?" + params.toString(), {
    method: "POST", headers,
    body: JSON.stringify({
      cmd: 3101,
      uplink_body: {batch_pull_singe_chain_uplink_body: {
        conversation_type: 3, direction: 3, limit: 1,
        params: ids.map(conversation_id => ({conversation_id})),
        evaluate_ab_params: "", evaluate_common_params: "", ext: {},
      }},
      sequence_id: crypto.randomUUID(), channel: 2, version: "1",
    }), credentials: "include",
  });
  if (!batch.ok) return {ok: true, texts: []};
  const data = await batch.json();
  const texts = [];
  const seen = new Set();
  const walk = (v) => {
    if (typeof v === "string") {
      if ((v.includes("ポイント") || v.includes("积分") || v.toLowerCase().includes("points")
        || v.includes("上限") || v.includes("limit")) && v.length < 1200 && !seen.has(v)) {
        seen.add(v); texts.push(v);
      }
      try {
        const t = v.trim();
        if (t.startsWith("{") || t.startsWith("[")) walk(JSON.parse(t));
      } catch (e) {}
      return;
    }
    if (!v || typeof v !== "object") return;
    if (Array.isArray(v)) { for (const x of v) walk(x); return; }
    for (const x of Object.values(v)) walk(x);
  };
  walk(data);
  return {ok: true, texts: texts.slice(-80)};
}
""";


def find_captcha_frame(page):
    for f in page.frames:
        if CAPTCHA_FRAME_KEY in f.url:
            return f
    return None


async def _fetch_bytes(url: str) -> bytes:
    async with aiohttp.ClientSession() as s:
        async with s.get(url, proxy=config.PROXY or None) as r:
            return await r.read()



async def attach_reference_images(page, image_paths: list[str]) -> None:
    """通过 Dola 原生文件 input 上传参考图片，等待 prepare_upload/TOS 完成。"""
    if not image_paths:
        return
    file_input = page.locator('input[type="file"]').first
    await file_input.wait_for(state="attached", timeout=10000)
    events = []

    def on_response(response):
        url = response.url
        if "/alice/resource/prepare_upload" in url or "/upload/v1/" in url:
            events.append((response.status, url))

    page.on("response", on_response)
    try:
        await file_input.set_input_files(image_paths)
        expected = len(image_paths)
        deadline = time.time() + max(60, expected * 20)
        while time.time() < deadline:
            prepare_count = sum("/alice/resource/prepare_upload" in url and 200 <= status < 300
                                for status, url in events)
            tos_count = sum("/upload/v1/" in url and 200 <= status < 300
                            for status, url in events)
            # 缩略图出现且每个 prepare/TOS 都成功，才允许发送。
            thumb_count = await page.locator('img[alt]').count()
            if prepare_count >= expected and tos_count >= expected and thumb_count >= expected:
                await page.wait_for_timeout(800)
                print(f"[upload] 参考图片上传完成: {expected} 张", flush=True)
                return
            await page.wait_for_timeout(250)
        raise TimeoutError(
            f"参考图片上传超时: prepare={prepare_count}/{expected}, tos={tos_count}/{expected}"
        )
    finally:
        page.remove_listener("response", on_response)


def _gen_track(distance: float):
    """拟人轨迹：smootherstep 主行程 + 过冲回稳 + y 轴抖动。返回 [(dx,dy,dt_ms),...]"""
    steps = random.randint(45, 65)
    overshoot = random.uniform(3, 9)
    pts = []
    for i in range(1, steps + 1):
        t = i / steps
        s = 10 * t**3 - 15 * t**4 + 6 * t**5
        x = (distance + overshoot) * s
        y = random.uniform(-1.5, 1.5) if 0.1 < t < 0.95 else 0
        pts.append((x, y, random.randint(8, 22)))
    for i in range(1, random.randint(3, 5) + 1):
        pts.append((distance + overshoot * (1 - i / 5), random.uniform(-0.8, 0.8), random.randint(15, 30)))
    return pts


async def solve_slider(page, frame, attempt: int) -> bool:
    """在 bdcaptcha iframe 内识别缺口并拖动。成功返回 True。"""
    # 等两张图加载完成（naturalWidth>0），否则 scale 算错
    await frame.wait_for_selector("img", timeout=15000)
    await frame.evaluate("""async () => {
        const t0 = Date.now();
        while (Date.now() - t0 < 10000) {
            const imgs = [...document.images];
            if (imgs.length >= 2 && imgs.every(im => im.complete && im.naturalWidth > 0)) return;
            await new Promise(r => setTimeout(r, 200));
        }
        throw new Error("captcha images load timeout");
    }""")
    await page.wait_for_timeout(800)

    imgs = await frame.evaluate("""() => [...document.images].map(im => ({
        src: im.src, w: im.naturalWidth, h: im.naturalHeight,
        bw: im.getBoundingClientRect().width,
        left: im.getBoundingClientRect().left,
    }))""")
    bg = next((i for i in imgs if ".jpeg" in i["src"] or "-2." in i["src"]), None)
    piece = next((i for i in imgs if i is not bg and (".png" in i["src"] or "-1." in i["src"])), None)
    if not bg or not piece:
        print("  ✗ 未找到背景/滑块图", flush=True)
        return False

    bg_bytes = await _fetch_bytes(bg["src"])
    piece_bytes = await _fetch_bytes(piece["src"])
    Path("dbg_bg.jpg").write_bytes(bg_bytes)
    Path("dbg_piece.png").write_bytes(piece_bytes)

    gap_x, conf = find_gap_x(bg_bytes, piece_bytes)
    scale = bg["bw"] / bg["w"] if bg["w"] else 340 / 552
    distance = (gap_x - (piece["left"] - bg["left"]) / scale) * scale
    print(f"  [solve#{attempt}] gap_x={gap_x} conf={conf:.3f} scale={scale:.2f} distance={distance:.0f}px", flush=True)

    btn = frame.locator(".captcha-slider-btn")
    bb = await btn.bounding_box()
    if not bb:
        print("  ✗ 未找到拖动按钮 .captcha-slider-btn", flush=True)
        return False
    sx, sy = bb["x"] + bb["width"] / 2, bb["y"] + bb["height"] / 2
    await page.mouse.move(sx, sy)
    await page.wait_for_timeout(random.randint(150, 350))
    await page.mouse.down()
    await page.wait_for_timeout(random.randint(80, 180))
    for dx, dy, dt in _gen_track(distance):
        await page.mouse.move(sx + dx, sy + dy)
        await asyncio.sleep(dt / 1000)
    await page.wait_for_timeout(random.randint(120, 260))
    await page.mouse.up()

    for _ in range(10):
        await page.wait_for_timeout(700)
        if not find_captcha_frame(page):
            return True
    return False



_BALANCE_PATTERNS = (
    re.compile(r"(?:本日は|今日(?:还剩|剩余)?|今天).*?(\d+)\s*(?:ポイント|积分|points?)", re.I),
    re.compile(r"(?:remaining|left)\s*[:：]?\s*(\d+)\s*points?", re.I),
    re.compile(r"(?:还剩|剩余|还有)\s*(\d+)\s*(?:积分|点)", re.I),
)


def _parse_balance_texts(texts: list[str]) -> tuple[int | None, bool, str]:
    for text in texts:
        if DAILY_LIMIT_PATTERN.search(text):
            return None, True, text
    for text in texts:
        for pattern in _BALANCE_PATTERNS:
            match = pattern.search(text)
            if match:
                return int(match.group(1)), False, text
    return None, False, ""


async def _preflight_balance(page, ms_token: str, fp: str, required: int) -> dict:
    """尽力读取已知积分；未知时不误阻止新账号。"""
    try:
        result = await asyncio.wait_for(page.evaluate(
            BALANCE_JS, {"msToken": ms_token, "fp": fp}), timeout=30)
        balance, daily_limited, source = _parse_balance_texts(result.get("texts", []))
        if daily_limited:
            raise AccountLimitedError(f"账号每日生成上限: {source[:120]}")
        if balance is not None and balance < required:
            raise CreditInsufficientError(
                f"生成前积分不足: 当前 {balance}，本次至少需要 {required}（来源: {source[:120]}）"
            )
        return {"balance": balance, "source": source}
    except (AccountLimitedError, CreditInsufficientError):
        raise
    except Exception as e:
        print(f"  余额预检未知（继续提交，不误拦截）: {str(e)[:120]}", flush=True)
        return {"balance": None, "source": ""}


async def poll_conversation(account: str, page, context, conversation_id: str,
                            timeout: int, on_poll=None, on_balance=None) -> dict:
    """轮询已受理会话直到出片（不限时）；可被初次提交和服务重启恢复流程共同调用。"""
    cookies = await context.cookies("https://www.dola.com")
    ms_token, fp = cookie_value(cookies, "msToken"), cookie_value(cookies, "s_v_web_id")
    start = time.time()
    last_callback = 0.0
    while True:
        await asyncio.sleep(5)
        try:
            poll = await asyncio.wait_for(page.evaluate(
                POLL_JS, {"conversationId": conversation_id, "msToken": ms_token, "fp": fp}), timeout=30)
        except Exception as e:
            print(f"  轮询异常: {e}", flush=True)
            continue
        now = time.time()
        if on_poll and now - last_callback >= 30:
            on_poll(now)
            last_callback = now
        for text in poll.get("texts", []):
            balance, _, source = _parse_balance_texts([text])
            if balance is not None and on_balance:
                on_balance(balance, source)
            if DAILY_LIMIT_PATTERN.search(text):
                raise AccountLimitedError(f"账号每日生成上限: {text[:120]}")
            if CREDIT_FAIL_PATTERN.search(text):
                raise CreditError(f"额度不足: {text[:80]}")
        if poll.get("videos"):
            video_models = poll.get("videoModels", [])
            url = extract_unwatermarked_url(
                video_models[0] if video_models else "", poll["videos"][0])
            print(f"[{account}] 出片! 下载中（无水印优先）...", flush=True)
            local = await _download(url, account)
            print(f"[{account}] 已下载 {local}（{local.stat().st_size / 1e6:.1f} MB）", flush=True)
            return {"video_url": url, "local_path": str(local),
                    "conversation_id": conversation_id, "account": account}
        print(f"  ...生成中（{int(time.time() - start)}s）", flush=True)


async def resume_video(account: str, conversation_id: str, timeout: int,
                       on_poll=None, on_balance=None) -> dict:
    """服务重启后恢复一个已受理会话，不重新发送 prompt。"""
    async with async_playwright() as p:
        context = await launch_account_context(p, account, headless=False, use_extension=True)
        try:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(f"https://www.dola.com/chat/{conversation_id}",
                            timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            return await poll_conversation(account, page, context, conversation_id, timeout, on_poll, on_balance)
        finally:
            await context.close()


async def generate_video(account: str, prompt: str, ratio: str = None,
                         duration: int = None, timeout: int = None,
                         model: str = "seedance_v2.0", use_extension: bool = True,
                         on_conversation_id=None, on_poll=None, on_balance=None,
                         reference_image_paths: list[str] | None = None) -> dict:
    """完整出片流程（UI 流）。返回 {"video_url","local_path","conversation_id","account"}。

    ratio/duration: None = 用 UI 默认（当前默认 10s）。
    异常：RiskControlError（滑块 3 次不过）、CreditError（额度不足）。出片轮询不限时。
    """
    timeout = timeout or config.VIDEO_TIMEOUT
    model_key = model.lower().replace("-", "_")
    if model_key in ("seedance_2.5", "seedance_v2.5", "seedance_25", "seedance_v25"):
        model_key = "seedance_v2.5"
    elif model_key in ("seedance_2.0", "seedance_v2.0", "seedance_20", "seedance_v20"):
        model_key = "seedance_v2.0"
    else:
        raise ValueError(f"不支持的模型: {model}（当前支持 seedance-2.0 / seedance-2.5）")
    if duration is not None and duration not in (10, 15, 30):
        raise ValueError("Dola 当前固定支持 10 秒、15 秒和扩展注入的 30 秒")
    if duration == 30 and not use_extension:
        raise ValueError("30 秒生成需要启用 Dola30 扩展")
    # 30 秒视频（尤其 Seedance 2.5）页面会明确提示约 15 分钟，不能沿用 5 分钟默认值。
    if duration == 30:
        timeout = max(timeout, 1800)
    if reference_image_paths:
        timeout = max(timeout, config.REFERENCE_VIDEO_TIMEOUT)
    async with async_playwright() as p:
        context = await launch_account_context(
            p, account, headless=False if use_extension else None,
            use_extension=use_extension)
        try:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto("https://www.dola.com/chat", timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            await dismiss_popups(page)
            cookies = await context.cookies("https://www.dola.com")
            ms_token, fp = cookie_value(cookies, "msToken"), cookie_value(cookies, "s_v_web_id")
            await _preflight_balance(page, ms_token, fp, config.VIDEO_REQUIRED_POINTS)

            # ---- UI 发送 ----
            try:
                await page.click(VIDEO_BTN)
            except Exception:
                await page.get_by_text("動画を作成", exact=False).first.evaluate("(el) => el.click()")
                await page.wait_for_timeout(500)
            await page.wait_for_timeout(1500)
            if reference_image_paths:
                await attach_reference_images(page, reference_image_paths)
            # 选择模型。扩展/当前网页端文案：モデル 2.0高速、モデル 2.5。
            try:
                current_model = None
                for label in ("モデル 2.0高速", "モデル 2.5"):
                    loc = page.get_by_text(label, exact=True).first
                    if await loc.count() and await loc.is_visible():
                        current_model = loc
                        break
                if current_model is None:
                    current_model = page.get_by_text(re.compile(r"^モデル "), exact=False).first
                await current_model.click(timeout=5000)
                await page.wait_for_timeout(500)
                options = (("Dreamina Seedance 2.5",)
                           if model_key == "seedance_v2.5"
                           else ("Dreamina Seedance 2.0高速", "Dreamina Seedance 2.0", "Seedance2.0Fast"))
                selected = False
                for option_text in options:
                    loc = page.get_by_text(option_text, exact=False).first
                    if await loc.count() and await loc.is_visible():
                        try:
                            await loc.click(timeout=5000)
                        except Exception:
                            # radix popper 子元素拦截点击时，用 JS 直接派发点击
                            await loc.evaluate("(el) => el.click()")
                            await page.wait_for_timeout(300)
                        selected = True
                        break
                if not selected:
                    raise RuntimeError("模型选项不存在")
                await page.wait_for_timeout(500)
            except Exception as e:
                raise RuntimeError(f"设置模型失败（{model_key}）: {str(e)[:120]}") from e
            if ratio:
                try:
                    await page.click("text=比率", timeout=3000)
                    await page.wait_for_timeout(500)
                    await page.click(f"text={ratio}", timeout=3000)
                except Exception as e:
                    print(f"  (设置比例失败，用默认: {str(e)[:80]})", flush=True)
            if duration:
                try:
                    await page.click(f"text={duration}s", timeout=3000)
                except Exception:
                    try:  # 先点开时长下拉（当前值按钮如「10s」）再选
                        await page.get_by_text(re.compile(r"^\d+s$")).first.click(timeout=3000)
                        await page.wait_for_timeout(500)
                        await page.click(f"text={duration}s", timeout=3000)
                    except Exception as e:
                        print(f"  (设置时长失败，用默认: {str(e)[:80]})", flush=True)
            await dismiss_popups(page)
            box = await page.query_selector("textarea") or await page.query_selector('[contenteditable="true"]')
            try:
                await box.click()
            except Exception:
                await box.evaluate("(el) => el.focus()")
            await page.keyboard.type(prompt, delay=100)
            await page.wait_for_timeout(600)
            await page.keyboard.press("Enter")
            print(f"[{account}] UI 已发送: {prompt[:40]}", flush=True)

            # ---- 滑块求解（最多 3 次）----
            solved_or_absent = False
            for attempt in range(1, 4):
                frame = None
                for _ in range(20):
                    await page.wait_for_timeout(1000)
                    frame = find_captcha_frame(page)
                    if frame:
                        break
                if not frame:
                    solved_or_absent = True
                    break
                print(f"[{account}] 滑块出现，第 {attempt} 次求解...", flush=True)
                if await solve_slider(page, frame, attempt):
                    print(f"[{account}] 滑块通过 ✓", flush=True)
                    await page.wait_for_timeout(3000)  # 等前端自动重试提交
                    solved_or_absent = True
                    break
                print(f"[{account}] 滑块未过，等刷新重试", flush=True)
            if not solved_or_absent:
                await page.screenshot(path="solve_fail.png")
                raise RiskControlError("滑块 3 次未通过")

            # ---- 等真实 conversation_id（URL 从 local_x 变成数字 id）----
            conv_id = ""
            for _ in range(30):
                await page.wait_for_timeout(1000)
                tail = page.url.rstrip("/").split("/")[-1]
                if tail.isdigit():
                    conv_id = tail
                    break
            if not conv_id:
                await page.screenshot(path="no_conv.png")
                raise TimeoutError("30s 未拿到 conversation_id")
            print(f"[{account}] conversation_id={conv_id}，轮询出片...", flush=True)

            deadline = time.time() + timeout
            if on_conversation_id:
                on_conversation_id(account, conv_id, deadline)
            return await poll_conversation(account, page, context, conv_id, timeout, on_poll, on_balance)
        finally:
            await context.close()


async def _main():
    account = sys.argv[1] if len(sys.argv) > 1 else "acc1"
    prompt = sys.argv[2] if len(sys.argv) > 2 else "一只橘猫在窗台上打盹，阳光洒进来"
    ratio = sys.argv[3] if len(sys.argv) > 3 else None
    duration = int(sys.argv[4]) if len(sys.argv) > 4 else None
    model = sys.argv[5] if len(sys.argv) > 5 else "seedance_v2.0"
    result = await generate_video(account, prompt, ratio, duration, model=model)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except Exception:
        import traceback
        traceback.print_exc()
