"""dola-pool OpenAI 兼容视频服务（FastAPI）+ 管理面板后端。

对外协议（异步两段式）：
  POST /v1/videos/generations   -> {"id","status","model","prompt"}
  GET  /v1/videos/{id}          -> {"id","status","video_url","error"}
  GET  /videos/<file>           -> 出片转存静态服务

管理面板：GET / -> web/index.html；管理接口 /api/admin/*。
"""
import asyncio
import aiohttp
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import config
from add_account import add_account_flow
from browser_pool import AllAccountsLimitedError, AllAccountsQuotaBlockedError, BrowserPool
from media import download_reference_images, validate_reference_urls
from store import PendingTaskLimitExceeded, TaskQuotaExceeded, TaskStore

Path(config.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
Path("web").mkdir(parents=True, exist_ok=True)

app = FastAPI(title="dola-pool", version="0.4.0")

store = TaskStore(config.DB_PATH)
pool = BrowserPool(max_concurrency=config.MAX_CONCURRENCY)

app.mount("/videos", StaticFiles(directory=config.DOWNLOAD_DIR), name="videos")

# 后台 jobs（加号/验证），内存态
JOBS: dict[str, dict] = {}
# 加号并发上限：1.9GB 内存机器上同时开多个浏览器登录会 OOM，批量添加时逐个排队登录
ADD_ACCOUNT_SEM = asyncio.Semaphore(1)

SIZE_TO_RATIO = {
    "1280x720": "16:9", "1920x1080": "16:9",
    "720x1280": "9:16", "1080x1920": "9:16",
    "1024x1024": "1:1", "1440x1080": "4:3", "1080x1440": "3:4",
}
VIDEO_MAX_ATTEMPTS = 3  # 生成失败自动换号重试上限（超时不重试）
SUPPORTED_DURATIONS = tuple(config.ALL_DURATIONS)  # (5, 10, 15, 30)
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


RATIO_TO_DIMENSIONS = {
    "16:9": (1280, 720), "9:16": (720, 1280),
    "1:1": (1024, 1024), "4:3": (1440, 1080), "3:4": (1080, 1440),
}


def _probe_with_ffprobe(path: str) -> dict | None:
    """用 ffprobe 读取真实视频元数据；未安装或解析失败返回 None。"""
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True, timeout=15,
        )
        if proc.returncode != 0:
            return None
        data = json.loads(proc.stdout or "{}")
    except Exception:
        return None
    stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
    fmt = data.get("format") or {}
    meta = {}
    if stream:
        try:
            meta["video_width"] = int(stream.get("width") or 0) or None
            meta["video_height"] = int(stream.get("height") or 0) or None
        except (TypeError, ValueError):
            pass
        dur = stream.get("duration") or fmt.get("duration")
        try:
            meta["video_duration"] = float(dur) if dur else None
        except (TypeError, ValueError):
            pass
    bit_rate = fmt.get("bit_rate")
    try:
        meta["video_bitrate"] = int(bit_rate) if bit_rate else None
    except (TypeError, ValueError):
        pass
    return {k: v for k, v in meta.items() if v is not None} or None


def _video_metadata(local_path: str, duration: int | None, ratio: str | None) -> dict:
    """出片完成后的视频元数据：优先 ffprobe 真实值，缺失时用任务记录推算。"""
    meta: dict = {}
    try:
        meta["video_size"] = os.path.getsize(local_path)
    except OSError:
        pass
    probed = _probe_with_ffprobe(local_path)
    if probed:
        meta.update(probed)
    else:
        w, h = RATIO_TO_DIMENSIONS.get(ratio or "", (0, 0))
        if w and h:
            meta["video_width"], meta["video_height"] = w, h
        if duration:
            meta["video_duration"] = float(duration)
    if meta.get("video_size") and meta.get("video_duration"):
        meta["video_bitrate"] = int(meta["video_size"] * 8 / meta["video_duration"])
    return meta


class KeyConcurrencyLimiter:
    """按 API Key 限制同时运行的任务；0 表示不限。"""

    def __init__(self):
        self._condition = asyncio.Condition()
        self._active: defaultdict[str, int] = defaultdict(int)

    async def acquire(self, api_key_hash: str | None, limit: int):
        if not api_key_hash or limit <= 0:
            return
        async with self._condition:
            while self._active[api_key_hash] >= limit:
                await self._condition.wait()
            self._active[api_key_hash] += 1

    async def release(self, api_key_hash: str | None):
        if not api_key_hash:
            return
        async with self._condition:
            if self._active[api_key_hash] > 0:
                self._active[api_key_hash] -= 1
            if self._active[api_key_hash] == 0:
                self._active.pop(api_key_hash, None)
            self._condition.notify_all()



key_limiter = KeyConcurrencyLimiter()


# ===== 鉴权 =====


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _anonymous_client() -> dict:
    return {
        "api_key_hash": None,
        "api_key_name": "匿名调用",
        "daily_limit": 0,
        "concurrency_limit": 0,
        "allowed_durations": list(SUPPORTED_DURATIONS),
    }


def _env_client(key: str) -> dict:
    return {
        "api_key_hash": _hash_key(key),
        "api_key_name": f"环境变量 Key ({key[:8]}…)",
        "daily_limit": 0,
        "concurrency_limit": 0,
        "allowed_durations": list(SUPPORTED_DURATIONS),
    }


def _auth(authorization):
    """返回本次调用的客户策略；没有任何 Key 配置时保持开发模式免鉴权。"""
    if not config.API_KEYS and not store.has_enabled_keys():
        return _anonymous_client()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    key = authorization[7:].strip()
    if not key:
        raise HTTPException(401, "missing bearer token")
    if key in config.API_KEYS:
        return _env_client(key)
    record = store.get_key(key)
    if not record or not store.is_key_valid(key):
        raise HTTPException(401, "invalid api key")
    store.touch_key(key)
    return {
        "api_key_hash": _hash_key(key),
        "api_key_name": record["name"] or "未命名客户",
        "daily_limit": record["daily_limit"],
        "concurrency_limit": record["concurrency_limit"],
        "allowed_durations": record["allowed_durations"],
    }


def _admin_auth(x_admin_key: str | None):
    if not config.ADMIN_KEY:
        return
    if x_admin_key != config.ADMIN_KEY:
        raise HTTPException(401, "invalid admin key")


def _normalize_allowed_durations(values) -> list[int]:
    if values is None:
        return list(SUPPORTED_DURATIONS)
    try:
        normalized = sorted({int(value) for value in values})
    except (TypeError, ValueError):
        raise HTTPException(422, "allowed_durations 必须是 5/10/15/30 的数组")
    if not normalized or any(value not in SUPPORTED_DURATIONS for value in normalized):
        raise HTTPException(422, "allowed_durations 只能包含 5/10/15/30，且至少选择一个")
    return normalized


# ===== 客户 API =====


class VideoGenRequest(BaseModel):
    model: str = "seedance-2.0"
    prompt: str = Field(..., min_length=1)
    size: str | None = None
    ratio: str | None = None
    duration: int | None = Field(None, ge=5, le=30)
    # 参考图：兼容各画布工具的常见字段形态（字符串/数组/{"url": ...}）
    reference_images: list[str] = Field(default_factory=list)
    image_url: Any = None
    image: Any = None
    input_image: Any = None
    input_images: Any = None


class TaskResponse(BaseModel):
    id: str
    status: str
    model: str | None = None
    prompt: str | None = None
    video_url: str | None = None
    error: str | None = None


def _resolve_ratio(size, ratio):
    if size and size in SIZE_TO_RATIO:
        return SIZE_TO_RATIO[size]
    return ratio


def _normalize_model(model: str) -> str:
    """把火山 Ark 的模型 ID 宽松映射到 dola-pool 支持的 seedance 版本。"""
    m = (model or "").lower().replace("_", "-")
    if "2.5" in m or "2-5" in m:
        return "seedance-2.5"
    return "seedance-2.0"


def _model_durations(model: str) -> tuple[int, ...]:
    """按模型返回允许时长（seedance-2.5: 10/30；seedance-2.0: 5/10/15）。"""
    return tuple(config.MODEL_DURATION_COSTS.get(_normalize_model(model), {}).keys())


def _duration_cost(model: str, duration: int) -> int:
    """单次出片消耗的额度点数（按用户确认的矩阵）。"""
    return config.MODEL_DURATION_COSTS.get(_normalize_model(model), {}).get(duration, 1)


def _pick_duration(requested, allowed: list[int]) -> int:
    """把 Ark 请求的时长映射到允许档位，优先取不小于请求值的档位。"""
    try:
        requested = max(1, int(requested or 0))
    except (TypeError, ValueError):
        requested = 10
    allowed = sorted(allowed) or list(SUPPORTED_DURATIONS)
    for duration in allowed:
        if duration >= requested:
            return duration
    return allowed[-1]


def _normalize_image_refs(*fields) -> list[str]:
    """兼容各画布工具常见的图片字段形态（字符串/列表/{"url": ...}）。"""
    out: list[str] = []
    for value in fields:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                out.append(value.strip())
        elif isinstance(value, dict):
            url = value.get("url")
            if isinstance(url, str) and url.strip():
                out.append(url.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    out.append(item.strip())
                elif isinstance(item, dict):
                    url = item.get("url")
                    if isinstance(url, str) and url.strip():
                        out.append(url.strip())
    seen = set()
    return [u for u in out if not (u in seen or seen.add(u))]


def _map_ark_ratio(ratio) -> str | None:
    """Ark 的 ratio 直接透传；adaptive/未知值回落 dola 默认。"""
    if not ratio:
        return None
    value = str(ratio).strip().lower()
    if value in ("adaptive", "auto", "default"):
        return None
    return value if value in {"16:9", "9:16", "1:1", "4:3", "3:4"} else None


async def _run_task(task_id, model, prompt, ratio, duration, reference_images, client):
    api_key_hash = client.get("api_key_hash")
    acquired = False
    reference_root = None
    try:
        await key_limiter.acquire(api_key_hash, client.get("concurrency_limit", 0))
        acquired = True
        store.update(task_id, status="processing", started_at=time.time())

        def on_conversation_id(account, conversation_id, deadline_at):
            store.update(task_id, status="processing", account=account,
                         conversation_id=conversation_id, deadline_at=deadline_at,
                         last_poll_at=time.time())

        def on_poll(now):
            store.update(task_id, last_poll_at=now)

        def on_account_try(account, attempt):
            store.update(task_id, status="processing", account=account,
                         attempt=attempt, last_poll_at=time.time())

        reference_root, reference_paths = await download_reference_images(
            reference_images or [], task_id)
        result = await pool.generate_video(
            prompt, ratio, duration, model,
            on_conversation_id=on_conversation_id, on_poll=on_poll,
            reference_image_paths=reference_paths,
            on_account_try=on_account_try, max_attempts=VIDEO_MAX_ATTEMPTS)
        public_url = f"{config.PUBLIC_BASE}/videos/{Path(result['local_path']).name}"
        meta = _video_metadata(result["local_path"], duration, ratio)
        store.update(task_id, status="completed", video_url=public_url,
                     account=result.get("account"), last_poll_at=time.time(),
                     finished_at=time.time(), **meta)
    except (AllAccountsLimitedError, AllAccountsQuotaBlockedError) as e:
        store.update(task_id, status="failed", error=str(e)[:500],
                     failure_code="429", finished_at=time.time())
    except Exception as e:
        store.update(task_id, status="failed", error=str(e)[:500],
                     finished_at=time.time())
    finally:
        if reference_root:
            shutil.rmtree(reference_root, ignore_errors=True)
        if acquired:
            await key_limiter.release(api_key_hash)


async def _resume_task(row: dict):
    task_id = row["id"]
    deadline = row.get("deadline_at") or (
        time.time() + (1800 if row.get("duration") == 30 else config.VIDEO_TIMEOUT)
    )
    remaining = max(1, int(deadline - time.time()))
    api_key_hash = row.get("api_key_hash")
    acquired = False
    try:
        await key_limiter.acquire(
            api_key_hash, int(row.get("client_concurrency_limit") or 0)
        )
        acquired = True
        store.update(task_id, status="processing", last_poll_at=time.time(),
                     started_at=row.get("started_at") or time.time())

        def on_poll(now):
            store.update(task_id, last_poll_at=now)

        result = await pool.resume_video(
            row["account"], row["conversation_id"], remaining, on_poll=on_poll,
            duration=row.get("duration"),
            ratio=None if row.get("ratio") == "default" else row.get("ratio"),
            cost=_duration_cost(row.get("model"), row.get("duration") or 10))
        public_url = f"{config.PUBLIC_BASE}/videos/{Path(result['local_path']).name}"
        meta = _video_metadata(result["local_path"], row.get("duration"), row.get("ratio"))
        store.update(task_id, status="completed", video_url=public_url,
                     account=result.get("account"), last_poll_at=time.time(),
                     finished_at=time.time(), **meta)
    except Exception as e:
        store.update(task_id, status="failed", error=str(e)[:500],
                     finished_at=time.time())
    finally:
        if acquired:
            await key_limiter.release(api_key_hash)


def _task_client(row: dict) -> dict:
    """从任务快照恢复调度所需的客户上下文，不依赖 Key 当前是否仍存在。"""
    return {
        "api_key_hash": row.get("api_key_hash"),
        "api_key_name": row.get("api_key_name") or "历史任务",
        "daily_limit": 0,
        "concurrency_limit": int(row.get("client_concurrency_limit") or 0),
        "allowed_durations": list(SUPPORTED_DURATIONS),
    }


def _task_reference_images(raw) -> list[str]:
    try:
        values = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    return values if isinstance(values, list) else []


@app.on_event("startup")
async def resume_incomplete_tasks():
    """服务重启后恢复已受理会话，并重新排队尚未开始的 queued 任务。"""
    for row in store.recoverable_tasks():
        asyncio.create_task(_resume_task(row))
    for row in store.recoverable_queued_tasks():
        ratio = row.get("ratio")
        if ratio == "default":
            ratio = None
        asyncio.create_task(_run_task(
            row["id"], row["model"], row["prompt"], ratio, row["duration"],
            _task_reference_images(row.get("reference_images")), _task_client(row),
        ))


@app.post("/v1/videos/generations", response_model=TaskResponse)
async def create_video(req: VideoGenRequest, authorization: str | None = Header(default=None)):
    client = _auth(authorization)
    duration = req.duration or 10
    ratio = _resolve_ratio(req.size, req.ratio)
    refs = _normalize_image_refs(
        req.reference_images, req.image_url, req.image,
        req.input_image, req.input_images,
    )
    if refs:
        print(f"[api] 收到参考图 {len(refs)} 张: {refs[0][:80]}", flush=True)
    return await _submit_task(client, req.model, req.prompt, ratio, duration, refs)


async def _submit_task(client, model, prompt, ratio, duration, reference_images):
    """公共受理逻辑：校验模型/时长/账号池 -> 入队 -> 后台跑，返回 TaskResponse。"""
    model_key = _normalize_model(model)
    model_durations = _model_durations(model_key)
    if duration not in model_durations:
        raise HTTPException(
            422,
            f"{model_key} 仅支持 {'/'.join(map(str, model_durations))} 秒视频"
            f"（收到 {duration} 秒）",
        )
    if duration not in client["allowed_durations"]:
        raise HTTPException(422, f"当前 API Key 不允许生成 {duration} 秒视频")
    try:
        reference_images = await validate_reference_urls(reference_images)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    # 账号暂时忙时允许任务进入 queued，由 BrowserPool 的全局并发控制实际排队。
    # 只有明确全部达到每日上限/积分不足时才立即拒绝。
    if not pool.available and pool.all_accounts_limited:
        raise HTTPException(429, "账号限流：所有已开启调度的账号均已达到 Dola 每日视频上限，请明天再试")
    if not pool.available and pool.all_accounts_quota_blocked:
        raise HTTPException(429, "积分不足：所有已开启调度的账号都没有足够积分，请等待额度刷新")
    if not pool.accounts:
        raise HTTPException(503, "no account in pool")
    task_id = "video_" + uuid.uuid4().hex
    try:
        store.create(
            task_id,
            model,
            prompt,
            ratio or "default",
            duration,
            reference_images=json.dumps(reference_images, ensure_ascii=False),
            api_key_hash=client["api_key_hash"],
            api_key_name=client["api_key_name"],
            daily_limit=client["daily_limit"],
            concurrency_limit=client["concurrency_limit"],
            max_pending=config.MAX_PENDING_TASKS,
        )
    except TaskQuotaExceeded as exc:
        raise HTTPException(429, str(exc)) from exc
    except PendingTaskLimitExceeded as exc:
        raise HTTPException(429, str(exc)) from exc
    asyncio.create_task(_run_task(
        task_id, model, prompt, ratio, duration, reference_images, client
    ))
    return TaskResponse(id=task_id, status="queued", model=model, prompt=prompt)


@app.get("/v1/videos/{task_id}", response_model=TaskResponse)
async def get_video(task_id: str, authorization: str | None = Header(default=None)):
    client = _auth(authorization)
    row = store.get_for_client(task_id, client["api_key_hash"])
    if not row:
        raise HTTPException(404, "task not found")
    return TaskResponse(
        id=row["id"], status=row["status"], model=row["model"],
        prompt=row["prompt"], video_url=row["video_url"], error=row["error"],
    )



@app.get("/v1/videos/{task_id}/content")
async def get_video_content(task_id: str, authorization: str | None = Header(default=None)):
    """new-api 任务插件兼容端点：把已完成的视频文件内容返回给客户端。"""
    client = _auth(authorization)
    row = store.get_for_client(task_id, client["api_key_hash"])
    if not row:
        raise HTTPException(404, "task not found")
    if row["status"] != "completed" or not row["video_url"]:
        raise HTTPException(409, "video not ready")
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(row["video_url"]) as resp:
            if resp.status != 200:
                raise HTTPException(502, "failed to fetch video from upstream")
            content_type = resp.headers.get("Content-Type", "video/mp4")
            data = await resp.read()
    return Response(content=data, media_type=content_type)



@app.get("/v1/models")
async def list_models():
    """OpenAI 风格模型列表，供 new-api 等上游获取模型用。"""
    return {
        "object": "list",
        "data": [
            {"id": "seedance-2.0", "object": "model", "owned_by": "dola-pool", "created": 0},
            {"id": "seedance-2.5", "object": "model", "owned_by": "dola-pool", "created": 0},
        ],
    }


# ===== 火山 Ark 任务式协议兼容端点（画布工具） =====

ARK_STATUS_MAP = {
    "queued": "queued",
    "processing": "running",
    "completed": "succeeded",
    "failed": "failed",
}


async def ark_create_task(body: dict, authorization: str | None = Header(default=None)):
    """POST .../contents/generations/tasks：Ark 风格创建，复用 dola 出片流程。"""
    client = _auth(authorization)
    body = body or {}
    prompt_parts: list[str] = []
    reference_images: list[str] = []
    for item in body.get("content") or []:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "").lower()
        if item_type == "text":
            text = str(item.get("text") or "").strip()
            if text:
                prompt_parts.append(text)
        elif item_type in ("image_url", "image"):
            url_obj = item.get("image_url")
            url = url_obj.get("url") if isinstance(url_obj, dict) else url_obj
            if isinstance(url, str) and url.strip():
                reference_images.append(url.strip())
    # 顶层兼容：部分画布工具把图片放在顶层 image_url/image 字段
    reference_images = _normalize_image_refs(
        reference_images, body.get("image_url"), body.get("image"),
        body.get("input_image"), body.get("input_images"),
    )
    prompt = "\n".join(prompt_parts).strip()
    if not prompt:
        raise HTTPException(422, "content 中缺少 text 提示词")
    model = _normalize_model(body.get("model"))
    ratio = _map_ark_ratio(body.get("ratio"))
    if reference_images:
        print(f"[api] Ark 收到参考图 {len(reference_images)} 张: {reference_images[0][:80]}", flush=True)
    allowed = [d for d in client["allowed_durations"] if d in _model_durations(model)]
    duration = _pick_duration(body.get("duration"), allowed)
    resp = await _submit_task(client, model, prompt, ratio, duration, reference_images)
    return {"id": resp.id, "status": resp.status}


async def ark_get_task(task_id: str, authorization: str | None = Header(default=None)):
    """GET .../contents/generations/tasks/{id}：Ark 风格查询。"""
    client = _auth(authorization)
    row = store.get_for_client(task_id, client["api_key_hash"])
    if not row:
        raise HTTPException(404, "task not found")
    result = {
        "id": row["id"],
        "model": row["model"],
        "status": ARK_STATUS_MAP.get(row["status"], row["status"]),
        "created_at": int(row["created_at"]),
        "updated_at": int(row.get("updated_at") or row["created_at"]),
    }
    if row["status"] == "completed" and row.get("video_url"):
        result["content"] = {"video_url": row["video_url"]}
    if row.get("error"):
        result["error"] = row["error"]
    return result


ARK_TASK_PATHS = (
    "/v1/video/contents/generations/tasks",
    "/api/v3/contents/generations/tasks",
    "/v1/contents/generations/tasks",
    "/seedance/v3/contents/generations/tasks",
)
for _ark_path in ARK_TASK_PATHS:
    app.add_api_route(_ark_path, ark_create_task, methods=["POST"])
    app.add_api_route(_ark_path + "/{task_id}", ark_get_task, methods=["GET"])


@app.get("/health")


async def health():
    return {
        "ok": True,
        "accounts": pool.account_status(),
        "available": pool.available,
        "pending_tasks": store.pending_task_count(),
        "max_pending_tasks": config.MAX_PENDING_TASKS,
    }


# ===== 管理面板 API =====


class AdminLogin(BaseModel):
    key: str


class AccountPatch(BaseModel):
    scheduling: bool | None = None
    note: str | None = None
    email: str | None = None


class AccountAdd(BaseModel):
    name: str
    email: str
    password: str
    totp: str


class KeyCreate(BaseModel):
    name: str = ""
    daily_limit: int = Field(0, ge=0, le=1_000_000)
    concurrency_limit: int = Field(0, ge=0, le=1_000)
    allowed_durations: list[int] = Field(default_factory=lambda: list(SUPPORTED_DURATIONS))
    expires_at: float | None = Field(None, ge=0)


class KeyPatch(BaseModel):
    name: str | None = None
    enabled: bool | None = None
    daily_limit: int | None = Field(None, ge=0, le=1_000_000)
    concurrency_limit: int | None = Field(None, ge=0, le=1_000)
    allowed_durations: list[int] | None = None
    expires_at: float | None = Field(None, ge=0)


@app.post("/api/admin/login")
async def admin_login(body: AdminLogin):
    if not config.ADMIN_KEY:
        return {"ok": True, "auth_required": False}
    if body.key == config.ADMIN_KEY:
        return {"ok": True, "auth_required": True}
    raise HTTPException(401, "wrong admin key")


@app.get("/api/admin/accounts")
async def admin_accounts(x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    return {"accounts": pool.list_accounts()}


@app.patch("/api/admin/accounts/{name}")
async def admin_account_patch(name: str, body: AccountPatch,
                              x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    if name not in pool.accounts:
        raise HTTPException(404, "account not found")
    if body.scheduling is not None:
        pool.set_scheduling(name, body.scheduling)
    if body.note is not None:
        pool.set_note(name, body.note)
    if body.email is not None:
        pool.set_email(name, body.email)
    return {"ok": True}


@app.delete("/api/admin/accounts/{name}")
async def admin_account_delete(name: str, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    if name not in pool.accounts:
        raise HTTPException(404, "account not found")
    try:
        pool.delete_account(name)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}


@app.post("/api/admin/accounts/{name}/verify")
async def admin_account_verify(name: str, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    try:
        ok = await pool.verify_account(name)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    return {"ok": ok}


async def _run_add_job(name: str, email: str, password: str, totp: str):
    JOBS[name] = {"kind": "add", "status": "running", "error": "", "started_at": time.time()}
    try:
        async with ADD_ACCOUNT_SEM:
            await add_account_flow(name, email, password, totp)
        pool.set_email(name, email)
        pool.set_login_status(name, True)
        JOBS[name] = {**JOBS[name], "status": "success"}
    except Exception as e:
        JOBS[name] = {**JOBS[name], "status": "failed", "error": str(e)[:300]}


@app.post("/api/admin/accounts", status_code=202)
async def admin_account_add(body: AccountAdd, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    if not NAME_RE.match(body.name):
        raise HTTPException(400, "invalid account name")
    if body.name in pool.accounts:
        raise HTTPException(409, "account exists")
    if JOBS.get(body.name, {}).get("status") == "running":
        raise HTTPException(409, "add job running")
    asyncio.create_task(_run_add_job(body.name, body.email, body.password, body.totp))
    return {"ok": True, "job": "running"}


@app.get("/api/admin/jobs")
async def admin_jobs(x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    return {"jobs": JOBS}


@app.get("/api/admin/tasks")
async def admin_tasks(limit: int = 50, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    return {"tasks": store.recent_tasks(min(max(limit, 1), 200))}


@app.get("/api/admin/videos")
async def admin_videos(limit: int = 100, x_admin_key: str | None = Header(default=None)):
    """视频管理：已完成任务的列表 + 文件元数据（老任务按需从磁盘补大小）。"""
    _admin_auth(x_admin_key)
    rows = store.recent_tasks(min(max(limit, 1), 500))
    out = []
    for r in rows:
        if r.get("status") != "completed":
            continue
        item = dict(r)
        if item.get("video_url"):
            local = Path(config.DOWNLOAD_DIR) / Path(item["video_url"]).name
        else:
            local = None
        if local and local.exists():
            item["file_exists"] = True
            try:
                item["file_size"] = os.path.getsize(local)
            except OSError:
                item["file_size"] = item.get("video_size")
            if not item.get("video_size"):
                item["video_size"] = item["file_size"]
        else:
            item["file_exists"] = False
        out.append(item)
    out.sort(key=lambda x: x.get("created_at") or 0, reverse=True)
    return {"videos": out}


@app.delete("/api/admin/videos/{task_id}")
async def admin_video_delete(task_id: str, x_admin_key: str | None = Header(default=None)):
    """删除视频文件，任务记录保留（清空 video_url 与视频元数据）。"""
    _admin_auth(x_admin_key)
    row = store.get(task_id)
    if not row or row.get("status") != "completed":
        raise HTTPException(404, "task not found")
    url = row.get("video_url")
    if url:
        local = Path(config.DOWNLOAD_DIR) / Path(url).name
        try:
            if local.exists():
                local.unlink()
        except OSError:
            pass
    store.update(
        task_id,
        video_url=None,
        video_size=None,
        video_width=None,
        video_height=None,
        video_duration=None,
        video_bitrate=None,
    )
    return {"ok": True}


@app.get("/api/admin/stats")
async def admin_stats(x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    st = store.stats()
    accs = pool.list_accounts()
    sched = [a for a in accs if a["scheduling"] and not a["cooling"]]
    st["total_accounts"] = len(accs)
    st["available_accounts"] = sum(1 for a in sched if a["remaining"] > 0)
    st["total_remaining"] = sum(a["remaining"] for a in sched)
    totals = st.pop("per_account_total", {})
    st["per_account"] = [{**a, "completed_total": totals.get(a["name"], 0)} for a in accs]
    return st


@app.get("/api/admin/keys")
async def admin_keys(x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    keys = []
    for key in store.list_keys():
        usage = store.key_usage(store.hash_api_key(key["key"]))
        keys.append({**key, **{
            "today_total": usage["total"],
            "today_completed": usage["completed"],
            "today_failed": usage["failed"],
            "today_active": usage["active"],
            "today_queued": usage["queued"],
        }})
    return {"keys": keys, "env_keys": len(config.API_KEYS)}


@app.post("/api/admin/keys")
async def admin_key_create(body: KeyCreate, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    allowed = _normalize_allowed_durations(body.allowed_durations)
    return {"created": store.create_key(
        body.name,
        daily_limit=body.daily_limit,
        concurrency_limit=body.concurrency_limit,
        allowed_durations=allowed,
        expires_at=body.expires_at,
    )}


@app.patch("/api/admin/keys/{key}")
async def admin_key_patch(key: str, body: KeyPatch,
                          x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    if not store.get_key(key):
        raise HTTPException(404, "api key not found")
    fields = {}
    if body.name is not None:
        fields["name"] = body.name
    if body.enabled is not None:
        fields["enabled"] = 1 if body.enabled else 0
    if body.daily_limit is not None:
        fields["daily_limit"] = body.daily_limit
    if body.concurrency_limit is not None:
        fields["concurrency_limit"] = body.concurrency_limit
    if body.allowed_durations is not None:
        fields["allowed_durations"] = _normalize_allowed_durations(body.allowed_durations)
    if body.expires_at is not None:
        fields["expires_at"] = body.expires_at
    store.update_key(key, **fields)
    return {"ok": True}


@app.delete("/api/admin/keys/{key}")
async def admin_key_delete(key: str, x_admin_key: str | None = Header(default=None)):
    _admin_auth(x_admin_key)
    if not store.get_key(key):
        raise HTTPException(404, "api key not found")
    store.delete_key(key)
    return {"ok": True}


# 面板单文件前端（放最后，保证上面的路由优先）
app.mount("/", StaticFiles(directory="web", html=True), name="web")
