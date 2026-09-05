"""代理池存储：代理记录 + 账号-代理映射，存 pool_usage.db。

浏览器每次启动/验证账号前通过 browser.py 查账号对应的代理；
未给账号指定代理时回落到 config.DOLA_PROXY（环境默认代理）。
"""
import sqlite3
import time
import uuid
from pathlib import Path

import config

DB_PATH = Path(config.POOL_DB_PATH if hasattr(config, "POOL_DB_PATH") else "pool_usage.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS proxies (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            protocol TEXT NOT NULL DEFAULT 'http',
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            username TEXT DEFAULT '',
            password TEXT DEFAULT '',
            remark TEXT DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_proxy (
            account TEXT PRIMARY KEY,
            proxy_id TEXT
        )
        """
    )
    conn.commit()
    return conn


def _safe_dict(row: sqlite3.Row, with_password: bool = False) -> dict:
    data = dict(row)
    if not with_password:
        data["has_password"] = bool(data.get("password"))
        data.pop("password", None)
    return data


def list_proxies() -> list[dict]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM proxies ORDER BY enabled DESC, created_at DESC"
        ).fetchall()
        out = []
        for row in rows:
            item = _safe_dict(row)
            used = conn.execute(
                "SELECT COUNT(*) AS c FROM account_proxy WHERE proxy_id=?",
                (row["id"],),
            ).fetchone()["c"]
            item["account_count"] = used
            out.append(item)
        return out
    finally:
        conn.close()


def get_proxy(proxy_id: str) -> dict | None:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM proxies WHERE id=?", (proxy_id,)
        ).fetchone()
        return _safe_dict(row, with_password=True) if row else None
    finally:
        conn.close()


def create_proxy(name: str, protocol: str, host: str, port: int,
                 username: str = "", password: str = "",
                 remark: str = "") -> dict:
    name = (name or "").strip()
    protocol = (protocol or "http").strip().lower()
    host = (host or "").strip()
    if not name or not host or not port:
        raise ValueError("代理名称/地址/端口不能为空")
    if protocol not in ("http", "https", "socks4", "socks5"):
        raise ValueError("协议仅支持 http/https/socks4/socks5")
    proxy_id = "px_" + uuid.uuid4().hex[:12]
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO proxies
               (id, name, protocol, host, port, username, password, remark, enabled, created_at)
               VALUES (?,?,?,?,?,?,?,?,1,?)""",
            (proxy_id, name, protocol, host, int(port),
             username or "", password or "", remark or "", time.time()),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise ValueError(f"代理名称已存在: {name}") from exc
    row = None
    try:
        row = conn.execute(
            "SELECT * FROM proxies WHERE id=?", (proxy_id,)
        ).fetchone()
    finally:
        conn.close()
    return _safe_dict(row, with_password=False) if row else {}


def update_proxy(proxy_id: str, *, name=None, protocol=None, host=None,
                 port=None, username=None, password=None, remark=None,
                 enabled=None) -> dict:
    fields = []
    values = []
    if name is not None:
        fields.append("name=?")
        values.append((name or "").strip())
    if protocol is not None:
        fields.append("protocol=?")
        values.append((protocol or "http").strip().lower())
    if host is not None:
        fields.append("host=?")
        values.append((host or "").strip())
    if port is not None:
        fields.append("port=?")
        values.append(int(port))
    if username is not None:
        fields.append("username=?")
        values.append(username or "")
    if password is not None:
        fields.append("password=?")
        values.append(password or "")
    if remark is not None:
        fields.append("remark=?")
        values.append(remark or "")
    if enabled is not None:
        fields.append("enabled=?")
        values.append(1 if enabled else 0)
    if not fields:
        return get_proxy(proxy_id) or {}
    conn = _connect()
    try:
        cur = conn.execute(
            f"UPDATE proxies SET {', '.join(fields)} WHERE id=?",
            (*values, proxy_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            conn.close()
            raise ValueError("代理不存在")
    except ValueError:
        conn.close()
        raise
    row = None
    try:
        row = conn.execute(
            "SELECT * FROM proxies WHERE id=?", (proxy_id,)
        ).fetchone()
    finally:
        conn.close()
    return _safe_dict(row, with_password=False) if row else {}


def delete_proxy(proxy_id: str):
    conn = _connect()
    try:
        conn.execute("DELETE FROM account_proxy WHERE proxy_id=?", (proxy_id,))
        conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
        conn.commit()
    finally:
        conn.close()


def get_account_proxy(account: str) -> dict | None:
    """返回账号绑定的代理（含密码）；未绑定返回 None。"""
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT p.* FROM account_proxy ap
               JOIN proxies p ON p.id = ap.proxy_id
               WHERE ap.account=? AND p.enabled=1""",
            (account,),
        ).fetchone()
        return _safe_dict(row, with_password=True) if row else None
    finally:
        conn.close()


def set_account_proxy(account: str, proxy_id: str | None):
    conn = _connect()
    try:
        if proxy_id:
            row = conn.execute(
                "SELECT id FROM proxies WHERE id=?", (proxy_id,)
            ).fetchone()
            if not row:
                raise ValueError("代理不存在")
            conn.execute(
                """INSERT INTO account_proxy(account, proxy_id) VALUES (?,?)
                   ON CONFLICT(account) DO UPDATE SET proxy_id=excluded.proxy_id""",
                (account, proxy_id),
            )
        else:
            conn.execute(
                "DELETE FROM account_proxy WHERE account=?", (account,)
            )
        conn.commit()
    finally:
        conn.close()


def account_proxy_url(account: str) -> str | None:
    """给 aiohttp/下载用：返回可直接使用的代理 URL（带账号密码）。"""
    info = get_account_proxy(account)
    if not info:
        return config.PROXY or None
    auth = ""
    if info.get("username"):
        import urllib.parse
        auth = urllib.parse.quote(info["username"], safe="")
        if info.get("password"):
            auth += ":" + urllib.parse.quote(info["password"], safe="")
        auth += "@"
    return f"{info['protocol']}://{auth}{info['host']}:{info['port']}"
