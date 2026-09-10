#!/usr/bin/env python3

import asyncio
import logging
import re
import sqlite3
import unicodedata
import time as _t
from pathlib import Path
from typing import List, Union, Deque, Dict, Tuple, Any
from datetime import datetime, timedelta, timezone
from collections import deque
import os
import sys

from prometheus_client import Counter, Gauge, start_http_server

METRICS_PORT = 9102

contracts_forwarded_total = Counter(
    "toncalls_contracts_forwarded_total",
    "Total messages successfully forwarded to TARGET_CHANNEL.",
)
posts_skipped_total = Counter(
    "toncalls_posts_skipped_total",
    "Posts skipped before forwarding, by reason.",
    ["reason"],
)
forward_errors_total = Counter(
    "toncalls_forward_errors_total",
    "Errors during forward (FloodWait, ChatWriteForbidden etc).",
    ["type"],
)
last_forward_time = Gauge(
    "toncalls_last_forward_time",
    "Unix timestamp of the most recent successful forward (for dead-detection).",
)

# одиночный запуск
try:
    import fcntl

    def single_instance(lock_path: str = ".forwarder.lock"):
        fh = open(lock_path, "w")
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            sys.stderr.write(f"another instance is already running (lock held): {e}\n")
            sys.exit(1)
        return fh
except Exception:

    def single_instance(lock_path: str = ".forwarder.lock"):
        return None


import time as _time
import aiohttp
from telethon import TelegramClient, events, types
from telethon.errors import (
    FloodWaitError,
    AuthKeyUnregisteredError,
    UsernameNotOccupiedError,
    ChannelInvalidError,
)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

SESSION_NAME = os.environ.get("SESSION_NAME", "session")
LOG_CHAT_ID = int(os.environ.get("LOG_CHAT_ID", "0"))
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "")
CHANNELS_FILE = os.environ.get("CHANNELS_FILE", "channels.txt")

ENRICHED_ENABLED = os.environ.get("ENRICHED_ENABLED", "1") == "1"
ENRICHED_TARGET_CHANNEL = os.environ.get("ENRICHED_TARGET_CHANNEL", "")
# Optional second channel: userbot send (no button, comments work, footer links).
ENRICHED_DUP_CHANNEL = os.environ.get("ENRICHED_DUP_CHANNEL", "")

# Публичные DTrade/Redotrade — TON trading bots, реферальные start-параметры.
# Свой referral-код получите в самом боте, поставьте в env как "myprefix_".
DTRADE_DEEPLINK_PREFIX = os.environ.get("DTRADE_DEEPLINK_PREFIX", "ref_")
REDO_DEEPLINK_PREFIX = os.environ.get("REDO_DEEPLINK_PREFIX", "ref_")

# Публичный домен chart-proxy сервера (см. chart-proxy/ в этом же репо).
# Пример: "chart.example.com". Без trailing slash.
CHART_PROXY_DOMAIN = os.environ.get("CHART_PROXY_DOMAIN", "")

# Brand mark в углу composite-картинки (пусто = без марки).
BRAND_MARK = os.environ.get("BRAND_MARK", "")

# Header caption template. {handle} = @username источника, если username валидный;
# иначе будет использован title канала. Пример: "{handle} posted:"
CAPTION_HEADER_TEMPLATE = os.environ.get("CAPTION_HEADER_TEMPLATE", "{handle} posted:")

# антифрод-настройки
WARMUP_SECONDS = 10  # прогрев после логина
MIN_DELAY_BETWEEN_ACTIONS = 1.5  # сек между форвардами/сообщениями
MAX_ACTIONS_PER_MINUTE = 20  # лимит действий в минуту

# лимиты на резолв каналов
RESOLVE_MIN_DELAY = 1.0  # сек между резолвами
RESOLVE_PER_MINUTE = 15  # лимит резолвов в минуту

# остальное
EDIT_GRACE_MINUTES = 10
DELETE_GRACE_SECONDS = (
    180  # если исходный пост удалили в первые 3 минуты — удаляем и у себя
)
SQLITE_PATH = "forwarder_seen.sqlite"
CONTRACT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])EQ[0-9a-zA-Z_-]{40,60}(?![A-Za-z0-9])"
)  # 2026-06-18: ловит EQ внутри dtrade/referral-ссылок (monk_EQ..., hey_EQ...)

# 🚫 игнор контрактов
BLOCK_CONTRACTS = {
    "EQCnZYzmmuhFttNEkrUZN9NiHhsqbBIIxyYzhW1sJVsZAVIL",
    "EQBJ55KMDqSbID9xVuI6fxTf9az2zYWSn2REZJ8ghdYohaSP",
}


def has_blocked_contract(text: str) -> bool:
    if not text:
        return False
    for bc in BLOCK_CONTRACTS:
        if bc in text:
            return True
    return any(c in BLOCK_CONTRACTS for c in CONTRACT_PATTERN.findall(text))


SKIP_NEW_STONKS = False  # 2026-05-25: фильтр выключен по запросу
STONKS_FILTER_CUTOFF_UTC = "2026-05-20T14:00:00Z"
token_cache: Dict[str, dict] = {}


def parse_utc(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except (ValueError, TypeError):
        return None


async def fetch_dedust_token_info(contract: str):
    url = "https://mainnet.api.dedust.io/v4/api/coins"
    params = {
        "compact": "false",
        "offset": "0",
        "limit": "10",
        "sort_by": "volume",
        "sort_direction": "desc",
        "sort_period": "24h",
        "query": contract,
        "exclude_assets": "native",
        "skip_total_count": "true",
        "include_without_price": "true",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://dedust.io/",
    }
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
        async with session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
    items = data.get("items") or []
    return items[0] if items else None


def is_stonks_token(item: dict) -> bool:
    tags = [str(tag).lower() for tag in item.get("tags") or []]
    return "stonks" in tags


def is_after_cutoff(item: dict) -> bool:
    created = parse_utc(item.get("created_at"))
    cutoff = parse_utc(STONKS_FILTER_CUTOFF_UTC)
    return created is not None and cutoff is not None and created >= cutoff


async def should_skip_contract(contract: str) -> bool:
    if not SKIP_NEW_STONKS:
        return False
    cached = token_cache.get(contract)
    if cached is not None:
        return bool(cached.get("skip"))
    try:
        item = await fetch_dedust_token_info(contract)
    except Exception as e:
        logger.warning(
            f"[STONKS] dedust fetch err for {contract[:16]}…: {e.__class__.__name__}: {e}"
        )
        return False  # сетевая ошибка → не блокируем пересыл
    if not item:
        token_cache[contract] = {"skip": False, "reason": "not_found"}
        return False
    skip = is_stonks_token(item) and is_after_cutoff(item)
    token_cache[contract] = {
        "skip": skip,
        "tags": item.get("tags") or [],
        "created_at": item.get("created_at"),
    }
    return skip


async def _should_skip_text_contracts(text: str) -> bool:
    """True если хотя бы один контракт в тексте — новый stonks-токен.
    Защищено try/except: при любых ошибках возвращает False (не блокирует пересыл)."""
    if not SKIP_NEW_STONKS or not text:
        return False
    try:
        seen = set()
        for c in CONTRACT_PATTERN.findall(text):
            if c in seen:
                continue
            seen.add(c)
            if await should_skip_contract(c):
                return True
    except Exception as e:
        logger.warning(f"[STONKS] filter err: {e.__class__.__name__}: {e}")
    return False


bot_client: "TelegramClient" = None  # инициализируется в run()


def _fmt_usd(v) -> str:
    if v is None:
        return "?"
    try:
        v = float(v)
        if v >= 1_000_000:
            return f"${v / 1_000_000:.2f}M"
        if v >= 1_000:
            return f"${v / 1_000:.1f}K"
        return f"${v:.2f}"
    except Exception:
        return str(v)


def dtrade_buy_link(contract: str) -> str:
    return f"https://t.me/dtrade?start={DTRADE_DEEPLINK_PREFIX}{contract}"


REDO_DEEPLINK_PREFIX = "suspect_"


def redo_buy_link(contract: str) -> str:
    return f"https://t.me/redotrade?start={REDO_DEEPLINK_PREFIX}{contract}"


def _dtrade_chart_url(contract: str, platform: str, mcap_usd=None, liq_usd=None) -> str:
    """Прямой URL DTrade image-api (без редиректа). platform: 'stonfi' или 'dedust'."""
    params = [
        "theme=dark",
        f"base={contract}",
        "quote=USD",
        f"platform={platform}",
    ]
    if mcap_usd:
        params.append(f"fdv={int(mcap_usd)}")
    if liq_usd:
        params.append(f"liquidity={int(liq_usd)}")
    params.append(f"timestamp={int(_time.time())}")
    return "https://image-api.xdtrade.com/api/v1/chart?" + "&".join(params)


def _redo_trade_url(contract: str) -> str:
    """RedoTrade preview-страница с OG-тегами → Telegram сам генерит rich link preview."""
    return f"https://redo.trade/start?c={contract}&m=5m&u=dark&t={int(_time.time())}"


async def fetch_dtrade_chart(contract: str, dex_id: str, mcap_usd=None, liq_usd=None):
    """Скачивает картинку графика. Если на stonfi 404 — пробует dedust.
    Возвращает (bytes, content_type) или (None, None)."""
    dex_l = (dex_id or "").lower()
    primary = "dedust" if "dedust" in dex_l else "stonfi"
    fallback = "stonfi" if primary == "dedust" else "dedust"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            for platform in (primary, fallback):
                url = _dtrade_chart_url(contract, platform, mcap_usd, liq_usd)
                async with s.get(url) as r:
                    ct = (r.headers.get("content-type") or "").lower()
                    if r.status == 200 and ct.startswith("image/"):
                        return await r.read(), ct
                    # не картинка — попробуем другой DEX (часто 404 для незнакомого пула)
    except Exception as e:
        logger.warning(f"[ENRICHED] chart fetch err: {e.__class__.__name__}: {e}")
    return None, None


async def _fetch_token_meta(contract: str):
    """DexScreener: вернуть top-pair {symbol, mcap, liq, dex_id, pair_addr} или None."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{contract}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
            async with s.get(url) as r:
                if r.status != 200:
                    return None
                data = await r.json()
    except Exception as e:
        logger.warning(f"[ENRICHED] dexscreener err: {e.__class__.__name__}")
        return None
    pairs = data.get("pairs") or []
    if not pairs:
        return None
    pairs.sort(key=lambda p: (p.get("liquidity") or {}).get("usd") or 0, reverse=True)
    top = pairs[0]
    base = top.get("baseToken") or {}
    return {
        "symbol": base.get("symbol") or "TOKEN",
        "name": base.get("name") or "",
        "mcap": top.get("marketCap") or top.get("fdv"),
        "liq": (top.get("liquidity") or {}).get("usd"),
        "price": top.get("priceUsd"),
        "dex_id": top.get("dexId") or "stonfi",
        "pair": top.get("pairAddress"),
        "change_6h": (top.get("priceChange") or {}).get("h6"),
    }


INF_STATS_TTL = 300  # секунд: обновляем стату инфла не чаще раза в 5 мин
INF_STATS_LOOKBACK = 50  # сколько последних постов автора учитываем

# 🤖 Ollama (через ssh-tunnel ollama-tunnel.service → sus:11434)
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:7b"
OLLAMA_TIMEOUT = 25  # сек: модель грузится первый раз дольше
AI_TAKE_TTL = 60 * 60  # 1 час — кэш ответа по hash входа


def _ensure_ai_cache_table():
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_cache (
                key   TEXT PRIMARY KEY,
                text  TEXT NOT NULL,
                ts    REAL NOT NULL
            )
        """)
        conn.commit()


def _ai_cache_get(key: str) -> str | None:
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            row = conn.execute(
                "SELECT text, ts FROM ai_cache WHERE key = ?", (key,)
            ).fetchone()
        if not row:
            return None
        if _time.time() - row[1] > AI_TAKE_TTL:
            return None
        return row[0]
    except Exception:
        return None


def _ai_cache_put(key: str, text: str):
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ai_cache(key, text, ts) VALUES(?,?,?)",
                (key, text, _time.time()),
            )
            conn.commit()
    except Exception as e:
        logger.debug("ai_cache_put failed: %s", e)


def _load_ai_prompt() -> str:
    """Prompt for the local LLM (Ollama etc.). Set AI_PROMPT_FILE to a text file
    with your custom prompt; see prompts/hot_take.example.txt for the shape."""
    path = os.environ.get("AI_PROMPT_FILE", "").strip()
    if path and Path(path).exists():
        return Path(path).read_text(encoding="utf-8")
    return (
        "You are a crypto trading assistant.\n"
        "Given a token contract, market metrics (MCAP, LIQ, 6h change), source-channel "
        "reputation (hearts/clowns), duplicate-count (how many times we've seen this CA "
        "in the last N days) and the original poster's text, produce ONE short comment.\n\n"
        "Constraints:\n"
        "- 1-2 sentences, max 180 characters.\n"
        "- No markdown, no lists, no emoji at the start of the reply.\n"
        "- If the metrics show a rug-pull / dump / stale-post pattern, call it out plainly.\n"
        "- If the token looks reasonable, a neutral one-liner is fine.\n"
        "- Do not quote the poster verbatim; comment on the substance.\n"
    )


AI_SYSTEM_PROMPT = _load_ai_prompt()


async def ai_hot_take(
    symbol: str,
    src_text: str,
    mcap,
    liq,
    change_6h,
    author: str,
    hearts: int,
    clowns: int,
    bayan_count: int,
) -> str | None:
    """Спрашивает Ollama про коротенький саркастичный коммент.
    Кэширует ответ в SQLite по хешу входа."""
    import hashlib

    src_short = (src_text or "")[:400]
    key_src = f"{symbol}|{src_short}|{int((mcap or 0) // 100)}|{int((liq or 0) // 100)}|{author}|{bayan_count}"
    key = hashlib.sha256(key_src.encode("utf-8")).hexdigest()
    cached = _ai_cache_get(key)
    if cached is not None:
        return cached

    user_msg = (
        f"Тикер: ${symbol}\n"
        f'Текст инфла @{author}: "{src_short.strip()}"\n'
        f"(Помни: в тексте могут быть ники других трейдеров маленькими буквами "
        f"— это люди, не числа и не предметы.)\n"
        f"Метрики: MCAP {_fmt_usd(mcap) if mcap else '?'}, "
        f"LIQ {_fmt_usd(liq) if liq else '?'}, "
        f"6h: {('+' + str(round(change_6h, 1)) + '%') if change_6h is not None else '?'}\n"
        f"Репутация автора (50 постов): ❤️ {hearts} / 🤡 {clowns}\n"
        f"Баян: этот CA уже видели в наших каналах {bayan_count} раз.\n\n"
        f"Дай одну короткую саркастичную фразу-комментарий. "
        f"Опирайся на МЕТРИКИ и баян, а не на буквальное содержание текста инфла."
    )

    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": AI_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "options": {
            "temperature": 0.6,
            "num_predict": 120,
        },
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=OLLAMA_TIMEOUT)
        ) as s:
            async with s.post(OLLAMA_URL, json=payload) as r:
                if r.status != 200:
                    logger.warning(f"[AI] HTTP {r.status}")
                    return None
                data = await r.json()
        text = ((data.get("message") or {}).get("content") or "").strip()
        # Чистим лишнее: разворачиваем <think>...</think> у reasoning-моделей
        if "<think>" in text and "</think>" in text:
            text = text.split("</think>", 1)[1].strip()
        text = text.strip().strip('"').strip()
        if len(text) > 240:
            text = text[:237].rstrip() + "..."
        if text:
            _ai_cache_put(key, text)
        return text or None
    except Exception as e:
        logger.warning(f"[AI] err: {e.__class__.__name__}: {e}")
        return None


def _ensure_contract_seen_table():
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contract_seen (
                contract     TEXT NOT NULL,
                chat_id      INTEGER NOT NULL,
                msg_id       INTEGER NOT NULL,
                src_username TEXT,
                ts           REAL NOT NULL,
                PRIMARY KEY (contract, chat_id, msg_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_contract_seen_contract ON contract_seen(contract)"
        )
        conn.commit()


def contract_seen_record(
    contract: str, chat_id: int, msg_id: int, src_username: str | None
):
    """Insert OR IGNORE — повторный insert того же поста не дублирует счётчик."""
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO contract_seen(contract, chat_id, msg_id, src_username, ts) VALUES(?,?,?,?,?)",
                (
                    contract,
                    int(chat_id),
                    int(msg_id),
                    (src_username or "").lstrip("@") or None,
                    _time.time(),
                ),
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"[BAYAN] insert err: {e.__class__.__name__}: {e}")


def contract_seen_summary(contract: str) -> dict:
    """Возвращает {count, first_username, first_ts} для контракта.
    count = сколько уникальных постов с этим CA мы видели."""
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            row = conn.execute(
                "SELECT COUNT(*), MIN(ts) FROM contract_seen WHERE contract = ?",
                (contract,),
            ).fetchone()
            count = int(row[0] or 0)
            first_ts = float(row[1]) if row[1] is not None else 0.0
            first_user = None
            if count:
                ur = conn.execute(
                    "SELECT src_username FROM contract_seen WHERE contract = ? AND ts = ? LIMIT 1",
                    (contract, first_ts),
                ).fetchone()
                first_user = ur[0] if ur else None
    except Exception as e:
        logger.warning(f"[BAYAN] read err: {e.__class__.__name__}: {e}")
        return {"count": 0, "first_username": None, "first_ts": 0}
    return {"count": count, "first_username": first_user, "first_ts": first_ts}


def _humanize_age(seconds: float) -> str:
    s = int(_time.time() - seconds)
    if s < 60:
        return f"{s} сек"
    if s < 3600:
        return f"{s // 60} мин"
    if s < 86400:
        return f"{s // 3600} ч"
    return f"{s // 86400} д"


def _ensure_inf_stats_table():
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inf_stats (
                username     TEXT PRIMARY KEY,
                hearts       INTEGER NOT NULL DEFAULT 0,
                clowns       INTEGER NOT NULL DEFAULT 0,
                last_update  REAL    NOT NULL DEFAULT 0
            )
        """)
        conn.commit()


def _inf_stats_read(username: str):
    with sqlite3.connect(SQLITE_PATH) as conn:
        row = conn.execute(
            "SELECT hearts, clowns, last_update FROM inf_stats WHERE username = ?",
            (username,),
        ).fetchone()
    if not row:
        return None
    return {"hearts": row[0], "clowns": row[1], "last_update": row[2]}


def _inf_stats_write(username: str, hearts: int, clowns: int):
    now = _time.time()
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO inf_stats(username, hearts, clowns, last_update) VALUES(?,?,?,?)",
            (username, hearts, clowns, now),
        )
        conn.commit()


def get_inf_stats_fast(username: str, userbot_client) -> dict:
    """Fire-and-forget вариант. НЕ блокирует pipeline.
    - Если в SQLite есть запись (даже устаревшая) — отдаём её сразу.
    - Если запись устарела (>INF_STATS_TTL) — фоновая задача обновит.
    - Если записи нет вообще — отдаём 0/0 и запускаем фон.
    Главное: возвращает за <1мс, не упирается во FloodWait."""
    if not username or username == "?":
        return {"hearts": 0, "clowns": 0}
    cached = _inf_stats_read(username)
    now = _time.time()
    stale = (not cached) or (now - cached["last_update"] >= INF_STATS_TTL)
    if stale and userbot_client:
        # фоновая задача обновит кэш — следующий enriched-пост увидит свежие данные
        asyncio.create_task(_inf_stats_refresh_bg(userbot_client, username))
    if cached:
        return {"hearts": cached["hearts"], "clowns": cached["clowns"]}
    return {"hearts": 0, "clowns": 0}


async def _inf_stats_refresh_bg(userbot_client, username: str):
    """Фоновое обновление inf_stats. Может висеть на FloodWait, но никого не блокирует."""
    try:
        hearts = clowns = 0
        async for m in userbot_client.iter_messages(username, limit=INF_STATS_LOOKBACK):
            rxns = _extract_reactions(m)
            hearts += rxns.get("❤") or rxns.get("❤️") or 0
            clowns += rxns.get("🤡") or 0
        _inf_stats_write(username, hearts, clowns)
    except Exception as e:
        logger.warning(f"[INF_STATS_BG] {username}: {e.__class__.__name__}: {e}")


async def get_or_update_inf_stats(userbot_client, username: str) -> dict:
    """Возвращает {hearts, clowns} автора. Кэш TTL 5 мин в SQLite.
    Свежий пересчёт: iter_messages(username, limit=50), суммируем реакции."""
    if not username or username == "?":
        return {"hearts": 0, "clowns": 0}
    cached = _inf_stats_read(username)
    now = _time.time()
    if cached and (now - cached["last_update"] < INF_STATS_TTL):
        return {"hearts": cached["hearts"], "clowns": cached["clowns"]}
    if not userbot_client:
        return cached or {"hearts": 0, "clowns": 0}
    try:
        hearts = clowns = 0
        async for m in userbot_client.iter_messages(username, limit=INF_STATS_LOOKBACK):
            rxns = _extract_reactions(m)
            hearts += rxns.get("❤") or rxns.get("❤️") or 0
            clowns += rxns.get("🤡") or 0
        _inf_stats_write(username, hearts, clowns)
        return {"hearts": hearts, "clowns": clowns}
    except Exception as e:
        logger.warning(f"[INF_STATS] {username}: {e.__class__.__name__}: {e}")
        return cached or {"hearts": 0, "clowns": 0}


INFLOW_DIR = Path("/var/cache/chart-proxy/inflow")
INFLOW_AVATARS = INFLOW_DIR / "avatars"
INFLOW_MEDIA = INFLOW_DIR / "media"
AVATAR_TTL = 24 * 3600  # сутки


def _avatar_path(username: str) -> Path:
    safe = (username or "").lstrip("@").lower().replace("/", "_") or "_unknown"
    return INFLOW_AVATARS / f"{safe}.jpg"


def _post_media_path(chat_id: int, msg_id: int) -> Path:
    return INFLOW_MEDIA / f"{int(chat_id)}_{int(msg_id)}.jpg"


async def ensure_channel_avatar(userbot_client, username: str) -> bool:
    """Скачивает аватар канала в /var/cache/chart-proxy/inflow/avatars/<u>.jpg.
    Лениво — если файл свежий, не трогаем."""
    if not username or username == "?":
        return False
    p = _avatar_path(username)
    try:
        INFLOW_AVATARS.mkdir(parents=True, exist_ok=True)
        if p.exists() and (_time.time() - p.stat().st_mtime < AVATAR_TTL):
            return True
        # download_profile_photo пишет в файл; передаём str(p)
        out = await userbot_client.download_profile_photo(
            username.lstrip("@"), file=str(p)
        )
        return bool(out)
    except Exception as e:
        logger.warning(f"[AVATAR] {username}: {e.__class__.__name__}: {e}")
        return False


async def ensure_post_media(userbot_client, msg, chat_id: int) -> bool:
    """Если у поста есть photo — скачивает в /var/cache/chart-proxy/inflow/media/.
    Возвращает True если файл создан или уже был."""
    try:
        if not msg or not getattr(msg, "photo", None):
            return False
        INFLOW_MEDIA.mkdir(parents=True, exist_ok=True)
        p = _post_media_path(chat_id, msg.id)
        if p.exists() and p.stat().st_size > 0:
            return True
        out = await userbot_client.download_media(msg, file=str(p))
        return bool(out) and p.exists()
    except Exception as e:
        logger.warning(f"[MEDIA] err: {e.__class__.__name__}: {e}")
        return False


def _extract_reactions(msg) -> dict:
    """Достаёт {emoji: count} из msg.reactions. Только ReactionEmoji (unicode)."""
    out = {}
    try:
        r = getattr(msg, "reactions", None)
        if not r or not getattr(r, "results", None):
            return out
        for rc in r.results:
            reaction = getattr(rc, "reaction", None)
            emo = getattr(reaction, "emoticon", None)
            if emo:
                out[emo] = int(getattr(rc, "count", 0) or 0)
    except Exception as e:
        logger.debug("_extract_reactions failed: %s", e)
    return out


async def _enrich_with_ai(
    *,
    sent_message_id: int,
    base_caption: str,
    api_link_preview,
    reply_markup,
    symbol: str,
    src_text: str,
    meta,
    author: str,
    hearts: int,
    clowns: int,
    bayan_count: int,
):
    """Дёргает Ollama и edit'ит caption уже отправленного поста — добавляет 🤖 строку."""
    try:
        mcap = (meta or {}).get("mcap")
        liq = (meta or {}).get("liq")
        change_6h = (meta or {}).get("change_6h")
        take = await ai_hot_take(
            symbol, src_text, mcap, liq, change_6h, author, hearts, clowns, bayan_count
        )
        if not take:
            return
        # Чистим спецсимволы → HTML-escape (тег вставляем сами 🤖)
        esc = take.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        new_caption = base_caption + f"\n\n🤖 {esc}"
        # Caption limit для sendMessage — 4096. На всякий случай.
        if len(new_caption) > 4090:
            new_caption = new_caption[:4090]
        edit_payload = {
            "chat_id": ENRICHED_TARGET_CHANNEL,
            "message_id": sent_message_id,
            "text": new_caption,
            "parse_mode": "HTML",
            "reply_markup": reply_markup,
        }
        if api_link_preview:
            edit_payload["link_preview_options"] = api_link_preview
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json=edit_payload,
            ) as r:
                resp = await r.json()
                if not resp.get("ok"):
                    logger.warning(f"[AI-EDIT] err: {resp}")
                else:
                    logger.info(f"[AI-EDIT] ${symbol}: {take[:80]}")
    except Exception as e:
        logger.warning(f"[AI-EDIT] exc: {e.__class__.__name__}: {e}")


async def post_enriched(
    contract: str,
    src_username: str,
    src_msg_id: int,
    src_text: str,
    userbot_client=None,
    reactions: dict | None = None,
    src_msg=None,
    src_chat_id: int | None = None,
):
    """Шлёт enriched-карточку в ENRICHED_TARGET_CHANNEL через bot_client.
    Безопасно — любые ошибки гасит логом, не аффектит основной форвард."""
    if not ENRICHED_ENABLED or not bot_client:
        return
    try:
        from telethon import Button

        # ⚡ Параллельный pipeline: все I/O вызовы запускаем одновременно через gather.
        # Экономия ~300мс по сравнению с последовательным await.
        username_clean = (
            (src_username or "").lstrip("@")
            if src_username and src_username != "?"
            else ""
        )

        meta_task = asyncio.create_task(_fetch_token_meta(contract))
        avatar_task = (
            asyncio.create_task(ensure_channel_avatar(userbot_client, username_clean))
            if userbot_client and username_clean
            else None
        )
        media_task = (
            asyncio.create_task(ensure_post_media(userbot_client, src_msg, src_chat_id))
            if userbot_client and src_msg is not None and src_chat_id is not None
            else None
        )
        # stats — fire-and-forget: возвращает cached если есть, обновление в фоне.
        stats = get_inf_stats_fast(username_clean, userbot_client)

        meta = await meta_task
        has_avatar = (await avatar_task) if avatar_task else False
        has_media = (await media_task) if media_task else False
        symbol = (meta and meta["symbol"]) or "Token"

        # 💩 баян (SQLite — синхронный, быстрый)
        bayan = contract_seen_summary(contract)

        # 📊 chart-URL → /chart (HTML с OG-тегами), og:image → /composite.
        # В /composite передаём ВСЕ метрики: график будет нарисован поверх стиля Stitch
        # с реальной статой инфла, баяном и цитатой поста.
        chart_url = None
        if meta:
            dex_l = (meta.get("dex_id") or "").lower()
            platform = "dedust" if "dedust" in dex_l else "stonfi"
            from urllib.parse import urlencode

            # Подготовим quote — обрезанный текст поста без EQ-адреса
            quote = (src_text or "").strip().replace(contract, "").strip()
            # Срезаем переносы → одной строкой
            quote = " ".join(quote.split())
            if len(quote) > 220:
                quote = quote[:217] + "..."
            bayan_count = bayan.get("count", 0) if isinstance(bayan, dict) else 0
            # Подсчёт постов автора в корпусе (опц.) — пропускаем, передадим только если есть
            params = {
                "c": contract,
                "s": symbol,
                "p": platform,
                "m": int(meta.get("mcap") or 0),
                "l": int(meta.get("liq") or 0),
                "ch": (meta.get("change_6h") or 0),
                "h": int(stats.get("hearts") or 0),
                "cl": int(stats.get("clowns") or 0),
                "bay": bayan_count,
                "q": quote,
                "t": int(_time.time()),
                "v": "5",
            }
            if username_clean:
                params["u"] = username_clean
            if has_media and src_chat_id and src_msg_id:
                params["cid"] = src_chat_id
                params["mid"] = src_msg_id
            # Unique path segment → Telegram guaranteed to re-parse the preview
            # even when the same CA is posted again (busts their per-URL cache).
            nonce = f"{int(_time.time() * 1000)}-{src_msg_id or 0}"
            if CHART_PROXY_DOMAIN:
                chart_url = f"https://{CHART_PROXY_DOMAIN}/chart/{nonce}?{urlencode(params)}"
            else:
                chart_url = None  # no chart-proxy configured, send without link preview

        # caption (HTML). Для link-preview chart-URL ВИДИМОЙ ссылкой в первой строке
        # (Telegram игнорирует TextUrl-ссылки в <a href>, нужен plain URL).
        # Скроем "уродливость" URL — поставим его за zero-width space в самом начале.
        lines = []

        # Header:  "{handle} posted:"  where {handle} is @username if valid,
        # otherwise the chat title (for private / fragment-only channels).
        uname = (
            (src_username or "").lstrip("@")
            if src_username and src_username != "?"
            else ""
        )
        # username: strict [A-Za-z0-9_], length 4..32 — otherwise treat as a title
        is_username = bool(uname) and bool(re.fullmatch(r"[A-Za-z0-9_]{4,32}", uname))
        esc_uname = (
            uname.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        if is_username:
            handle = f"@{esc_uname}"
        elif uname:
            handle = esc_uname
        else:
            handle = ""
        if handle:
            lines.append(f"<b>{CAPTION_HEADER_TEMPLATE.format(handle=handle)}</b>")
        else:
            # No handle at all — omit the header line.
            pass

        # текст оригинала — в expandable blockquote (сворачивается при длинном тексте)
        clean = (src_text or "").strip().replace(contract, "").strip()
        if clean:
            if len(clean) > 1500:
                clean = clean[:1500].rstrip() + "…"
            # Безопасный HTML-escape — пользовательский текст не должен сломать разметку
            esc = clean.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append("")
            lines.append(f"<blockquote expandable>{esc}</blockquote>")

        # 💩 баян-детектор в caption — отключено (баян виден в композите справа в footer)
        # if bayan["count"] > 1:
        #     first_age = _humanize_age(bayan["first_ts"])
        #     first_u = bayan["first_username"]
        #     if first_u:
        #         bline = f"💩 Уже видели {bayan['count']} раз. Первым @{first_u} {first_age} назад."
        #     else:
        #         bline = f"💩 Уже видели {bayan['count']} раз. Первый раз {first_age} назад."
        #     lines.append("")
        #     lines.append(bline)

        # накопительная стата автора (всё время, кэш в SQLite 5 мин)
        s_parts = []
        if stats.get("hearts"):
            s_parts.append(f"❤️ {stats['hearts']}")
        if stats.get("clowns"):
            s_parts.append(f"🤡 {stats['clowns']}")
        if s_parts:
            lines.append("")
            lines.append("  ".join(s_parts))

        lines.append("")
        lines.append(f"<code>{contract}</code>")

        caption = "\n".join(lines)

        # кнопки — только DTrade-реферал (Telegram inline-кнопки серые,
        # цвет менять нельзя — даём зелёную эмоджи перед текстом)
        buttons = [[Button.url(f"🟢 Dtrade {symbol}", dtrade_buy_link(contract))]]

        try:
            # Отправляем через HTTP Bot API напрямую (Bot API 9.4+ — style:"success" → зелёная кнопка).
            api_payload = {
                "chat_id": ENRICHED_TARGET_CHANNEL,
                "text": caption,
                "parse_mode": "HTML",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Купить в Dtrade",
                                "url": dtrade_buy_link(contract),
                                "style": "success",
                            }
                        ]
                    ]
                },
            }
            if chart_url:
                api_payload["link_preview_options"] = {
                    "url": chart_url,
                    "prefer_large_media": True,
                    "show_above_text": True,
                }
            else:
                api_payload["link_preview_options"] = {"is_disabled": True}

            sent_message_id = None
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as s:
                async with s.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=api_payload,
                ) as r:
                    resp = await r.json()
                    if not resp.get("ok"):
                        logger.warning(f"[ENRICHED] Bot API err: {resp}")
                    else:
                        sent_message_id = (resp.get("result") or {}).get("message_id")
            logger.info(
                f"[ENRICHED] sent → {ENRICHED_TARGET_CHANNEL} ${symbol} (@{src_username} #{src_msg_id})"
            )

            # 🧪 Дубль в тестовый канал ОТ USERBOT (без кнопки, чтобы работали комменты).
            # Preview наверху (invert_media=True), цитата свёрнута (collapsed blockquote).
            if (
                ENRICHED_DUP_CHANNEL
                and ENRICHED_DUP_CHANNEL != ENRICHED_TARGET_CHANNEL
                and userbot_client
            ):
                # Footer-ссылки: "Купить в Dtrade | Redotrade"
                dtr_url = dtrade_buy_link(contract)
                redo_url = redo_buy_link(contract)
                footer_html = (
                    f'\n\n<a href="{dtr_url}">Купить в Dtrade</a>'
                    f' | <a href="{redo_url}">Redotrade</a>'
                )

                async def _send_dup_userbot(
                    cap=caption + footer_html,
                    ch_url=chart_url,
                    sym=symbol,
                    ub=userbot_client,
                ):
                    try:
                        from telethon.tl.functions.messages import (
                            SendMediaRequest,
                            SendMessageRequest,
                        )
                        from telethon.tl.types import (
                            InputMediaWebPage,
                            MessageEntityBlockquote,
                        )
                        from telethon.extensions import html as tl_html

                        text_plain, entities = tl_html.parse(cap)
                        # Force blockquote collapsed=True (Telethon >= 1.36 supports the field;
                        # older versions raise AttributeError, which we tolerate).
                        for e in entities or []:
                            if isinstance(e, MessageEntityBlockquote):
                                try:
                                    e.collapsed = True
                                except AttributeError as exc:
                                    logger.debug("collapsed=True not supported: %s", exc)

                        peer = await ub.get_input_entity(ENRICHED_DUP_CHANNEL)
                        if ch_url:
                            # Preview из chart_url, наверху (invert_media=True)
                            req = SendMediaRequest(
                                peer=peer,
                                media=InputMediaWebPage(
                                    url=ch_url, force_large_media=True, optional=True
                                ),
                                message=text_plain,
                                entities=entities,
                                invert_media=True,
                            )
                        else:
                            req = SendMessageRequest(
                                peer=peer,
                                message=text_plain,
                                entities=entities,
                                no_webpage=True,
                            )
                        await ub(req)
                        logger.info(
                            f"[ENRICHED_DUP] sent → {ENRICHED_DUP_CHANNEL} ${sym} (userbot)"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[ENRICHED_DUP] err: {e.__class__.__name__}: {e}"
                        )

                asyncio.create_task(_send_dup_userbot())

            # 🤖 AI hot take в caption — отключено (по запросу)
            # if sent_message_id:
            #     asyncio.create_task(_enrich_with_ai(
            #         sent_message_id=sent_message_id,
            #         base_caption=caption,
            #         api_link_preview=api_payload.get("link_preview_options"),
            #         reply_markup=api_payload["reply_markup"],
            #         symbol=symbol,
            #         src_text=src_text,
            #         meta=meta,
            #         author=(src_username or "?").lstrip("@"),
            #         hearts=stats.get("hearts", 0),
            #         clowns=stats.get("clowns", 0),
            #         bayan_count=bayan["count"],
            #     ))
        except Exception as e:
            logger.warning(f"[ENRICHED] send err: {e.__class__.__name__}: {e}")
            # fallback — без preview
            try:
                await bot_client.send_message(
                    ENRICHED_TARGET_CHANNEL,
                    caption,
                    buttons=buttons,
                    parse_mode="html",
                    link_preview=False,
                )
                logger.info(f"[ENRICHED] sent (fallback no-preview) → ${symbol}")
            except Exception as e2:
                logger.warning(
                    f"[ENRICHED] fallback also failed: {e2.__class__.__name__}: {e2}"
                )

    except Exception as e:
        logger.warning(f"[ENRICHED] outer err: {e.__class__.__name__}: {e}")


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
)
# Telethon слишком шумный на DEBUG — оставляем INFO
logging.getLogger("telethon").setLevel(logging.INFO)
logger = logging.getLogger("forwarder")

AD_TAGS = (
    r"#реклама",
    r"#promo",
    r"#promо",  # на случай русской "о"
    r"#ads",
    r"#ad",
    r"#sponsored",
    r"#партнерка",
    r"#adverts",
    r"#промо",
)
AD_REGEX = re.compile("(" + "|".join(AD_TAGS) + ")", flags=re.IGNORECASE)


class RateLimiter:
    def __init__(self, per_minute: int, min_delay: float):
        self.per_minute = per_minute
        self.min_delay = min_delay
        self.events: Deque[float] = deque()
        self._last_ts: float = 0.0
        self._lock = asyncio.Lock()

    async def throttle(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()

            # минимальная пауза
            since_last = now - self._last_ts
            if since_last < self.min_delay:
                await asyncio.sleep(self.min_delay - since_last)
                now = loop.time()

            # окно 60с
            window_start = now - 60.0
            while self.events and self.events[0] < window_start:
                self.events.popleft()

            if len(self.events) >= self.per_minute:
                sleep_for = self.events[0] + 60.0 - now
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                    now = loop.time()
                    window_start = now - 60.0
                    while self.events and self.events[0] < window_start:
                        self.events.popleft()

            self.events.append(loop.time())
            self._last_ts = loop.time()


forward_limiter = RateLimiter(MAX_ACTIONS_PER_MINUTE, MIN_DELAY_BETWEEN_ACTIONS)
resolve_limiter = RateLimiter(RESOLVE_PER_MINUTE, RESOLVE_MIN_DELAY)


class Dedupe:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self):
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY)")
            conn.commit()
        finally:
            conn.close()

    def _exists(self, key: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM seen WHERE key=?", (key,))
            return cur.fetchone() is not None
        finally:
            conn.close()

    def _insert(self, key: str):
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO seen(key) VALUES (?)", (key,))
            conn.commit()
        finally:
            conn.close()

    # события/сообщения
    def seen_msg(self, chat_id: int, msg_id: int) -> bool:
        return self._exists(f"msg:{chat_id}:{msg_id}")

    def mark_msg(self, chat_id: int, msg_id: int):
        self._insert(f"msg:{chat_id}:{msg_id}")

    # альбомы
    def seen_album(self, chat_id: int, gid: int) -> bool:
        return self._exists(f"album:{chat_id}:{gid}")

    def mark_album(self, chat_id: int, gid: int):
        self._insert(f"album:{chat_id}:{gid}")

    # ✅ уже ОТПРАВЛЕНО (чтобы EDIT не спамил)
    def seen_fwd(self, chat_id: int, msg_id: int) -> bool:
        return self._exists(f"fwd:{chat_id}:{msg_id}")

    def mark_fwd(self, chat_id: int, msg_id: int):
        self._insert(f"fwd:{chat_id}:{msg_id}")


dedupe = Dedupe(SQLITE_PATH)
_ensure_inf_stats_table()
_ensure_contract_seen_table()
_ensure_ai_cache_table()

INFLIGHT: set[tuple[int, int]] = set()
INFLIGHT_LOCK = asyncio.Lock()

FORWARDED_MAP: Dict[Tuple[int, int], Dict[str, Any]] = {}

# Маппинг chat_id → username (заполняется при resolve_channels из channels.txt).
# Нужен потому что Telethon 1.36 в Channel-объекте отдаёт username=None
# для каналов с collectible/fragment-username (хотя по сути @username активен).
USERNAME_BY_CHAT_ID: Dict[int, str] = {}


def normalize_channel(line: str) -> Union[int, str, None]:
    s = unicodedata.normalize("NFKC", (line or "").strip())
    if not s or s.startswith("#"):
        return None
    if "t.me/" in s:
        s = (
            s.replace("https://t.me/", "")
            .replace("http://t.me/", "")
            .replace("t.me/", "")
        )
        s = s.split("?")[0].strip().strip("/")
    if s.startswith("@"):
        s = s[1:]
    if (s.startswith("-") and s[1:].isdigit()) or s.isdigit():
        try:
            return int(s)
        except Exception:
            return None
    return "@" + s


def dedupe_keep_order(seq: List[Union[int, str]]) -> List[Union[int, str]]:
    seen = set()
    out = []
    for x in seq:
        key = x if isinstance(x, int) else x.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def read_channels(path: str) -> List[Union[int, str]]:
    out = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                ch = normalize_channel(raw)
                if ch:
                    out.append(ch)
    except FileNotFoundError:
        logger.error(f"Файл {path} не найден")
    return dedupe_keep_order(out)


async def safe_log(client: TelegramClient, text: str, level="info"):
    if LOG_CHAT_ID:
        try:
            await client.send_message(LOG_CHAT_ID, text)
        except Exception as e:
            logger.debug("safe_log DM failed: %s", e)
    getattr(logger, level)(text if level != "error" else f"ERROR: {text}")


def msg_text(m: types.Message) -> str:
    return (m.message or "").strip()


def source_str(event) -> str:
    try:
        chat = event.chat
        if isinstance(chat, (types.User, types.Chat, types.Channel)) and getattr(
            chat, "username", None
        ):
            return f"@{chat.username}"
        return f"id:{getattr(event, 'chat_id', 'unknown')}"
    except Exception:
        return f"id:{getattr(event, 'chat_id', 'unknown')}"


def get_chat_id(msg: types.Message, src_event=None) -> int:
    """
    Важно: чтобы NEW и EDIT гарантированно видели один и тот же chat_id.
    """
    if src_event is not None:
        cid = getattr(src_event, "chat_id", None)
        if cid is not None:
            return cid

    cid = getattr(msg, "chat_id", None)
    if cid is not None:
        return cid

    peer = getattr(msg, "peer_id", None)
    if isinstance(peer, types.PeerChannel):
        return -(10**12) - int(peer.channel_id)
    if isinstance(peer, types.PeerChat):
        return -int(peer.chat_id)
    if isinstance(peer, types.PeerUser):
        return int(peer.user_id)

    return 0


async def build_client() -> TelegramClient:
    return TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        device_model="Linux",
        system_version="Ubuntu 22.04",
        app_version="10.5",
        lang_code="ru",
    )


START_TS_UTC = datetime.now(timezone.utc)
EDIT_CUTOFF_UTC = START_TS_UTC - timedelta(minutes=EDIT_GRACE_MINUTES)

# 2026-05-30: жёсткий cutoff по времени публикации.
# Если Telegram задержал доставку (холодный канал, "проснулся" через час и т.д.),
# мы не должны форвардить старый пост как новый.
MAX_PUBLISH_AGE_SEC = 300  # 5 минут


def is_within_publish_window(m: types.Message) -> bool:
    """True если пост опубликован не более MAX_PUBLISH_AGE_SEC секунд назад."""
    d = getattr(m, "date", None)
    if d is None:
        return True
    age = (datetime.now(timezone.utc) - d).total_seconds()
    return age <= MAX_PUBLISH_AGE_SEC


def is_fresh_new(m: types.Message) -> bool:
    d = getattr(m, "date", None)
    if d is None:
        return True
    return d >= (START_TS_UTC - timedelta(seconds=5))


def is_fresh_edit(m: types.Message) -> bool:
    d = getattr(m, "date", None)
    if d is None:
        return True
    return d >= EDIT_CUTOFF_UTC


async def resolve_channels(client: TelegramClient, raw_chats: List[Union[int, str]]):
    resolved = []
    skipped: Dict[Union[int, str], str] = {}

    for ch in raw_chats:
        await resolve_limiter.throttle()
        try:
            ent = await client.get_input_entity(ch)
            resolved.append(ent)
            # Сохраняем маппинг chat_id → username из channels.txt
            # (нужно потому что Telethon 1.36 в Channel-объекте отдаёт username=None
            # для каналов с collectible/fragment-username)
            if isinstance(ch, str) and ch.startswith("@"):
                uname = ch[1:]
                cid = getattr(ent, "channel_id", None) or getattr(ent, "id", None)
                if cid:
                    USERNAME_BY_CHAT_ID[-(10**12) - int(cid)] = uname
                    USERNAME_BY_CHAT_ID[int(cid)] = uname
        except FloodWaitError as e:
            wait = int(e.seconds) + 3
            logger.warning(f"Resolve FloodWait {e.seconds}s для {ch} — жду {wait}s…")
            await asyncio.sleep(wait)
            try:
                ent = await client.get_input_entity(ch)
                resolved.append(ent)
            except Exception as e2:
                skipped[ch] = f"после ожидания: {e2.__class__.__name__}"
        except UsernameNotOccupiedError:
            skipped[ch] = "username не занят (ошибка/опечатка)"
        except ChannelInvalidError:
            skipped[ch] = "ChannelInvalid (нет доступа/не канал)"
        except ValueError as e:
            skipped[ch] = f"ValueError: {e}"
        except AuthKeyUnregisteredError:
            raise
        except Exception as e:
            skipped[ch] = f"{e.__class__.__name__}: {e}"

    if skipped:
        bad = ", ".join([f"{k} ({v})" for k, v in list(skipped.items())[:10]])
        more = "" if len(skipped) <= 10 else f" и ещё {len(skipped) - 10}"
        logger.warning(
            f"Отфильтровано проблемных каналов: {len(skipped)} — {bad}{more}"
        )
    logger.info(f"К прослушиванию допущено: {len(resolved)} из {len(raw_chats)}")

    return resolved


async def warmup_channels(client: TelegramClient, resolved_chats):
    """
    Telethon не получает UpdateNewChannelMessage от каналов, для которых
    в session нет PTS-стейта. Загружаем ВСЕ диалоги через iter_dialogs(),
    что инициализирует PTS in-memory для всех каналов юзербота.
    Дополнительно дёргаем get_messages для каждого resolved канала.
    """
    # Шаг 1: iter_dialogs — загружает PTS in-memory для ВСЕХ каналов юзербота
    logger.info("[WARMUP] Шаг 1: загрузка диалогов через iter_dialogs()...")
    dialog_count = 0
    channel_count = 0
    try:
        async for dialog in client.iter_dialogs():
            dialog_count += 1
            if dialog.is_channel:
                channel_count += 1
        logger.info(
            f"[WARMUP] iter_dialogs: {dialog_count} диалогов, {channel_count} каналов"
        )
    except Exception as e:
        logger.error(f"[WARMUP] iter_dialogs ОШИБКА: {e.__class__.__name__}: {e}")

    # Шаг 2: catch_up — подтягивает пропущенные апдейты
    logger.info("[WARMUP] Шаг 2: catch_up()...")
    try:
        await client.catch_up()
        logger.info("[WARMUP] catch_up() завершён")
    except Exception as e:
        logger.error(f"[WARMUP] catch_up ОШИБКА: {e.__class__.__name__}: {e}")

    # Шаг 3: get_messages для каждого resolved канала — доп. гарантия
    logger.info("[WARMUP] Шаг 3: get_messages для resolved каналов...")
    primed = 0
    failed = 0
    for ent in resolved_chats:
        await resolve_limiter.throttle()
        try:
            msgs = await client.get_messages(ent, limit=1)
            for m in msgs:
                cid = get_chat_id(m)
                if cid and m.id:
                    dedupe.mark_msg(cid, m.id)
                    dedupe.mark_fwd(cid, m.id)
            primed += 1
        except FloodWaitError as e:
            wait = int(e.seconds) + 3
            logger.warning(f"[WARMUP] FloodWait {e.seconds}s — жду {wait}s…")
            await asyncio.sleep(wait)
            try:
                msgs = await client.get_messages(ent, limit=1)
                for m in msgs:
                    cid = get_chat_id(m)
                    if cid and m.id:
                        dedupe.mark_msg(cid, m.id)
                        dedupe.mark_fwd(cid, m.id)
                primed += 1
            except Exception as e2:
                failed += 1
                logger.warning(f"[WARMUP] error after wait: {e2}")
        except Exception as e:
            failed += 1
            logger.warning(f"[WARMUP] error: {e.__class__.__name__}: {e}")

    # Шаг 4: проверяем update_state в session
    try:
        import sqlite3 as _sq

        _conn = _sq.connect(
            SQLITE_PATH.replace("forwarder_seen", "session").replace(
                ".sqlite", ".session"
            )
            if "forwarder" in SQLITE_PATH
            else "session.session"
        )
        _cur = _conn.cursor()
        _cur.execute("SELECT count(*) FROM update_state WHERE id != 0")
        us_count = _cur.fetchone()[0]
        # проверяем конкретно SvirydDegens (2123458599)
        _cur.execute("SELECT id, pts FROM update_state WHERE id = 2123458599")
        sv_row = _cur.fetchone()
        _conn.close()
        logger.info(
            f"[WARMUP] update_state: {us_count} каналов в БД. SvirydDegens(2123458599): {sv_row}"
        )
    except Exception as e:
        logger.warning(f"[WARMUP] не смог проверить update_state: {e}")

    logger.info(
        f"[WARMUP] Завершён: get_messages ok={primed}/{len(resolved_chats)} (failed={failed})"
    )


async def forward_safe(client: TelegramClient, payload):
    await forward_limiter.throttle()
    try:
        res = await client.forward_messages(TARGET_CHANNEL, payload)
        contracts_forwarded_total.inc()
        last_forward_time.set(_t.time())
        return res
    except FloodWaitError as e:
        forward_errors_total.labels(type="FloodWait").inc()
        wait = int(e.seconds) + 3
        logger.warning(f"FloodWait {e.seconds}s — жду {wait}s…")
        await asyncio.sleep(wait)
        try:
            res = await client.forward_messages(TARGET_CHANNEL, payload)
            contracts_forwarded_total.inc()
            last_forward_time.set(_t.time())
            return res
        except Exception as e2:
            forward_errors_total.labels(type=type(e2).__name__).inc()
            raise
    except Exception as e:
        forward_errors_total.labels(type=type(e).__name__).inc()
        raise


def _store_forward_mapping(src_chat_id: int, src_msg_id: int, out_msg: types.Message):
    if src_chat_id is None or src_msg_id is None or out_msg is None:
        return
    key = (src_chat_id, src_msg_id)
    FORWARDED_MAP[key] = {
        "target_id": out_msg.id,
        "ts": datetime.now(timezone.utc),
    }


async def handle_single(
    client: TelegramClient, msg: types.Message, tag: str, src_event=None
):
    """
    - NEW: отправляем
    - EDIT: НЕ отправляем повторно
    - Анти-гонка: если NEW и EDIT пришли почти одновременно, второй не пройдёт
    """
    chat_id = get_chat_id(msg, src_event)
    msg_id = getattr(msg, "id", None)
    src = source_str(src_event or msg)

    logger.debug(
        f"[DBG][{tag}] {src} #{msg_id} chat={chat_id} text={msg_text(msg)[:200]} fwd={msg.fwd_from is not None} grouped={getattr(msg, 'grouped_id', None)}"
    )

    gid = getattr(msg, "grouped_id", None)
    if gid is not None:
        # Альбомы обычно ловит handle_album, но через getDifference он может не сработать.
        # Fallback: ждём 5 сек, если альбом не обработан — обрабатываем текст как обычное сообщение.
        logger.debug(f"[DBG][SKIP:ALBUM] {src} #{msg_id}")
        if chat_id and msg_id:
            dedupe.mark_msg(chat_id, msg_id)
        if tag == "NEW" and chat_id and gid:

            async def _album_fallback(
                c=client, m=msg, ch=chat_id, g=gid, s=src, mid=msg_id, se=src_event
            ):
                await asyncio.sleep(5)
                if dedupe.seen_album(ch, g):
                    return  # handle_album уже обработал
                text = msg_text(m)
                if not text or not CONTRACT_PATTERN.search(text):
                    return
                if has_blocked_contract(text):
                    return
                if await _should_skip_text_contracts(text):
                    logger.info(f"[SKIP][STONKS_NEW][ALBUM_FALLBACK] {s} #{mid}")
                    return
                if not is_within_publish_window(m):
                    d = getattr(m, "date", None)
                    age_sec = (
                        int((datetime.now(timezone.utc) - d).total_seconds())
                        if d
                        else -1
                    )
                    logger.info(
                        f"[SKIP][TOO_OLD][ALBUM_FALLBACK] {s} #{mid} age={age_sec}s"
                    )
                    return
                # Legacy forward отключён — альбомы не пересылаем (нет enriched для альбомов).
                logger.info(f"[ALBUM_FALLBACK][SKIP] {s} #{mid} — forward отключён")
                dedupe.mark_fwd(ch, mid)
                dedupe.mark_album(ch, g)

            asyncio.create_task(_album_fallback())
        return

    if tag == "NEW" and not is_fresh_new(msg):
        logger.debug(f"[DBG][SKIP:NOT_FRESH] {src} #{msg_id}")
        return
    if tag == "EDIT" and not is_fresh_edit(msg):
        logger.debug(f"[DBG][SKIP:EDIT_OLD] {src} #{msg_id}")
        return
    # 🚫 не пересылаем посты старше MAX_PUBLISH_AGE_SEC (защита от вспышек холодных каналов / переименований)
    if tag in ("NEW", "PULL") and not is_within_publish_window(msg):
        d = getattr(msg, "date", None)
        age_sec = int((datetime.now(timezone.utc) - d).total_seconds()) if d else -1
        logger.info(f"[DBG][SKIP:TOO_OLD][{tag}] {src} #{msg_id} age={age_sec}s")
        return

    if not chat_id or not msg_id:
        logger.debug(f"[DBG][SKIP:NO_ID] {src}")
        return

    inflight_key = (chat_id, msg_id)

    # ✅ анти-гонка NEW/EDIT
    async with INFLIGHT_LOCK:
        if inflight_key in INFLIGHT:
            logger.debug(f"[DBG][SKIP:INFLIGHT] {src} #{msg_id}")
            dedupe.mark_msg(chat_id, msg_id)
            return
        INFLIGHT.add(inflight_key)

    try:
        # ✅ если уже отправляли — на EDIT не шлём повторно
        if tag == "EDIT" and dedupe.seen_fwd(chat_id, msg_id):
            logger.debug(f"[DBG][SKIP:EDIT_ALREADY_FWD] {src} #{msg_id}")
            return

        # базовый дедуп событий (если уже полностью обработали)
        if dedupe.seen_msg(chat_id, msg_id):
            logger.debug(f"[DBG][SKIP:SEEN] {src} #{msg_id}")
            return

        text = msg_text(msg)
        if not text:
            logger.debug(f"[DBG][SKIP:EMPTY] {src} #{msg_id}")
            return

        # Фильтр рекламы по хэштегам
        if AD_REGEX.search(text):
            await safe_log(
                client, f"[SKIP][AD][{tag}] {src} #{msg_id} — найден рекламный хэштег"
            )
            return

        m = CONTRACT_PATTERN.search(text)
        if not m:
            logger.debug(f"[DBG][SKIP:NO_CONTRACT] {src} #{msg_id} text={text[:200]}")
            return

        # 🚫 блок по контракту
        if has_blocked_contract(text):
            await safe_log(client, f"[SKIP][BLOCK_CONTRACT][{tag}] {src} #{msg_id}")
            return

        # 🚫 фильтр новых stonks-токенов
        if await _should_skip_text_contracts(text):
            await safe_log(client, f"[SKIP][STONKS_NEW][{tag}] {src} #{msg_id}")
            return

        contract = m.group(0)

        # Legacy forward в TARGET_CHANNEL — отключён. Теперь шлём ТОЛЬКО enriched-карточку.
        chat = getattr(src_event, "chat", None) or getattr(msg, "chat", None)
        chan_username = getattr(chat, "username", None) if chat else None
        chan_title = getattr(chat, "title", None) if chat else None
        # Telethon 1.36 для каналов с collectible/fragment-username возвращает None —
        # лезем в локальный маппинг (заполняется при resolve_channels из channels.txt).
        if not chan_username and chat_id:
            chan_username = USERNAME_BY_CHAT_ID.get(int(chat_id))
        src_display = chan_username or chan_title or "?"

        dedupe.mark_fwd(chat_id, msg_id)  # помечаем что обработали
        await safe_log(
            client,
            f"[ENRICHED_ONLY][{tag}] {src} -> {ENRICHED_TARGET_CHANNEL} (#{msg_id})",
        )

        # 💩 баян-учёт
        contract_seen_record(contract, chat_id, msg_id, src_display)

        # 💎 enriched-постинг (НЕ блокирует)
        asyncio.create_task(
            post_enriched(
                contract,
                src_display,
                msg_id,
                text,
                userbot_client=client,
                reactions=_extract_reactions(msg),
                src_msg=msg,
                src_chat_id=chat_id,
            )
        )

    except AuthKeyUnregisteredError:
        await safe_log(
            client,
            "Сессия сброшена сервером. Перезапустите после повторного логина.",
            level="error",
        )
        os._exit(1)
    except Exception as e:
        await safe_log(client, f"[ERR][{tag}] {src} #{msg_id}: {e}", level="error")
    finally:
        # mark обработанным (чтобы повторно не входить) и снять inflight
        dedupe.mark_msg(chat_id, msg_id)
        async with INFLIGHT_LOCK:
            INFLIGHT.discard(inflight_key)


async def handle_album(client: TelegramClient, event: events.Album.Event):
    src = source_str(event)
    chat_id = getattr(event, "chat_id", None)
    gid = getattr(event, "grouped_id", None)
    if gid is None or chat_id is None:
        return

    first = event.messages[0] if event.messages else None
    if first and not is_fresh_new(first):
        return
    if first and not is_within_publish_window(first):
        d = getattr(first, "date", None)
        age_sec = int((datetime.now(timezone.utc) - d).total_seconds()) if d else -1
        logger.info(f"[SKIP][TOO_OLD][ALBUM] group#{gid} age={age_sec}s")
        return
    if dedupe.seen_album(chat_id, gid):
        return

    parts = []
    if getattr(event, "text", None):
        parts.append(event.text)
    for m in event.messages:
        if getattr(m, "message", None):
            parts.append(m.message)
    text = "\n".join(p for p in parts if p).strip()

    # Фильтр рекламы по хэштегам для альбомов
    if text and AD_REGEX.search(text):
        await safe_log(
            client, f"[SKIP][AD][ALBUM] {src} group#{gid} — найден рекламный хэштег"
        )
        dedupe.mark_album(chat_id, gid)
        for m in event.messages:
            dedupe.mark_msg(chat_id, m.id)
        return

    # 🚫 блок по контракту (для альбомов)
    if text and CONTRACT_PATTERN.search(text) and has_blocked_contract(text):
        await safe_log(client, f"[SKIP][BLOCK_CONTRACT][ALBUM] {src} group#{gid}")
        dedupe.mark_album(chat_id, gid)
        for m in event.messages:
            dedupe.mark_msg(chat_id, m.id)
        return

    # 🚫 фильтр новых stonks-токенов (для альбомов)
    if (
        text
        and CONTRACT_PATTERN.search(text)
        and await _should_skip_text_contracts(text)
    ):
        await safe_log(client, f"[SKIP][STONKS_NEW][ALBUM] {src} group#{gid}")
        dedupe.mark_album(chat_id, gid)
        for m in event.messages:
            dedupe.mark_msg(chat_id, m.id)
        return

    if text and CONTRACT_PATTERN.search(text):
        # Legacy forward отключён — альбомы пока не enriched-постим, просто отмечаем.
        for m in event.messages:
            dedupe.mark_fwd(chat_id, m.id)
        await safe_log(client, f"[ALBUM_SKIP_FWD] {src} group#{gid} — forward отключён")
        dedupe.mark_album(chat_id, gid)
        for m in event.messages:
            dedupe.mark_msg(chat_id, m.id)
    else:
        dedupe.mark_album(chat_id, gid)
        for m in event.messages:
            dedupe.mark_msg(chat_id, m.id)


async def handle_deleted(client: TelegramClient, event: events.MessageDeleted.Event):
    chat_id = getattr(event, "chat_id", None)
    if chat_id is None:
        return

    now = datetime.now(timezone.utc)
    for src_msg_id in event.deleted_ids:
        key = (chat_id, src_msg_id)
        info = FORWARDED_MAP.get(key)
        if not info:
            continue

        age = now - info["ts"]
        if age.total_seconds() > DELETE_GRACE_SECONDS:
            del FORWARDED_MAP[key]
            continue

        target_id = info["target_id"]
        try:
            await client.delete_messages(TARGET_CHANNEL, target_id)
            await safe_log(
                client,
                f"[DEL] Удалил в {TARGET_CHANNEL} сообщение #{target_id} "
                f"(оригинал {chat_id}/{src_msg_id} удалён, возраст {int(age.total_seconds())}s)",
                level="info",
            )
        except Exception as e:
            await safe_log(
                client,
                f"[ERR][DEL] Не смог удалить #{target_id} в {TARGET_CHANNEL}: {e}",
                level="error",
            )
        finally:
            FORWARDED_MAP.pop(key, None)


PERIODIC_CATCHUP_INTERVAL = 90  # сек


async def periodic_catchup(client: TelegramClient):
    """Раз в PERIODIC_CATCHUP_INTERVAL сек дергаем catch_up(),
    чтобы Telegram отдавал апдейты для каналов, которые не приходят push-ом.
    Это срезает задержку с ~30 мин (стандартный poll Telethon) до ~1.5 мин."""
    await asyncio.sleep(60)  # дать warmup доделаться
    while True:
        try:
            await client.catch_up()
            logger.debug("[CATCHUP] tick")
        except Exception as e:
            logger.warning(f"[CATCHUP] error: {e.__class__.__name__}: {e}")
        await asyncio.sleep(PERIODIC_CATCHUP_INTERVAL)


KEEP_WARM_INTERVAL = 300  # каждые 5 минут полный круг
KEEP_WARM_PER_CHANNEL_DELAY = 0.4  # секунд между read_ack между каналами


async def keep_warm_loop(client: TelegramClient, resolved_chats):
    """Регулярно шлёт send_read_acknowledge по каждому каналу, чтобы Telegram
    считал юзербот активным читателем и слал push (UpdateNewChannelMessage)
    мгновенно, а не копил апдейты в getDifference раз в 15-60 минут."""
    await asyncio.sleep(90)  # дать warmup и catch_up отработать
    while True:
        ok = 0
        err = 0
        flood = 0
        for ent in resolved_chats:
            try:
                await client.send_read_acknowledge(ent)
                ok += 1
            except FloodWaitError as e:
                wait = int(e.seconds) + 3
                flood += 1
                logger.warning(f"[WARM] FloodWait {e.seconds}s — sleep {wait}s")
                await asyncio.sleep(wait)
            except Exception as e:
                err += 1
                logger.debug(f"[WARM] read_ack err {ent}: {e.__class__.__name__}: {e}")
            await asyncio.sleep(KEEP_WARM_PER_CHANNEL_DELAY)
        logger.info(
            f"[WARM] tick ok={ok}/{len(resolved_chats)} err={err} flood={flood}"
        )
        await asyncio.sleep(KEEP_WARM_INTERVAL)


FAST_PULL_INTERVAL = 6.0  # 2026-06-27: 8s → 6s. 3s давало FloodWait, мониторим.
FAST_PULL_PARALLELISM = 5  # 2026-06-01: снижено с 15 чтобы убрать FloodWait
FAST_PULL_LIMIT = 10  # сколько сообщений брать за один iter_messages
FAST_PULL_BOOTSTRAP_DELAY = 130  # сек: даём warmup/keep_warm стартануть


def _ensure_pull_table():
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pull_offsets ("
            "channel_id INTEGER PRIMARY KEY, last_msg_id INTEGER NOT NULL, ts INTEGER NOT NULL)"
        )
        conn.commit()


def _pull_offset_get(channel_id: int):
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            r = conn.execute(
                "SELECT last_msg_id FROM pull_offsets WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            return r[0] if r else None
    except Exception:
        return None


def _pull_offset_set(channel_id: int, last_msg_id: int):
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO pull_offsets(channel_id, last_msg_id, ts) VALUES (?, ?, strftime('%s','now'))",
                (channel_id, last_msg_id),
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"[PULL] offset_set err: {e}")


def _entity_channel_id(ent):
    cid = getattr(ent, "channel_id", None) or getattr(ent, "id", None)
    return cid


async def _pull_one(client, ent, sem, stats):
    cid = _entity_channel_id(ent)
    if not cid:
        return
    async with sem:
        last_id = _pull_offset_get(cid)
        try:
            if last_id is None:
                # bootstrap: запоминаем top msg id, не форвардим историю
                top = await client.get_messages(ent, limit=1)
                if top:
                    top_id = top[0].id if isinstance(top, list) else top.id
                    _pull_offset_set(cid, top_id)
                    stats["bootstrap"] += 1
                return
            # обычный pull: новые сообщения с id > last_id
            msgs = await client.get_messages(ent, limit=FAST_PULL_LIMIT, min_id=last_id)
        except FloodWaitError as e:
            stats["flood"] += 1
            wait = int(e.seconds) + 3
            logger.warning(f"[PULL] FloodWait {e.seconds}s ch={cid}")
            await asyncio.sleep(wait)
            return
        except Exception as e:
            stats["err"] += 1
            logger.debug(f"[PULL] req err ch={cid}: {e.__class__.__name__}: {e}")
            return

        if not msgs:
            return
        if not isinstance(msgs, list):
            msgs = [msgs]
        # iter_messages/get_messages отдаёт новые → старые; обрабатываем от старого к новому
        msgs_sorted = sorted(
            [m for m in msgs if isinstance(m, types.Message)], key=lambda m: m.id
        )
        new_max_id = last_id
        for msg in msgs_sorted:
            if msg.id <= last_id:
                continue
            try:
                await handle_single(client, msg, "PULL")
                stats["new"] += 1
            except Exception as e:
                logger.warning(
                    f"[PULL] handle_single err ch={cid} msg={msg.id}: {e.__class__.__name__}: {e}"
                )
            if msg.id > new_max_id:
                new_max_id = msg.id
        if new_max_id > last_id:
            _pull_offset_set(cid, new_max_id)


async def fast_pull_loop(client: TelegramClient, resolved_chats):
    """Активно опрашивает каждый канал через get_messages(min_id=last_seen) с
    интервалом ~FAST_PULL_INTERVAL сек. Прямой запрос истории — гарантированно
    актуальное состояние, не зависит от телеграмовского cooldown'а на холодные каналы."""
    await asyncio.sleep(FAST_PULL_BOOTSTRAP_DELAY)
    _ensure_pull_table()
    sem = asyncio.Semaphore(FAST_PULL_PARALLELISM)
    logger.info(
        f"[PULL] start: channels={len(resolved_chats)} parallelism={FAST_PULL_PARALLELISM} interval={FAST_PULL_INTERVAL}s"
    )
    while True:
        stats = {"new": 0, "flood": 0, "err": 0, "too_long": 0, "bootstrap": 0}
        ts0 = _time.monotonic()
        tasks = [_pull_one(client, ch, sem, stats) for ch in resolved_chats]
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = _time.monotonic() - ts0
        if stats["new"] or stats["flood"] or stats["too_long"] or stats["bootstrap"]:
            logger.info(
                f"[PULL] tick {elapsed:.1f}s new={stats['new']} flood={stats['flood']} err={stats['err']} too_long={stats['too_long']} bootstrap={stats['bootstrap']}"
            )
        else:
            logger.debug(f"[PULL] tick {elapsed:.1f}s err={stats['err']}")
        if elapsed < FAST_PULL_INTERVAL:
            await asyncio.sleep(FAST_PULL_INTERVAL - elapsed)


def register_handlers(client, resolved_chats):
    @client.on(events.NewMessage(chats=resolved_chats))
    async def _on_new(e):
        await handle_single(client, e.message, "NEW", e)

    @client.on(events.MessageEdited(chats=resolved_chats))
    async def _on_edit(e):
        await handle_single(client, e.message, "EDIT", e)

    @client.on(events.Album(chats=resolved_chats))
    async def _on_album(e):
        await handle_album(client, e)

    @client.on(events.MessageDeleted(chats=resolved_chats))
    async def _on_del(e):
        await handle_deleted(client, e)


async def run():
    single_instance()
    Path(SQLITE_PATH).parent.mkdir(parents=True, exist_ok=True)

    # Start /metrics HTTP server (scraped by VictoriaMetrics on sus).
    start_http_server(METRICS_PORT)
    logger.info(f"metrics server on :{METRICS_PORT}")

    client = await build_client()
    try:
        await client.start()
    except AuthKeyUnregisteredError:
        logger.error(
            "Сессия недействительна. Удалите session.session и войдите заново обычным скриптом."
        )
        return

    # Start the Bot API client for enriched posts (inline buttons require a bot).
    if ENRICHED_ENABLED and BOT_TOKEN:
        global bot_client
        try:
            bot_client = TelegramClient("bot_session", API_ID, API_HASH)
            await bot_client.start(bot_token=BOT_TOKEN)
            me_bot = await bot_client.get_me()
            logger.info(
                f"[ENRICHED] bot started: @{me_bot.username} id={me_bot.id} → target={ENRICHED_TARGET_CHANNEL}"
            )
        except Exception as e:
            logger.error(f"[ENRICHED] bot start failed: {e.__class__.__name__}: {e}")
            bot_client = None

    raw_channels = read_channels(CHANNELS_FILE)
    if not raw_channels:
        logger.warning("Список каналов пуст. Заполните channels.txt")
    else:
        logger.info(f"Сырый список ({len(raw_channels)}): {raw_channels}")

    await safe_log(client, f"Login ок. Прогрев {WARMUP_SECONDS}s перед стартом.")
    await asyncio.sleep(WARMUP_SECONDS)

    resolved = await resolve_channels(client, raw_channels)
    if not resolved:
        await safe_log(
            client, "Не осталось валидных каналов после фильтрации.", level="error"
        )
        return

    register_handlers(client, resolved)

    # Прогрев PTS-стейта каналов: без него Telethon не получает UpdateNewChannelMessage
    # от "тихих" каналов (даже если юзербот в них состоит). Запускается фоном чтобы
    # не блокировать обработку событий.
    asyncio.create_task(warmup_channels(client, resolved))
    # asyncio.create_task(periodic_catchup(client))  # 2026-06-06: A/B test — выключен, проверяем влияние на FloodWait
    asyncio.create_task(keep_warm_loop(client, resolved))
    asyncio.create_task(fast_pull_loop(client, resolved))

    await safe_log(
        client,
        f"Forwarder запущен (каналов: {len(resolved)}; анти-гонка NEW/EDIT включена; блок контрактов включён).",
    )

    try:
        await client.run_until_disconnected()
    except AuthKeyUnregisteredError:
        logger.error("Сессия сброшена сервером во время работы.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
