"""chart-proxy — OG preview server for TonCalls enriched posts.

Endpoints:
  GET /chart      — HTML page with OG tags + optional redirect to the DTrade deep-link.
                    Telegram parses og:image and renders the rich preview.
  GET /img        — byte proxy to a public DTrade chart image API (with memory + disk cache).
  GET /composite  — Pillow-rendered PNG (author avatar + chart + quote + stats).
  GET /health     — liveness probe.

Env:
  CHART_PROXY_DOMAIN         — public https domain of THIS service, used in redirects
                               and links (e.g. "preview.example.com"). No trailing slash.
  DTRADE_DEEPLINK_PREFIX     — your DTrade referral prefix (e.g. "myref_"). Empty = no
                               ref parameter is appended.
  BRAND_MARK                 — small text drawn in the corner of the composite
                               (e.g. "by @your_channel"). Empty = no brand mark.
"""
import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

log = logging.getLogger("chart-proxy")

CHART_PROXY_DOMAIN = os.environ.get("CHART_PROXY_DOMAIN", "").strip().rstrip("/")
DTRADE_DEEPLINK_PREFIX = os.environ.get("DTRADE_DEEPLINK_PREFIX", "")
BRAND_MARK = os.environ.get("BRAND_MARK", "")

# Public DTrade chart image endpoint. Change if the DTrade API moves.
DTRADE_IMG_API = "https://image-api.xdtrade.com/api/v1/chart"

# Слой 1: in-memory hot cache. Возвращаем без сети.
HOT_TTL = 60 * 5

# Слой 2: disk-cache. Свежесть HOT_TTL — отдаём сразу. Старше — пробуем upstream;
# если upstream лежит, всё равно отдаём stale (preview не пропадёт).
DISK_DIR = Path("/var/cache/chart-proxy")
DISK_DIR.mkdir(parents=True, exist_ok=True)

# in-memory cache: key=(contract, platform) → (ts, bytes, content_type)
_img_cache: dict[tuple[str, str], tuple[float, bytes, str]] = {}


def _disk_path(contract: str, platform: str) -> Path:
    safe = contract.replace("/", "_")
    return DISK_DIR / f"{safe}_{platform}.bin"


def _disk_read(contract: str, platform: str):
    p = _disk_path(contract, platform)
    meta = p.with_suffix(".json")
    if not (p.exists() and meta.exists()):
        return None
    try:
        m = json.loads(meta.read_text())
        return p.stat().st_mtime, p.read_bytes(), m.get("ct", "image/png")
    except Exception:
        return None


def _disk_write(contract: str, platform: str, body: bytes, ct: str):
    p = _disk_path(contract, platform)
    meta = p.with_suffix(".json")
    try:
        p.write_bytes(body)
        meta.write_text(json.dumps({"ct": ct}))
    except OSError as e:
        log.debug("disk cache write failed: %s", e)


async def health(_request):
    return web.Response(text="ok")


def _validate_contract(c: str) -> bool:
    if not c or len(c) < 40 or len(c) > 80:
        return False
    if not c.startswith("EQ") and not c.startswith("UQ"):
        return False
    return all(ch.isalnum() or ch in "_-" for ch in c)


async def handle_chart(request: web.Request):
    """Отдаём HTML с OG-тегами. Telegram дёрнет, увидит og:image, покажет preview."""
    q = request.query
    contract = q.get("c", "")
    symbol = q.get("s", "Token")
    platform = q.get("p", "stonfi")
    mcap = q.get("m", "")
    liq = q.get("l", "")
    ts = q.get("t", str(int(time.time())))

    if not _validate_contract(contract):
        return web.Response(status=400, text="bad contract")
    if platform not in ("stonfi", "dedust"):
        platform = "stonfi"

    # URL картинки — композит. Прокидываем ВСЕ параметры из /chart в /composite,
    # чтобы рендер использовал реальные hearts/clowns/quote/etc.
    username = (q.get("u") or "").lstrip("@").lower()
    cid = q.get("cid", "")
    mid = q.get("mid", "")
    img_params = {"c": contract, "p": platform, "m": mcap, "l": liq,
                  "s": symbol, "t": ts}
    # Прокидываем все опциональные параметры
    for k in ("u", "cid", "mid", "h", "cl", "n", "bay", "ch", "q", "v"):
        v = q.get(k)
        if v:
            img_params[k] = v
    base = f"https://{CHART_PROXY_DOMAIN}" if CHART_PROXY_DOMAIN else ""
    if username:
        img_url = f"{base}/composite?{urlencode(img_params)}"
    else:
        img_url = (
            f"{base}/img?"
            + urlencode({k: img_params[k] for k in ("c", "p", "m", "l", "t")})
        )

    # Where to send the human on click (DTrade deep-link).
    redirect_url = f"https://t.me/dtrade?start={DTRADE_DEEPLINK_PREFIX}{contract}"

    title = f"${symbol}"
    description = f"by @{username}" if username else (BRAND_MARK or "preview")

    # Безопасный escape для HTML-атрибутов
    def esc(s: str) -> str:
        return (
            s.replace("&", "&amp;")
            .replace('"', "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    # Telegram preview-бот следует за <meta refresh> и берёт preview конечного URL.
    # → для него отдаём страницу БЕЗ refresh, только OG-теги.
    # Для обычных браузеров — JS-redirect (preview-бот JS не исполняет).
    ua = (request.headers.get("User-Agent") or "").lower()
    is_tg_bot = "telegrambot" in ua or "tdesktop" in ua

    redirect_block = "" if is_tg_bot else (
        f'<script>location.replace("{esc(redirect_url)}")</script>'
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{esc(title)}</title>
<meta property="og:type" content="website">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(description)}">
<meta property="og:image" content="{esc(img_url)}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:image" content="{esc(img_url)}">
{redirect_block}
</head>
<body>
<a href="{esc(redirect_url)}">by Sus</a>
</body>
</html>
"""
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def _fetch_dtrade(session, contract, platform, mcap, liq):
    params = ["theme=dark", f"base={contract}", "quote=USD", f"platform={platform}"]
    if mcap and mcap != "0":
        params.append(f"fdv={mcap}")
    if liq and liq != "0":
        params.append(f"liquidity={liq}")
    url = DTRADE_IMG_API + "?" + "&".join(params)
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200:
            return None
        ct = r.headers.get("content-type", "")
        if not ct.startswith("image/"):
            return None
        return await r.read(), ct


async def _fetch_dexscreener_header(session, contract):
    """DEXscreener token header (отдаёт PNG-чарт-snapshot)."""
    url = f"https://dd.dexscreener.com/ds-data/tokens/ton/{contract}/header.png"
    async with session.get(url, allow_redirects=True) as r:
        if r.status != 200:
            return None
        ct = r.headers.get("content-type", "image/png")
        if not ct.startswith("image/"):
            return None
        return await r.read(), ct


async def handle_img(request: web.Request):
    """Прокси к chart-картинке.
    Пробуем: dtrade(указанный platform) → dtrade(другой) → DEXscreener header.
    Кэшируем результат на CACHE_TTL."""
    q = request.query
    contract = q.get("c", "")
    platform = q.get("p", "stonfi")
    mcap = q.get("m", "")
    liq = q.get("l", "")

    if not _validate_contract(contract):
        return web.Response(status=400, text="bad contract")
    if platform not in ("stonfi", "dedust"):
        platform = "stonfi"

    key = (contract, platform)
    now = time.time()

    # Слой 1: hot in-memory
    hit = _img_cache.get(key)
    if hit and (now - hit[0] < HOT_TTL):
        _, body, ct = hit
        return web.Response(body=body, content_type=ct, headers={"Cache-Control": "public, max-age=300"})

    # Слой 2: disk-cache. Если свежий — отдаём без сети.
    disk = _disk_read(contract, platform)
    if disk and (now - disk[0] < HOT_TTL):
        _, body, ct = disk
        _img_cache[key] = (now, body, ct)
        return web.Response(body=body, content_type=ct, headers={"Cache-Control": "public, max-age=300"})

    # Сеть: пробуем dtrade(оба platform) → dexscreener.
    other = "dedust" if platform == "stonfi" else "stonfi"
    result = None
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "chart-proxy/1.0"},
        ) as s:
            for plat in (platform, other):
                try:
                    result = await _fetch_dtrade(s, contract, plat, mcap, liq)
                    if result:
                        break
                except Exception:
                    continue
            if not result:
                try:
                    result = await _fetch_dexscreener_header(s, contract)
                except aiohttp.ClientError as e:
                    log.debug("dexscreener header fetch failed: %s", e)
    except aiohttp.ClientError as e:
        log.debug("upstream session failed: %s", e)
        result = None

    if result:
        body, ct = result
        _img_cache[key] = (now, body, ct)
        _disk_write(contract, platform, body, ct)
        if len(_img_cache) > 500:
            cutoff = now - HOT_TTL
            for k in [k for k, v in _img_cache.items() if v[0] < cutoff]:
                _img_cache.pop(k, None)
        return web.Response(body=body, content_type=ct, headers={"Cache-Control": "public, max-age=300"})

    # Сеть упала — отдаём stale из диска (даже старый — лучше чем пусто).
    if disk:
        _, body, ct = disk
        return web.Response(
            body=body, content_type=ct,
            headers={"Cache-Control": "public, max-age=60", "X-Stale": "1"},
        )

    return web.Response(status=502, text="all upstreams failed, no cache")


# ============= /composite =============
# Композит-картинка: график dtrade слева + аватар инфла + ник + (опц.) фото поста
import io
from pathlib import Path as _Path

try:
    from PIL import Image, ImageDraw, ImageFilter, ImageFont
    _PIL_OK = True
except Exception:
    _PIL_OK = False

INFLOW_DIR = _Path("/var/cache/chart-proxy/inflow")
AVATARS_DIR = INFLOW_DIR / "avatars"
MEDIA_DIR = INFLOW_DIR / "media"

# Шрифты (Geist / Inter / GeistMono — установлены в /usr/local/share/fonts/custom)
FONT_BASE = "/usr/local/share/fonts/custom"
FONTS = {
    "geist_black":    f"{FONT_BASE}/Geist-Black.ttf",
    "geist_bold":     f"{FONT_BASE}/Geist-Bold.ttf",
    "geist_semi":     f"{FONT_BASE}/Geist-SemiBold.ttf",
    "geist_reg":      f"{FONT_BASE}/Geist-Regular.ttf",
    "geist_mono_bold":f"{FONT_BASE}/GeistMono-Bold.ttf",
    "geist_mono":     f"{FONT_BASE}/GeistMono-Regular.ttf",
    "inter_bold":     f"{FONT_BASE}/Inter-Bold.ttf",
    "inter_semi":     f"{FONT_BASE}/Inter-SemiBold.ttf",
    "inter":          f"{FONT_BASE}/Inter-Regular.ttf",
}
# fallback
FONT_FALLBACK = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


def _load_font(key: str, size: int):
    try:
        return ImageFont.truetype(FONTS.get(key, FONT_FALLBACK), size)
    except Exception:
        try:
            return ImageFont.truetype(FONT_FALLBACK, size)
        except Exception:
            return ImageFont.load_default()


# Color palette из tailwind конфига Stitch
PAL = {
    "bg":            (13, 14, 16),       # surface-container-lowest #0d0e10
    "surface":       (18, 19, 21),       # surface #121315
    "surf_low":      (27, 28, 30),       # surface-container-low
    "surf_high":     (41, 42, 44),       # surface-container-high
    "surf_var":      (52, 53, 55),       # surface-variant
    "primary":       (150, 204, 255),    # primary #96ccff
    "primary_d":     (0, 152, 234),      # #0098EA
    "secondary":     (0, 226, 144),      # #00e290
    "secondary2":    (82, 255, 172),     # #52ffac
    "tertiary":      (255, 178, 187),    # #ffb2bb
    "tertiary2":     (255, 81, 117),     # #ff5175
    "on_surface":    (227, 226, 229),    # #e3e2e5
    "on_var":        (190, 199, 211),    # #bec7d3
    "outline":       (137, 146, 157),    # #89929d
    "outline_v":     (63, 72, 81),       # #3f4851
}


def _open_img_safe(path: _Path):
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        im = Image.open(path).convert("RGB")
        return im
    except Exception:
        return None


def _looks_like_dtrade_screenshot(path: _Path) -> bool:
    """Эвристика: пост-медиа — это скриншот dtrade-чарта (а не мем/стикер).
    Признаки:
      - landscape aspect (1.3 - 2.0)
      - тёмный фон (mean brightness < 40 на 255)
    Если оба — это, скорее всего, dtrade-скрин → не показываем как hero.
    """
    try:
        im = _open_img_safe(path)
        if not im:
            return False
        w, h = im.size
        if h == 0:
            return False
        aspect = w / h
        if not (1.25 < aspect < 2.2):
            return False
        # быстрый sample mean
        thumb = im.resize((48, 32), Image.LANCZOS)
        px = list(thumb.getdata())
        mean = sum(sum(p) // 3 for p in px) / len(px)
        return mean < 45  # тёмный фон
    except Exception:
        return False


def _circle_crop(im: Image.Image, size: int) -> Image.Image:
    im = im.copy()
    im.thumbnail((size * 2, size * 2))
    w, h = im.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    im = im.crop((left, top, left + side, top + side)).resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    d = ImageDraw.Draw(mask)
    d.ellipse((0, 0, size, size), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(im, (0, 0))
    out.putalpha(mask)
    return out


# ============== Helpers для эффектов ==============

def _text_w(draw, text, font):
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0]
    except Exception:
        return len(text) * (font.size // 2)


def _rounded(draw, xy, radius, fill=None, outline=None, width=1):
    try:
        draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)
    except AttributeError:
        x0, y0, x1, y1 = xy
        draw.rectangle(xy, fill=fill, outline=outline, width=width)


def _glass_card(canvas, xy, radius=16, fill_alpha=10, border=(150, 204, 255, 38)):
    """Стеклянная карточка с rounded + полупрозрачным fill + неоновой рамкой."""
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    _rounded(d, xy, radius, fill=(255, 255, 255, fill_alpha))
    _rounded(d, xy, radius, outline=border, width=1)
    canvas.alpha_composite(layer)


def _grid_bg(W, H, dot=(150, 204, 255, 14), step=24):
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for x in range(0, W, step):
        for y in range(0, H, step):
            d.ellipse((x, y, x + 2, y + 2), fill=dot)
    return layer


def _vignette(W, H, strength=180):
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    # Радиальное затемнение по краям — рисуем серию эллипсов от центра
    max_r = int((W ** 2 + H ** 2) ** 0.5 / 2)
    cx, cy = W // 2, H // 2
    for i in range(0, 40):
        r = max_r - i * (max_r // 60)
        alpha = int(strength * (i / 40))
        d.ellipse((cx - r, cy - r, cx + r, cy + r), outline=(0, 0, 0, alpha))
    return layer.filter(ImageFilter.GaussianBlur(40))


def _scanlines(W, H, alpha=8):
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for y in range(0, H, 2):
        d.line((0, y, W, y), fill=(150, 204, 255, alpha), width=1)
    return layer


def _neon_glow(W, H, xy, color=(150, 204, 255), strength=80, blur=30):
    """Создаёт мягкое свечение вокруг прямоугольника."""
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    _rounded(d, xy, 16, fill=(*color, strength))
    return layer.filter(ImageFilter.GaussianBlur(blur))


def _avatar_square(path, size, ring_color, ring_width=4, radius=20):
    """Квадрат с rounded corners + цветной рамкой (как в Stitch-референсе)."""
    out_size = size + ring_width * 2
    out = Image.new("RGBA", (out_size, out_size), (0, 0, 0, 0))
    d = ImageDraw.Draw(out)
    # Ring (rounded square)
    _rounded(d, (0, 0, out_size, out_size), radius + ring_width,
             fill=(*ring_color, 255))

    im = _open_img_safe(path) if path else None
    if im:
        im.thumbnail((size * 2, size * 2))
        w, h = im.size
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        im = im.crop((left, top, left + side, top + side)).resize((size, size), Image.LANCZOS)
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).rounded_rectangle((0, 0, size, size), radius, fill=255)
        out.paste(im, (ring_width, ring_width), mask)
    else:
        # placeholder
        d2 = ImageDraw.Draw(out)
        _rounded(d2, (ring_width, ring_width, ring_width + size, ring_width + size),
                 radius, fill=(60, 65, 80, 255))
    return out


# ============== Главный render ==============

def _render_composite(chart_bytes: bytes, username: str, avatar_path: _Path,
                       media_path: _Path, *, symbol: str = "", mcap=None, liq=None,
                       change_6h=None, hearts: int = 0, clowns: int = 0,
                       n_posts: int = 0, bayan: int = 0, quote: str = "") -> bytes:
    """Composite 1200×630.
    Layout:
      TOP (440):
        - Left 460×460 — post media (если есть). Если нет — аватар инфла крупно.
        - Right остаток — chart-card.
      BOTTOM (180):
        - Footer-bar: маленький аватар + @username + цитата + микро-статы.
    """
    W, H = 1200, 630
    has_media = bool(media_path and media_path.exists() and media_path.stat().st_size > 0)
    # Если post-media — это скриншот dtrade (часто инфлы прикрепляют),
    # не показываем как hero — слева ставим большой аватар инфла.
    if has_media and _looks_like_dtrade_screenshot(media_path):
        has_media = False
    bg = Image.new("RGBA", (W, H), (*PAL["bg"], 255))

    # Grid background (точки)
    bg.alpha_composite(_grid_bg(W, H))

    # ===== LEFT (full-height): post-media (или аватар как fallback) =====
    LEFT_W = 460
    HERO_PAD = 18
    hero_x = HERO_PAD
    hero_y = HERO_PAD
    hero_w = LEFT_W - HERO_PAD * 2
    hero_h = H - HERO_PAD * 2     # на ВСЮ высоту

    if has_media:
        media_im = _open_img_safe(media_path)
        if media_im:
            ow, oh = media_im.size
            # CONTAIN: вписываем картинку в hero без обрезки.
            # Картинка всегда видна целиком. По сторонам / сверху-снизу будут поля.
            scale = min(hero_w / ow, hero_h / oh)
            nw, nh = max(1, int(ow * scale)), max(1, int(oh * scale))
            media_im = media_im.resize((nw, nh), Image.LANCZOS)
            # Подложка для всего hero-блока с тонкой рамкой (rounded)
            hero_layer = Image.new("RGBA", (hero_w, hero_h), (*PAL["surf_low"], 255))
            # Центрируем картинку внутри
            cx_off = (hero_w - nw) // 2
            cy_off = (hero_h - nh) // 2
            # Маска для самой картинки — со скруглением
            img_radius = 18
            img_mask = Image.new("L", (nw, nh), 0)
            ImageDraw.Draw(img_mask).rounded_rectangle((0, 0, nw, nh), img_radius, fill=255)
            hero_layer.paste(media_im.convert("RGBA"), (cx_off, cy_off), img_mask)
            # Маска для всего hero (скругление углов блока)
            hero_mask = Image.new("L", (hero_w, hero_h), 0)
            ImageDraw.Draw(hero_mask).rounded_rectangle((0, 0, hero_w, hero_h), 24, fill=255)
            bg.paste(hero_layer, (hero_x, hero_y), hero_mask)
            d = ImageDraw.Draw(bg)
            _rounded(d, (hero_x, hero_y, hero_x + hero_w, hero_y + hero_h), 24,
                     outline=(*PAL["primary"], 70), width=1)
    else:
        # Нет фото из поста — крупная аватарка инфла по центру всей высоты
        av_size = 300
        av_cx = LEFT_W // 2
        av_cy = H // 2
        glow_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        gd = ImageDraw.Draw(glow_layer)
        gd.ellipse((av_cx - 170, av_cy - 170, av_cx + 170, av_cy + 170),
                   fill=(150, 204, 255, 50))
        glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(50))
        bg.alpha_composite(glow_layer)
        av_im = _avatar_square(avatar_path, av_size, PAL["primary"], ring_width=4, radius=32)
        bg.alpha_composite(av_im, (av_cx - (av_size + 8) // 2, av_cy - (av_size + 8) // 2))

    # ===== RIGHT: chart (верхняя часть) + footer (нижняя) =====
    RIGHT_X = LEFT_W + 14
    RIGHT_W = W - RIGHT_X - HERO_PAD

    chart_card_y = hero_y
    chart_card_h = 440   # верхняя часть правой колонки
    chart_card_x1 = RIGHT_X + RIGHT_W

    # Картинка графика — COVER (растягиваем + crop) чтобы заполнить блок без полей
    chart_pad = 6
    target_w = chart_card_x1 - RIGHT_X - chart_pad * 2
    target_h = chart_card_h - chart_pad * 2
    cx = RIGHT_X + chart_pad
    cy = chart_card_y + chart_pad

    chart_painted = False
    if chart_bytes:
        try:
            chart_im = Image.open(io.BytesIO(chart_bytes)).convert("RGBA")
            ow, oh = chart_im.size
            scale = max(target_w / ow, target_h / oh)
            new_w = int(ow * scale)
            new_h = int(oh * scale)
            chart_im = chart_im.resize((new_w, new_h), Image.LANCZOS)
            left = max(0, (new_w - target_w) // 2)
            top  = max(0, (new_h - target_h) // 2)
            chart_im = chart_im.crop((left, top, left + target_w, top + target_h))
            mask = Image.new("L", (target_w, target_h), 0)
            ImageDraw.Draw(mask).rounded_rectangle((0, 0, target_w, target_h), 22, fill=255)
            bg.paste(chart_im, (cx, cy), mask)
            chart_painted = True
        except (OSError, ValueError) as e:
            log.debug("chart paint failed: %s", e)

    if not chart_painted:
        # Placeholder: тёмный rounded блок с подписью "chart unavailable"
        d_ph = ImageDraw.Draw(bg)
        _rounded(d_ph, (cx, cy, cx + target_w, cy + target_h), 22,
                 fill=(*PAL["surf_low"], 255))
        ph_font = _load_font("geist_mono_bold", 14)
        ph_txt = "chart unavailable"
        ph_w = _text_w(d_ph, ph_txt, ph_font)
        d_ph.text((cx + (target_w - ph_w) // 2, cy + target_h // 2 - 10),
                  ph_txt, fill=(*PAL["outline"], 200), font=ph_font)

    # Светящаяся тонкая рамка вокруг chart-блока
    d = ImageDraw.Draw(bg)
    _rounded(d, (cx, cy, cx + target_w, cy + target_h), 22,
             outline=(*PAL["primary"], 60), width=1)

    # ===== RIGHT-BOTTOM FOOTER: аватар канала + @username + цитата + микро-статы =====
    # Footer ограничен правой колонкой (LEFT-секция занята post-media на всю высоту)
    foot_y = chart_card_y + chart_card_h + 14
    foot_h = H - foot_y - HERO_PAD
    foot_x = RIGHT_X
    foot_x1 = W - HERO_PAD
    _glass_card(bg, (foot_x, foot_y, foot_x1, foot_y + foot_h), radius=20)
    d = ImageDraw.Draw(bg)

    # Аватар в footer показываем только если в hero — post-media (мем/стикер).
    # Если в hero уже большой аватар инфла (нет media или dtrade-скрин) — не дублируем.
    show_avatar_in_footer = has_media
    if show_avatar_in_footer:
        av_size = foot_h - 32
        av_x = foot_x + 16
        av_y = foot_y + 16
        av_im = _avatar_square(avatar_path, av_size, PAL["primary"], ring_width=3, radius=18)
        bg.alpha_composite(av_im, (av_x - 3, av_y - 3))
        text_x = av_x + av_size + 18
    else:
        av_x = foot_x
        av_y = foot_y + 16
        av_size = 0
        text_x = foot_x + 22

    nick = f"@{username}" if username else "@unknown"
    nick_font = _load_font("geist_bold", 26)
    d.text((text_x, av_y + 2), nick, fill=(*PAL["primary"], 255), font=nick_font)

    # Stats — компактно справа: hearts • clowns • posts • bayan
    stats_font = _load_font("geist_mono_bold", 18)
    stats_lbl = _load_font("geist_mono", 10)
    items = []
    items.append((str(hearts), "hearts", PAL["secondary"]))
    items.append((str(clowns), "clowns", PAL["tertiary2"]))
    if n_posts: items.append((str(n_posts), "calls", PAL["primary"]))
    if bayan:   items.append((str(bayan), "баян", PAL["outline"]))
    # рассчитываем ширину справа
    item_w = 70
    stats_total_w = item_w * len(items)
    stats_x0 = foot_x1 - stats_total_w - 16
    for i, (val, lbl, col) in enumerate(items):
        ix = stats_x0 + i * item_w
        tw = _text_w(d, val, stats_font)
        d.text((ix + (item_w - tw) // 2, av_y + 4), val,
               fill=(*col, 255), font=stats_font)
        tw2 = _text_w(d, lbl, stats_lbl)
        d.text((ix + (item_w - tw2) // 2, av_y + 30), lbl,
               fill=(*PAL["outline"], 255), font=stats_lbl)

    # Цитата под ником (если есть)
    q_font = _load_font("inter", 16)
    q_text = (quote or "").strip()
    quote_top = av_y + 50
    quote_right = stats_x0 - 18
    if q_text:
        max_w = quote_right - text_x
        words = q_text.split()
        lines, cur = [], ""
        for w in words:
            test = (cur + " " + w).strip()
            if _text_w(d, test, q_font) <= max_w:
                cur = test
            else:
                if cur: lines.append(cur)
                cur = w
        if cur: lines.append(cur)
        max_lines = max(1, (foot_y + foot_h - quote_top - 14) // 22)
        if len(lines) > max_lines:
            lines = lines[:max_lines]
            lines[-1] = lines[-1].rstrip() + "…"
        for i, line in enumerate(lines):
            d.text((text_x, quote_top + i * 22), line,
                   fill=(*PAL["on_surface"], 230), font=q_font)

    # ===== Эффекты сверху =====
    bg.alpha_composite(_scanlines(W, H, alpha=8))
    bg.alpha_composite(_vignette(W, H, strength=140))

    # Brand mark (BRAND_MARK env). Empty string = skip.
    if BRAND_MARK:
        d = ImageDraw.Draw(bg)
        brand_font = _load_font("geist_mono", 10)
        tw = _text_w(d, BRAND_MARK, brand_font)
        d.text((W - tw - 16, H - 22), BRAND_MARK,
               fill=(*PAL["outline"], 200), font=brand_font)

    out = io.BytesIO()
    bg.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()


def _human_short(n) -> str:
    try:
        n = float(n)
    except Exception:
        return "?"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


async def _fetch_chart_bytes(contract: str, platform: str, mcap, liq) -> bytes | None:
    """Получаем график тем же путём что и /img (через локальный handler)."""
    # Прямо переиспользуем тот же набор: dtrade(stonfi) → dtrade(dedust) → dexscreener
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15),
        headers={"User-Agent": "chart-proxy/1.0"},
    ) as s:
        other = "dedust" if platform == "stonfi" else "stonfi"
        for plat in (platform, other):
            try:
                r = await _fetch_dtrade(s, contract, plat, mcap, liq)
                if r:
                    return r[0]
            except Exception:
                continue
        try:
            r = await _fetch_dexscreener_header(s, contract)
            if r:
                return r[0]
        except aiohttp.ClientError as e:
            log.debug("dexscreener header fetch failed: %s", e)
    return None


async def handle_composite(request: web.Request):
    if not _PIL_OK:
        return web.Response(status=500, text="PIL not available")
    q = request.query
    contract = q.get("c", "")
    platform = q.get("p", "stonfi")
    username = (q.get("u") or "").lstrip("@").lower()
    symbol = q.get("s", "")
    mcap = q.get("m", "")
    liq = q.get("l", "")
    cid = q.get("cid", "")
    mid = q.get("mid", "")
    # Real stats (passed by main.py)
    try: hearts = int(q.get("h", "0") or 0)
    except ValueError: hearts = 0
    try: clowns = int(q.get("cl", "0") or 0)
    except ValueError: clowns = 0
    try: n_posts = int(q.get("n", "0") or 0)
    except ValueError: n_posts = 0
    try: bayan = int(q.get("bay", "0") or 0)
    except ValueError: bayan = 0
    try: change_6h = float(q.get("ch", "")) if q.get("ch") else None
    except ValueError: change_6h = None
    quote = q.get("q", "") or ""

    if not _validate_contract(contract):
        return web.Response(status=400, text="bad contract")
    if platform not in ("stonfi", "dedust"):
        platform = "stonfi"

    # disk-cache по композиту (по полному набору параметров)
    cache_key = f"v2_{contract}_{platform}_{username}_{cid}_{mid}_{hearts}_{clowns}_{n_posts}_{bayan}_{hash(quote) & 0xffffff}"
    safe = cache_key.replace("/", "_")
    cache_file = INFLOW_DIR / f"{safe}.png"
    now = time.time()
    if cache_file.exists() and (now - cache_file.stat().st_mtime < HOT_TTL):
        return web.Response(
            body=cache_file.read_bytes(),
            content_type="image/png",
            headers={"Cache-Control": "public, max-age=300"},
        )

    # 1) График — байты. Если upstream лежит → рендерим композит БЕЗ chart
    # (placeholder), preview всё равно покажется Telegram'ом.
    chart_bytes = await _fetch_chart_bytes(contract, platform, mcap, liq)
    if not chart_bytes:
        disk = _disk_read(contract, platform)
        if disk:
            chart_bytes = disk[1]
    # chart_bytes может остаться None — рендер должен это пережить

    # 2) Пути к локальным файлам
    avatar_path = AVATARS_DIR / f"{username}.jpg" if username else None
    media_path = None
    if cid and mid:
        try:
            media_path = MEDIA_DIR / f"{int(cid)}_{int(mid)}.jpg"
        except ValueError:
            media_path = None

    # 3) Рендер
    try:
        mc = float(mcap) if mcap else None
    except ValueError:
        mc = None
    try:
        lq = float(liq) if liq else None
    except ValueError:
        lq = None
    try:
        png = _render_composite(
            chart_bytes, username, avatar_path, media_path,
            symbol=symbol, mcap=mc, liq=lq, change_6h=change_6h,
            hearts=hearts, clowns=clowns, n_posts=n_posts, bayan=bayan,
            quote=quote,
        )
    except Exception as e:
        return web.Response(status=500, text=f"render err: {e.__class__.__name__}: {e}")

    try:
        cache_file.write_bytes(png)
    except OSError as e:
        log.debug("composite disk cache write failed: %s", e)

    return web.Response(body=png, content_type="image/png",
                        headers={"Cache-Control": "public, max-age=300"})


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/chart", handle_chart)
    # Уникальный path-сегмент — заставляет Telegram пересканировать preview
    # даже для одного и того же контракта. `nonce` игнорируется в логике.
    app.router.add_get("/chart/{nonce}", handle_chart)
    app.router.add_get("/img", handle_img)
    app.router.add_get("/composite", handle_composite)
    app.router.add_get("/health", health)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="127.0.0.1", port=8090)
