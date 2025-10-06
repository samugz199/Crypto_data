# ---------- imports ----------
import os
import re
import logging
from datetime import datetime
from threading import Thread
from typing import cast
from typing import Optional
import time
import aiosqlite

import pytz
from flask import Flask
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
from telegram import BotCommand


# ---------- feedback command ----------
async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = (
        "💡 *We truly value your opinion!*\n\n"
        "Your ideas and suggestions are an important part of making this community better every day. "
        "While messages sent here won’t be reviewed directly, our programming staff will be happy to hear from you.\n\n"
        "👉 To share your feedback, please write to us at:\n"
        "📩 support.cryptodatahub@gmail.com\n\n"
        "Thank you for helping us grow and improve. Together we are building a stronger and smarter community 🚀"
    )
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(message, parse_mode="Markdown")


# ---------- logging ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# --- helpers para enviar mensajes largos en trozos seguros (Markdown) ---
async def _chunk_iter(text: str, chunk_size: int = 3800):
    """Genera trozos <= chunk_size intentando cortar por líneas."""
    lines = text.splitlines(True)  # keepends=True
    buf = []
    length = 0
    for ln in lines:
        # si una línea sola es larguísima, trocéala "a lo bruto"
        while len(ln) > chunk_size:
            part, ln = ln[:chunk_size], ln[chunk_size:]
            yield part
        if length + len(ln) > chunk_size:
            yield "".join(buf)
            buf, length = [ln], len(ln)
        else:
            buf.append(ln)
            length += len(ln)
    if buf:
        yield "".join(buf)

async def send_markdown_chunks(chat, text: str, chunk_size: int = 3800):
    """Para usar con msg.chat: envía varios mensajes si hace falta."""
    async for part in _chunk_iter(text, chunk_size):
        await chat.send_message(part, parse_mode="Markdown")

async def send_markdown_chunks_bot(bot, chat_id: int | str, text: str, chunk_size: int = 3800):
    """Versión para usar con app.bot + chat_id (canal, jobs, etc.)."""
    async for part in _chunk_iter(text, chunk_size):
        await bot.send_message(chat_id=chat_id, text=part, parse_mode="Markdown")

# ---------- config ----------
TZ = pytz.timezone("Europe/Madrid")
TOKEN: str = cast(str, os.getenv("TELEGRAM_TOKEN") or "")
CHANNEL_ID: str = cast(str, os.getenv("CHANNEL_ID") or "")
FNG_URL = "https://api.alternative.me/fng/?limit=1"
FNG_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}

# --- alertas de caídas/recuperación ---
ALERT_RECOVERY = os.getenv("ALERT_RECOVERY",
                           "1") == "1"  # 1=activar avisos de recuperación
STRESS_MIN_UP = float(os.getenv(
    "STRESS_MIN_UP",
    "5.0"))  # Δmínimo para avisar si ya está en RED (subida del score)
STRESS_MIN_DOWN = float(
    os.getenv("STRESS_MIN_DOWN",
              "5.0"))  # Δmínimo para avisar recuperación (bajada del score)

# debug y quote (cámbialo por env si quieres)
DEBUG_ERRORS = os.getenv("DEBUG_ERRORS", "0") == "1"
LAST_WEB_PING = "nunca"
BINANCE_QUOTE = os.getenv("BINANCE_QUOTE", "USDC").upper()
BYBIT_QUOTE = os.getenv("BYBIT_QUOTE", "USDC").upper()
OKX_QUOTE = os.getenv("OKX_QUOTE", "USDC").upper()
KRAKEN_QUOTE = os.getenv("KRAKEN_QUOTE", "USDC").upper()

if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN no configurado en Secrets")
if not CHANNEL_ID:
    log.warning("CHANNEL_ID vacío: no se enviarán mensajes al canal")

# ---------- subscripciones (SQLite) ----------
DB_PATH = os.getenv("DB_PATH", "bot.db")


async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS subs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            kind TEXT NOT NULL,                 -- 'stress' | 'fng'
            rule TEXT NOT NULL,                 -- 'level>=ORANGE' | 'score>=70' | 'value<=20' ...
            cooldown_sec INTEGER NOT NULL,      -- anti-spam
            last_sent_ts INTEGER DEFAULT 0
        );
        """)
        await db.commit()


async def add_sub(chat_id: int,
                  kind: str,
                  rule: str,
                  cooldown_sec: int = 1800):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subs (chat_id, kind, rule, cooldown_sec) VALUES (?,?,?,?)",
            (chat_id, kind, rule, cooldown_sec))
        await db.commit()


async def del_sub(chat_id: int, kind: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if kind:
            await db.execute("DELETE FROM subs WHERE chat_id=? AND kind=?",
                             (chat_id, kind))
        else:
            await db.execute("DELETE FROM subs WHERE chat_id=?", (chat_id, ))
        await db.commit()


async def list_subs(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, kind, rule, cooldown_sec FROM subs WHERE chat_id=?",
            (chat_id, ))
        rows = await cur.fetchall()
        return rows


async def _should_notify(sub, now_ts: int) -> bool:
    last = int(sub["last_sent_ts"] or 0)
    return (now_ts - last) >= int(sub["cooldown_sec"] or 0)


async def _touch_notified(sub_id: int, now_ts: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subs SET last_sent_ts=? WHERE id=?",
                         (now_ts, sub_id))
        await db.commit()


# ---------- keep-alive web (Replit) ----------
app = Flask(__name__)


@app.get("/")
def health():
    global LAST_WEB_PING
    LAST_WEB_PING = datetime.now(TZ).strftime("%d/%m %H:%M:%S")
    return "OK", 200


def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))


# ---------- data sources: BINANCE ----------
# Usamos el dominio de datos para esquivar 451 y añadimos mirrors:
BINANCE_BASES = [
    "https://data-api.binance.vision",  # preferido (datos públicos)
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]
HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}


async def _binance_get(path: str, timeout_sec: float = 12.0):
    """
    GET con reintentos por varias bases (para esquivar 451/rate-limits).
    Devuelve json o levanta una excepción con el detalle de todos los intentos.
    """
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    errors = []
    async with httpx.AsyncClient(timeout=timeout,
                                 follow_redirects=True,
                                 headers=HEADERS) as client:
        for base in BINANCE_BASES:
            url = f"{base}{path}"
            try:
                r = await client.get(url)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases Binance fallaron: " +
                       " | ".join(f"{b}: {e}" for b, e in errors))


async def fetch_binance_klines(symbol: str,
                               interval: str = "1h",
                               limit: int = 120):
    path = f"/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    rows = await _binance_get(path)
    ohlcv = []
    for r in rows:
        ohlcv.append({
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        })
    return ohlcv


async def attach_indicators_binance(top20: list[dict],
                                    interval: str = "1h") -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. BTCUSDC
        try:
            ohlcv = await fetch_binance_klines(sym,
                                               interval=interval,
                                               limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"] = calc_rsi(closes, 14)
            d["ema20"] = ema(closes, 20)
            d["ema50"] = ema(closes, 50)
            d["atr_pct"] = calc_atr_pct(ohlcv, 14)

            # PRO
            macd_l, macd_s, macd_h = macd(closes, 12, 26, 9)
            d["macd"] = macd_l
            d["macd_sig"] = macd_s
            d["macd_hist"] = macd_h

            k, kd = stoch_kd(ohlcv, 14, 3)
            d["stoch_k"] = k
            d["stoch_d"] = kd

            mid, up, lo, pctb, bw = bollinger(closes, 20, 2.0)
            d["bb_pctb"] = pctb
            d["bb_bw"] = bw

            d["mfi14"] = mfi(ohlcv, 14)
            d["obv"] = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores Binance fallaron para {sym}: {e}")
            for key in ("rsi14", "ema20", "ema50", "atr_pct", "macd",
                        "macd_sig", "macd_hist", "stoch_k", "stoch_d",
                        "bb_pctb", "bb_bw", "mfi14", "obv", "vwap_dev_pct",
                        "adx14", "+di", "-di"):
                d[key] = None
        out.append(d)
    return out


async def fetch_binance_top20_quote():
    """
    Top 20 por quoteVolume con quote = BINANCE_QUOTE (USDC por defecto).
    Devuelve (top20:list[dict], total_quote_vol:float)
    """
    exi_json = await _binance_get("/api/v3/exchangeInfo")
    symbols_info = exi_json.get("symbols", [])
    quote = BINANCE_QUOTE
    valid_symbols = {
        s["symbol"]
        for s in symbols_info
        if s.get("status") == "TRADING" and s.get("quoteAsset") == quote
    }

    rows = await _binance_get("/api/v3/ticker/24hr")

    data = []
    total_quote_vol = 0.0
    for r in rows:
        sym = r.get("symbol")
        if sym not in valid_symbols:
            continue
        try:
            qv = float(r.get("quoteVolume", "0"))
            lp = float(r.get("lastPrice", "0"))
            pcp = float(r.get("priceChangePercent", "0"))
        except (TypeError, ValueError):
            continue
        total_quote_vol += qv
        data.append({
            "symbol": sym,
            "lastPrice": lp,
            "priceChangePercent": pcp,
            "quoteVolume": qv,
        })

    if total_quote_vol <= 0:
        return [], 0.0

    data.sort(key=lambda x: x["quoteVolume"], reverse=True)
    top20 = data[:20]
    for d in top20:
        d["dominance"] = (d["quoteVolume"] / total_quote_vol) * 100.0
    return top20, total_quote_vol

def fmt_num(n, decimals=2):
    try:
        return f"{n:,.{decimals}f}".replace(",", "X").replace(".",
                                                              ",").replace(
                                                                  "X", ".")
    except Exception:
        return str(n)


# ================== ENGLISH TABLE HELPERS (insert right after fmt_num) ==================


def _human_int_en(n: float) -> str:
    # 1,234,567
    try:
        return f"{int(round(n)):,.0f}"
    except Exception:
        return "0"


def _human_price_en(x: float) -> str:
    # Flexible decimals: more for small prices
    if not isinstance(x, (int, float)):
        return "-"
    if x == 0:
        return "0"
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.2f}"
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _arrow_en(pct: float) -> str:
    if pct > 0.1:  # > +0.10%
        return "⬆️"
    if pct < -0.1:  # < -0.10%
        return "⬇️"
    return "↔︎"


def _pct_str_en(pct: float) -> str:
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


# --- NUEVO: abreviador 1.2K / 854M / 3.6B
def _human_int_short_en(n: float) -> str:
    try:
        n = float(n)
    except Exception:
        return "0"
    absn = abs(n)
    if absn >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B".rstrip("0").rstrip(".")
    if absn >= 1_000_000:
        return f"{n/1_000_000:.1f}M".rstrip("0").rstrip(".")
    if absn >= 1_000:
        return f"{n/1_000:.1f}K".rstrip("0").rstrip(".")
    return f"{int(round(n)):,}"

def _podium(i: int) -> str:
    return "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i:>2}"))

def _build_top_msg_en(exchange: str, quote: str, total_vol_usdc: float,
                      rows: list[dict]) -> str:
    up = sum(1 for r in rows if r["pct_24h"] > 0.1)
    down = sum(1 for r in rows if r["pct_24h"] < -0.1)
    flat = len(rows) - up - down

    header = (
        f"📊 *Prices & Volume* — {exchange} · Quote: {quote}\n"
        f"💵 *Total 24h Volume:* {_human_int_short_en(total_vol_usdc)} {quote}\n"
        f"📈 Market 24h: {up} ⬆️ · {down} ⬇️ · {flat} ↔︎\n\n"
        f"🏆 *Top {len(rows)} by Volume (24h)*\n"
    )

    lines = []
    for i, r in enumerate(rows, 1):
        lines.append(
            f"{_podium(i):>2}  "
            f"{(r['ticker'] or ''):<10} "
            f"{_human_price_en(r['price']):>12}  "
            f"{_human_int_short_en(r['vol_usdc']):>8}  "
            f"{_pct_str_en(r['pct_24h']):>7} {_arrow_en(r['pct_24h'])}  "
            f"{r['share_pct']:.2f}%"
        )

    table = (
        "```\n"
        "#  Pair        💵Price        📦Vol     %24h   Share\n" +
        "\n".join(lines) + "\n```"
    )
    legend = (
        "\n🧭 *Quick Legend*\n"
        "Price · 24h Volume (quote) · %24h · Share (% of total 24h volume)\n\n"
        "NFA | DYOR"
    )
    return header + table + legend

# ---------- indicadores (básicos + PRO) ----------
def ema(prices: list[float], period: int) -> float | None:
    if not prices or len(prices) < period:
        return None
    k = 2 / (period + 1)
    v = prices[0]
    for p in prices[1:]:
        v = p * k + v * (1 - k)
    return v


def calc_rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i - 1]
        gains.append(max(0.0, d))
        losses.append(abs(min(0.0, d)))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi = None
    for i in range(period, len(prices)):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
        rsi = 100 - (100 / (1 + rs))
    return rsi


def calc_atr_pct(ohlcv: list[dict], period: int = 14) -> float | None:
    if len(ohlcv) <= period:
        return None
    trs = []
    for i in range(1, len(ohlcv)):
        high_val = ohlcv[i]["high"]
        low_val = ohlcv[i]["low"]
        pc = ohlcv[i - 1]["close"]
        tr = max(high_val - low_val, abs(high_val - pc), abs(low_val - pc))
        trs.append(tr)
    atr = sum(trs[-period:]) / period
    last_close = ohlcv[-1]["close"]
    return (atr / last_close) * 100 if last_close > 0 else None


# helpers
def sma(arr: list[float], period: int) -> float | None:
    if len(arr) < period:
        return None
    return sum(arr[-period:]) / period


def ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


# MACD 12/26/9
def macd(closes: list[float], fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return None, None, None
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    m = min(len(ema_fast), len(ema_slow))
    macd_line = [ema_fast[-m + i] - ema_slow[-m + i] for i in range(m)]
    signal_line = ema_series(macd_line, signal)
    hist = macd_line[-1] - signal_line[-1]
    return macd_line[-1], signal_line[-1], hist


# Estocástico %K/%D (14,3)
def stoch_kd(ohlcv: list[dict], k_period=14, d_period=3):
    if len(ohlcv) < k_period + d_period:
        return None, None
    highs = [c["high"] for c in ohlcv]
    lows = [c["low"] for c in ohlcv]
    closes = [c["close"] for c in ohlcv]

    k_vals: list[float] = []
    for i in range(k_period - 1, len(closes)):
        window_high = max(highs[i - k_period + 1:i + 1])
        window_low = min(lows[i - k_period + 1:i + 1])
        if window_high == window_low:
            k_vals.append(50.0)
        else:
            k_vals.append(
                (closes[i] - window_low) / (window_high - window_low) * 100.0)

    if len(k_vals) < d_period:
        return None, None
    k = k_vals[-1]
    d = sum(k_vals[-d_period:]) / d_period
    return k, d


# Bollinger (20, 2σ) -> %B y bandwidth
def bollinger(closes: list[float], period=20, mult=2.0):
    if len(closes) < period:
        return None, None, None, None, None

    mid = sma(closes, period)
    if mid is None:
        return None, None, None, None, None

    mean = float(mid)  # fuerzo a float para Pyright
    var = sum((c - mean)**2 for c in closes[-period:]) / period
    sd = var**0.5
    upper = mean + mult * sd
    lower = mean - mult * sd
    last = closes[-1]

    pctb = (last - lower) / (upper - lower) * 100 if upper != lower else None
    bw = (upper - lower) / mean * 100 if mean else None
    return mid, upper, lower, pctb, bw


# MFI 14
def mfi(ohlcv: list[dict], period=14):
    if len(ohlcv) <= period:
        return None
    pos_flow = 0.0
    neg_flow = 0.0
    prev_typ = None
    for i in range(1, len(ohlcv)):
        high_val = ohlcv[i]["high"]
        low_val = ohlcv[i]["low"]
        close_val = ohlcv[i]["close"]
        vol = ohlcv[i]["volume"]
        typ = (high_val + low_val + close_val) / 3.0
        raw_flow = typ * vol
        if prev_typ is not None:
            if typ > prev_typ:
                pos_flow += raw_flow
            elif typ < prev_typ:
                neg_flow += raw_flow
        prev_typ = typ
    if neg_flow == 0:
        return 100.0
    mr = pos_flow / neg_flow
    return 100 - (100 / (1 + mr))


# OBV
def obv(ohlcv: list[dict]):
    if not ohlcv:
        return None
    val = 0.0
    for i in range(1, len(ohlcv)):
        if ohlcv[i]["close"] > ohlcv[i - 1]["close"]:
            val += ohlcv[i]["volume"]
        elif ohlcv[i]["close"] < ohlcv[i - 1]["close"]:
            val -= ohlcv[i]["volume"]
    return val


# VWAP (desviación % sobre ventana)
def vwap_dev_pct(ohlcv: list[dict], period=20):
    if len(ohlcv) < period:
        return None
    win = ohlcv[-period:]
    num = sum(
        ((c["high"] + c["low"] + c["close"]) / 3.0) * c["volume"] for c in win)
    den = sum(c["volume"] for c in win)
    if den == 0:
        return None
    vwap = num / den
    last = win[-1]["close"]
    return ((last - vwap) / vwap) * 100.0


# ADX +DI -DI (14) simplificado
def adx_dmi(ohlcv: list[dict], period=14):
    n = len(ohlcv)
    if n <= period + 1:
        return None, None, None

    trs: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []

    for i in range(1, n):
        hi = ohlcv[i]["high"]
        lo = ohlcv[i]["low"]
        ph = ohlcv[i - 1]["high"]
        pl = ohlcv[i - 1]["low"]
        pc = ohlcv[i - 1]["close"]

        up_move = hi - ph
        down_move = pl - lo
        plus_dm.append(up_move if up_move > 0 and up_move > down_move else 0.0)
        minus_dm.append(
            down_move if down_move > 0 and down_move > up_move else 0.0)
        trs.append(max(hi - lo, abs(hi - pc), abs(lo - pc)))

    def smooth(arr: list[float]) -> list[float]:
        sm = [sum(arr[:period])]
        for i in range(period, len(arr)):
            sm.append(sm[-1] - (sm[-1] / period) + arr[i])
        return sm

    tr_s = smooth(trs)
    pdm_s = smooth(plus_dm)
    mdm_s = smooth(minus_dm)

    if not tr_s or tr_s[-1] == 0:
        return None, None, None

    plus_di = 100 * (pdm_s[-1] / tr_s[-1])
    minus_di = 100 * (mdm_s[-1] / tr_s[-1])
    denom = plus_di + minus_di
    if denom == 0:
        return None, plus_di, minus_di

    dx = 100 * abs(plus_di - minus_di) / denom
    adx_vals = ema_series([dx] * period, period)  # aproximación
    adx_val = adx_vals[-1] if adx_vals else None
    return adx_val, plus_di, minus_di


def fmt_or_dash(x, dec=2):
    return (f"{x:+.{dec}f}" if isinstance(x, (int, float)) else "-")

# ========= CLASIFICADORES + FORMATEADOR UNIFICADO =========

def classify_ema2050(e20: float | None, e50: float | None) -> str:
    if not isinstance(e20, (int,float)) or not isinstance(e50, (int,float)):
        return "-"
    # margen relativo para distinguir "cruce / empate"
    if e50 != 0 and abs((e20 - e50)/e50) < 0.0005:  # <0.05%
        return "≈cruce"
    return "alcista" if e20 > e50 else "bajista"

def classify_rsi14(rsi: float | None) -> str:
    if not isinstance(rsi, (int,float)):
        return "-"
    if rsi < 30:
        return "sobreventa"
    if rsi < 50:
        return "débil bajista"
    if rsi < 70:
        return "alcista moderado"
    return "sobrecompra"

def classify_macdh(macdh: float | None) -> str:
    if not isinstance(macdh, (int,float)):
        return "-"
    if abs(macdh) < 1e-6:
        return "plano"
    return "momento alcista" if macdh > 0 else "momento bajista"

def classify_atr_pct(atrp: float | None) -> str:
    if not isinstance(atrp, (int,float)):
        return "-"
    if atrp < 1.0:
        return "vol. baja"
    if atrp < 2.0:
        return "vol. media"
    if atrp < 5.0:
        return "vol. alta"
    return "vol. muy alta"

def classify_percent_b(pctb: float | None) -> str:
    # %B de Bollinger: 0..100 dentro de bandas; <0 / >100 = ruptura
    if not isinstance(pctb, (int,float)):
        return "-"
    if pctb < 0:
        return "ruptura ↓"
    if pctb < 20:
        return "zona muy baja"
    if pctb < 40:
        return "zona baja"
    if pctb < 60:
        return "zona media"
    if pctb < 80:
        return "zona alta"
    if pctb <= 100:
        return "zona muy alta"
    return "ruptura ↑"

def classify_adx14(adx: float | None) -> str:
    if not isinstance(adx, (int,float)):
        return "-"
    if adx < 20:
        return "rango"
    if adx < 25:
        return "inicio tendencia"
    if adx < 40:
        return "tendencia"
    if adx < 60:
        return "tendencia fuerte"
    return "clímax/agotamiento"

def format_indicadores_unificado(exchange_name: str, quote: str, rows: list[dict], top_n: int = 20) -> str:
    lines = []
    lines.append(f"📐 Indicadores — {exchange_name} ({quote})")
    lines.append(f"(Top {min(top_n, len(rows))} por volumen 24h)\n")

    def _fmt_price(x: float) -> str:
        if not isinstance(x, (int,float)):
            return "-"
        if x >= 100:
            return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")
        if x >= 1:
            return f"{x:,.4f}".replace(",", "X").replace(".", ",").replace("X",".")
        return f"{x:.6f}".rstrip("0").rstrip(".").replace(".", ",")

    for i, d in enumerate(rows[:top_n], 1):
        sym   = d.get("symbol","-")
        e20 = d.get("ema20")
        e50 = d.get("ema50")
        rsi   = d.get("rsi14")
        macdh = d.get("macd_hist")
        atrp  = d.get("atr_pct")
        pctb  = d.get("bb_pctb")
        adx   = d.get("adx14")

        ema_s   = f"{_fmt_price(e20)}/{_fmt_price(e50)}" if isinstance(e20,(int,float)) and isinstance(e50,(int,float)) else "-/-"
        ema_lbl = classify_ema2050(e20, e50)

        if isinstance(rsi, (int, float)):
            rsi_val = int(round(rsi))
            rsi_s   = f"{rsi_val}"
            rsi_lbl = classify_rsi14(rsi_val)   # <-- clasifica con el mismo redondeado
        else:
            rsi_s, rsi_lbl = "-", "-"

        mac_s   = f"{macdh:+.4f}" if isinstance(macdh,(int,float)) else "-"
        mac_lbl = classify_macdh(macdh)

        atr_s   = f"{atrp:.2f}" if isinstance(atrp,(int,float)) else "-"
        atr_lbl = classify_atr_pct(atrp)

        if isinstance(pctb, (int, float)):
            pb_val = int(round(pctb))
            pb_s   = f"{pb_val}"
            pb_lbl = classify_percent_b(pb_val)  # <-- usa el redondeado para clasificar
        else:
            pb_s, pb_lbl = "-", "-"

        if isinstance(adx, (int, float)):
            adx_val = int(round(adx))
            adx_s   = f"{adx_val}"
            adx_lbl = classify_adx14(adx_val)    # <-- usa el redondeado para clasificar
        else:
            adx_s, adx_lbl = "-", "-"

        lines.append(
            f"{i:>2}) {sym} · "
            f"EMA20/50 {ema_s} ({ema_lbl}) · "
            f"RSI {rsi_s} ({rsi_lbl}) · "
            f"MACDh {mac_s} ({mac_lbl}) · "
            f"ATR% {atr_s} ({atr_lbl}) · "
            f"%B {pb_s} ({pb_lbl}) · "
            f"ADX {adx_s} ({adx_lbl})"
        )

    lines.append("\nNFA | DYOR")
    return "\n".join(lines)
# ================== INDICATORS: VISUAL ENGLISH FORMATTER ==================

# ======== NEW helpers (reemplaza/añade cerca de los otros badges) ========

def _trend_badge_en(e20: float | None, e50: float | None) -> str:
    if not isinstance(e20, (int,float)) or not isinstance(e50, (int,float)):
        return "• Neutral"
    # neutro si están muy cerca (±0.05%)
    if e50 != 0 and abs((e20 - e50)/e50) < 0.0005:
        return "🔺 Neutral"
    return "🟢 Uptrend" if e20 > e50 else "🔻 Downtrend"

def _rsi_cell_en(rsi: float | None) -> str:
    if not isinstance(rsi, (int,float)):
        return "- (—)"
    v = int(round(rsi))
    # bandas pensadas para que 49/51 sean "Neutral", 55 "Mild bull", 45 "Mild bear"
    if v >= 70:
        label = "Overb."
    elif v >= 60:
        label = "Bullish"
    elif v > 55:
        label = "Mild bull"
    elif v >= 45:
        label = "Neutral"
    elif v > 40:
        label = "Mild bear"
    elif v > 30:
        label = "Bearish"
    else:
        label = "Oversold"
    return f"{v} ({label})"

def _macd_cell_en(macdh: float | None) -> str:
    if not isinstance(macdh, (int,float)):
        return "⚪ Neutral"
    if abs(macdh) < 1e-6:
        return "⚪ Neutral"
    return "🟢 Bullish" if macdh > 0 else "🔴 Bearish"

def _atr_cell_en(atrp: float | None) -> str:
    if not isinstance(atrp,(int,float)):
        return "– (—)"
    # icono primero + porcentaje con 2 decimales + etiqueta
    if atrp < 1.0:
        return f"😴 {atrp:.2f}% (Low)"
    if atrp < 2.0:
        return f"🙂 {atrp:.2f}% (Mid)"
    return f"🌪️ {atrp:.2f}% (High)"

def _pctb_cell_en(pctb: float | None) -> str:
    if not isinstance(pctb,(int,float)):
        return "– (—)"
    v = int(round(pctb))
    if v >= 80:
        return f"🔼 {v} (Top)"
    if v >= 60:
        return f"🔼 {v} (High)"
    if v >= 40:
        return f"⏸ {v} (Mid)"
    if v >= 20:
        return f"⏸ {v} (Low)"
    return f"🔽 {v} (Bottom)"

def _adx_cell_en(adx: float | None) -> str:
    if not isinstance(adx,(int,float)):
        return "⏸ – (Range)"
    v = int(round(adx))
    if v < 20:
        return f"⏸ {v} (Range)"
    if v < 25:
        return f"📈 {v} (Trend)"
    if v < 40:
        return f"📈 {v} (Trend)"
    if v < 50:
        return f"📈 {v} (Trend)"
    if v < 60:
        return f"🚀 {v} (Strong)"
    return f"🚀 {v} (Strong)"

def _price_en(x: float | None) -> str:
    if not isinstance(x, (int,float)):
        return "-"
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}"
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"

# ======== REEMPLAZA ESTA FUNCIÓN COMPLETA ========

def format_indicators_visual_en(exchange_name: str, quote: str, rows: list[dict], top_n: int = 20) -> str:
    """
    Produce exactamente el layout del ejemplo:
    - Trend con emoji + texto (Uptrend/Downtrend/Neutral)
    - RSI con valor + etiqueta (Neutral/Mild bull/Overb./...)
    - MACD con Bullish/Bearish/Neutral
    - Vol. como ATR% con emoji (Low/Mid/High)
    - Band como %B con zona (Top/High/Mid/Low/Bottom) e icono
    - ADX con valor + (Range/Trend/Strong) e icono
    """
    n = min(top_n, len(rows))
    header = (
        f"📐 Indicators — {exchange_name} ({quote})\n"
        f"(Top {n} by 24h volume)\n\n"
    )

    # encabezado fijo, fuente monoespaciada para alinear
    lines = ["```",
             "#  Pair      Trend        RSI (value)        MACD         Vol. ATR% (vol)      Band %B (zone)       ADX (trend)"]
    for i, d in enumerate(rows[:n], 1):
        sym   = d.get("symbol","-")
        e20   = d.get("ema20")
        e50   = d.get("ema50")
        rsi   = d.get("rsi14")
        macdh = d.get("macd_hist")
        atrp  = d.get("atr_pct")
        pctb  = d.get("bb_pctb")
        adx   = d.get("adx14")

        trend = _trend_badge_en(e20, e50)
        rsi_s = _rsi_cell_en(rsi)
        mac_s = _macd_cell_en(macdh)
        atr_s = _atr_cell_en(atrp)
        pb_s  = _pctb_cell_en(pctb)
        adx_s = _adx_cell_en(adx)

        # Ajusta los anchos para que cuadre visualmente como tu mock
        lines.append(
            f"{i:>2}) {sym:<8} {trend:<12} {rsi_s:<18} {mac_s:<12} {atr_s:<18} {pb_s:<20} {adx_s:<14}"
        )
    lines.append("```")
    footer = ""
    return header + "\n".join(lines) + footer

def format_binance_top_message(top20, total_quote_vol):
    quote = BINANCE_QUOTE
    rows = [{
        "ticker": d["symbol"],
        "price": d["lastPrice"],
        "vol_usdc": d["quoteVolume"],
        "pct_24h": d["priceChangePercent"],
        "share_pct": d["dominance"],
    } for d in top20]
    return _build_top_msg_en("BINANCE", quote, total_quote_vol, rows)


# ---------- data sources: BYBIT ----------
# 1) Intento directo a Bybit con headers estilo navegador + varios dominios
BYBIT_BASES = [
    "https://api.bybitglobal.com",
    "https://api.bybit.com",
    "https://api.bytick.com",
]
BYBIT_HEADERS = {
    # UA realista de navegador
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.bybit.com",
    "Referer": "https://www.bybit.com/",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def _bybit_get(path: str,
                     params: dict | None = None,
                     timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    errors = []
    async with httpx.AsyncClient(timeout=timeout,
                                 follow_redirects=True,
                                 headers=BYBIT_HEADERS) as client:
        for base in BYBIT_BASES:
            try:
                r = await client.get(base + path, params=params or {})
                r.raise_for_status()
                j = r.json()
                # algunos endpoints no traen retCode; si lo traen, 0 = OK
                if "retCode" in j and j.get("retCode") != 0:
                    errors.append(
                        (base,
                         f"retCode={j.get('retCode')} {j.get('retMsg')}"))
                    continue
                return j
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases Bybit fallaron: " +
                       " | ".join(f"{b}: {e}" for b, e in errors))


# 2) Fallback: CoinGecko → /exchanges/bybit/tickers (filtramos target=USDC)
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}


async def _coingecko_get(path: str,
                         params: dict | None = None,
                         timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    async with httpx.AsyncClient(timeout=timeout,
                                 follow_redirects=True,
                                 headers=COINGECKO_HEADERS) as client:
        r = await client.get(COINGECKO_BASE + path, params=params or {})
        r.raise_for_status()
        return r.json()


async def _fetch_bybit_top20_quote_direct():
    """
    Intenta directamente contra Bybit.
    """
    quote = BYBIT_QUOTE
    tick = await _bybit_get("/v5/market/tickers", params={"category": "spot"})
    rows = (tick.get("result") or {}).get("list") or []

    data, total_quote_vol = [], 0.0
    for r in rows:
        sym = (r.get("symbol") or "").upper()
        if not sym.endswith(quote):
            continue
        try:
            lp = float(r.get("lastPrice", "0"))
            pcp = float(r.get("price24hPcnt", "0")) * 100.0  # 0.0123 => 1.23%
            qv = float(r.get("turnover24h", "0"))  # volumen en quote
        except (TypeError, ValueError):
            continue
        total_quote_vol += qv
        data.append({
            "symbol": sym,
            "lastPrice": lp,
            "priceChangePercent": pcp,
            "quoteVolume": qv,
        })

    if total_quote_vol <= 0:
        return [], 0.0, False
    data.sort(key=lambda x: x["quoteVolume"], reverse=True)
    top20 = data[:20]
    for d in top20:
        d["dominance"] = (d["quoteVolume"] / total_quote_vol) * 100.0
    return top20, total_quote_vol, False  # False => no es fallback


async def _fetch_bybit_top20_quote_fallback():
    """
    Fallback via CoinGecko si Bybit 403.
    SOLO target = BYBIT_QUOTE (USDC).
    Recorre varias páginas (1..5) para capturar todos los tickers del exchange.
    - Volumen en quote: preferimos converted_volume.usd; si no hay, usamos last * volume (base).
    - %24h: no disponible → 0.00%.
    Devuelve: (top20, total, used_fallback=True, note="")
    """
    target = BYBIT_QUOTE  # debe ser "USDC"
    all_rows = []
    for page in range(1, 6):  # intenta hasta 5 páginas
        j = await _coingecko_get("/exchanges/bybit/tickers",
                                 params={
                                     "include_exchange_logo": "false",
                                     "depth": "false",
                                     "page": page
                                 })
        rows = (j or {}).get("tickers") or []
        if not rows:
            break
        all_rows.extend(rows)

    data, total = [], 0.0
    for r in all_rows:
        tgt = (r.get("target") or "").upper()
        if tgt != target:
            continue
        base = (r.get("base") or "").upper()
        sym = f"{base}{target}"  # mostramos *USDC
        try:
            last = float(r.get("last") or 0.0)
            vol_base = float(r.get("volume") or 0.0)  # 24h en moneda base
        except (TypeError, ValueError):
            continue

        # volumen en USD/USDC
        qv_usd = 0.0
        conv = r.get("converted_volume") or {}
        try:
            qv_usd = float(conv.get("usd") or 0.0)
        except (TypeError, ValueError):
            qv_usd = 0.0
        if qv_usd <= 0 and last > 0 and vol_base > 0:
            qv_usd = last * vol_base

        if qv_usd <= 0:
            continue

        total += qv_usd
        data.append({
            "symbol": sym,
            "lastPrice": last,
            "priceChangePercent": 0.0,  # no disponible
            "quoteVolume": qv_usd,
        })

    if total <= 0:
        return [], 0.0, True, ""

    data.sort(key=lambda x: x["quoteVolume"], reverse=True)
    top20 = data[:20]
    for d in top20:
        d["dominance"] = (d["quoteVolume"] / total) * 100.0
    return top20, total, True, ""  # used_fallback=True, sin nota


async def fetch_bybit_top20_quote():
    try:
        top20, total, used_fallback_direct = await _fetch_bybit_top20_quote_direct(
        )
        return top20, total, used_fallback_direct, ""
    except Exception as e:
        log.warning(f"Bybit directo falló, usando fallback CoinGecko: {e}")
        return await _fetch_bybit_top20_quote_fallback()


def format_bybit_top_message(top20,
                             total_quote_vol,
                             used_fallback=False,
                             note: str = ""):
    quote = BYBIT_QUOTE  # "USDC"
    lines = []
    lines.append(f"📊 Precios & Volumen — BYBIT ({quote})")
    lines.append(
        f"Volumen total 24h ({quote}): **{fmt_num(total_quote_vol, 0)}**")
    lines.append("")
    lines.append("Top 20 por volumen (24h):")
    for i, d in enumerate(top20, 1):
        sym = d["symbol"]
        price = d["lastPrice"]
        decimals = 6 if price < 1 else (4 if price < 100 else 2)
        price_s = fmt_num(price, decimals)
        vol_s = fmt_num(d["quoteVolume"], 0)
        chg_s = f"{d['priceChangePercent']:+.2f}%"
        dom_s = f"{d['dominance']:.2f}%"
        lines.append(
            f"{i:>2}. {sym} · {price_s} · {vol_s} {quote} · {chg_s} · {dom_s}")
    if used_fallback:
        lines.append(
            "\nℹ️ Fuente: CoinGecko (Bybit bloqueado 403). %24h no disponible por par."
        )
    # (sin nota extra; solo USDC)
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)


# ---------- data sources: OKX ----------
OKX_BASES = [
    "https://www.okx.com",
    "https://aws.okx.com",  # mirror
]
OKX_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}


async def _okx_get(path: str,
                   params: dict | None = None,
                   timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    errors = []
    async with httpx.AsyncClient(timeout=timeout,
                                 follow_redirects=True,
                                 headers=OKX_HEADERS) as client:
        for base in OKX_BASES:
            try:
                r = await client.get(base + path, params=params or {})
                r.raise_for_status()
                j = r.json()
                # OKX OK => {"code":"0","msg":"","data":[...]}
                if j.get("code") not in ("0", 0, None):
                    errors.append(
                        (base, f"code={j.get('code')} {j.get('msg')}"))
                    continue
                return j
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases OKX fallaron: " +
                       " | ".join(f"{b}: {e}" for b, e in errors))


def _okx_symbol_to_inst(sym_no_dash: str) -> str:
    # convierte "BTCUSDC" -> "BTC-USDC" usando la quote actual
    q = OKX_QUOTE
    base = sym_no_dash[:-len(q)]
    quote = sym_no_dash[-len(q):]
    return f"{base}-{quote}"


async def fetch_okx_candles(inst_id: str, bar: str = "1H", limit: int = 120):
    j = await _okx_get("/api/v5/market/candles",
                       params={
                           "instId": inst_id,
                           "bar": bar,
                           "limit": str(limit)
                       })
    rows = (j.get("data") or [])
    ohlcv = []
    for r in reversed(rows):  # OKX devuelve descendente
        ohlcv.append({
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        })
    return ohlcv


async def attach_indicators_okx(top20: list[dict],
                                bar: str = "1H") -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. BTCUSDC
        inst = _okx_symbol_to_inst(sym)  # BTC-USDC
        try:
            ohlcv = await fetch_okx_candles(inst, bar=bar, limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"] = calc_rsi(closes, 14)
            d["ema20"] = ema(closes, 20)
            d["ema50"] = ema(closes, 50)
            d["atr_pct"] = calc_atr_pct(ohlcv, 14)

            # PRO
            macd_l, macd_s, macd_h = macd(closes, 12, 26, 9)
            d["macd"] = macd_l
            d["macd_sig"] = macd_s
            d["macd_hist"] = macd_h

            k, kd = stoch_kd(ohlcv, 14, 3)
            d["stoch_k"] = k
            d["stoch_d"] = kd

            mid, up, lo, pctb, bw = bollinger(closes, 20, 2.0)
            d["bb_pctb"] = pctb
            d["bb_bw"] = bw

            d["mfi14"] = mfi(ohlcv, 14)
            d["obv"] = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores OKX fallaron para {sym}: {e}")
            for key in ("rsi14", "ema20", "ema50", "atr_pct", "macd",
                        "macd_sig", "macd_hist", "stoch_k", "stoch_d",
                        "bb_pctb", "bb_bw", "mfi14", "obv", "vwap_dev_pct",
                        "adx14", "+di", "-di"):
                d[key] = None
        out.append(d)
    return out


async def fetch_okx_top20_quote():
    """
    Top 20 spot por volumen en QUOTE = OKX_QUOTE (USDC por defecto).
    Filtra instrumentos cuyo instId termina en -QUOTE (p.ej., BTC-USDC).
    Usa volCcy24h (volumen en QUOTE 24h). % = (last - open24h)/open24h*100.
    Devuelve (top20:list[dict], total_quote_vol:float)
    """
    quote = OKX_QUOTE
    tick = await _okx_get("/api/v5/market/tickers",
                          params={"instType": "SPOT"})
    rows = (tick.get("data") or [])

    data = []
    total_quote_vol = 0.0
    for r in rows:
        inst = (r.get("instId") or "")  # p.ej. "BTC-USDC"
        if not inst.endswith(f"-{quote}"):
            continue
        try:
            last = float(r.get("last", "0"))
            open24h = float(r.get("open24h", "0"))
            vol_quote = float(r.get("volCcy24h", "0"))  # volumen en QUOTE
        except (TypeError, ValueError):
            continue
        if vol_quote <= 0:
            continue

        chg_pct = 0.0
        if open24h > 0:
            chg_pct = (last - open24h) / open24h * 100.0

        total_quote_vol += vol_quote
        data.append({
            "symbol": inst.replace("-", ""),  # estilo BTCUSDC para coherencia
            "lastPrice": last,
            "priceChangePercent": chg_pct,
            "quoteVolume": vol_quote,
        })

    if total_quote_vol <= 0:
        return [], 0.0

    data.sort(key=lambda x: x["quoteVolume"], reverse=True)
    top20 = data[:20]
    for d in top20:
        d["dominance"] = (d["quoteVolume"] / total_quote_vol) * 100.0
    return top20, total_quote_vol


def format_okx_top_message(top20, total_quote_vol):
    quote = OKX_QUOTE
    rows = [{
        "ticker": d["symbol"],
        "price": d["lastPrice"],
        "vol_usdc": d["quoteVolume"],
        "pct_24h": d["priceChangePercent"],
        "share_pct": d["dominance"],
    } for d in top20]
    return _build_top_msg_en("OKX", quote, total_quote_vol, rows)


# ---------- data sources: KRAKEN ----------
KRAKEN_BASE = "https://api.kraken.com"
KRAKEN_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}


async def _kraken_get(path: str,
                      params: dict | None = None,
                      timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    async with httpx.AsyncClient(timeout=timeout,
                                 follow_redirects=True,
                                 headers=KRAKEN_HEADERS) as client:
        r = await client.get(KRAKEN_BASE + path, params=params or {})
        r.raise_for_status()
        j = r.json()
        # Kraken OK => {"error":[], "result":{...}}
        if j.get("error"):
            raise RuntimeError(f"Kraken error: {j.get('error')}")
        return j


def _norm_asset(a: str) -> str:
    # Kraken usa prefijos X/Z en algunos assets (p.ej. ZUSD, XXBT).
    return (a or "").upper().lstrip("XZ")


async def fetch_kraken_top20_quote():
    """
    Top 20 por volumen en QUOTE = KRAKEN_QUOTE (USDC por defecto).
    - AssetPairs: filtramos pares SPOT cuyo 'quote' normalizado == KRAKEN_QUOTE
    - Ticker: v[1] = volumen 24h en BASE; c[0] = last; o = open
      -> quoteVolume ≈ last * vol_base
      -> % = (last - open)/open*100
    Devuelve (top20:list[dict], total_quote_vol:float)
    """
    quote = KRAKEN_QUOTE

    # 1) Pares disponibles
    ap = await _kraken_get("/0/public/AssetPairs")
    pairs = (ap.get("result") or {})

    # Mapeos: altname -> wsname y altname -> válido por quote
    alt_to_ws: dict[str, str] = {}
    valid_altnames: list[str] = []
    for alt, info in pairs.items():
        q = _norm_asset(info.get("quote", ""))
        if q != quote:
            continue
        ws = (info.get("wsname") or alt).replace("/", "")
        alt_to_ws[alt] = ws
        valid_altnames.append(alt)

    if not valid_altnames:
        return [], 0.0

    # 2) Ticker en lotes (Kraken acepta lista separada por comas)
    data: list[dict] = []
    total_quote_vol = 0.0

    for i in range(0, len(valid_altnames), 80):
        chunk = valid_altnames[i:i + 80]
        tk = await _kraken_get("/0/public/Ticker",
                               params={"pair": ",".join(chunk)})
        trows = (tk.get("result") or {})
        for k, v in trows.items():
            ws = alt_to_ws.get(k, k).upper()  # BTCUSDC, ETHUSDC, ...
            try:
                last = float((v.get("c") or ["0"])[0])
                openp = float(v.get("o") or 0.0)
                vol_base = float((v.get("v") or ["0", "0"])[1])  # 24h base
            except (TypeError, ValueError):
                continue
            if last <= 0 or vol_base <= 0:
                continue

            qv = last * vol_base
            chg = ((last - openp) / openp * 100.0) if openp > 0 else 0.0

            total_quote_vol += qv
            data.append({
                "symbol": ws,
                "lastPrice": last,
                "priceChangePercent": chg,
                "quoteVolume": qv,
            })

    if total_quote_vol <= 0:
        return [], 0.0

    data.sort(key=lambda x: x["quoteVolume"], reverse=True)
    top20 = data[:20]
    for d in top20:
        d["dominance"] = (d["quoteVolume"] / total_quote_vol) * 100.0
    return top20, total_quote_vol


def format_kraken_top_message(top20, total_quote_vol):
    quote = KRAKEN_QUOTE
    rows = [{
        "ticker": d["symbol"],
        "price": d["lastPrice"],
        "vol_usdc": d["quoteVolume"],
        "pct_24h": d["priceChangePercent"],
        "share_pct": d["dominance"],
    } for d in top20]
    return _build_top_msg_en("KRAKEN", quote, total_quote_vol, rows)


# --- Kraken OHLC (velas) + attach indicadores ---


async def fetch_kraken_candles(pair: str,
                               interval: int = 60,
                               limit: int = 120):
    """
    pair: usa el símbolo que ya guardas como wsname sin barra (ej. 'XBTUSDC').
    interval=60 => 1h
    """
    j = await _kraken_get("/0/public/OHLC",
                          params={
                              "pair": pair,
                              "interval": interval
                          })
    rows = (j.get("result") or {}).get(pair, [])
    ohlcv = []
    for r in rows[-limit:]:
        ohlcv.append({
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[6]),
        })
    return ohlcv


async def attach_indicators_kraken(top20: list[dict],
                                   interval: int = 60) -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. XBTUSDC
        try:
            ohlcv = await fetch_kraken_candles(sym,
                                               interval=interval,
                                               limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"] = calc_rsi(closes, 14)
            d["ema20"] = ema(closes, 20)
            d["ema50"] = ema(closes, 50)
            d["atr_pct"] = calc_atr_pct(ohlcv, 14)

            # PRO
            macd_l, macd_s, macd_h = macd(closes, 12, 26, 9)
            d["macd"] = macd_l
            d["macd_sig"] = macd_s
            d["macd_hist"] = macd_h

            k, kd = stoch_kd(ohlcv, 14, 3)
            d["stoch_k"] = k
            d["stoch_d"] = kd

            _mid, _up, _lo, pctb, bw = bollinger(closes, 20, 2.0)
            d["bb_pctb"] = pctb
            d["bb_bw"] = bw

            d["mfi14"] = mfi(ohlcv, 14)
            d["obv"] = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores Kraken fallaron para {sym}: {e}")
            for key in ("rsi14", "ema20", "ema50", "atr_pct", "macd",
                        "macd_sig", "macd_hist", "stoch_k", "stoch_d",
                        "bb_pctb", "bb_bw", "mfi14", "obv", "vwap_dev_pct",
                        "adx14", "+di", "-di"):
                d[key] = None
        out.append(d)
    return out


# ---------- single-ticker helpers (precio / %24h / vol24h) ----------


async def fetch_binance_ticker(symbol: str) -> dict | None:
    try:
        j = await _binance_get(f"/api/v3/ticker/24hr?symbol={symbol}")
        # Binance devuelve dict directamente
        return {
            "lastPrice": float(j.get("lastPrice", 0.0)),
            "priceChangePercent": float(j.get("priceChangePercent", 0.0)),
            "quoteVolume": float(j.get("quoteVolume", 0.0)),
        }
    except Exception:
        return None


async def fetch_okx_ticker(sym_no_dash: str) -> dict | None:
    try:
        inst = _okx_symbol_to_inst(sym_no_dash)  # BTCUSDC -> BTC-USDC
        j = await _okx_get("/api/v5/market/ticker", params={"instId": inst})
        rows = (j.get("data") or [])
        if not rows:
            return None
        r = rows[0]
        last = float(r.get("last", 0.0))
        open24h = float(r.get("open24h", 0.0))
        chg = ((last - open24h) / open24h * 100.0) if open24h > 0 else 0.0
        return {
            "lastPrice": last,
            "priceChangePercent": chg,
            "quoteVolume": float(r.get("volCcy24h", 0.0)),  # en QUOTE
        }
    except Exception:
        return None


# Kraken acepta 'pair' tal como lo usas en OHLC (WS name sin '/')
async def fetch_kraken_ticker(pair_ws: str) -> dict | None:
    try:
        j = await _kraken_get("/0/public/Ticker", params={"pair": pair_ws})
        res = (j.get("result") or {})
        if pair_ws not in res:
            # a veces la clave es distinta; toma la primera
            if res:
                k = list(res.keys())[0]
            else:
                return None
        else:
            k = pair_ws
        v = res[k]
        last = float((v.get("c") or ["0"])[0])
        openp = float(v.get("o") or 0.0)
        vol_base = float((v.get("v") or ["0", "0"])[1])
        qv = last * vol_base if last > 0 and vol_base > 0 else 0.0
        chg = ((last - openp) / openp * 100.0) if openp > 0 else 0.0
        return {
            "lastPrice": last,
            "priceChangePercent": chg,
            "quoteVolume": qv,
        }
    except Exception:
        return None


async def get_ticker(ex: str, symbol: str) -> dict | None:
    exu = ex.upper()
    if exu == "BINANCE":
        return await fetch_binance_ticker(symbol)
    if exu == "OKX":
        return await fetch_okx_ticker(symbol)
    if exu == "KRAKEN":
        return await fetch_kraken_ticker(symbol)
    return None

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(
        f"🛰️ Último ping web: {LAST_WEB_PING}\n"
        f"⏰ Zona horaria: {TZ}\n"
        f"📮 Canal: {CHANNEL_ID or '—'}",
        parse_mode="Markdown")


# ---------- Market Stress Detector (15m/60m) ----------

STRESS_LAST_LEVEL = "NONE"
STRESS_LAST_SCORE = 0.0


def _safe_pct(now_close: float, prev_close: float) -> float | None:
    try:
        if prev_close <= 0:
            return None
        return (now_close / prev_close) - 1.0
    except Exception:
        return None


# --- BINANCE: helpers 15m / 1h
async def _binance_ret15(symbol: str) -> float | None:
    try:
        rows = await fetch_binance_klines(symbol, interval="15m", limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


async def _binance_ret60(symbol: str) -> float | None:
    try:
        rows = await fetch_binance_klines(symbol, interval="1h", limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


# --- OKX: helpers 15m / 1h
async def _okx_ret15(sym_no_dash: str) -> float | None:
    try:
        inst = _okx_symbol_to_inst(sym_no_dash)  # "BTC-USDC"
        rows = await fetch_okx_candles(inst, bar="15m", limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


async def _okx_ret60(sym_no_dash: str) -> float | None:
    try:
        inst = _okx_symbol_to_inst(sym_no_dash)
        rows = await fetch_okx_candles(inst, bar="1H", limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


# --- KRAKEN: helpers 15m / 1h
async def _kraken_ret15(pair_ws: str) -> float | None:
    try:
        rows = await fetch_kraken_candles(pair_ws, interval=15, limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


async def _kraken_ret60(pair_ws: str) -> float | None:
    try:
        rows = await fetch_kraken_candles(pair_ws, interval=60, limit=2)
        if len(rows) < 2:
            return None
        return _safe_pct(rows[-1]["close"], rows[-2]["close"])
    except Exception:
        return None


def _fng_bucket(value: int) -> str:
    # Mapeo estilo Binance-ish:
    # 0-24: Extreme Fear, 25-49: Fear, 50-54: Neutral, 55-74: Greed, 75-100: Extreme Greed
    v = int(value)
    if v <= 24:
        return "😱 *Extreme Fear*"
    if v <= 49:
        return "😟 *Fear*"
    if v <= 54:
        return "😐 *Neutral*"
    if v <= 74:
        return "😏 *Greed*"
    return "🤩 *Extreme Greed*"


async def fetch_fng_index() -> dict:
    """
    Devuelve un dict con:
      { 'value': int, 'classification': str, 'time': datetime }
    Lanza excepción si falla.
    """
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout,
                                 headers=FNG_HEADERS,
                                 follow_redirects=True) as client:
        r = await client.get(FNG_URL)
        r.raise_for_status()
        j = r.json() or {}
        data = (j.get("data") or [{}])[0]
        value = int(float(data.get("value", "0")))
        # timestamp unix (segundos)
        ts = int(data.get("timestamp", 0))
        when = datetime.fromtimestamp(ts, TZ)
        return {
            "value": value,
            "classification": _fng_bucket(value),
            "time": when,
        }


def format_fng_message(d: dict) -> str:
    v = int(d["value"])
    when = d["time"].strftime("%d/%m %H:%M")

    # Etiqueta + emoji según valor (mapeo estilo Binance-ish)
    if v <= 24:
        label = "😱 Extreme Fear"
    elif v <= 49:
        label = "😟 Fear"
    elif v <= 54:
        label = "😐 Neutral"
    elif v <= 74:
        label = "😏 Greed"
    else:
        label = "🤩 Extreme Greed"
        

    # Barra 0..100 con 20 bloques (cada bloque = 5 puntos)
    # Gradiente por tramos (0-24 rojos, 25-49 naranjas, 50-74 amarillos, 75-100 verdes)
    blocks = []
    for i in range(20):
        lo, hi = i*5, i*5 + 4
        if hi <= 24:
            blocks.append("🟥")
        elif lo >= 75:
            blocks.append("🟩")
        elif hi <= 49:
            blocks.append("🟧")
        else:
            blocks.append("🟨")
    bar = "".join(blocks)

    # Puntero en la posición (0..19)
    idx = min(19, max(0, int(round(v / 5 - 0.5))))
    pointer = " " * idx + "↑"

    # Marcas inferiores
    ticks = "0     25     50     75    100"

    # Cuerpo final
    return (
        "📈 *Fear & Greed Index (Crypto)*\n\n"
        f"*Valor:* *{v}/100*  ·  {label}\n\n"
        "```\n"
        f"{bar}\n"
        f"{pointer}\n"
        f"{ticks}\n"
        "```\n"
        f"🕒 Actualizado: {when} {TZ}\n\n"
        "NFA | DYOR"
    )

def _clip01(x: float) -> float:
    try:
        if x < 0:
            return 0.0
        if x > 1:
            return 1.0
        return float(x)
    except Exception:
        return 0.0


def _level_from_score(score: float) -> str:
    if score >= 80:
        return "RED"
    if score >= 60:
        return "ORANGE"
    if score >= 40:
        return "YELLOW"
    return "NONE"


def _fmt_pct(x: float | None, dec=2) -> str:
    return (f"{x:+.{dec}%}" if isinstance(x, (int, float)) else "—")


async def _compute_exchange_components(name: str, top20: list[dict],
                                       quote: str) -> dict:
    """
    Calcula ret_15m/ret_60m del índice VW, breadth_15m y leaders para un exchange.
    name: 'BINANCE' | 'OKX' | 'KRAKEN'
    """
    symbols = [d["symbol"] for d in top20]
    weights = {d["symbol"]: float(d.get("quoteVolume") or 0.0) for d in top20}
    total_w = sum(weights.values()) or 1.0

    if name == "BINANCE":
        f15 = _binance_ret15
        f60 = _binance_ret60
        leader_btc = f"BTC{quote}"
        leader_eth = f"ETH{quote}"
    elif name == "OKX":
        f15 = _okx_ret15
        f60 = _okx_ret60
        leader_btc = f"BTC{quote}"
        leader_eth = f"ETH{quote}"
    else:  # KRAKEN
        f15 = _kraken_ret15
        f60 = _kraken_ret60
        leader_btc = f"XBT{quote}"  # Kraken usa XBT
        leader_eth = f"ETH{quote}"

    rets15_by_sym: dict[str, float] = {}
    rets60_by_sym: dict[str, float] = {}

    for sym in symbols:
        r15 = await f15(sym)
        if r15 is not None:
            rets15_by_sym[sym] = float(r15)
        r60 = await f60(sym)
        if r60 is not None:
            rets60_by_sym[sym] = float(r60)

    vw15 = sum(weights[s] * rets15_by_sym.get(s, 0.0)
               for s in symbols) / total_w
    vw60 = sum(weights[s] * rets60_by_sym.get(s, 0.0)
               for s in symbols) / total_w

    have15 = [rets15_by_sym.get(s) for s in symbols if s in rets15_by_sym]
    breadth = (sum(1 for v in have15 if v is not None and v <= -0.01) /
               max(1, len(have15))) * 100.0 if have15 else 0.0

    r_btc = await f15(leader_btc)
    r_eth = await f15(leader_eth)
    min_leader = None
    if r_btc is not None and r_eth is not None:
        min_leader = min(r_btc, r_eth)
    elif r_btc is not None:
        min_leader = r_btc
    elif r_eth is not None:
        min_leader = r_eth

    return {
        "name": name,
        "ret_15m": vw15,
        "ret_60m": vw60,
        "breadth_15m": breadth,
        "ret_15m_BTC": r_btc,
        "ret_15m_ETH": r_eth,
        "min_leader_15m": min_leader,
        "weight_total": total_w,
    }


async def compute_market_stress_global() -> dict:
    """
    Agrega BINANCE + OKX + KRAKEN ponderando por volumen quote.
    Devuelve dict con score, level y componentes por exchange + agregados.
    """
    exchanges = []

    # BINANCE
    try:
        b_top, b_tot = await fetch_binance_top20_quote()
        if b_top:
            comp = await _compute_exchange_components("BINANCE", b_top,
                                                      BINANCE_QUOTE)
            comp["ex_total_qv"] = b_tot
            exchanges.append(comp)
    except Exception as e:
        log.warning(f"Stress BINANCE error: {e}")

    # OKX
    try:
        o_top, o_tot = await fetch_okx_top20_quote()
        if o_top:
            comp = await _compute_exchange_components("OKX", o_top, OKX_QUOTE)
            comp["ex_total_qv"] = o_tot
            exchanges.append(comp)
    except Exception as e:
        log.warning(f"Stress OKX error: {e}")

    # KRAKEN
    try:
        k_top, k_tot = await fetch_kraken_top20_quote()
        if k_top:
            comp = await _compute_exchange_components("KRAKEN", k_top,
                                                      KRAKEN_QUOTE)
            comp["ex_total_qv"] = k_tot
            exchanges.append(comp)
    except Exception as e:
        log.warning(f"Stress KRAKEN error: {e}")

    if not exchanges:
        return {"score": 0.0, "level": "NONE", "components": {}, "ex": []}

    tot_qv = sum(e["ex_total_qv"] for e in exchanges) or 1.0

    agg = {
        "ret_15m":
        sum(e["ret_15m"] * e["ex_total_qv"] for e in exchanges) / tot_qv,
        "ret_60m":
        sum(e["ret_60m"] * e["ex_total_qv"] for e in exchanges) / tot_qv,
        "breadth_15m":
        sum(e["breadth_15m"] * e["ex_total_qv"] for e in exchanges) / tot_qv,
    }

    leaders = [
        e["min_leader_15m"] for e in exchanges
        if isinstance(e.get("min_leader_15m"), (int, float))
    ]
    min_leader = min(leaders) if leaders else None

    # PUNTOS (p1..p4). p5=p6=0 (añadibles si guardas estado 7d)
    p1 = _clip01((-(agg["ret_15m"]) - 0.005) / 0.010) * 30.0  # 30 pts
    p2 = _clip01((-(agg["ret_60m"]) - 0.015) / 0.020) * 30.0  # 30 pts
    p3 = _clip01(((agg["breadth_15m"]) - 60.0) / 40.0) * 25.0  # 25 pts
    p4 = _clip01((-(min_leader or 0.0) - 0.007) / 0.013) * 15.0  # 15 pts

    score = p1 + p2 + p3 + p4
    level = _level_from_score(score)

    components = {
        "ret_15m": agg["ret_15m"],
        "ret_60m": agg["ret_60m"],
        "breadth_<=-1%_15m": agg["breadth_15m"],
        "min_leader_15m": min_leader,
        "puntos": {
            "p1": round(p1, 1),
            "p2": round(p2, 1),
            "p3": round(p3, 1),
            "p4": round(p4, 1),
            "p5": 0,
            "p6": 0
        }
    }
    return {
        "score": round(score, 1),
        "level": level,
        "components": components,
        "ex": exchanges
    }


def build_stress_msg(result: dict) -> str:
    s = result["score"]
    lvl = result["level"]
    c = result["components"]

    emoji = {"YELLOW": "🟡", "ORANGE": "🟠", "RED": "🔴"}.get(lvl, "ℹ️")
    lines = []
    lines.append(f"{emoji} *Market Stress* {emoji}")
    lines.append(f"*Nivel:* {lvl}  |  *Score:* {s}/100")
    lines.append(
        f"*Índice 15m:* {_fmt_pct(c.get('ret_15m'))}  |  *Índice 60m:* {_fmt_pct(c.get('ret_60m'))}"
    )

    b = c.get("breadth_<=-1%_15m")
    if isinstance(b, (int, float)):
        lines.append(f"*Breadth 15m ≤−1%:* {b:.0f}% ")
    else:
        lines.append("*Breadth 15m ≤−1%:* —")

    ml = c.get("min_leader_15m")
    lines.append(f"*Leaders 15m (min BTC/ETH):* {_fmt_pct(ml)}")
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)


def build_recovery_msg(prev_level: str, result: dict) -> str:
    s = result["score"]
    lvl = result["level"]
    c = result["components"]

    lines = []
    lines.append("🟢 *Market Recovery* 🟢")
    lines.append(f"*De:* {prev_level}  →  *A:* {lvl}  |  *Score:* {s}/100")
    lines.append(
        f"*Índice 15m:* {_fmt_pct(c.get('ret_15m'))}  |  *Índice 60m:* {_fmt_pct(c.get('ret_60m'))}"
    )

    b = c.get("breadth_<=-1%_15m")
    if isinstance(b, (int, float)):
        lines.append(f"*Breadth 15m ≤−1%:* {b:.0f}% ")
    else:
        lines.append("*Breadth 15m ≤−1%:* —")

    ml = c.get("min_leader_15m")
    lines.append(f"*Leaders 15m (min BTC/ETH):* {_fmt_pct(ml)}")
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)


# --- comando /alerta (on-demand)
async def alerta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.chat.send_action(action=ChatAction.TYPING)
        res = await compute_market_stress_global()
        await msg.reply_text(build_stress_msg(res), parse_mode="Markdown")
    except Exception as e:
        log.exception(f"/alerta error: {e}")
        await msg.reply_text("❌ No se pudo calcular el Market Stress ahora.")


# ---------- handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(
        "👋 Bienvenido a Crypto Data Hub ES\n\n"
        "Comandos disponibles:\n"
        "/metodo - Ver metodología\n"
        "/nowprecios [exchange] - Precios y volumen (demo/real)\n"
        "/nowindicadores [exchange] - Indicadores (básicos)\n"
        "/nowindicadorespro [exchange] [top_n] - Indicadores avanzados (PRO)\n"
        "/status - Estado del keep-alive\n")


async def metodo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(
        "📐 Metodología (MVP)\n\n"
        "• Top 20 por volumen USD en cada exchange (Binance, Bybit, Coinbase, OKX, Kraken)\n"
        "• Variación % = precio vs 24h\n"
        "• Dominancia = vol token / vol total exchange\n"
        "• Indicadores: RSI14, ATR%, EMA20/50, VWAP dev%, OB imbalance\n"
        "• Fear & Greed: índice compuesto\n\n"
        "NFA | DYOR")


async def now_precios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return

    try:
        await msg.chat.send_action(action=ChatAction.TYPING)

        if not context.args:
            # Mostrar TODOS (Binance + OKX + Kraken)
            lines = []
            try:
                top20, total_qv = await fetch_binance_top20_quote()
                if top20:
                    lines.append(format_binance_top_message(top20, total_qv))
            except Exception as e:
                log.warning(f"Error Binance sin args: {e}")

            try:
                top20, total_qv = await fetch_okx_top20_quote()
                if top20:
                    lines.append(format_okx_top_message(top20, total_qv))
            except Exception as e:
                log.warning(f"Error OKX sin args: {e}")

            try:
                top20, total_qv = await fetch_kraken_top20_quote()
                if top20:
                    lines.append(format_kraken_top_message(top20, total_qv))
            except Exception as e:
                log.warning(f"Error Kraken sin args: {e}")

            if lines:
                for block in lines:
                    await send_markdown_chunks(msg.chat, block)
            else:
                await msg.reply_text("❌ No se pudieron obtener datos.")
            return

        ex = context.args[0].lower()

        if ex == "binance":
            top20, total_qv = await fetch_binance_top20_quote()
            if not top20:
                await msg.reply_text(
                    "No se pudo obtener datos de Binance en este momento.")
                return
            await msg.reply_text(format_binance_top_message(top20, total_qv),
                                 parse_mode="Markdown")
            return

        if ex == "okx":
            top20, total_qv = await fetch_okx_top20_quote()
            if not top20:
                await msg.reply_text(
                    "No se pudo obtener datos de OKX en este momento.")
                return
            await msg.reply_text(format_okx_top_message(top20, total_qv),
                                 parse_mode="Markdown")
            return

        if ex == "kraken":
            top20, total_qv = await fetch_kraken_top20_quote()
            if not top20:
                await msg.reply_text(
                    "No se pudo obtener datos de Kraken en este momento.")
                return
            await msg.reply_text(format_kraken_top_message(top20, total_qv),
                                 parse_mode="Markdown")
            return

        await msg.reply_text(
            "⚠️ Exchanges soportados: `binance`, `okx`, `kraken`.\n"
            "Ejemplos: `/nowprecios binance`, `/nowprecios okx`, `/nowprecios kraken`",
            parse_mode="Markdown",
        )

    except Exception as e:
        log.exception(f"now_precios error: {e}")
        if DEBUG_ERRORS:
            await msg.reply_text(f"❌ Error: {e}")
        else:
            await msg.reply_text(
                "❌ Error recuperando datos. Inténtalo de nuevo en unos minutos."
            )

async def now_indicadores(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.chat.send_action(action=ChatAction.TYPING)

        # Sintaxis: /nowindicadores [exchange] [top_n]
        # - exchange: binance | okx | kraken
        # - top_n: 5..20 (opcional; por defecto 20)
        args = list(context.args or [])
        ex = args[0].lower() if args else None

        top_n = 20
        if len(args) >= 2 and str(args[1]).isdigit():
            top_n = max(5, min(20, int(args[1])))

        parts = []

        async def _do_exchange(name: str):
            if name == "binance":
                top20, _ = await fetch_binance_top20_quote()
                if top20:
                    with_ind = await attach_indicators_binance(top20, interval="1h")
                    parts.append(format_indicators_visual_en("BINANCE", BINANCE_QUOTE, with_ind, top_n=top_n))

            elif name == "okx":
                top20, _ = await fetch_okx_top20_quote()
                if top20:
                    with_ind = await attach_indicators_okx(top20, bar="1H")
                    parts.append(format_indicators_visual_en("OKX", OKX_QUOTE, with_ind, top_n=top_n))

            elif name == "kraken":
                top20, _ = await fetch_kraken_top20_quote()
                if top20:
                    with_ind = await attach_indicators_kraken(top20, interval=60)
                    parts.append(format_indicators_visual_en("KRAKEN", KRAKEN_QUOTE, with_ind, top_n=top_n))


        if ex in {"binance", "okx", "kraken"}:
            await _do_exchange(ex)
        else:
            # Sin args → muestra varios (como hacías antes)
            await _do_exchange("binance")
            await _do_exchange("okx")
            await _do_exchange("kraken")

        if parts:
            # envía cada bloque con troceo seguro
            for block in parts:
                await send_markdown_chunks(msg.chat, block)
        else:
            await msg.reply_text("❌ No se pudieron calcular indicadores.")

    except Exception as e:
        log.exception(f"now_indicadores (unificado) error: {e}")
        if DEBUG_ERRORS:
            await msg.reply_text(f"❌ Error: {e}")
        else:
            await msg.reply_text("❌ Error calculando indicadores. Inténtalo en unos minutos.")

async def now_indicadorespro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Mantener compat con usuarios antiguos: delega al unificado
    await now_indicadores(update, context)

async def pingchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ts = datetime.now(TZ).strftime("%d/%m %H:%M")
    await context.bot.send_message(chat_id=CHANNEL_ID,
                                   text=f"✅ Ping de prueba al canal ({ts})")


# ---------- helpers ----------
async def build_nowprecios_blocks() -> list[str]:
    parts: list[str] = []

    try:
        top20, total_qv = await fetch_binance_top20_quote()
        if top20:
            parts.append(format_binance_top_message(top20, total_qv))
    except Exception as e:
        log.exception(f"Scheduler Binance error: {e}")

    try:
        top20, total_qv = await fetch_okx_top20_quote()
        if top20:
            parts.append(format_okx_top_message(top20, total_qv))
    except Exception as e:
        log.exception(f"Scheduler OKX error: {e}")

    try:
        top20, total_qv = await fetch_kraken_top20_quote()
        if top20:
            parts.append(format_kraken_top_message(top20, total_qv))
    except Exception as e:
        log.exception(f"Scheduler Kraken error: {e}")

    return parts

async def fng(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.chat.send_action(action=ChatAction.TYPING)
        d = await fetch_fng_index()
        await msg.reply_text(format_fng_message(d), parse_mode="Markdown")
    except Exception as e:
        log.exception(f"/fng error: {e}")
        await msg.reply_text(
            "❌ No se pudo obtener el Fear & Greed ahora mismo.")


# ---------- comandos de suscripción ----------
async def substress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    level = args[0].upper() if args else "ORANGE"
    if level not in {"YELLOW", "ORANGE", "RED"}:
        await msg.reply_text("Niveles válidos: YELLOW | ORANGE | RED")
        return
    await add_sub(msg.chat_id, "stress", f"level>={level}", 1800)
    await msg.reply_text(
        f"🔔 Suscrito a Market Stress ≥ {level} (cooldown 30 min)")


async def substressscore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    if not args:
        await msg.reply_text("Uso: /substressscore <entero 0..100>")
        return
    try:
        thr = int(args[0])
    except Exception:
        await msg.reply_text("Uso: /substressscore <entero 0..100>")
        return
    await add_sub(msg.chat_id, "stress", f"score>={thr}", 1800)
    await msg.reply_text(
        f"🔔 Suscrito a Market Stress score ≥ {thr} (cooldown 30 min)")


async def subfng(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    if len(args) < 2:
        await msg.reply_text(
            "Uso: /subfng <umbral> <up|down>\nEj: /subfng 80 up  |  /subfng 20 down"
        )
        return
    try:
        thr = int(args[0])
    except Exception:
        await msg.reply_text("El umbral debe ser un número entero.")
        return
    direction = args[1].lower()
    if direction not in {"up", "down"}:
        await msg.reply_text("Dirección debe ser up|down")
        return
    op = ">=" if direction == "up" else "<="
    await add_sub(msg.chat_id, "fng", f"value{op}{thr}", 10800)  # 3h
    await msg.reply_text(f"🔔 Suscrito a F&G: value {op} {thr} (cooldown 3h)")


def _parse_op_value(s: str) -> tuple[str, float] | None:
    if not isinstance(s, str):
        return None
    s = s.strip().replace(",", ".")
    if s.startswith(">="):
        try:
            return ">=", float(s[2:])
        except ValueError:
            return None

    if s.startswith("<="):
        try:
            return "<=", float(s[2:])
        except ValueError:
            return None
    return None


async def subprice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    if len(args) != 3:
        await msg.reply_text(
            "Uso: /subprice <binance|okx|kraken> <SYMBOL> >=<valor>\nEj: /subprice binance BTCUSDC >=65000"
        )
        return
    ex, sym, raw = args[0].upper(), args[1].upper(), args[2]
    opv = _parse_op_value(raw)
    if ex not in {"BINANCE", "OKX", "KRAKEN"} or not opv:
        await msg.reply_text(
            "Uso: /subprice <binance|okx|kraken> <SYMBOL> >=<valor>")
        return
    op, thr = opv
    rule = f"{ex}:{sym}{op}{thr}"
    await add_sub(msg.chat_id, "price", rule, 900)
    await msg.reply_text(f"🔔 Precio {ex} {sym} {op} {thr}. (cooldown 15 min)")


async def subpct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    if len(args) != 3:
        await msg.reply_text(
            "Uso: /subpct <binance|okx|kraken> <SYMBOL> >=<pct>\nEj: /subpct okx BTCUSDC <=-4"
        )
        return
    ex, sym, raw = args[0].upper(), args[1].upper(), args[2]
    opv = _parse_op_value(raw)
    if ex not in {"BINANCE", "OKX", "KRAKEN"} or not opv:
        await msg.reply_text(
            "Uso: /subpct <binance|okx|kraken> <SYMBOL> >=<pct>")
        return
    op, thr = opv
    rule = f"{ex}:{sym}{op}{thr}"
    await add_sub(msg.chat_id, "pct", rule, 900)
    await msg.reply_text(f"🔔 %24h {ex} {sym} {op} {thr}%. (cooldown 15 min)")


async def subvol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    if len(args) != 3:
        await msg.reply_text(
            "Uso: /subvol <binance|okx|kraken> <SYMBOL> >=<vol>\nEj: /subvol binance BTCUSDC >=25000000"
        )
        return
    ex, sym, raw = args[0].upper(), args[1].upper(), args[2]
    opv = _parse_op_value(raw)
    if ex not in {"BINANCE", "OKX", "KRAKEN"} or not opv:
        await msg.reply_text(
            "Uso: /subvol <binance|okx|kraken> <SYMBOL> >=<vol>")
        return
    op, thr = opv
    rule = f"{ex}:{sym}{op}{thr}"
    await add_sub(msg.chat_id, "vol", rule, 1800)
    await msg.reply_text(
        f"🔔 Volumen 24h {ex} {sym} {op} {thr}. (cooldown 30 min)")


async def mysubs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    rows = await list_subs(msg.chat_id)
    if not rows:
        await msg.reply_text("No tienes suscripciones activas.")
        return
    lines = [f"• #{r[0]} {r[1]} — {r[2]} (cd={r[3]}s)" for r in rows]
    await msg.reply_text("🔔 Tus suscripciones:\n" + "\n".join(lines))


async def unsub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    args = list(context.args or [])
    kind = (args[0].lower() if args else None)
    if kind not in {None, "stress", "fng", "price", "pct", "vol"}:
        await msg.reply_text("Uso: /unsub [stress|fng|price|pct|vol]")
        return
    await del_sub(msg.chat_id, kind)
    await msg.reply_text("✅ Suscripciones eliminadas" +
                         (f" ({kind})" if kind else ""))
# ---------- market sessions (NYSE/LSE/TSE) ----------
ENABLE_MARKET_NOTIFS = os.getenv("ENABLE_MARKET_NOTIFS", "1") == "1"

MARKET_LABELS = {
    "NYSE": "New York Stock Exchange",
    "LSE":  "London Stock Exchange",
    "TSE":  "Tokyo Stock Exchange",
}

def _fmt_session_times(tz_exchange: str, hour: int, minute: int) -> tuple[str, str]:
    """
    Devuelve (hora_en_bolsa, hora_en_Madrid) formateadas para el *día actual*,
    respetando DST de ambos husos.
    """
    tz_ex = pytz.timezone(tz_exchange)
    tz_mad = TZ  # Europe/Madrid ya definido arriba

    now_ex = datetime.now(tz_ex)
    # construimos el datetime 'de hoy' en la zona de la bolsa
    dt_ex = now_ex.replace(hour=hour, minute=minute, second=0, microsecond=0)
    # si por cualquier razón el job se ejecuta tras el minuto exacto, seguimos mostrando el tiempo nominal
    dt_mad = dt_ex.astimezone(tz_mad)

    s_ex = dt_ex.strftime("%H:%M %Z")
    s_mad = dt_mad.strftime("%H:%M %Z")
    return s_ex, s_mad

async def notify_market_event(app: Application, market: str, kind: str,
                              tz_exchange: str, hour: int, minute: int):
    """
    kind: 'open' | 'close'
    """
    label = MARKET_LABELS.get(market, market)
    s_ex, s_mad = _fmt_session_times(tz_exchange, hour, minute)
    emoji = "🟢" if kind == "open" else "🔴"
    kind_es = "Apertura" if kind == "open" else "Cierre"

    text = (
        f"{emoji} *{kind_es} {market}* ({label})\n\n"
        f"🕒 Hora local: *{s_ex}*  |  Madrid: *{s_mad}*\n"
        f"📣 Recordatorio automático"
    )
    try:
        await app.bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        log.exception(f"notify_market_event {market}/{kind} error: {e}")


# ---------- jobs ----------
async def post_precios_volumen(app: Application):
    try:
        blocks = await build_nowprecios_blocks()
        if not blocks:
            await app.bot.send_message(
                chat_id=CHANNEL_ID,
                text="❌ No se pudieron obtener datos en este momento."
            )
            return

        # Envía cada bloque por separado (no se trocean las fences)
        for block in blocks:
            await send_markdown_chunks_bot(app.bot, CHANNEL_ID, block)

        log.info("Enviado precios/volumen a canal (Binance + OKX + Kraken)")
    except Exception as e:
        log.exception(f"Error enviando precios/volumen: {e}")
        if DEBUG_ERRORS:
            await app.bot.send_message(chat_id=CHANNEL_ID, text=f"❌ Error: {e}")

async def post_indicadores(app: Application):
    try:
        parts = []
        try:
            top20, _total = await fetch_binance_top20_quote()
            if top20:
                top20 = await attach_indicators_binance(top20, interval="1h")
                parts.append(format_indicators_visual_en("BINANCE", BINANCE_QUOTE, top20, top_n=20))
        except Exception as e:
            log.warning(f"post_indicadores Binance error: {e}")

        try:
            top20, _total = await fetch_okx_top20_quote()
            if top20:
                top20 = await attach_indicators_okx(top20, bar="1H")
                parts.append(format_indicators_visual_en("OKX", OKX_QUOTE, top20, top_n=20))
        except Exception as e:
            log.warning(f"post_indicadores OKX error: {e}")

        try:
            top20, _total = await fetch_kraken_top20_quote()
            if top20:
                top20 = await attach_indicators_kraken(top20, interval=60)
                parts.append(format_indicators_visual_en("KRAKEN", KRAKEN_QUOTE, top20, top_n=20))
        except Exception as e:
            log.warning(f"post_indicadores Kraken error: {e}")


        if parts:
            for block in parts:
                await send_markdown_chunks_bot(app.bot, CHANNEL_ID, block)
            log.info("Sent indicators (EN, visual) to channel (Binance+OKX)")
        else:
            await app.bot.send_message(
                chat_id=CHANNEL_ID,
                text="❌ Could not compute indicators right now.")
    except Exception as e:
        log.exception(f"Error sending indicators: {e}")
        if DEBUG_ERRORS:
            await app.bot.send_message(chat_id=CHANNEL_ID,
                                       text=f"❌ Error: {e}")

async def _post_init(app: Application):
    await db_init()
    await app.bot.set_my_commands([
        BotCommand("start", "Mostrar ayuda"),
        BotCommand("metodo", "Ver metodología"),
        BotCommand("nowprecios", "Top 20 por volumen"),
        BotCommand("nowindicadores", "Indicadores técnicos (unificado) [ex] [top_n]"),
        BotCommand("nowindicadorespro", "Alias de /nowindicadores"),
        BotCommand("pingchannel", "Mensaje de prueba al canal"),
        BotCommand("status", "Estado/último ping web"),
        BotCommand("alerta", "Detector Market Stress"),
        BotCommand("fng", "Fear & Greed Index"),
        # suscripciones
        BotCommand("substress", "Aviso stress ≥ nivel"),
        BotCommand("substressscore", "Aviso stress ≥ score"),
        BotCommand("subfng", "Aviso F&G cruza umbral"),
        BotCommand("mysubs", "Listar mis suscripciones"),
        BotCommand("unsub", "Borrar suscripciones"),
        # nuevos (coinciden con tus handlers reales)
        BotCommand("subprice", "Aviso precio ≥ o ≤ valor"),
        BotCommand("subpct", "Aviso variación 24h ≥ o ≤ %"),
        BotCommand("subvol", "Aviso volumen 24h ≥ o ≤"),
    ])


async def notifications_job(app: Application):
    now_ts = int(time.time())

    # 1) Calcula señales una sola vez
    stress = None
    fng_data = None
    try:
        stress = await compute_market_stress_global()  # {score, level, ...}
    except Exception as e:
        log.warning(f"notif stress error: {e}")
    try:
        fng_data = await fetch_fng_index(
        )  # {'value', 'classification', 'time'}
    except Exception as e:
        log.warning(f"notif fng error: {e}")

    # 2) Recorre subs y decide
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM subs")
        subs = await cur.fetchall()

    rank = {"NONE": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}

    for s in subs:
        try:
            if not await _should_notify(s, now_ts):
                continue

            kind = s["kind"]
            rule = s["rule"]

            must_send = False
            text = None

            if kind == "stress" and stress:
                lvl = stress.get("level", "NONE")
                score = float(stress.get("score", 0.0))

                if rule.startswith("level>="):
                    want = rule.split(">=")[1]
                    if rank.get(lvl, 0) >= rank.get(want, 0):
                        must_send = True
                        text = build_stress_msg(stress)
                elif rule.startswith("score>="):
                    thr = float(rule.split(">=")[1])
                    if score >= thr:
                        must_send = True
                        text = build_stress_msg(stress)

            elif kind == "fng" and fng_data:
                val = int(fng_data.get("value", 0))
                if ">=" in rule:
                    thr = int(rule.split(">=")[1])
                    if val >= thr:
                        must_send = True
                        text = format_fng_message(fng_data)
                elif "<=" in rule:
                    thr = int(rule.split("<=")[1])
                    if val <= thr:
                        must_send = True
                        text = format_fng_message(fng_data)

            elif kind in {"price", "pct", "vol"}:
                # Regla esperada: EX:SYMBOL>=THR  o EX:SYMBOL<=THR
                ex_sym = None
                sym = None
                op = None
                thr = None
                try:
                    ex_sym_part, cond = rule.split(":")
                    m = re.match(r"^([^<>]+)(>=|<=)(-?\d+(?:\.\d+)?)$",
                                 cond.strip())
                    if not m:
                        raise ValueError("Regla inválida")
                    sym, op, thr = m.group(1).upper(), m.group(2), float(
                        m.group(3))
                    ex_sym = ex_sym_part.upper()
                    tkr = await get_ticker(ex_sym, sym)
                except Exception:
                    tkr = None

                if tkr is not None and op in {">=", "<="} and thr is not None:
                    val = {
                        "price": tkr["lastPrice"],
                        "pct": tkr["priceChangePercent"],
                        "vol": tkr["quoteVolume"],
                    }[kind]
                    ok = (val >= thr) if op == ">=" else (val <= thr)
                    if ok:
                        label = {
                            "price": "Precio",
                            "pct": "Variación 24h",
                            "vol": "Volumen 24h"
                        }[kind]
                        unit = {"price": "", "pct": "%", "vol": ""}[kind]
                        text = (
                            "🔔 *Alerta personalizada*\n\n"
                            f"*{label}:* {ex_sym} {sym} = {val:.4f}{unit}  ({op} {thr}{unit})"
                        )
                        await app.bot.send_message(chat_id=s["chat_id"],
                                                   text=text,
                                                   parse_mode="Markdown")
                        await _touch_notified(s["id"], now_ts)

            if must_send and text:
                await app.bot.send_message(
                    chat_id=s["chat_id"],
                    text="🔔 *Alerta personalizada*\n\n" + text,
                    parse_mode="Markdown")
                await _touch_notified(s["id"], now_ts)

        except Exception as e:
            log.warning(f"send notif error sub#{s['id']}: {e}")


async def post_alerta(app: Application):
    global STRESS_LAST_LEVEL, STRESS_LAST_SCORE
    try:
        res = await compute_market_stress_global()
        lvl = res["level"]
        score = float(res["score"])

        rank = {"NONE": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}
        prev_lvl = STRESS_LAST_LEVEL
        prev_score = float(STRESS_LAST_SCORE or 0.0)

        must_post_fall = False  # avisos de caída
        must_post_reco = False  # avisos de recuperación

        # --- Reglas de CAÍDA (como las originales) ---
        if lvl == "NONE":
            must_post_fall = False
        elif prev_lvl == "NONE" and lvl in {"YELLOW", "ORANGE", "RED"}:
            must_post_fall = True
        elif lvl in {"YELLOW", "ORANGE"
                     } and lvl != prev_lvl and rank[lvl] > rank[prev_lvl]:
            must_post_fall = True
        elif lvl == "RED" and (prev_lvl != "RED" or
                               (score - prev_score) >= STRESS_MIN_UP):
            must_post_fall = True

        # --- Reglas de RECUPERACIÓN (si está activado) ---
        if ALERT_RECOVERY:
            if rank[lvl] < rank[prev_lvl] and (prev_score -
                                               score) >= STRESS_MIN_DOWN:
                must_post_reco = True
            elif lvl in {"RED", "ORANGE"} and prev_lvl == lvl and (
                    prev_score - score) >= (STRESS_MIN_DOWN * 2):
                must_post_reco = True
            elif lvl == "NONE" and prev_lvl in {
                    "YELLOW", "ORANGE", "RED"
            } and (prev_score - score) >= STRESS_MIN_DOWN:
                must_post_reco = True

        if must_post_fall:
            await app.bot.send_message(chat_id=CHANNEL_ID,
                                       text=build_stress_msg(res),
                                       parse_mode="Markdown")
        elif must_post_reco:
            await app.bot.send_message(chat_id=CHANNEL_ID,
                                       text=build_recovery_msg(prev_lvl, res),
                                       parse_mode="Markdown")

        STRESS_LAST_LEVEL = lvl
        STRESS_LAST_SCORE = score
    except Exception as e:
        log.exception(f"post_alerta error: {e}")


# ---------- main ----------
def main():
    application = Application.builder().token(TOKEN).post_init(
        _post_init).build()

    # comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("metodo", metodo))
    application.add_handler(CommandHandler("nowprecios", now_precios))
    application.add_handler(CommandHandler("nowindicadores", now_indicadores))
    application.add_handler(CommandHandler("pingchannel", pingchannel))
    application.add_handler(
        CommandHandler("nowindicadorespro", now_indicadorespro))
    application.add_handler(CommandHandler("feedback", feedback))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("alerta", alerta))
    application.add_handler(CommandHandler("fng", fng))
    # si quieres alias en español:
    application.add_handler(CommandHandler("miedo", fng))
    application.add_handler(CommandHandler("miedoycodicia", fng))
    application.add_handler(CommandHandler("substress", substress))
    application.add_handler(CommandHandler("substressscore", substressscore))
    application.add_handler(CommandHandler("subfng", subfng))
    application.add_handler(CommandHandler("mysubs", mysubs))
    application.add_handler(CommandHandler("unsub", unsub))
    application.add_handler(CommandHandler("subprice", subprice))
    application.add_handler(CommandHandler("subpct", subpct))
    application.add_handler(CommandHandler("subvol", subvol))

    # scheduler
    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(post_precios_volumen,
                      CronTrigger(minute="0"),
                      args=[application])
    scheduler.add_job(post_indicadores,
                      CronTrigger(minute="30"),
                      args=[application])
    scheduler.start()
    scheduler.add_job(post_alerta,
                      CronTrigger(minute="*/5"),
                      args=[application])
    scheduler.add_job(notifications_job,
                      CronTrigger(minute="*/5"),
                      args=[application])
    # --- Market sessions (NYSE/LSE/TSE) ---
    if ENABLE_MARKET_NOTIFS:
        # NYSE — America/New_York
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=9, minute=30, timezone="America/New_York"),
            args=[application, "NYSE", "open", "America/New_York", 9, 30],
        )
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=16, minute=0, timezone="America/New_York"),
            args=[application, "NYSE", "close", "America/New_York", 16, 0],
        )

        # LSE — Europe/London
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=8, minute=0, timezone="Europe/London"),
            args=[application, "LSE", "open", "Europe/London", 8, 0],
        )
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=16, minute=30, timezone="Europe/London"),
            args=[application, "LSE", "close", "Europe/London", 16, 30],
        )

        # TSE (Tokio) — Asia/Tokyo
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=9, minute=0, timezone="Asia/Tokyo"),
            args=[application, "TSE", "open", "Asia/Tokyo", 9, 0],
        )
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=15, minute=0, timezone="Asia/Tokyo"),
            args=[application, "TSE", "close", "Asia/Tokyo", 15, 0],
        )

        # TSE - Reapertura post-lunch (12:30 JST)
        scheduler.add_job(
            notify_market_event,
            CronTrigger(hour=12, minute=30, timezone="Asia/Tokyo"),
            args=[application, "TSE", "open", "Asia/Tokyo", 12, 30],
        )


    # keep-alive web
    Thread(target=run_web, daemon=True).start()

    logging.info("Bot iniciado.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
