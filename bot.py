# ---------- imports ----------
import os
import logging
from datetime import datetime
from threading import Thread
from typing import cast

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

# ---------- config ----------
TZ = pytz.timezone("Europe/Madrid")
TOKEN: str = cast(str, os.getenv("TELEGRAM_TOKEN") or "")
CHANNEL_ID: str = cast(str, os.getenv("CHANNEL_ID") or "")

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
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=HEADERS) as client:
        for base in BINANCE_BASES:
            url = f"{base}{path}"
            try:
                r = await client.get(url)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases Binance fallaron: " + " | ".join(f"{b}: {e}" for b, e in errors))

async def fetch_binance_klines(symbol: str, interval: str = "1h", limit: int = 120):
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

async def attach_indicators_binance(top20: list[dict], interval: str = "1h") -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. BTCUSDC
        try:
            ohlcv = await fetch_binance_klines(sym, interval=interval, limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"]   = calc_rsi(closes, 14)
            d["ema20"]   = ema(closes, 20)
            d["ema50"]   = ema(closes, 50)
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
            d["obv"]   = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores Binance fallaron para {sym}: {e}")
            for key in ("rsi14","ema20","ema50","atr_pct","macd","macd_sig","macd_hist",
                        "stoch_k","stoch_d","bb_pctb","bb_bw","mfi14","obv","vwap_dev_pct",
                        "adx14","+di","-di"):
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
        return f"{n:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(n)

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
        d = prices[i] - prices[i-1]
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
        low_val  = ohlcv[i]["low"]
        pc       = ohlcv[i-1]["close"]
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
    highs  = [c["high"]  for c in ohlcv]
    lows   = [c["low"]   for c in ohlcv]
    closes = [c["close"] for c in ohlcv]

    k_vals: list[float] = []
    for i in range(k_period - 1, len(closes)):
        window_high = max(highs[i - k_period + 1:i + 1])
        window_low  = min(lows[i - k_period + 1:i + 1])
        if window_high == window_low:
            k_vals.append(50.0)
        else:
            k_vals.append((closes[i] - window_low) / (window_high - window_low) * 100.0)

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
    var = sum((c - mean) ** 2 for c in closes[-period:]) / period
    sd = var ** 0.5
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
        high_val  = ohlcv[i]["high"]
        low_val   = ohlcv[i]["low"]
        close_val = ohlcv[i]["close"]
        vol       = ohlcv[i]["volume"]
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
        if ohlcv[i]["close"] > ohlcv[i-1]["close"]:
            val += ohlcv[i]["volume"]
        elif ohlcv[i]["close"] < ohlcv[i-1]["close"]:
            val -= ohlcv[i]["volume"]
    return val

# VWAP (desviación % sobre ventana)
def vwap_dev_pct(ohlcv: list[dict], period=20):
    if len(ohlcv) < period:
        return None
    win = ohlcv[-period:]
    num = sum(((c["high"] + c["low"] + c["close"]) / 3.0) * c["volume"] for c in win)
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
        ph = ohlcv[i-1]["high"]
        pl = ohlcv[i-1]["low"]
        pc = ohlcv[i-1]["close"]

        up_move = hi - ph
        down_move = pl - lo
        plus_dm.append(up_move if up_move > 0 and up_move > down_move else 0.0)
        minus_dm.append(down_move if down_move > 0 and down_move > up_move else 0.0)
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

def format_binance_top_message(top20, total_quote_vol):
    quote = BINANCE_QUOTE
    lines = []
    lines.append(f"📊 Precios & Volumen — BINANCE ({quote})")
    lines.append(f"Volumen total 24h ({quote}): **{fmt_num(total_quote_vol, 0)}**")
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
        lines.append(f"{i:>2}. {sym} · {price_s} · {vol_s} {quote} · {chg_s} · {dom_s}")
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

# ---------- data sources: BYBIT ----------
# 1) Intento directo a Bybit con headers estilo navegador + varios dominios
BYBIT_BASES = [
    "https://api.bybitglobal.com",
    "https://api.bybit.com",
    "https://api.bytick.com",
]
BYBIT_HEADERS = {
    # UA realista de navegador
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.bybit.com",
    "Referer": "https://www.bybit.com/",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

async def _bybit_get(path: str, params: dict | None = None, timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    errors = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=BYBIT_HEADERS) as client:
        for base in BYBIT_BASES:
            try:
                r = await client.get(base + path, params=params or {})
                r.raise_for_status()
                j = r.json()
                # algunos endpoints no traen retCode; si lo traen, 0 = OK
                if "retCode" in j and j.get("retCode") != 0:
                    errors.append((base, f"retCode={j.get('retCode')} {j.get('retMsg')}"))
                    continue
                return j
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases Bybit fallaron: " + " | ".join(f"{b}: {e}" for b, e in errors))

# 2) Fallback: CoinGecko → /exchanges/bybit/tickers (filtramos target=USDC)
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}

async def _coingecko_get(path: str, params: dict | None = None, timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=COINGECKO_HEADERS) as client:
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
            lp  = float(r.get("lastPrice", "0"))
            pcp = float(r.get("price24hPcnt", "0")) * 100.0   # 0.0123 => 1.23%
            qv  = float(r.get("turnover24h", "0"))            # volumen en quote
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
        j = await _coingecko_get("/exchanges/bybit/tickers", params={
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
        top20, total, used_fallback_direct = await _fetch_bybit_top20_quote_direct()
        return top20, total, used_fallback_direct, ""
    except Exception as e:
        log.warning(f"Bybit directo falló, usando fallback CoinGecko: {e}")
        return await _fetch_bybit_top20_quote_fallback()

def format_bybit_top_message(top20, total_quote_vol, used_fallback=False, note: str = ""):
    quote = BYBIT_QUOTE  # "USDC"
    lines = []
    lines.append(f"📊 Precios & Volumen — BYBIT ({quote})")
    lines.append(f"Volumen total 24h ({quote}): **{fmt_num(total_quote_vol, 0)}**")
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
        lines.append(f"{i:>2}. {sym} · {price_s} · {vol_s} {quote} · {chg_s} · {dom_s}")
    if used_fallback:
        lines.append("\nℹ️ Fuente: CoinGecko (Bybit bloqueado 403). %24h no disponible por par.")
    # (sin nota extra; solo USDC)
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

# ---------- data sources: OKX ----------
OKX_BASES = [
    "https://www.okx.com",
    "https://aws.okx.com",  # mirror
]
OKX_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}

async def _okx_get(path: str, params: dict | None = None, timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    errors = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=OKX_HEADERS) as client:
        for base in OKX_BASES:
            try:
                r = await client.get(base + path, params=params or {})
                r.raise_for_status()
                j = r.json()
                # OKX OK => {"code":"0","msg":"","data":[...]}
                if j.get("code") not in ("0", 0, None):
                    errors.append((base, f"code={j.get('code')} {j.get('msg')}"))
                    continue
                return j
            except Exception as e:
                errors.append((base, repr(e)))
                continue
    raise RuntimeError("Todas las bases OKX fallaron: " + " | ".join(f"{b}: {e}" for b, e in errors))

def _okx_symbol_to_inst(sym_no_dash: str) -> str:
    # convierte "BTCUSDC" -> "BTC-USDC" usando la quote actual
    q = OKX_QUOTE
    base = sym_no_dash[:-len(q)]
    quote = sym_no_dash[-len(q):]
    return f"{base}-{quote}"

async def fetch_okx_candles(inst_id: str, bar: str = "1H", limit: int = 120):
    j = await _okx_get("/api/v5/market/candles", params={"instId": inst_id, "bar": bar, "limit": str(limit)})
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

async def attach_indicators_okx(top20: list[dict], bar: str = "1H") -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. BTCUSDC
        inst = _okx_symbol_to_inst(sym)  # BTC-USDC
        try:
            ohlcv = await fetch_okx_candles(inst, bar=bar, limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"]   = calc_rsi(closes, 14)
            d["ema20"]   = ema(closes, 20)
            d["ema50"]   = ema(closes, 50)
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
            d["obv"]   = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores OKX fallaron para {sym}: {e}")
            for key in ("rsi14","ema20","ema50","atr_pct","macd","macd_sig","macd_hist",
                        "stoch_k","stoch_d","bb_pctb","bb_bw","mfi14","obv","vwap_dev_pct",
                        "adx14","+di","-di"):
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
    tick = await _okx_get("/api/v5/market/tickers", params={"instType": "SPOT"})
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
            "symbol": inst.replace("-", ""),   # estilo BTCUSDC para coherencia
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
    lines = []
    lines.append(f"📊 Precios & Volumen — OKX ({quote})")
    lines.append(f"Volumen total 24h ({quote}): **{fmt_num(total_quote_vol, 0)}**")
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
        lines.append(f"{i:>2}. {sym} · {price_s} · {vol_s} {quote} · {chg_s} · {dom_s}")
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

# ---------- data sources: KRAKEN ----------
KRAKEN_BASE = "https://api.kraken.com"
KRAKEN_HEADERS = {"User-Agent": "crypto-data-bot/0.1 (+telegram)"}

async def _kraken_get(path: str, params: dict | None = None, timeout_sec: float = 12.0):
    timeout = httpx.Timeout(timeout_sec, connect=6.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=KRAKEN_HEADERS) as client:
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
        chunk = valid_altnames[i:i+80]
        tk = await _kraken_get("/0/public/Ticker", params={"pair": ",".join(chunk)})
        trows = (tk.get("result") or {})
        for k, v in trows.items():
            ws = alt_to_ws.get(k, k).upper()       # BTCUSDC, ETHUSDC, ...
            try:
                last = float((v.get("c") or ["0"])[0])
                openp = float(v.get("o") or 0.0)
                vol_base = float((v.get("v") or ["0","0"])[1])  # 24h base
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
    lines = []
    lines.append(f"📊 Precios & Volumen — KRAKEN ({quote})")
    lines.append(f"Volumen total 24h ({quote}): **{fmt_num(total_quote_vol, 0)}**")
    lines.append("")
    lines.append("Top 20 por volumen (24h):")
    for i, d in enumerate(top20, 1):
        sym = d["symbol"]
        price = d["lastPrice"]
        decimals = 8 if price < 0.01 else (6 if price < 1 else (4 if price < 100 else 2))
        price_s = fmt_num(price, decimals)
        vol_s = fmt_num(d["quoteVolume"], 0)
        chg_s = f"{d['priceChangePercent']:+.2f}%"
        dom_s = f"{d['dominance']:.2f}%"
        lines.append(f"{i:>2}. {sym} · {price_s} · {vol_s} {quote} · {chg_s} · {dom_s}")
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

# --- Kraken OHLC (velas) + attach indicadores ---

async def fetch_kraken_candles(pair: str, interval: int = 60, limit: int = 120):
    """
    pair: usa el símbolo que ya guardas como wsname sin barra (ej. 'XBTUSDC').
    interval=60 => 1h
    """
    j = await _kraken_get("/0/public/OHLC", params={"pair": pair, "interval": interval})
    rows = (j.get("result") or {}).get(pair, [])
    ohlcv = []
    for r in rows[-limit:]:
        ohlcv.append({
            "open": float(r[1]),
            "high": float(r[2]),
            "low":  float(r[3]),
            "close":float(r[4]),
            "volume":float(r[6]),
        })
    return ohlcv

async def attach_indicators_kraken(top20: list[dict], interval: int = 60) -> list[dict]:
    out = []
    for d in top20:
        sym = d["symbol"]  # p.ej. XBTUSDC
        try:
            ohlcv = await fetch_kraken_candles(sym, interval=interval, limit=120)
            closes = [c["close"] for c in ohlcv]

            # básicos
            d["rsi14"]   = calc_rsi(closes, 14)
            d["ema20"]   = ema(closes, 20)
            d["ema50"]   = ema(closes, 50)
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
            d["obv"]   = obv(ohlcv)
            d["vwap_dev_pct"] = vwap_dev_pct(ohlcv, 20)

            adx, pdi, ndi = adx_dmi(ohlcv, 14)
            d["adx14"] = adx
            d["+di"] = pdi
            d["-di"] = ndi

        except Exception as e:
            log.warning(f"Indicadores Kraken fallaron para {sym}: {e}")
            for key in ("rsi14","ema20","ema50","atr_pct","macd","macd_sig","macd_hist",
                        "stoch_k","stoch_d","bb_pctb","bb_bw","mfi14","obv","vwap_dev_pct",
                        "adx14","+di","-di"):
                d[key] = None
        out.append(d)
    return out

def format_indicators_table(exchange_name: str, quote: str, rows: list[dict]) -> str:
    lines = []
    lines.append(f"📐 Indicadores — {exchange_name} ({quote})")
    lines.append("(Top 20 por volumen 24h)")
    lines.append("")
    for i, d in enumerate(rows, 1):
        sym = d["symbol"]
        rsi = d.get("rsi14")
        e20 = d.get("ema20")
        e50 = d.get("ema50")
        atrp = d.get("atr_pct")
        bits = []
        bits.append(f"RSI14 {rsi:.1f}" if isinstance(rsi, (int, float)) else "RSI14 -")
        if isinstance(e20, (int, float)) and isinstance(e50, (int, float)):
            bits.append(f"EMA20/50 {fmt_num(e20, 2)}/{fmt_num(e50, 2)}")
        else:
            bits.append("EMA20/50 -/-")
        bits.append(f"ATR% {atrp:.2f}%" if isinstance(atrp, (int, float)) else "ATR% -")
        lines.append(f"{i:>2}. {sym} · " + " · ".join(bits))
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

def format_indicatorspro_table(exchange_name: str, quote: str, rows: list[dict], top_n: int = 20) -> str:
    lines = []
    lines.append(f"🧠 Indicadores PRO — {exchange_name} ({quote})")
    lines.append(f"(Top {min(top_n, len(rows))} por volumen 24h)")
    lines.append("")
    for i, d in enumerate(rows[:top_n], 1):
        sym = d["symbol"]
        rsi = d.get("rsi14")
        macdh = d.get("macd_hist")
        pctb = d.get("bb_pctb")
        mfi_ = d.get("mfi14")
        adx_ = d.get("adx14")
        vwapd = d.get("vwap_dev_pct")
        parts = [
            f"RSI {rsi:.0f}" if isinstance(rsi, (int,float)) else "RSI -",
            f"MACDh {fmt_or_dash(macdh, 4)}",
            f"%B {pctb:.0f}" if isinstance(pctb, (int,float)) else "%B -",
            f"MFI {mfi_:.0f}" if isinstance(mfi_, (int,float)) else "MFI -",
            f"ADX {adx_:.0f}" if isinstance(adx_, (int,float)) else "ADX -",
            f"VWAPΔ {fmt_or_dash(vwapd, 2)}%"
        ]
        lines.append(f"{i:>2}. {sym} · " + " · ".join(parts))
    lines.append("\nNFA | DYOR")
    return "\n".join(lines)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text(
        f"🛰️ Último ping web: {LAST_WEB_PING}\n"
        f"⏰ Zona horaria: {TZ}\n"
        f"📮 Canal: {CHANNEL_ID or '—'}",
        parse_mode="Markdown"
    )

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
        "/status - Estado del keep-alive\n" 
    )

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
        "NFA | DYOR"
    )

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

            await msg.reply_text("\n\n".join(lines) if lines else "❌ No se pudieron obtener datos.", parse_mode="Markdown")
            return

        ex = context.args[0].lower()

        if ex == "binance":
            top20, total_qv = await fetch_binance_top20_quote()
            if not top20:
                await msg.reply_text("No se pudo obtener datos de Binance en este momento.")
                return
            await msg.reply_text(format_binance_top_message(top20, total_qv), parse_mode="Markdown")
            return

        if ex == "okx":
            top20, total_qv = await fetch_okx_top20_quote()
            if not top20:
                await msg.reply_text("No se pudo obtener datos de OKX en este momento.")
                return
            await msg.reply_text(format_okx_top_message(top20, total_qv), parse_mode="Markdown")
            return

        if ex == "kraken":
            top20, total_qv = await fetch_kraken_top20_quote()
            if not top20:
                await msg.reply_text("No se pudo obtener datos de Kraken en este momento.")
                return
            await msg.reply_text(format_kraken_top_message(top20, total_qv), parse_mode="Markdown")
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
            await msg.reply_text("❌ Error recuperando datos. Inténtalo de nuevo en unos minutos.")

async def now_indicadores(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.chat.send_action(action=ChatAction.TYPING)

        if not context.args:
            parts = []
            # BINANCE
            try:
                top20, _total = await fetch_binance_top20_quote()
                if top20:
                    top20 = await attach_indicators_binance(top20, interval="1h")
                    parts.append(format_indicators_table("BINANCE", BINANCE_QUOTE, top20))
            except Exception as e:
                log.warning(f"now_indicadores Binance error: {e}")
            # OKX
            try:
                top20, _total = await fetch_okx_top20_quote()
                if top20:
                    top20 = await attach_indicators_okx(top20, bar="1H")
                    parts.append(format_indicators_table("OKX", OKX_QUOTE, top20))
            except Exception as e:
                log.warning(f"now_indicadores OKX error: {e}")

            # KRAKEN
            try:
                top20, _total = await fetch_kraken_top20_quote()
                if top20:
                    top20 = await attach_indicators_kraken(top20, interval=60)  # 60 min = 1H
                    parts.append(format_indicators_table("KRAKEN", KRAKEN_QUOTE, top20))
            except Exception as e:
                log.warning(f"now_indicadores Kraken error: {e}")

            if parts:
                await msg.reply_text("\n\n".join(parts), parse_mode="Markdown")
            else:
                await msg.reply_text("❌ No se pudieron calcular indicadores.")
            return

        ex = context.args[0].lower()
        if ex == "binance":
            top20, _total = await fetch_binance_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de Binance ahora mismo.")
                return
            top20 = await attach_indicators_binance(top20, interval="1h")
            await msg.reply_text(format_indicators_table("BINANCE", BINANCE_QUOTE, top20), parse_mode="Markdown")
            return

        if ex == "okx":
            top20, _total = await fetch_okx_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de OKX ahora mismo.")
                return
            top20 = await attach_indicators_okx(top20, bar="1H")
            await msg.reply_text(format_indicators_table("OKX", OKX_QUOTE, top20), parse_mode="Markdown")
            return

        if ex == "kraken":
            top20, _total = await fetch_kraken_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de Kraken ahora mismo.")
                return
            top20 = await attach_indicators_kraken(top20, interval=60)
            await msg.reply_text(format_indicators_table("KRAKEN", KRAKEN_QUOTE, top20), parse_mode="Markdown")
            return

        await msg.reply_text(
            "⚠️ Exchanges soportados: `binance`, `okx`, `kraken`.\n"
            "Ejemplos: `/nowindicadores binance` o `/nowindicadores okx` o `/nowindicadores kraken`",
            parse_mode="Markdown",
        )

    except Exception as e:
        log.exception(f"now_indicadores error: {e}")
        if DEBUG_ERRORS:
            await msg.reply_text(f"❌ Error: {e}")
        else:
            await msg.reply_text("❌ Error calculando indicadores. Inténtalo en unos minutos.")
            
async def now_indicadorespro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.chat.send_action(action=ChatAction.TYPING)

        args = list(context.args or [])
        top_n = 20
        if len(args) >= 2 and args[1].isdigit():
            top_n = max(5, min(20, int(args[1])))

        if not args:
            parts = []
            try:
                top20, _t = await fetch_binance_top20_quote()
                if top20:
                    top20 = await attach_indicators_binance(top20, interval="1h")
                    parts.append(format_indicatorspro_table("BINANCE", BINANCE_QUOTE, top20, top_n))
            except Exception as e:
                log.warning(f"now_indicadorespro Binance error: {e}")
            try:
                top20, _t = await fetch_okx_top20_quote()
                if top20:
                    top20 = await attach_indicators_okx(top20, bar="1H")
                    parts.append(format_indicatorspro_table("OKX", OKX_QUOTE, top20, top_n))
            except Exception as e:
                log.warning(f"now_indicadorespro OKX error: {e}")

            try:
                top20, _t = await fetch_kraken_top20_quote()
                if top20:
                    top20 = await attach_indicators_kraken(top20, interval=60)
                    parts.append(format_indicatorspro_table("KRAKEN", KRAKEN_QUOTE, top20, top_n))
            except Exception as e:
                log.warning(f"now_indicadorespro Kraken error: {e}")

            if parts:
                await msg.reply_text("\n\n".join(parts), parse_mode="Markdown")
            else:
                await msg.reply_text("❌ No se pudieron calcular indicadores PRO.")
            return

        ex = args[0].lower()
        if ex == "binance":
            top20, _t = await fetch_binance_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de Binance ahora mismo.")
                return
            top20 = await attach_indicators_binance(top20, interval="1h")
            await msg.reply_text(format_indicatorspro_table("BINANCE", BINANCE_QUOTE, top20, top_n), parse_mode="Markdown")
            return

        if ex == "okx":
            top20, _t = await fetch_okx_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de OKX ahora mismo.")
                return
            top20 = await attach_indicators_okx(top20, bar="1H")
            await msg.reply_text(format_indicatorspro_table("OKX", OKX_QUOTE, top20, top_n), parse_mode="Markdown")
            return

        if ex == "kraken":
            top20, _t = await fetch_kraken_top20_quote()
            if not top20:
                await msg.reply_text("No hay datos de Kraken ahora mismo.")
                return
            top20 = await attach_indicators_kraken(top20, interval=60)
            await msg.reply_text(format_indicatorspro_table("KRAKEN", KRAKEN_QUOTE, top20, top_n), parse_mode="Markdown")
            return

        await msg.reply_text(
            "⚠️ Uso: `/nowindicadorespro [binance|okx|kraken] [top_n 5..20]`\n"
            "Ejemplos: `/nowindicadorespro binance 10`  |  `/nowindicadorespro kraken 15`  |  `/nowindicadorespro` (ambos/varios, top20)",
            parse_mode="Markdown",
        )

    except Exception as e:
        log.exception(f"now_indicadorespro error: {e}")
        await msg.reply_text("❌ Error calculando indicadores PRO.")

async def pingchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ts = datetime.now(TZ).strftime("%d/%m %H:%M")
    await context.bot.send_message(
        chat_id=CHANNEL_ID, text=f"✅ Ping de prueba al canal ({ts})"
    )

# ---------- helpers ----------
async def build_nowprecios_all_text() -> str:
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

    return "\n\n".join(parts) if parts else "❌ No se pudieron obtener datos en este momento."

# ---------- jobs ----------
async def post_precios_volumen(app: Application):
    try:
        text = await build_nowprecios_all_text()
        await app.bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode="Markdown")
        log.info("Enviado precios/volumen a canal (Binance + OKX)")
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
                parts.append(format_indicators_table("BINANCE", BINANCE_QUOTE, top20))
        except Exception as e:
            log.warning(f"post_indicadores Binance error: {e}")

        try:
            top20, _total = await fetch_okx_top20_quote()
            if top20:
                top20 = await attach_indicators_okx(top20, bar="1H")
                parts.append(format_indicators_table("OKX", OKX_QUOTE, top20))
        except Exception as e:
            log.warning(f"post_indicadores OKX error: {e}")

        if parts:
            await app.bot.send_message(chat_id=CHANNEL_ID, text="\n\n".join(parts), parse_mode="Markdown")
            log.info("Enviados indicadores (Binance+OKX) al canal")
        else:
            await app.bot.send_message(chat_id=CHANNEL_ID, text="❌ No se pudieron calcular indicadores ahora.")
    except Exception as e:
        log.exception(f"Error enviando indicadores: {e}")
        if DEBUG_ERRORS:
            await app.bot.send_message(chat_id=CHANNEL_ID, text=f"❌ Error: {e}")

async def _post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start", "Mostrar ayuda"),
        BotCommand("metodo", "Ver metodología"),
        BotCommand("nowprecios", "Top 20 por volumen: /nowprecios binance|okx|kraken"),
        BotCommand("nowindicadores", "Indicadores técnicos (demo)"),
        BotCommand("nowindicadorespro", "Indicadores avanzados (PRO)"),
        BotCommand("pingchannel", "Enviar mensaje de prueba al canal"),
        BotCommand("status", "Estado/último ping web"),   # 👈 añadir esto
    ])
# ---------- main ----------
def main():
    application = Application.builder().token(TOKEN).post_init(_post_init).build()

    # comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("metodo", metodo))
    application.add_handler(CommandHandler("nowprecios", now_precios))
    application.add_handler(CommandHandler("nowindicadores", now_indicadores))
    application.add_handler(CommandHandler("pingchannel", pingchannel))
    application.add_handler(CommandHandler("nowindicadorespro", now_indicadorespro))
    application.add_handler(CommandHandler("feedback", feedback))
    application.add_handler(CommandHandler("status", status))

    # scheduler
    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(post_precios_volumen, CronTrigger(minute="0"), args=[application])
    scheduler.add_job(post_indicadores,  CronTrigger(minute="30"), args=[application])
    scheduler.start()

    # keep-alive web
    Thread(target=run_web, daemon=True).start()

    logging.info("Bot iniciado.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
