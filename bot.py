import os, logging, asyncio
from datetime import datetime
import pytz
from flask import Flask
from threading import Thread

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ========= Config =========
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # ej. "@CryptoDataHubES"  (el bot debe ser admin)
TZ = pytz.timezone("Europe/Madrid")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ========= Web keep-alive (Replit + UptimeRobot) =========
app = Flask(__name__)

@app.get("/")
def health():
    return "OK", 200

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ========= Bot handlers =========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Bienvenido a Crypto Data Hub ES\n\n"
        "Comandos disponibles:\n"
        "/metodo - Ver metodología\n"
        "/nowprecios [exchange] - Precios y volumen (demo)\n"
        "/nowindicadores [exchange] - Indicadores (demo)"
    )

async def metodo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "📐 Metodología (MVP)\n\n"
        "• Top 20 por volumen USD en cada exchange (Binance, Bybit, Coinbase, OKX, Kraken)\n"
        "• Variación % = precio vs 24h\n"
        "• Dominancia = vol token / vol total exchange\n"
        "• Indicadores: RSI14, ATR%, EMA20/50, VWAP dev%, OB imbalance\n"
        "• Fear & Greed: índice compuesto\n\n"
        "NFA | DYOR"
    )
    await update.message.reply_text(msg)

async def now_precios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ex = (context.args[0].lower() if context.args else "binance")
    await update.message.reply_text(f"📊 Precios & Volumen — {ex.upper()} (demo)\nPendiente conectar datos reales.")

async def now_indicadores(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ex = (context.args[0].lower() if context.args else "binance")
    await update.message.reply_text(f"📐 Indicadores — {ex.upper()} (demo)\nPendiente conectar datos reales.")

# ========= Jobs automáticos (cada :00 y :30) =========
async def post_precios_volumen(app: Application):
    ts = datetime.now(TZ).strftime("%H:%M")
    text = f"📊 Precios & Volumen — {ts}\n(Demo) Próximamente datos reales."
    await app.bot.send_message(chat_id=CHANNEL_ID, text=text)

async def post_indicadores(app: Application):
    ts = datetime.now(TZ).strftime("%H:%M")
    text = f"📐 Indicadores — {ts}\n(Demo) Próximamente datos reales."
    await app.bot.send_message(chat_id=CHANNEL_ID, text=text)

# ========= Main =========
async def main():
    application = Application.builder().token(TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("metodo", metodo))
    application.add_handler(CommandHandler("nowprecios", now_precios))
    application.add_handler(CommandHandler("nowindicadores", now_indicadores))

    # Scheduler
    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(lambda: post_precios_volumen(application), CronTrigger(minute="0"))
    scheduler.add_job(lambda: post_indicadores(application), CronTrigger(minute="30"))
    scheduler.start()

    # Web keep-alive
    Thread(target=run_web, daemon=True).start()

    log.info("Bot iniciado.")
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    asyncio.run(main())
