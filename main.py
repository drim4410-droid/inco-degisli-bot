import os
import math
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import httpx
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set it in Railway Variables.")

BINANCE_BASE = "https://api.binance.com"

# Top-20 by market cap changes over time, но для бесплатного старта берём устойчивый список.
TOP20_USDT = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","TRXUSDT","TONUSDT","AVAXUSDT",
    "DOTUSDT","LINKUSDT","MATICUSDT","BCHUSDT","LTCUSDT",
    "UNIUSDT","ATOMUSDT","XLMUSDT","NEARUSDT","ETCUSDT",
]

# Таймфреймы по твоему запросу
TF_TREND = "15m"
TF_ENTRY = "5m"

# Режим B: реже, но качественнее
SETTINGS = {
    "use_adx_filter": True,
    "use_atr_filter": True,
    "min_adx": 18.0,            # фильтр флэта
    "min_atr_pct": 0.10,        # ATR% от цены (0.10% как старт)
    "cooldown_minutes": 10,     # не спамим по одной монете
}

# Память для анти-спама сигналов
LAST_SIGNAL_TS: Dict[str, float] = {}  # symbol -> loop.time()

@dataclass
class Signal:
    symbol: str
    side: str            # LONG / SHORT
    price_now: float
    score: int
    reason: str
    tf: str              # "15m+5m"
    tp: float
    sl: float
    cancel_if: str

# ---------- Binance helpers ----------

async def fetch_klines(symbol: str, interval: str, limit: int = 300) -> pd.DataFrame:
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    # kline: [open_time, open, high, low, close, volume, close_time, ...]
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","tbbav","tbqav","ignore"
    ])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df[["open_time","open","high","low","close","volume"]]

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)

    atr_ = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean() / atr_)
    minus_di = 100 * (pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean() / atr_)

    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0)
    return dx.ewm(alpha=1/period, adjust=False).mean()

# ---------- Strategy (15m trend + 5m entry) ----------

def compute_signal(trend_df: pd.DataFrame, entry_df: pd.DataFrame, symbol: str) -> Optional[Signal]:
    # Trend timeframe (15m)
    t_close = trend_df["close"]
    t_ema50 = ema(t_close, 50).iloc[-1]
    t_ema200 = ema(t_close, 200).iloc[-1]
    t_adx = adx(trend_df, 14).iloc[-1]
    t_atr = atr(trend_df, 14).iloc[-1]
    t_price = float(t_close.iloc[-1])
    atr_pct = (t_atr / t_price) * 100 if t_price else 0

    # Determine trend direction
    trend_up = t_ema50 > t_ema200
    trend_down = t_ema50 < t_ema200

    score = 0
    reasons = []

    # Filters
    if SETTINGS["use_adx_filter"]:
        if t_adx < SETTINGS["min_adx"]:
            return None
        score += 1
        reasons.append(f"ADX {t_adx:.1f} (trend ok)")

    if SETTINGS["use_atr_filter"]:
        if atr_pct < SETTINGS["min_atr_pct"]:
            return None
        score += 1
        reasons.append(f"ATR {atr_pct:.2f}% (vol ok)")

    if not (trend_up or trend_down):
        return None

    score += 1
    reasons.append("15m EMA50/200 trend")

    # Entry timeframe (5m): pullback + confirmation
    e = entry_df.copy()
    e_close = e["close"]
    e_open = e["open"]

    e_ema20 = ema(e_close, 20)
    e_ema50 = ema(e_close, 50)
    e_rsi = rsi(e_close, 14)

    price_now = float(e_close.iloc[-1])

    # Pullback: price near EMA20/EMA50 zone (within 0.25% of price)
    near_ema = (abs(price_now - float(e_ema20.iloc[-1])) / price_now) < 0.0025 or \
               (abs(price_now - float(e_ema50.iloc[-1])) / price_now) < 0.0025
    if not near_ema:
        return None
    score += 1
    reasons.append("5m pullback to EMA zone")

    # Confirmation candle: bullish/bearish close + RSI direction
    last_close = float(e_close.iloc[-1])
    last_open = float(e_open.iloc[-1])
    prev_rsi = float(e_rsi.iloc[-2])
    last_rsi = float(e_rsi.iloc[-1])

    bullish = last_close > last_open
    bearish = last_close < last_open
    rsi_up = last_rsi > prev_rsi
    rsi_down = last_rsi < prev_rsi

    side = None
    if trend_up and bullish and rsi_up and last_rsi > 45:
        side = "LONG"
        score += 2
        reasons.append("5m bullish + RSI rising")
    elif trend_down and bearish and rsi_down and last_rsi < 55:
        side = "SHORT"
        score += 2
        reasons.append("5m bearish + RSI falling")
    else:
        return None

    # Risk framework (not limit orders): give guidance for immediate market entry
    e_atr = float(atr(entry_df, 14).iloc[-1])
    if e_atr <= 0:
        return None

    # SL/TP as reference levels (you enter market, these are management levels)
    if side == "LONG":
        sl = price_now - 1.2 * e_atr
        tp = price_now + 1.6 * e_atr
        cancel = f"отменить, если цена ушла ниже {price_now - 0.6 * e_atr:.6f} до входа"
    else:
        sl = price_now + 1.2 * e_atr
        tp = price_now - 1.6 * e_atr
        cancel = f"отменить, если цена ушла выше {price_now + 0.6 * e_atr:.6f} до входа"

    reason = " | ".join(reasons)
    return Signal(
        symbol=symbol,
        side=side,
        price_now=price_now,
        score=score,
        reason=reason,
        tf=f"{TF_TREND}+{TF_ENTRY}",
        tp=tp,
        sl=sl,
        cancel_if=cancel
    )

def on_cooldown(symbol: str, now_ts: float) -> bool:
    last = LAST_SIGNAL_TS.get(symbol)
    if last is None:
        return False
    return (now_ts - last) < (SETTINGS["cooldown_minutes"] * 60)

# ---------- Telegram UI ----------

def main_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📣 Сигнал (Top-20)", callback_data="signal_now")
    kb.button(text="📊 Последние сигналы", callback_data="history")
    kb.button(text="⚙️ Настройки", callback_data="settings")
    kb.adjust(1)
    return kb.as_markup()

SIGNAL_HISTORY: List[str] = []

def format_signal(sig: Signal) -> str:
    # “Не лимитные ордера”: entry is market now
    return (
        f"📣 <b>{sig.symbol}</b>\n"
        f"Направление: <b>{sig.side}</b>\n"
        f"ТФ: <b>{sig.tf}</b> (15m фильтр + 5m вход)\n"
        f"Вход: <b>MARKET NOW</b> ≈ <code>{sig.price_now:.6f}</code>\n"
        f"TP (ориентир): <code>{sig.tp:.6f}</code>\n"
        f"SL (ориентир): <code>{sig.sl:.6f}</code>\n"
        f"Сила: <b>{sig.score}/7</b>\n"
        f"Фильтры: {sig.reason}\n"
        f"⚠️ Если не успел: {sig.cancel_if}\n"
    )

# ---------- Bot handlers ----------

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

@dp.message(F.text.in_({"/start", "start"}))
async def start(m: Message):
    await m.answer(
        "Привет! Я крипто-сигнал бот.\n"
        "Режим: <b>B</b> (реже, но качественнее)\n"
        "Жми кнопку, чтобы получить лучший сигнал по Top-20 USDT.",
        reply_markup=main_kb()
    )

@dp.callback_query(F.data == "signal_now")
async def signal_now(cb: CallbackQuery):
    await cb.answer("Сканирую Top-20…")
    now_ts = asyncio.get_running_loop().time()

    best: Optional[Signal] = None

    # Сканируем по очереди (надёжнее для бесплатных лимитов)
    for sym in TOP20_USDT:
        if on_cooldown(sym, now_ts):
            continue
        try:
            trend_df = await fetch_klines(sym, TF_TREND, 300)
            entry_df = await fetch_klines(sym, TF_ENTRY, 300)
            sig = compute_signal(trend_df, entry_df, sym)
            if sig:
                if (best is None) or (sig.score > best.score):
                    best = sig
        except Exception:
            continue

    if not best:
        await cb.message.answer(
            "Сейчас нет качественного сигнала по Top-20 (флет/низкая волатильность/нет подтверждения).\n"
            "Попробуй позже.",
            reply_markup=main_kb()
        )
        return

    LAST_SIGNAL_TS[best.symbol] = now_ts
    msg = format_signal(best)
    SIGNAL_HISTORY.append(msg)
    SIGNAL_HISTORY[:] = SIGNAL_HISTORY[-20:]

    await cb.message.answer(msg, reply_markup=main_kb())

@dp.callback_query(F.data == "history")
async def history(cb: CallbackQuery):
    await cb.answer()
    if not SIGNAL_HISTORY:
        await cb.message.answer("История пустая. Нажми 📣 Сигнал.", reply_markup=main_kb())
        return
    text = "📊 <b>Последние сигналы</b>\n\n" + "\n— — —\n".join(SIGNAL_HISTORY[-5:])
    await cb.message.answer(text, reply_markup=main_kb())

@dp.callback_query(F.data == "settings")
async def settings(cb: CallbackQuery):
    await cb.answer()
    text = (
        "⚙️ <b>Настройки (пока фиксированы под режим B)</b>\n"
        f"• ADX фильтр: {'✅' if SETTINGS['use_adx_filter'] else '❌'} (min {SETTINGS['min_adx']})\n"
        f"• ATR фильтр: {'✅' if SETTINGS['use_atr_filter'] else '❌'} (min {SETTINGS['min_atr_pct']}%)\n"
        f"• Cooldown: {SETTINGS['cooldown_minutes']} мин/монета\n\n"
        "Дальше можем добавить переключатели кнопками."
    )
    await cb.message.answer(text, reply_markup=main_kb())

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
