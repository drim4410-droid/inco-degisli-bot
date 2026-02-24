import os
import math
import time
import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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

TOP20_USDT = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","TRXUSDT","TONUSDT","AVAXUSDT",
    "DOTUSDT","LINKUSDT","MATICUSDT","BCHUSDT","LTCUSDT",
    "UNIUSDT","ATOMUSDT","XLMUSDT","NEARUSDT","ETCUSDT",
]

# Усиленная схема: 1H -> 15m -> 5m
TF_HTF = "1h"
TF_TREND = "15m"
TF_ENTRY = "5m"

# Интервал автосканера
AUTO_SCAN_EVERY_MIN = 15

# Параметры "режим B++" (реже, но точнее)
SETTINGS = {
    "use_adx_filter": True,
    "use_atr_filter": True,
    "min_adx_1h": 18.0,
    "min_adx_15m": 18.0,
    "min_atr_pct_15m": 0.12,       # минимум волатильности на 15m
    "max_ext_pct_15m": 0.90,       # не входить если цена слишком далеко от EMA50 (в %)
    "cooldown_minutes": 20,        # по одной монете не чаще
    "min_score_to_send": 8,        # порог силы для автосканера
    "evaluation_bars_5m": 3,        # оценка сигнала через N свечей 5m
    "tp_atr_mult": 1.6,
    "sl_atr_mult": 1.2,
    "cancel_atr_mult": 0.6,
}

LAST_SIGNAL_TS: Dict[str, float] = {}  # symbol -> monotonic time
AUTO_SCAN_ENABLED: Dict[int, bool] = {}  # user_id -> enabled

DB_PATH = "signals.db"

@dataclass
class Signal:
    symbol: str
    side: str  # LONG / SHORT
    price_now: float
    score: int
    tf: str
    reason: str
    tp: float
    sl: float
    cancel_if: str

# ---------- DB ----------

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            tf TEXT NOT NULL,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            tp REAL NOT NULL,
            sl REAL NOT NULL,
            eval_after_bars INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING/WIN/LOSE/NEUTRAL
            resolved_ts INTEGER,
            resolved_price REAL,
            score INTEGER NOT NULL,
            reason TEXT NOT NULL
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_pending
        ON signals(status, ts);
    """)
    con.commit()
    con.close()

def db_add_signal(user_id: int, sig: Signal, eval_after_bars: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO signals(ts, user_id, symbol, tf, side, entry, tp, sl, eval_after_bars, score, reason)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (
        int(time.time()), user_id, sig.symbol, sig.tf, sig.side,
        float(sig.price_now), float(sig.tp), float(sig.sl),
        int(eval_after_bars), int(sig.score), sig.reason
    ))
    con.commit()
    con.close()

def db_stats(user_id: int) -> Tuple[int,int,int,int]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT
          SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) AS win,
          SUM(CASE WHEN status='LOSE' THEN 1 ELSE 0 END) AS lose,
          SUM(CASE WHEN status='NEUTRAL' THEN 1 ELSE 0 END) AS neutral,
          COUNT(*) AS total
        FROM signals
        WHERE user_id=?
    """, (user_id,))
    row = cur.fetchone()
    con.close()
    win, lose, neutral, total = row if row else (0,0,0,0)
    return int(win or 0), int(lose or 0), int(neutral or 0), int(total or 0)

# ---------- Binance helpers ----------

async def fetch_klines(symbol: str, interval: str, limit: int = 300):
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    # return arrays: open, high, low, close
    o = [float(x[1]) for x in data]
    h = [float(x[2]) for x in data]
    l = [float(x[3]) for x in data]
    c = [float(x[4]) for x in data]
    return o, h, l, c

def ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return values[:]
    alpha = 2 / (period + 1)
    out = [values[0]]
    for i in range(1, len(values)):
        out.append(alpha * values[i] + (1 - alpha) * out[i - 1])
    return out

def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 2:
        return [50.0] * len(values)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    # EMA smoothing
    avg_g = []
    avg_l = []
    alpha = 1 / period
    g = gains[0]
    l = losses[0]
    for i in range(len(values)):
        g = alpha * gains[i] + (1 - alpha) * g
        l = alpha * losses[i] + (1 - alpha) * l
        avg_g.append(g)
        avg_l.append(l)

    out = []
    for g, l in zip(avg_g, avg_l):
        if l == 0:
            out.append(100.0 if g > 0 else 50.0)
        else:
            rs = g / l
            out.append(100 - (100 / (1 + rs)))
    return out

def atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[float]:
    if len(close) < 2:
        return [0.0] * len(close)
    tr = [high[0] - low[0]]
    for i in range(1, len(close)):
        tr.append(max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        ))
    # EMA smoothing
    alpha = 1 / period
    out = []
    a = tr[0]
    for i in range(len(tr)):
        a = alpha * tr[i] + (1 - alpha) * a
        out.append(a)
    return out

def adx(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[float]:
    if len(close) < period + 2:
        return [0.0] * len(close)

    plus_dm = [0.0]
    minus_dm = [0.0]
    for i in range(1, len(close)):
        up = high[i] - high[i - 1]
        dn = low[i - 1] - low[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)

    tr = [high[0] - low[0]]
    for i in range(1, len(close)):
        tr.append(max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        ))

    # Wilder smoothing (EMA alpha=1/period approximated)
    alpha = 1 / period
    atr_s = []
    p_s = []
    m_s = []
    a = tr[0]
    p = plus_dm[0]
    m = minus_dm[0]
    for i in range(len(close)):
        a = alpha * tr[i] + (1 - alpha) * a
        p = alpha * plus_dm[i] + (1 - alpha) * p
        m = alpha * minus_dm[i] + (1 - alpha) * m
        atr_s.append(a)
        p_s.append(p)
        m_s.append(m)

    plus_di = [0.0]*len(close)
    minus_di = [0.0]*len(close)
    for i in range(len(close)):
        if atr_s[i] == 0:
            plus_di[i] = 0.0
            minus_di[i] = 0.0
        else:
            plus_di[i] = 100 * (p_s[i] / atr_s[i])
            minus_di[i] = 100 * (m_s[i] / atr_s[i])

    dx = [0.0]*len(close)
    for i in range(len(close)):
        denom = plus_di[i] + minus_di[i]
        dx[i] = 0.0 if denom == 0 else 100 * (abs(plus_di[i] - minus_di[i]) / denom)

    # Smooth DX to ADX
    adx_out = []
    a = dx[0]
    for i in range(len(dx)):
        a = alpha * dx[i] + (1 - alpha) * a
        adx_out.append(a)
    return adx_out

def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return abs(a - b) / b * 100

def on_cooldown(symbol: str, now_ts: float) -> bool:
    last = LAST_SIGNAL_TS.get(symbol)
    if last is None:
        return False
    return (now_ts - last) < (SETTINGS["cooldown_minutes"] * 60)

# ---------- Strategy ----------

def compute_signal(symbol: str,
                   htf_1h: Tuple[List[float],List[float],List[float],List[float]],
                   trend_15m: Tuple[List[float],List[float],List[float],List[float]],
                   entry_5m: Tuple[List[float],List[float],List[float],List[float]]) -> Optional[Signal]:

    o1,h1,l1,c1 = htf_1h
    o15,h15,l15,c15 = trend_15m
    o5,h5,l5,c5 = entry_5m

    score = 0
    reasons = []

    # 1H: direction + trend quality
    ema50_1h = ema(c1, 50)[-1]
    ema200_1h = ema(c1, 200)[-1]
    adx_1h = adx(h1, l1, c1, 14)[-1]
    trend_up_1h = ema50_1h > ema200_1h
    trend_dn_1h = ema50_1h < ema200_1h

    if SETTINGS["use_adx_filter"]:
        if adx_1h < SETTINGS["min_adx_1h"]:
            return None
        score += 2
        reasons.append(f"1H ADX {adx_1h:.1f}")

    if not (trend_up_1h or trend_dn_1h):
        return None
    score += 2
    reasons.append("1H EMA50/200")

    # 15m: confirm direction + volatility + not too extended
    ema50_15 = ema(c15, 50)[-1]
    ema200_15 = ema(c15, 200)[-1]
    adx_15 = adx(h15, l15, c15, 14)[-1]
    atr_15 = atr(h15, l15, c15, 14)[-1]
    price_15 = c15[-1]
    atr_pct_15 = (atr_15 / price_15) * 100 if price_15 else 0.0

    trend_up_15 = ema50_15 > ema200_15
    trend_dn_15 = ema50_15 < ema200_15

    # direction agreement
    if trend_up_1h and trend_up_15:
        score += 2
        reasons.append("15m aligned UP")
        direction = "LONG"
    elif trend_dn_1h and trend_dn_15:
        score += 2
        reasons.append("15m aligned DOWN")
        direction = "SHORT"
    else:
        return None

    if SETTINGS["use_adx_filter"]:
        if adx_15 < SETTINGS["min_adx_15m"]:
            return None
        score += 1
        reasons.append(f"15m ADX {adx_15:.1f}")

    if SETTINGS["use_atr_filter"]:
        if atr_pct_15 < SETTINGS["min_atr_pct_15m"]:
            return None
        score += 1
        reasons.append(f"15m ATR {atr_pct_15:.2f}%")

    # Not too extended from EMA50 on 15m
    ext = pct(price_15, ema50_15)  # % distance
    if ext > SETTINGS["max_ext_pct_15m"]:
        return None
    score += 1
    reasons.append(f"15m not-extended ({ext:.2f}%)")

    # 5m entry: pullback to EMA20/50 + candle + RSI momentum
    price_now = c5[-1]
    ema20_5 = ema(c5, 20)
    ema50_5 = ema(c5, 50)
    rsi_5 = rsi(c5, 14)

    near_zone = (pct(price_now, ema20_5[-1]) < 0.25) or (pct(price_now, ema50_5[-1]) < 0.25)
    if not near_zone:
        return None
    score += 1
    reasons.append("5m pullback EMA zone")

    last_open = o5[-1]
    last_close = c5[-1]
    prev_r = rsi_5[-2]
    last_r = rsi_5[-1]

    bullish = last_close > last_open
    bearish = last_close < last_open
    rsi_up = last_r > prev_r
    rsi_dn = last_r < prev_r

    # confirmation
    if direction == "LONG":
        if not (bullish and rsi_up and last_r > 48):
            return None
        score += 2
        reasons.append("5m bullish + RSI up")
    else:
        if not (bearish and rsi_dn and last_r < 52):
            return None
        score += 2
        reasons.append("5m bearish + RSI down")

    # Build TP/SL guidance from 5m ATR
    atr_5 = atr(h5, l5, c5, 14)[-1]
    if atr_5 <= 0:
        return None

    tp_mult = SETTINGS["tp_atr_mult"]
    sl_mult = SETTINGS["sl_atr_mult"]
    cancel_mult = SETTINGS["cancel_atr_mult"]

    if direction == "LONG":
        sl = price_now - sl_mult * atr_5
        tp = price_now + tp_mult * atr_5
        cancel = f"отменить, если до входа цена упала ниже {price_now - cancel_mult * atr_5:.6f}"
    else:
        sl = price_now + sl_mult * atr_5
        tp = price_now - tp_mult * atr_5
        cancel = f"отменить, если до входа цена выросла выше {price_now + cancel_mult * atr_5:.6f}"

    return Signal(
        symbol=symbol,
        side=direction,
        price_now=float(price_now),
        score=int(score),
        tf=f"{TF_HTF}+{TF_TREND}+{TF_ENTRY}",
        reason=" | ".join(reasons),
        tp=float(tp),
        sl=float(sl),
        cancel_if=cancel
    )

# ---------- Telegram UI ----------

def main_kb(is_auto_on: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="📣 Сигнал (Top-20)", callback_data="signal_now")
    kb.button(text=f"🤖 Автосканер: {'ON' if is_auto_on else 'OFF'}", callback_data="toggle_auto")
    kb.button(text="📊 Статистика", callback_data="stats")
    kb.button(text="⚙️ Настройки", callback_data="settings")
    kb.adjust(1)
    return kb.as_markup()

def format_signal(sig: Signal) -> str:
    return (
        f"📣 <b>{sig.symbol}</b>\n"
        f"Направление: <b>{sig.side}</b>\n"
        f"ТФ: <b>{sig.tf}</b> (1H→15m фильтр + 5m вход)\n"
        f"Вход: <b>MARKET NOW</b> ≈ <code>{sig.price_now:.6f}</code>\n"
        f"TP (ориентир): <code>{sig.tp:.6f}</code>\n"
        f"SL (ориентир): <code>{sig.sl:.6f}</code>\n"
        f"Сила: <b>{sig.score}</b>\n"
        f"Фильтры: {sig.reason}\n"
        f"⚠️ Если не успел: {sig.cancel_if}\n"
    )

# ---------- Bot ----------

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

@dp.message(F.text.in_({"/start", "start"}))
async def start(m: Message):
    db_init()
    uid = m.from_user.id
    if uid not in AUTO_SCAN_ENABLED:
        AUTO_SCAN_ENABLED[uid] = True
    await m.answer(
        "Готово ✅\n"
        "Режим: <b>максимально качественные сигналы</b>\n"
        "Схема: <b>1H → 15m → 5m</b>\n"
        f"Автосканер: <b>{'ON' if AUTO_SCAN_ENABLED[uid] else 'OFF'}</b> (каждые {AUTO_SCAN_EVERY_MIN} мин)\n\n"
        "Жми кнопку или жди авто-сигнал.",
        reply_markup=main_kb(AUTO_SCAN_ENABLED[uid])
    )

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(cb: CallbackQuery):
    uid = cb.from_user.id
    AUTO_SCAN_ENABLED[uid] = not AUTO_SCAN_ENABLED.get(uid, True)
    await cb.answer("Ок")
    await cb.message.answer(
        f"Автосканер теперь: <b>{'ON' if AUTO_SCAN_ENABLED[uid] else 'OFF'}</b>",
        reply_markup=main_kb(AUTO_SCAN_ENABLED[uid])
    )

@dp.callback_query(F.data == "settings")
async def settings(cb: CallbackQuery):
    await cb.answer()
    text = (
        "⚙️ <b>Настройки (режим точности)</b>\n"
        f"• ADX 1H ≥ {SETTINGS['min_adx_1h']}\n"
        f"• ADX 15m ≥ {SETTINGS['min_adx_15m']}\n"
        f"• ATR 15m ≥ {SETTINGS['min_atr_pct_15m']}%\n"
        f"• Не входить если цена далеко от EMA50 (15m) > {SETTINGS['max_ext_pct_15m']}%\n"
        f"• Cooldown: {SETTINGS['cooldown_minutes']} мин/монета\n"
        f"• Автосигнал от силы: ≥ {SETTINGS['min_score_to_send']}\n"
        f"• Оценка результата: через {SETTINGS['evaluation_bars_5m']} свечи 5m\n\n"
        "Дальше можем сделать переключатели кнопками (агрессивнее/консервативнее)."
    )
    await cb.message.answer(text, reply_markup=main_kb(AUTO_SCAN_ENABLED.get(cb.from_user.id, True)))

@dp.callback_query(F.data == "stats")
async def stats(cb: CallbackQuery):
    await cb.answer()
    db_init()
    uid = cb.from_user.id
    win, lose, neutral, total = db_stats(uid)
    wr = (win / total * 100) if total > 0 else 0.0
    await cb.message.answer(
        "📊 <b>Статистика</b>\n"
        f"Всего: <b>{total}</b>\n"
        f"WIN: <b>{win}</b>\n"
        f"LOSE: <b>{lose}</b>\n"
        f"NEUTRAL: <b>{neutral}</b>\n"
        f"Winrate: <b>{wr:.1f}%</b>\n\n"
        "⚠️ Смотри на дистанции (минимум 30–50 сигналов).",
        reply_markup=main_kb(AUTO_SCAN_ENABLED.get(uid, True))
    )

@dp.callback_query(F.data == "signal_now")
async def signal_now(cb: CallbackQuery):
    await cb.answer("Сканирую Top-20…")
    uid = cb.from_user.id
    await send_best_signal(uid, cb.message)

async def send_best_signal(user_id: int, msg_target: Message, only_if_strong: bool = False):
    now_ts = asyncio.get_running_loop().time()
    best: Optional[Signal] = None

    for sym in TOP20_USDT:
        if on_cooldown(sym, now_ts):
            continue
        try:
            htf = await fetch_klines(sym, TF_HTF, 300)
            t15 = await fetch_klines(sym, TF_TREND, 300)
            e5 = await fetch_klines(sym, TF_ENTRY, 300)
            sig = compute_signal(sym, htf, t15, e5)
            if sig:
                if (best is None) or (sig.score > best.score):
                    best = sig
        except Exception:
            continue

    if not best:
        await msg_target.answer(
            "Сейчас нет качественного сигнала по Top-20 (флет/низкая волатильность/нет подтверждения).",
            reply_markup=main_kb(AUTO_SCAN_ENABLED.get(user_id, True))
        )
        return

    if only_if_strong and best.score < SETTINGS["min_score_to_send"]:
        return

    LAST_SIGNAL_TS[best.symbol] = now_ts
    text = format_signal(best)

    # Сохраняем в базу и позже оценим
    db_add_signal(user_id, best, SETTINGS["evaluation_bars_5m"])

    await msg_target.answer(text, reply_markup=main_kb(AUTO_SCAN_ENABLED.get(user_id, True)))

async def evaluator_loop():
    """
    Периодически проверяем PENDING сигналы и выставляем WIN/LOSE/NEUTRAL
    по цене через N свечей 5m.
    """
    while True:
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("""
                SELECT id, user_id, symbol, side, entry, tp, sl, ts, eval_after_bars
                FROM signals
                WHERE status='PENDING'
                ORDER BY ts ASC
                LIMIT 50
            """)
            rows = cur.fetchall()
            con.close()

            if rows:
                for (sid, uid, sym, side, entry, tp, sl, ts0, bars) in rows:
                    # ждать пока пройдёт достаточное время (bars * 5 минут)
                    need_seconds = int(bars) * 5 * 60
                    if int(time.time()) - int(ts0) < need_seconds:
                        continue

                    try:
                        # берем последние bars+2 свечи 5m
                        o,h,l,c = await fetch_klines(sym, TF_ENTRY, max(50, int(bars) + 10))
                        last_price = float(c[-1])

                        status = "NEUTRAL"
                        # Примитивная оценка: если к моменту оценки цена оказалась
                        # "в сторону" TP — WIN, если "против" в сторону SL — LOSE.
                        # (Следующий апгрейд: проверка внутри диапазона свечей, was TP/SL touched)
                        if side == "LONG":
                            if last_price >= tp:
                                status = "WIN"
                            elif last_price <= sl:
                                status = "LOSE"
                        else:
                            if last_price <= tp:
                                status = "WIN"
                            elif last_price >= sl:
                                status = "LOSE"

                        con = sqlite3.connect(DB_PATH)
                        cur = con.cursor()
                        cur.execute("""
                            UPDATE signals
                            SET status=?, resolved_ts=?, resolved_price=?
                            WHERE id=?
                        """, (status, int(time.time()), float(last_price), sid))
                        con.commit()
                        con.close()
                    except Exception:
                        continue

        except Exception:
            pass

        await asyncio.sleep(30)

async def autoscan_loop():
    """
    Каждые AUTO_SCAN_EVERY_MIN минут — если у пользователя ON — ищем сильный сигнал.
    """
    while True:
        try:
            # users that started the bot
            users = [uid for uid, on in AUTO_SCAN_ENABLED.items() if on]
            for uid in users:
                # отправляем в личку пользователю (нужен chat_id = user_id в личке)
                try:
                    dummy = Message.model_validate({
                        "message_id": 0,
                        "date": int(time.time()),
                        "chat": {"id": uid, "type": "private"},
                        "from": {"id": uid, "is_bot": False, "first_name": "User"},
                        "text": ""
                    })
                except Exception:
                    dummy = None

                # Если не можем создать dummy message — просто отправим напрямую
                try:
                    if dummy:
                        await send_best_signal(uid, dummy, only_if_strong=True)
                    else:
                        # fallback: direct send after computing
                        pass
                except Exception:
                    continue
        except Exception:
            pass

        await asyncio.sleep(AUTO_SCAN_EVERY_MIN * 60)

async def main():
    db_init()
    # фоновые задачи
    asyncio.create_task(evaluator_loop())
    asyncio.create_task(autoscan_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
