import os
import time
import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Optional

import httpx
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ================= CONFIG =================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

BINANCE = "https://api.binance.com"
DB_PATH = "signals.db"

TOP_N = 50
MIN_VOLUME = 50_000_000
MIN_PRICE = 0.01

DAILY_LIMIT = 5
MAX_LOSS_STREAK = 2
EVAL_BARS = 3
TKM_OFFSET = 5 * 3600

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# ================= DATABASE =================

def db():
    return sqlite3.connect(DB_PATH)

def init_db():
    con = db()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        status TEXT DEFAULT 'PENDING',
        access_until INTEGER DEFAULT 0,
        autoscan INTEGER DEFAULT 1
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        user_id INTEGER,
        symbol TEXT,
        side TEXT,
        entry REAL,
        tp REAL,
        sl REAL,
        status TEXT DEFAULT 'PENDING'
    )""")

    con.commit()
    con.close()

def ensure_user(uid: int):
    con = db()
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)", (uid,))
    con.commit()
    con.close()

def approve(uid: int, days: int = 3650):
    """Approve user for N days (default ~10 years for admin auto-approve)."""
    until = int(time.time()) + int(days) * 86400
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET status='APPROVED', access_until=? WHERE user_id=?", (until, uid))
    con.commit()
    con.close()
    return until

def active(uid: int) -> bool:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT status, access_until FROM users WHERE user_id=?", (uid,))
    r = cur.fetchone()
    con.close()
    return bool(r) and r[0] == "APPROVED" and int(r[1]) > int(time.time())

def fmt_until(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))

# ================= INDICATORS =================

def ema(values, period):
    alpha = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out

def rsi(values, period=14):
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rs = avg_gain / avg_loss if avg_loss else 0
    rsi_vals = [100 - (100 / (1 + rs))]
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss else 0
        rsi_vals.append(100 - (100 / (1 + rs)))
    return [50] * (len(values) - len(rsi_vals)) + rsi_vals

def atr(high, low, close, period=14):
    trs = []
    for i in range(1, len(close)):
        tr = max(high[i] - low[i],
                 abs(high[i] - close[i - 1]),
                 abs(low[i] - close[i - 1]))
        trs.append(tr)
    a = sum(trs[:period]) / period
    out = [a]
    for i in range(period, len(trs)):
        a = (a * (period - 1) + trs[i]) / period
        out.append(a)
    return [0] * (len(close) - len(out)) + out

# ================= BINANCE =================

async def fetch(url, params=None):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

async def klines(symbol, interval, limit=200):
    return await fetch(f"{BINANCE}/api/v3/klines",
                       {"symbol": symbol, "interval": interval, "limit": limit})

async def top_symbols():
    data = await fetch(f"{BINANCE}/api/v3/ticker/24hr")
    arr = []
    for i in data:
        s = i["symbol"]
        if not s.endswith("USDT"):
            continue
        # exclude leveraged tokens
        if s.endswith(("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        vol = float(i["quoteVolume"])
        price = float(i["lastPrice"])
        if vol >= MIN_VOLUME and price >= MIN_PRICE:
            arr.append((s, vol))
    arr.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in arr[:TOP_N]]

# ================= STRATEGY (STRICT) =================

@dataclass
class Signal:
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float

async def build_signal(symbol) -> Optional[Signal]:
    k1 = await klines(symbol, "1h", 250)
    k15 = await klines(symbol, "15m", 250)
    k5 = await klines(symbol, "5m", 120)

    c1 = [float(x[4]) for x in k1]
    c15 = [float(x[4]) for x in k15]
    c5 = [float(x[4]) for x in k5]
    h15 = [float(x[2]) for x in k15]
    l15 = [float(x[3]) for x in k15]

    ema50_1 = ema(c1, 50)[-1]
    ema200_1 = ema(c1, 200)[-1]
    ema50_15 = ema(c15, 50)[-1]
    ema200_15 = ema(c15, 200)[-1]

    atr15 = atr(h15, l15, c15)[-1]
    atr_pct = (atr15 / c15[-1]) * 100 if c15[-1] else 0.0
    if atr_pct < 0.30:
        return None

    trend_up = ema50_1 > ema200_1 and ema50_15 > ema200_15
    trend_down = ema50_1 < ema200_1 and ema50_15 < ema200_15

    last = c5[-1]
    prev = c5[-2]
    r = rsi(c5)[-1]

    if trend_up and last > prev and r > 55:
        return Signal(symbol, "LONG", last, last * 1.01, last * 0.995)

    if trend_down and last < prev and r < 45:
        return Signal(symbol, "SHORT", last, last * 0.99, last * 1.005)

    return None

# ================= RISK =================

def today_start():
    t = int(time.time()) + TKM_OFFSET
    day = time.strftime("%Y%m%d", time.gmtime(t))
    return int(time.mktime(time.strptime(day, "%Y%m%d")))

def daily_control(uid):
    con = db()
    cur = con.cursor()
    cur.execute("""
    SELECT status FROM signals
    WHERE user_id=? AND ts>=?
    """, (uid, today_start()))
    rows = cur.fetchall()
    con.close()

    wins = losses = streak = 0
    for (st,) in rows:
        if st == "WIN":
            streak = 0
            wins += 1
        elif st == "LOSE":
            streak += 1
            losses += 1
    return wins, losses, streak

# ================= EVALUATOR =================

async def evaluator():
    while True:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT id,symbol,side,tp,ts FROM signals WHERE status='PENDING'")
        rows = cur.fetchall()
        con.close()

        for sid, sym, side, tp, ts0 in rows:
            if time.time() - ts0 < EVAL_BARS * 300:
                continue
            try:
                k = await klines(sym, "5m", limit=EVAL_BARS + 5)
                highs = [float(x[2]) for x in k[:EVAL_BARS]]
                lows = [float(x[3]) for x in k[:EVAL_BARS]]

                if side == "LONG":
                    status = "WIN" if max(highs) >= tp else "LOSE"
                else:
                    status = "WIN" if min(lows) <= tp else "LOSE"

                con2 = db()
                cur2 = con2.cursor()
                cur2.execute("UPDATE signals SET status=? WHERE id=?", (status, sid))
                con2.commit()
                con2.close()
            except Exception:
                continue

        await asyncio.sleep(30)

# ================= UI =================

def kb(uid):
    k = InlineKeyboardBuilder()
    if active(uid):
        k.button(text="📣 Сигнал", callback_data="signal")
        k.button(text="📊 Статистика", callback_data="stats")
    else:
        k.button(text="📝 Запросить доступ", callback_data="req")
    k.adjust(1)
    return k.as_markup()

# ================= BOT =================

@dp.message(F.text.in_({"/start", "start"}))
async def start(m: Message):
    init_db()
    ensure_user(m.from_user.id)

    # auto-approve admin instantly
    if ADMIN_ID and m.from_user.id == ADMIN_ID and not active(ADMIN_ID):
        until = approve(ADMIN_ID, days=3650)
        await m.answer(f"✅ Админ доступ активирован до: <b>{fmt_until(until)}</b>", reply_markup=kb(m.from_user.id))
        return

    await m.answer("Бот готов.", reply_markup=kb(m.from_user.id))

@dp.message(F.text == "/myid")
async def myid(m: Message):
    await m.answer(f"ID: <code>{m.from_user.id}</code>")

@dp.message(F.text.startswith("/approve"))
async def cmd_approve(m: Message):
    if not ADMIN_ID or m.from_user.id != ADMIN_ID:
        return
    parts = m.text.split()
    if len(parts) != 3:
        await m.answer("Формат: /approve USER_ID DAYS")
        return
    uid = int(parts[1])
    days = int(parts[2])
    ensure_user(uid)
    until = approve(uid, days)
    await m.answer(f"✅ Одобрено <code>{uid}</code> на {days} дней (до {fmt_until(until)})")
    try:
        await bot.send_message(uid, f"✅ Доступ выдан на {days} дней.\nНажми /start")
    except Exception:
        pass

@dp.callback_query(F.data == "req")
async def req(cb: CallbackQuery):
    uid = cb.from_user.id
    ensure_user(uid)

    # Always respond + message in chat
    await cb.answer("✅ Заявка отправлена")
    await cb.message.answer("✅ Заявка отправлена админу. Ожидай одобрения.")

    # If ADMIN_ID not set, inform user
    if not ADMIN_ID:
        await cb.message.answer("⚠️ ADMIN_ID не задан в Railway Variables. Задай ADMIN_ID и перезапусти сервис.")
        return

    # If user is admin, auto-approve
    if uid == ADMIN_ID:
        until = approve(uid, days=3650)
        await cb.message.answer(f"✅ Ты админ. Доступ активирован до: <b>{fmt_until(until)}</b>", reply_markup=kb(uid))
        return

    # Notify admin
    try:
        await bot.send_message(ADMIN_ID, f"🛂 Запрос доступа от <code>{uid}</code>\nОдобрить: /approve {uid} 30")
    except Exception:
        pass

@dp.callback_query(F.data == "signal")
async def signal(cb: CallbackQuery):
    uid = cb.from_user.id
    if not active(uid):
        await cb.answer("Нет доступа", show_alert=True)
        return

    wins, losses, streak = daily_control(uid)
    if streak >= MAX_LOSS_STREAK:
        await cb.answer("Пауза до конца дня (2 лося подряд)", show_alert=True)
        return
    if wins + losses >= DAILY_LIMIT:
        await cb.answer("Дневной лимит (5) достигнут", show_alert=True)
        return

    await cb.answer("Сканирую Top-50…")

    syms = await top_symbols()
    for s in syms:
        try:
            sig = await build_signal(s)
            if not sig:
                continue

            con = db()
            cur = con.cursor()
            cur.execute("""
            INSERT INTO signals(ts,user_id,symbol,side,entry,tp,sl,status)
            VALUES(?,?,?,?,?,?,?,'PENDING')
            """, (int(time.time()), uid, sig.symbol, sig.side, sig.entry, sig.tp, sig.sl))
            con.commit()
            con.close()

            await cb.message.answer(
                f"📣 <b>{sig.symbol}</b>\n"
                f"{sig.side}\n"
                f"Entry: <code>{sig.entry:.6f}</code>\n"
                f"TP: <code>{sig.tp:.6f}</code>\n"
                f"SL: <code>{sig.sl:.6f}</code>\n"
                f"Оценка через {EVAL_BARS} свечи 5m",
                reply_markup=kb(uid)
            )
            return
        except Exception:
            continue

    await cb.message.answer("Сейчас нет подходящего сигнала.", reply_markup=kb(uid))

@dp.callback_query(F.data == "stats")
async def stats(cb: CallbackQuery):
    uid = cb.from_user.id
    if not active(uid):
        await cb.answer("Нет доступа", show_alert=True)
        return

    con = db()
    cur = con.cursor()
    cur.execute("""
    SELECT
      SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END),
      SUM(CASE WHEN status='LOSE' THEN 1 ELSE 0 END),
      COUNT(*)
    FROM signals
    WHERE user_id=? AND status!='PENDING'
    """, (uid,))
    w, l, t = cur.fetchone()
    con.close()

    w = int(w or 0)
    l = int(l or 0)
    t = int(t or 0)
    wr = (w / t * 100.0) if t else 0.0

    await cb.answer()
    await cb.message.answer(
        f"📊 WIN: <b>{w}</b>\n"
        f"📊 LOSE: <b>{l}</b>\n"
        f"📊 TOTAL: <b>{t}</b>\n"
        f"📊 Winrate: <b>{wr:.1f}%</b>",
        reply_markup=kb(uid)
    )

# ================= MAIN =================

async def main():
    init_db()
    asyncio.create_task(evaluator())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
