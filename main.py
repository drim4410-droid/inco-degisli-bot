import os
import time
import math
import asyncio
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

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
DAILY_LIMIT = 10
MAX_LOSS_STREAK = 3

MIN_VOLUME = 50_000_000
MIN_PRICE = 0.01

EVAL_BARS = 3
AUTO_INTERVAL = 15
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
        autoscan INTEGER DEFAULT 1,
        mode TEXT DEFAULT 'STRICT'
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

def ensure_user(uid):
    con=db()
    cur=con.cursor()
    cur.execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)",(uid,))
    con.commit()
    con.close()

def active(uid):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT status,access_until FROM users WHERE user_id=?",(uid,))
    r=cur.fetchone()
    con.close()
    if not r:
        return False
    return r[0]=="APPROVED" and r[1]>int(time.time())

# ================= INDICATORS =================

def ema(values, period):
    alpha=2/(period+1)
    result=[values[0]]
    for v in values[1:]:
        result.append(alpha*v+(1-alpha)*result[-1])
    return result

def rsi(values, period=14):
    gains=[]
    losses=[]
    for i in range(1,len(values)):
        diff=values[i]-values[i-1]
        gains.append(max(diff,0))
        losses.append(max(-diff,0))
    avg_gain=sum(gains[:period])/period
    avg_loss=sum(losses[:period])/period
    rs=avg_gain/avg_loss if avg_loss!=0 else 0
    rsi_vals=[100-(100/(1+rs))]
    for i in range(period,len(gains)):
        avg_gain=(avg_gain*(period-1)+gains[i])/period
        avg_loss=(avg_loss*(period-1)+losses[i])/period
        rs=avg_gain/avg_loss if avg_loss!=0 else 0
        rsi_vals.append(100-(100/(1+rs)))
    return [50]*(len(values)-len(rsi_vals))+rsi_vals

def atr(high,low,close,period=14):
    trs=[]
    for i in range(1,len(close)):
        tr=max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1]))
        trs.append(tr)
    atr=sum(trs[:period])/period
    atrs=[atr]
    for i in range(period,len(trs)):
        atr=(atr*(period-1)+trs[i])/period
        atrs.append(atr)
    return [0]*(len(close)-len(atrs))+atrs
    # ================= BINANCE API =================

async def fetch(url, params=None):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

async def klines(symbol, interval, limit=200):
    return await fetch(
        f"{BINANCE}/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )

async def top_symbols():
    data = await fetch(f"{BINANCE}/api/v3/ticker/24hr")
    arr = []
    for i in data:
        s = i["symbol"]
        if not s.endswith("USDT"):
            continue
        vol = float(i["quoteVolume"])
        price = float(i["lastPrice"])
        if vol >= MIN_VOLUME and price >= MIN_PRICE:
            arr.append((s, vol))
    arr.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in arr[:TOP_N]]

# ================= ADVANCED STRATEGY =================

@dataclass
class Signal:
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float

async def build_advanced_signal(symbol, mode):
    k1 = await klines(symbol, "1h", 250)
    k15 = await klines(symbol, "15m", 250)
    k5 = await klines(symbol, "5m", 100)

    c1 = [float(x[4]) for x in k1]
    c15 = [float(x[4]) for x in k15]
    c5 = [float(x[4]) for x in k5]
    h15 = [float(x[2]) for x in k15]
    l15 = [float(x[3]) for x in k15]

    ema50_1 = ema(c1,50)[-1]
    ema200_1 = ema(c1,200)[-1]

    ema50_15 = ema(c15,50)[-1]
    ema200_15 = ema(c15,200)[-1]

    rsi5 = rsi(c5)[-1]
    atr15 = atr(h15,l15,c15)[-1]
    atr_pct = atr15 / c15[-1] * 100

    # STRICT vs BALANCED
    min_atr = 0.25 if mode=="STRICT" else 0.15
    rsi_filter = 55 if mode=="STRICT" else 50

    if atr_pct < min_atr:
        return None

    trend_up = ema50_1 > ema200_1 and ema50_15 > ema200_15
    trend_down = ema50_1 < ema200_1 and ema50_15 < ema200_15

    last = c5[-1]
    prev = c5[-2]

    # confirmation: candle closed in direction
    bullish = last > prev
    bearish = last < prev

    if trend_up and bullish and rsi5 > rsi_filter:
        tp = last * 1.01
        sl = last * 0.995
        return Signal(symbol,"LONG",last,tp,sl)

    if trend_down and bearish and rsi5 < 100-rsi_filter:
        tp = last * 0.99
        sl = last * 1.005
        return Signal(symbol,"SHORT",last,tp,sl)

    return None

# ================= RISK CONTROL =================

def today_key():
    t = int(time.time()) + TKM_OFFSET
    return time.strftime("%Y%m%d", time.gmtime(t))

def daily_stats(uid):
    con=db()
    cur=con.cursor()
    key=today_key()
    start=int(time.mktime(time.strptime(key,"%Y%m%d")))
    cur.execute("""
    SELECT status FROM signals
    WHERE user_id=? AND ts>=?
    """,(uid,start))
    rows=cur.fetchall()
    con.close()

    wins=0
    losses=0
    streak=0

    for r in rows:
        if r[0]=="WIN":
            streak=0
            wins+=1
        elif r[0]=="LOSE":
            streak+=1
            losses+=1

    return wins,losses,streak

# ================= EVALUATOR =================

async def evaluator_loop():
    while True:
        con=db()
        cur=con.cursor()
        cur.execute("SELECT id,symbol,side,tp,sl,ts FROM signals WHERE status='PENDING'")
        rows=cur.fetchall()
        con.close()

        for sid,sym,side,tp,sl,ts0 in rows:
            if time.time()-ts0 < EVAL_BARS*300:
                continue
            try:
                k = await klines(sym,"5m",limit=EVAL_BARS+5)
                highs=[float(x[2]) for x in k[:EVAL_BARS]]
                lows=[float(x[3]) for x in k[:EVAL_BARS]]

                if side=="LONG":
                    if max(highs)>=tp:
                        status="WIN"
                    else:
                        status="LOSE"
                else:
                    if min(lows)<=tp:
                        status="WIN"
                    else:
                        status="LOSE"

                con2=db()
                cur2=con2.cursor()
                cur2.execute("UPDATE signals SET status=? WHERE id=?",(status,sid))
                con2.commit()
                con2.close()
            except:
                continue

        await asyncio.sleep(30)
        # ================= SAVE SIGNAL =================

def save_signal(uid, s: Signal):
    con=db()
    cur=con.cursor()
    cur.execute("""
    INSERT INTO signals(ts,user_id,symbol,side,entry,tp,sl,status)
    VALUES(?,?,?,?,?,?,?,'PENDING')
    """,(int(time.time()),uid,s.symbol,s.side,s.entry,s.tp,s.sl))
    con.commit()
    con.close()

# ================= STATS =================

def total_stats(uid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    SELECT
    SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status='LOSE' THEN 1 ELSE 0 END),
    COUNT(*)
    FROM signals WHERE user_id=? AND status!='PENDING'
    """,(uid,))
    w,l,t=cur.fetchone()
    con.close()
    w=w or 0
    l=l or 0
    t=t or 0
    wr=(w/t*100) if t else 0
    return w,l,t,wr

def last_signals(uid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    SELECT symbol,side,status
    FROM signals WHERE user_id=?
    ORDER BY id DESC LIMIT 10
    """,(uid,))
    rows=cur.fetchall()
    con.close()
    return rows

# ================= KEYBOARD =================

def main_kb(uid):
    kb=InlineKeyboardBuilder()
    if active(uid):
        kb.button(text="📣 Сигнал",callback_data="signal")
        kb.button(text="📊 Статистика",callback_data="stats")
        kb.button(text="📜 Последние 10",callback_data="history")
        kb.button(text="🔄 Режим",callback_data="mode")
        kb.button(text="🤖 Авто ON/OFF",callback_data="auto")
    else:
        kb.button(text="📝 Запросить доступ",callback_data="request")
    kb.adjust(1)
    return kb.as_markup()

# ================= BOT =================

@dp.message(F.text=="/start")
async def start(m:Message):
    init_db()
    ensure_user(m.from_user.id)
    if active(m.from_user.id):
        await m.answer("✅ Доступ активен",reply_markup=main_kb(m.from_user.id))
    else:
        await m.answer("⛔️ Нет доступа",reply_markup=main_kb(m.from_user.id))

@dp.callback_query(F.data=="request")
async def request(cb:CallbackQuery):
    await cb.answer("Заявка отправлена админу")
    if ADMIN_ID:
        await bot.send_message(ADMIN_ID,f"Запрос от {cb.from_user.id}")

@dp.callback_query(F.data=="mode")
async def mode_toggle(cb:CallbackQuery):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT mode FROM users WHERE user_id=?",(cb.from_user.id,))
    mode=cur.fetchone()[0]
    new="BALANCED" if mode=="STRICT" else "STRICT"
    cur.execute("UPDATE users SET mode=? WHERE user_id=?",(new,cb.from_user.id))
    con.commit()
    con.close()
    await cb.answer(f"Режим: {new}")
    await cb.message.edit_reply_markup(reply_markup=main_kb(cb.from_user.id))

@dp.callback_query(F.data=="signal")
async def signal(cb:CallbackQuery):
    uid=cb.from_user.id
    if not active(uid):
        await cb.answer("Нет доступа",show_alert=True)
        return

    wins,losses,streak=daily_stats(uid)
    if streak>=MAX_LOSS_STREAK:
        await cb.answer("3 лося подряд. Пауза до конца дня.",show_alert=True)
        return

    if wins+losses>=DAILY_LIMIT:
        await cb.answer("Дневной лимит достигнут",show_alert=True)
        return

    con=db()
    cur=con.cursor()
    cur.execute("SELECT mode FROM users WHERE user_id=?",(uid,))
    mode=cur.fetchone()[0]
    con.close()

    syms=await top_symbols()
    for s in syms:
        sig=await build_advanced_signal(s,mode)
        if sig:
            save_signal(uid,sig)
            await cb.message.answer(
                f"{sig.symbol}\n{sig.side}\nEntry:{sig.entry}\nTP:{sig.tp}\nSL:{sig.sl}",
                reply_markup=main_kb(uid)
            )
            return

    await cb.answer("Нет сигнала сейчас")

@dp.callback_query(F.data=="stats")
async def stats(cb:CallbackQuery):
    w,l,t,wr=total_stats(cb.from_user.id)
    await cb.message.answer(
        f"WIN:{w}\nLOSE:{l}\nВсего:{t}\nWinrate:{wr:.1f}%",
        reply_markup=main_kb(cb.from_user.id)
    )

@dp.callback_query(F.data=="history")
async def history(cb:CallbackQuery):
    rows=last_signals(cb.from_user.id)
    text="Последние сигналы:\n"
    for s in rows:
        text+=f"{s[0]} {s[1]} {s[2]}\n"
    await cb.message.answer(text,reply_markup=main_kb(cb.from_user.id))

# ================= AUTOSCAN =================

async def autoscan():
    while True:
        con=db()
        cur=con.cursor()
        cur.execute("SELECT user_id,mode FROM users WHERE status='APPROVED' AND autoscan=1")
        users=cur.fetchall()
        con.close()

        syms=await top_symbols()

        for uid,mode in users:
            wins,losses,streak=daily_stats(uid)
            if streak>=MAX_LOSS_STREAK:
                continue
            if wins+losses>=DAILY_LIMIT:
                continue

            for s in syms:
                sig=await build_advanced_signal(s,mode)
                if sig:
                    save_signal(uid,sig)
                    await bot.send_message(
                        uid,
                        f"🤖 Автосигнал\n{sig.symbol}\n{sig.side}\nEntry:{sig.entry}\nTP:{sig.tp}\nSL:{sig.sl}"
                    )
                    break

        await asyncio.sleep(AUTO_INTERVAL*60)

# ================= MAIN =================

async def main():
    init_db()
    asyncio.create_task(evaluator_loop())
    asyncio.create_task(autoscan())
    await dp.start_polling(bot)

if __name__=="__main__":
    asyncio.run(main())
