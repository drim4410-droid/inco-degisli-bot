import os
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


# =========================
# ENV
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set it in Railway Variables.")

ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0

BINANCE_BASE = "https://api.binance.com"
DB_PATH = "signals.db"

# Top-20 (устойчивый стартовый список)
TOP20_USDT = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","TRXUSDT","TONUSDT","AVAXUSDT",
    "DOTUSDT","LINKUSDT","MATICUSDT","BCHUSDT","LTCUSDT",
    "UNIUSDT","ATOMUSDT","XLMUSDT","NEARUSDT","ETCUSDT",
]

# Таймфреймы
TF_HTF = "1h"
TF_TREND = "15m"
TF_ENTRY = "5m"

AUTO_SCAN_EVERY_MIN = 15

# Режимы (Нормальный даёт больше сигналов, Строгий — меньше, но “чище”)
MODE_PRESETS = {
    "normal": {
        "use_adx_filter": True,
        "use_atr_filter": True,
        "min_adx_1h": 16.0,
        "min_adx_15m": 16.0,
        "min_atr_pct_15m": 0.08,   # ATR% на 15m
        "max_ext_pct_15m": 1.30,   # не слишком далеко от EMA50 (15m)
        "cooldown_minutes": 15,
        "min_score_to_send": 7,    # порог для автосканера
        "evaluation_bars_5m": 3,   # проверка результата через N свечей 5m
        "tp_atr_mult": 1.4,
        "sl_atr_mult": 1.1,
        "cancel_atr_mult": 0.55,
        "near_zone_pct_5m": 0.30,  # насколько близко к EMA20/50 на 5m (%)
    },
    "strict": {
        "use_adx_filter": True,
        "use_atr_filter": True,
        "min_adx_1h": 18.0,
        "min_adx_15m": 18.0,
        "min_atr_pct_15m": 0.12,
        "max_ext_pct_15m": 0.90,
        "cooldown_minutes": 20,
        "min_score_to_send": 8,
        "evaluation_bars_5m": 3,
        "tp_atr_mult": 1.6,
        "sl_atr_mult": 1.2,
        "cancel_atr_mult": 0.60,
        "near_zone_pct_5m": 0.25,
    }
}

# анти-спам сигналов (по символу)
LAST_SIGNAL_TS: Dict[str, float] = {}

# =========================
# DATA STRUCTURES
# =========================
@dataclass
class Signal:
    symbol: str
    side: str      # LONG / SHORT
    price_now: float
    score: int
    tf: str
    reason: str
    tp: float
    sl: float
    cancel_if: str


# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def db_init():
    con = db()
    cur = con.cursor()

    # users: доступ + режим + автосканер
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            created_ts INTEGER NOT NULL,
            access_until INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/BANNED
            autoscan INTEGER NOT NULL DEFAULT 1,     -- 1/0
            mode TEXT NOT NULL DEFAULT 'normal'      -- normal/strict
        );
    """)

    # заявки
    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING'   -- PENDING/APPROVED/REJECTED
        );
    """)

    # сигналы
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_user ON signals(user_id, ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_pending ON signals(status, ts);")

    con.commit()
    con.close()

def ensure_user(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    if not row:
        cur.execute(
            "INSERT INTO users(user_id, created_ts, access_until, status, autoscan, mode) VALUES(?,?,?,?,?,?)",
            (user_id, int(time.time()), 0, "PENDING", 1, "normal")
        )
    con.commit()
    con.close()

def get_user(user_id: int) -> dict:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id, created_ts, access_until, status, autoscan, mode FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {"user_id": user_id, "created_ts": int(time.time()), "access_until": 0, "status": "PENDING", "autoscan": 1, "mode": "normal"}
    return {"user_id": row[0], "created_ts": row[1], "access_until": row[2], "status": row[3], "autoscan": row[4], "mode": row[5]}

def set_autoscan(user_id: int, enabled: bool):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET autoscan=? WHERE user_id=?", (1 if enabled else 0, user_id))
    con.commit()
    con.close()

def set_mode(user_id: int, mode: str):
    if mode not in MODE_PRESETS:
        mode = "normal"
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET mode=? WHERE user_id=?", (mode, user_id))
    con.commit()
    con.close()

def is_access_active(user_id: int) -> Tuple[bool, int]:
    u = get_user(user_id)
    now = int(time.time())
    return (u["status"] == "APPROVED" and u["access_until"] > now), u["access_until"]

def create_request(user_id: int) -> bool:
    """
    Создаёт заявку, если нет активной PENDING заявки.
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT id FROM access_requests WHERE user_id=? AND status='PENDING' ORDER BY ts DESC LIMIT 1", (user_id,))
    if cur.fetchone():
        con.close()
        return False
    cur.execute("INSERT INTO access_requests(ts, user_id, status) VALUES(?,?, 'PENDING')", (int(time.time()), user_id))
    con.commit()
    con.close()
    return True

def list_pending_requests(limit: int = 20) -> List[int]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM access_requests WHERE status='PENDING' ORDER BY ts ASC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows]

def approve_user(user_id: int, days: int):
    until = int(time.time()) + days * 24 * 3600
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET status='APPROVED', access_until=? WHERE user_id=?", (until, user_id))
    cur.execute("UPDATE access_requests SET status='APPROVED' WHERE user_id=? AND status='PENDING'", (user_id,))
    con.commit()
    con.close()
    return until

def reject_user_request(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE access_requests SET status='REJECTED' WHERE user_id=? AND status='PENDING'", (user_id,))
    con.commit()
    con.close()

def db_add_signal(user_id: int, sig: Signal, eval_after_bars: int):
    con = db()
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
    con = db()
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


# =========================
# BINANCE HELPERS
# =========================
async def fetch_klines(symbol: str, interval: str, limit: int = 300) -> Tuple[List[float], List[float], List[float], List[float], List[int]]:
    """
    Returns: open[], high[], low[], close[], close_time_ms[]
    """
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    o = [float(x[1]) for x in data]
    h = [float(x[2]) for x in data]
    l = [float(x[3]) for x in data]
    c = [float(x[4]) for x in data]
    ct = [int(x[6]) for x in data]  # close time ms
    return o, h, l, c, ct

def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
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
    alpha = 1 / period
    avg_g, avg_l = [], []
    g = gains[0]
    l = losses[0]
    for i in range(len(values)):
        g = alpha * gains[i] + (1 - alpha) * g
        l = alpha * losses[i] + (1 - alpha) * l
        avg_g.append(g)
        avg_l.append(l)

    out = []
    for gg, ll in zip(avg_g, avg_l):
        if ll == 0:
            out.append(100.0 if gg > 0 else 50.0)
        else:
            rs = gg / ll
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

    alpha = 1 / period
    atr_s, p_s, m_s = [], [], []
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
            continue
        plus_di[i] = 100 * (p_s[i] / atr_s[i])
        minus_di[i] = 100 * (m_s[i] / atr_s[i])

    dx = [0.0]*len(close)
    for i in range(len(close)):
        denom = plus_di[i] + minus_di[i]
        dx[i] = 0.0 if denom == 0 else 100 * (abs(plus_di[i] - minus_di[i]) / denom)

    adx_out = []
    a = dx[0]
    for i in range(len(dx)):
        a = alpha * dx[i] + (1 - alpha) * a
        adx_out.append(a)
    return adx_out

def pct_dist(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return abs(a - b) / b * 100.0

def on_cooldown(symbol: str, now_ts: float, cooldown_minutes: int) -> bool:
    last = LAST_SIGNAL_TS.get(symbol)
    if last is None:
        return False
    return (now_ts - last) < (cooldown_minutes * 60)


# =========================
# STRATEGY: 1H -> 15m -> 5m
# =========================
def compute_signal(symbol: str,
                   mode: str,
                   h1: Tuple[List[float],List[float],List[float],List[float]],
                   m15: Tuple[List[float],List[float],List[float],List[float]],
                   m5: Tuple[List[float],List[float],List[float],List[float]]) -> Tuple[Optional[Signal], Optional[str]]:
    """
    Returns (Signal or None, reject_reason)
    reject_reason is a short string for diagnostics.
    """
    cfg = MODE_PRESETS.get(mode, MODE_PRESETS["normal"])

    o1, hi1, lo1, c1 = h1
    o15, hi15, lo15, c15 = m15
    o5, hi5, lo5, c5 = m5

    if len(c1) < 210 or len(c15) < 210 or len(c5) < 60:
        return None, "мало данных"

    score = 0
    reasons = []

    # 1H trend
    ema50_1 = ema(c1, 50)[-1]
    ema200_1 = ema(c1, 200)[-1]
    adx_1 = adx(hi1, lo1, c1, 14)[-1]
    up_1 = ema50_1 > ema200_1
    dn_1 = ema50_1 < ema200_1

    if cfg["use_adx_filter"] and adx_1 < cfg["min_adx_1h"]:
        return None, "1H ADX низкий"
    if not (up_1 or dn_1):
        return None, "1H нет тренда"
    score += 3
    reasons.append(f"1H trend + ADX {adx_1:.1f}")

    # 15m confirm + volatility + not extended
    ema50_15 = ema(c15, 50)[-1]
    ema200_15 = ema(c15, 200)[-1]
    adx_15 = adx(hi15, lo15, c15, 14)[-1]
    atr_15 = atr(hi15, lo15, c15, 14)[-1]
    price_15 = c15[-1]
    atr_pct_15 = (atr_15 / price_15) * 100 if price_15 else 0.0

    up_15 = ema50_15 > ema200_15
    dn_15 = ema50_15 < ema200_15

    if up_1 and up_15:
        direction = "LONG"
        score += 2
        reasons.append("15m aligned UP")
    elif dn_1 and dn_15:
        direction = "SHORT"
        score += 2
        reasons.append("15m aligned DOWN")
    else:
        return None, "15m не совпало с 1H"

    if cfg["use_adx_filter"] and adx_15 < cfg["min_adx_15m"]:
        return None, "15m ADX низкий"
    score += 1
    reasons.append(f"15m ADX {adx_15:.1f}")

    if cfg["use_atr_filter"] and atr_pct_15 < cfg["min_atr_pct_15m"]:
        return None, "15m ATR низкий"
    score += 1
    reasons.append(f"15m ATR {atr_pct_15:.2f}%")

    ext = pct_dist(price_15, ema50_15)
    if ext > cfg["max_ext_pct_15m"]:
        return None, "15m цена далеко от EMA50"
    score += 1
    reasons.append(f"15m ext {ext:.2f}%")

    # 5m entry: pullback + candle + RSI momentum
    price_now = c5[-1]
    ema20_5 = ema(c5, 20)[-1]
    ema50_5 = ema(c5, 50)[-1]
    rsi_5 = rsi(c5, 14)
    prev_r, last_r = rsi_5[-2], rsi_5[-1]

    near_zone = (pct_dist(price_now, ema20_5) < cfg["near_zone_pct_5m"]) or (pct_dist(price_now, ema50_5) < cfg["near_zone_pct_5m"])
    if not near_zone:
        return None, "5m нет отката к EMA"
    score += 1
    reasons.append("5m pullback EMA")

    last_open, last_close = o5[-1], c5[-1]
    bullish = last_close > last_open
    bearish = last_close < last_open
    rsi_up = last_r > prev_r
    rsi_dn = last_r < prev_r

    if direction == "LONG":
        if not (bullish and rsi_up and last_r > 48):
            return None, "5m нет подтверждения LONG"
        score += 2
        reasons.append("5m bullish + RSI up")
    else:
        if not (bearish and rsi_dn and last_r < 52):
            return None, "5m нет подтверждения SHORT"
        score += 2
        reasons.append("5m bearish + RSI down")

    # Risk guidance (market entry now)
    atr_5 = atr(hi5, lo5, c5, 14)[-1]
    if atr_5 <= 0:
        return None, "ATR 5m = 0"

    tp_mult = cfg["tp_atr_mult"]
    sl_mult = cfg["sl_atr_mult"]
    cancel_mult = cfg["cancel_atr_mult"]

    if direction == "LONG":
        sl = price_now - sl_mult * atr_5
        tp = price_now + tp_mult * atr_5
        cancel = f"отменить, если до входа цена упала ниже {price_now - cancel_mult * atr_5:.6f}"
    else:
        sl = price_now + sl_mult * atr_5
        tp = price_now - tp_mult * atr_5
        cancel = f"отменить, если до входа цена выросла выше {price_now + cancel_mult * atr_5:.6f}"

    sig = Signal(
        symbol=symbol,
        side=direction,
        price_now=float(price_now),
        score=int(score),
        tf=f"{TF_HTF}+{TF_TREND}+{TF_ENTRY}",
        reason=" | ".join(reasons),
        tp=float(tp),
        sl=float(sl),
        cancel_if=cancel,
    )
    return sig, None


# =========================
# UI
# =========================
def kb_main(user_id: int) -> InlineKeyboardBuilder:
    u = get_user(user_id)
    active, until = is_access_active(user_id)
    mode = u["mode"]
    autoscan = bool(u["autoscan"])

    kb = InlineKeyboardBuilder()

    if active:
        kb.button(text="📣 Сигнал (Top-20)", callback_data="signal_now")
        kb.button(text=f"🤖 Автосканер: {'ON' if autoscan else 'OFF'}", callback_data="toggle_auto")
        kb.button(text=f"🧠 Режим: {'СТРОГИЙ' if mode=='strict' else 'НОРМ'}", callback_data="toggle_mode")
        kb.button(text="📊 Статистика", callback_data="stats")
    else:
        kb.button(text="📝 Запросить доступ", callback_data="request_access")
        kb.button(text="ℹ️ Как это работает", callback_data="howitworks")

    kb.adjust(1)
    return kb

def kb_admin_request(user_id: int) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить +7 дней", callback_data=f"approve:{user_id}:7")
    kb.button(text="✅ Одобрить +15 дней", callback_data=f"approve:{user_id}:15")
    kb.button(text="✅ Одобрить +30 дней", callback_data=f"approve:{user_id}:30")
    kb.button(text="❌ Отклонить", callback_data=f"reject:{user_id}")
    kb.adjust(1)
    return kb

def fmt_until(ts: int) -> str:
    if ts <= 0:
        return "нет"
    # просто unix -> human (UTC)
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))

def format_signal(sig: Signal) -> str:
    # Вход по рынку (market now), НЕ лимитка
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


# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID

@dp.message(F.text.in_({"/myid", "myid"}))
async def myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.message(F.text.in_({"/start", "start"}))
async def start(m: Message):
    db_init()
    uid = m.from_user.id
    ensure_user(uid)
    u = get_user(uid)
    active, until = is_access_active(uid)

    if active:
        await m.answer(
            "✅ Доступ активен\n"
            f"До: <b>{fmt_until(until)}</b>\n"
            f"Режим: <b>{'СТРОГИЙ' if u['mode']=='strict' else 'НОРМАЛЬНЫЙ'}</b>\n"
            f"Автосканер: <b>{'ON' if u['autoscan'] else 'OFF'}</b> (каждые {AUTO_SCAN_EVERY_MIN} минут)\n\n"
            "Жми 📣 для сигнала или жди автосканер.",
            reply_markup=kb_main(uid).as_markup()
        )
    else:
        await m.answer(
            "⛔️ Доступ по одобрению.\n\n"
            "Нажми «Запросить доступ», я получу заявку и одобрю на 7/15/30 дней.\n"
            "После одобрения бот начнёт давать сигналы и статистику.",
            reply_markup=kb_main(uid).as_markup()
        )

@dp.message(F.text.in_({"/admin", "admin"}))
async def admin(m: Message):
    if not is_admin(m.from_user.id):
        return
    db_init()
    pending = list_pending_requests(30)
    if not pending:
        await m.answer("Заявок нет.")
        return
    text = "🛂 <b>Заявки PENDING</b>\n\n" + "\n".join([f"• <code>{uid}</code>" for uid in pending[:20]])
    await m.answer(text)
    # отправим кнопки отдельно на каждого (чтобы одним кликом одобрять)
    for uid in pending[:10]:
        await m.answer(f"Заявка от <code>{uid}</code>", reply_markup=kb_admin_request(uid).as_markup())

@dp.callback_query(F.data == "howitworks")
async def how_it_works(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer(
        "ℹ️ <b>Как бот даёт сигналы</b>\n"
        "• Сканирует Top-20 USDT\n"
        "• Фильтр направления: 1H + 15m (EMA50/200)\n"
        "• Фильтр качества: ADX + ATR (чтобы не лезть во флет)\n"
        "• Вход: 5m откат к EMA + подтверждение свечой и RSI\n\n"
        "Сигнал = <b>MARKET NOW</b> (вход сразу), плюс ориентиры TP/SL.\n"
        "Есть автосканер и статистика по результатам.",
        reply_markup=kb_main(cb.from_user.id).as_markup()
    )

@dp.callback_query(F.data == "request_access")
async def request_access(cb: CallbackQuery):
    uid = cb.from_user.id
    ensure_user(uid)
    ok = create_request(uid)
    await cb.answer("Ок")
    if ok:
        await cb.message.answer(
            "✅ Заявка отправлена. Жди одобрения.",
            reply_markup=kb_main(uid).as_markup()
        )
        # notify admin
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🛂 Новая заявка на доступ от <code>{uid}</code>",
                    reply_markup=kb_admin_request(uid).as_markup()
                )
            except Exception:
                pass
    else:
        await cb.message.answer(
            "⏳ У тебя уже есть активная заявка. Жди одобрения.",
            reply_markup=kb_main(uid).as_markup()
        )

@dp.callback_query(F.data.startswith("approve:"))
async def approve(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    if len(parts) != 3:
        await cb.answer("Ошибка", show_alert=True)
        return
    uid = int(parts[1])
    days = int(parts[2])
    until = approve_user(uid, days)
    await cb.answer("Одобрено ✅")
    await cb.message.answer(f"✅ Одобрено для <code>{uid}</code> на {days} дней (до {fmt_until(until)}).")
    # notify user
    try:
        ensure_user(uid)
        await bot.send_message(
            uid,
            "✅ Доступ одобрен!\n"
            f"До: <b>{fmt_until(until)}</b>\n\n"
            "Нажми /start",
        )
    except Exception:
        pass

@dp.callback_query(F.data.startswith("reject:"))
async def reject(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    if len(parts) != 2:
        await cb.answer("Ошибка", show_alert=True)
        return
    uid = int(parts[1])
    reject_user_request(uid)
    await cb.answer("Отклонено")
    await cb.message.answer(f"❌ Отклонено для <code>{uid}</code>.")
    try:
        await bot.send_message(uid, "❌ Доступ не одобрен.")
    except Exception:
        pass

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(cb: CallbackQuery):
    uid = cb.from_user.id
    active, _ = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return
    u = get_user(uid)
    new_val = not bool(u["autoscan"])
    set_autoscan(uid, new_val)
    await cb.answer("Ок")
    await cb.message.answer(
        f"🤖 Автосканер теперь: <b>{'ON' if new_val else 'OFF'}</b>",
        reply_markup=kb_main(uid).as_markup()
    )

@dp.callback_query(F.data == "toggle_mode")
async def toggle_mode(cb: CallbackQuery):
    uid = cb.from_user.id
    active, _ = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return
    u = get_user(uid)
    new_mode = "strict" if u["mode"] != "strict" else "normal"
    set_mode(uid, new_mode)
    await cb.answer("Ок")
    await cb.message.answer(
        f"🧠 Режим теперь: <b>{'СТРОГИЙ' if new_mode=='strict' else 'НОРМАЛЬНЫЙ'}</b>",
        reply_markup=kb_main(uid).as_markup()
    )

@dp.callback_query(F.data == "stats")
async def stats(cb: CallbackQuery):
    uid = cb.from_user.id
    active, until = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return
    db_init()
    win, lose, neutral, total = db_stats(uid)
    wr = (win / total * 100) if total else 0.0
    await cb.answer()
    await cb.message.answer(
        "📊 <b>Статистика</b>\n"
        f"Доступ до: <b>{fmt_until(until)}</b>\n"
        f"Всего: <b>{total}</b>\n"
        f"WIN: <b>{win}</b>\n"
        f"LOSE: <b>{lose}</b>\n"
        f"NEUTRAL: <b>{neutral}</b>\n"
        f"Winrate: <b>{wr:.1f}%</b>\n\n"
        "⚠️ Смотри минимум 30–50 сигналов.",
        reply_markup=kb_main(uid).as_markup()
    )

@dp.callback_query(F.data == "signal_now")
async def signal_now(cb: CallbackQuery):
    uid = cb.from_user.id
    active, _ = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return

    await cb.answer("Сканирую Top-20…")
    await send_best_signal(uid, cb.message, only_if_strong=False)


# =========================
# CORE: scanning + diagnostics
# =========================
async def send_best_signal(user_id: int, target: Message, only_if_strong: bool):
    u = get_user(user_id)
    mode = u["mode"]
    cfg = MODE_PRESETS.get(mode, MODE_PRESETS["normal"])

    now_ts = asyncio.get_running_loop().time()

    best: Optional[Signal] = None
    diag = {
        "cooldown": 0,
        "exception": 0,
        "reject:1h_adx": 0,
        "reject:1h_trend": 0,
        "reject:15_align": 0,
        "reject:15_adx": 0,
        "reject:15_atr": 0,
        "reject:15_ext": 0,
        "reject:5_pullback": 0,
        "reject:5_confirm": 0,
        "reject:other": 0,
    }

    for sym in TOP20_USDT:
        if on_cooldown(sym, now_ts, cfg["cooldown_minutes"]):
            diag["cooldown"] += 1
            continue

        try:
            o1,h1,l1,c1,_ = await fetch_klines(sym, TF_HTF, 300)
            o15,h15,l15,c15,_ = await fetch_klines(sym, TF_TREND, 300)
            o5,h5,l5,c5,_ = await fetch_klines(sym, TF_ENTRY, 300)

            sig, rej = compute_signal(sym, mode, (o1,h1,l1,c1), (o15,h15,l15,c15), (o5,h5,l5,c5))

            if sig:
                if (best is None) or (sig.score > best.score):
                    best = sig
            else:
                # диагностика причин (укрупнённо)
                if rej:
                    r = rej.lower()
                    if "1h adx" in r:
                        diag["reject:1h_adx"] += 1
                    elif "1h" in r and "тренд" in r:
                        diag["reject:1h_trend"] += 1
                    elif "15m не совпало" in r:
                        diag["reject:15_align"] += 1
                    elif "15m adx" in r:
                        diag["reject:15_adx"] += 1
                    elif "15m atr" in r:
                        diag["reject:15_atr"] += 1
                    elif "далеко от ema50" in r:
                        diag["reject:15_ext"] += 1
                    elif "нет отката" in r:
                        diag["reject:5_pullback"] += 1
                    elif "подтверждения" in r:
                        diag["reject:5_confirm"] += 1
                    else:
                        diag["reject:other"] += 1
                else:
                    diag["reject:other"] += 1

        except Exception:
            diag["exception"] += 1
            continue

    if not best:
        await target.answer(
            "Сейчас нет качественного сигнала по Top-20.\n\n"
            "<b>Диагностика (почему отвалились монеты):</b>\n"
            f"• cooldown: {diag['cooldown']}\n"
            f"• ошибки API: {diag['exception']}\n"
            f"• 1H ADX низкий: {diag['reject:1h_adx']}\n"
            f"• 1H нет тренда: {diag['reject:1h_trend']}\n"
            f"• 15m не совпало с 1H: {diag['reject:15_align']}\n"
            f"• 15m ADX низкий: {diag['reject:15_adx']}\n"
            f"• 15m ATR низкий: {diag['reject:15_atr']}\n"
            f"• 15m далеко от EMA50: {diag['reject:15_ext']}\n"
            f"• 5m нет отката: {diag['reject:5_pullback']}\n"
            f"• 5m нет подтверждения: {diag['reject:5_confirm']}\n"
            f"• другое: {diag['reject:other']}\n\n"
            "Попробуй позже или переключи режим (Норм/Строгий).",
            reply_markup=kb_main(user_id).as_markup()
        )
        return

    # Автосканер присылает только если сильный
    if only_if_strong and best.score < cfg["min_score_to_send"]:
        return

    LAST_SIGNAL_TS[best.symbol] = now_ts
    db_add_signal(user_id, best, cfg["evaluation_bars_5m"])

    await target.answer(format_signal(best), reply_markup=kb_main(user_id).as_markup())


# =========================
# Evaluator: honest TP/SL touch check (high/low)
# =========================
async def evaluator_loop():
    """
    Каждые 30 секунд проверяем PENDING сигналы.
    Через N свечей 5m смотрим, касалась ли цена TP или SL (по high/low).
    Важно: порядок касаний внутри свечи неизвестен, поэтому:
      - если касались и TP и SL в одном окне -> NEUTRAL
      - если только TP -> WIN
      - если только SL -> LOSE
      - если ни того ни другого -> NEUTRAL
    """
    while True:
        try:
            con = db()
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

            now = int(time.time())
            for (sid, uid, sym, side, entry, tp, sl, ts0, bars) in rows:
                need_seconds = int(bars) * 5 * 60
                if now - int(ts0) < need_seconds:
                    continue

                try:
                    # берём последние (bars + 3) свечей 5m
                    lim = max(20, int(bars) + 6)
                    o,h,l,c,ct = await fetch_klines(sym, TF_ENTRY, lim)

                    # возьмём окно последних bars свечей
                    window_h = h[-int(bars):]
                    window_l = l[-int(bars):]
                    last_price = float(c[-1])

                    hit_tp = False
                    hit_sl = False

                    if side == "LONG":
                        hit_tp = max(window_h) >= tp
                        hit_sl = min(window_l) <= sl
                    else:
                        hit_tp = min(window_l) <= tp
                        hit_sl = max(window_h) >= sl

                    if hit_tp and (not hit_sl):
                        status = "WIN"
                    elif hit_sl and (not hit_tp):
                        status = "LOSE"
                    else:
                        status = "NEUTRAL"

                    con = db()
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


# =========================
# Autoscan loop (approved only)
# =========================
async def autoscan_loop():
    while True:
        try:
            # берём пользователей с autoscan=1 и активным доступом
            con = db()
            cur = con.cursor()
            cur.execute("SELECT user_id FROM users WHERE autoscan=1 AND status='APPROVED'")
            users = [r[0] for r in cur.fetchall()]
            con.close()

            now = int(time.time())
            for uid in users:
                active, _ = is_access_active(uid)
                if not active:
                    continue

                u = get_user(uid)
                mode = u["mode"]
                cfg = MODE_PRESETS.get(mode, MODE_PRESETS["normal"])

                # создаём "вставку" в личку через bot.send_message
                # но нам нужно target Message для send_best_signal — сделаем упрощённо:
                # посчитаем лучший сигнал и отправим напрямую.
                try:
                    # вычислим лучший сигнал (strong only)
                    # используем тот же метод, но с временным target через send_message:
                    # Чтобы не дублировать код — просто отправим “пробник” через искусственный объект нельзя,
                    # поэтому ниже делаем маленький inline scan.
                    now_ts = asyncio.get_running_loop().time()
                    best: Optional[Signal] = None

                    for sym in TOP20_USDT:
                        if on_cooldown(sym, now_ts, cfg["cooldown_minutes"]):
                            continue
                        try:
                            o1,h1,l1,c1,_ = await fetch_klines(sym, TF_HTF, 300)
                            o15,h15,l15,c15,_ = await fetch_klines(sym, TF_TREND, 300)
                            o5,h5,l5,c5,_ = await fetch_klines(sym, TF_ENTRY, 300)
                            sig, _rej = compute_signal(sym, mode, (o1,h1,l1,c1), (o15,h15,l15,c15), (o5,h5,l5,c5))
                            if sig:
                                if (best is None) or (sig.score > best.score):
                                    best = sig
                        except Exception:
                            continue

                    if best and best.score >= cfg["min_score_to_send"]:
                        LAST_SIGNAL_TS[best.symbol] = now_ts
                        db_add_signal(uid, best, cfg["evaluation_bars_5m"])
                        await bot.send_message(uid, format_signal(best), reply_markup=kb_main(uid).as_markup())

                except Exception:
                    continue

        except Exception:
            pass

        await asyncio.sleep(AUTO_SCAN_EVERY_MIN * 60)


# =========================
# MAIN
# =========================
async def main():
    db_init()
    asyncio.create_task(evaluator_loop())
    asyncio.create_task(autoscan_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
