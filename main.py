import os
import time
import asyncio
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

import httpx
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder


# =========================
# ENV / CONFIG
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set it in Railway Variables.")

BINANCE = "https://api.binance.com"
DB_PATH = "signals.db"

TOP_N = 100
MIN_QUOTE_VOL_USDT_24H = 50_000_000.0
MIN_PRICE = 0.01

AUTO_SCAN_EVERY_MIN = 15
TOPLIST_CACHE_TTL = 30 * 60  # 30 min

# Winrate engine
EVAL_INTERVAL = "5m"
EVAL_BARS = 3            # evaluate after N 5m bars
EVAL_LOOP_SECONDS = 30   # how often to check pending

# Signal model (simple базовый вход, без лимиток)
TP_PCT = 1.0
SL_PCT = 0.5

# If neither TP nor SL hit in window -> treat as LOSE (no NEUTRAL)
FORCE_BINARY_RESULT = True

# HTTP concurrency
HTTP_SEM = asyncio.Semaphore(6)
HTTP_CLIENT: Optional[httpx.AsyncClient] = None


# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def db_init():
    con = db()
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            created_ts INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/BANNED
            access_until INTEGER NOT NULL DEFAULT 0,
            autoscan INTEGER NOT NULL DEFAULT 1
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_requests(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING' -- PENDING/APPROVED/REJECTED
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,         -- LONG/SHORT
            entry REAL NOT NULL,
            tp REAL NOT NULL,
            sl REAL NOT NULL,
            score INTEGER NOT NULL,
            eval_bars INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/WIN/LOSE
            resolved_ts INTEGER,
            resolved_price REAL
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_pending ON signals(status, ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_user ON signals(user_id, ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol, ts);")

    con.commit()
    con.close()

def ensure_user(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users(user_id, created_ts, status, access_until, autoscan) VALUES(?,?,?,?,?)",
            (user_id, int(time.time()), "PENDING", 0, 1)
        )
    con.commit()
    con.close()

def get_user(user_id: int) -> dict:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id, status, access_until, autoscan FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {"user_id": user_id, "status": "PENDING", "access_until": 0, "autoscan": 1}
    return {"user_id": row[0], "status": row[1], "access_until": row[2], "autoscan": row[3]}

def set_autoscan(user_id: int, enabled: bool):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET autoscan=? WHERE user_id=?", (1 if enabled else 0, user_id))
    con.commit()
    con.close()

def is_access_active(user_id: int) -> Tuple[bool, int]:
    u = get_user(user_id)
    now = int(time.time())
    return (u["status"] == "APPROVED" and int(u["access_until"]) > now), int(u["access_until"])

def create_request(user_id: int) -> bool:
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

def approve_user(user_id: int, days: int) -> int:
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

def db_add_signal(user_id: int, symbol: str, side: str, entry: float, tp: float, sl: float, score: int, eval_bars: int):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO signals(ts, user_id, symbol, side, entry, tp, sl, score, eval_bars, status)
        VALUES(?,?,?,?,?,?,?,?,?, 'PENDING')
    """, (int(time.time()), user_id, symbol, side, float(entry), float(tp), float(sl), int(score), int(eval_bars)))
    con.commit()
    con.close()

def stats_total(user_id: int) -> Tuple[int, int, int, float, int]:
    """
    Returns (win, lose, total_done, winrate%, pending)
    """
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT
          SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END),
          SUM(CASE WHEN status='LOSE' THEN 1 ELSE 0 END),
          SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END),
          COUNT(*)
        FROM signals
        WHERE user_id=?
    """, (user_id,))
    row = cur.fetchone()
    con.close()
    w = int(row[0] or 0)
    l = int(row[1] or 0)
    p = int(row[2] or 0)
    total = int(row[3] or 0)
    done = w + l
    wr = (w / done * 100.0) if done > 0 else 0.0
    return w, l, done, wr, p

def stats_by_coin(user_id: int, min_trades: int = 5, limit: int = 10) -> List[Tuple[str, int, float]]:
    """
    Top coins by winrate (only WIN/LOSE), min trades threshold.
    """
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT symbol,
               SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) AS w,
               SUM(CASE WHEN status='LOSE' THEN 1 ELSE 0 END) AS l
        FROM signals
        WHERE user_id=? AND status IN ('WIN','LOSE')
        GROUP BY symbol
        HAVING (w + l) >= ?
    """, (user_id, min_trades))
    rows = cur.fetchall()
    con.close()

    out = []
    for sym, w, l in rows:
        w = int(w or 0)
        l = int(l or 0)
        t = w + l
        wr = (w / t * 100.0) if t > 0 else 0.0
        out.append((sym, t, wr))

    out.sort(key=lambda x: (x[2], x[1]), reverse=True)
    return out[:limit]


# =========================
# HTTP / BINANCE
# =========================
async def get_client() -> httpx.AsyncClient:
    global HTTP_CLIENT
    if HTTP_CLIENT is None:
        HTTP_CLIENT = httpx.AsyncClient(timeout=15)
    return HTTP_CLIENT

async def http_get_json(url: str, params: Optional[dict] = None):
    async with HTTP_SEM:
        client = await get_client()
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

def symbol_allowed(sym: str) -> bool:
    if not sym.endswith("USDT"):
        return False
    bad_suffixes = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
    for s in bad_suffixes:
        if sym.endswith(s):
            return False
    # exclude stable-like bases (optional)
    base = sym[:-4]
    if base in {"USDC", "TUSD", "FDUSD", "USDP", "DAI", "BUSD"}:
        return False
    return True

TOPLIST_CACHE = {"ts": 0.0, "symbols": []}  # simple cache

async def top_symbols() -> List[str]:
    now = time.time()
    if TOPLIST_CACHE["symbols"] and (now - TOPLIST_CACHE["ts"] < TOPLIST_CACHE_TTL):
        return TOPLIST_CACHE["symbols"]

    data = await http_get_json(f"{BINANCE}/api/v3/ticker/24hr")
    arr = []
    for i in data:
        sym = i.get("symbol", "")
        if not symbol_allowed(sym):
            continue
        try:
            vol = float(i.get("quoteVolume", "0") or 0.0)
            price = float(i.get("lastPrice", "0") or 0.0)
        except Exception:
            continue
        if vol >= MIN_QUOTE_VOL_USDT_24H and price >= MIN_PRICE:
            arr.append((sym, vol))

    arr.sort(key=lambda x: x[1], reverse=True)
    syms = [x[0] for x in arr[:TOP_N]]

    TOPLIST_CACHE["ts"] = now
    TOPLIST_CACHE["symbols"] = syms
    return syms

async def klines(symbol: str, interval: str, start_ms: Optional[int] = None, limit: int = 100) -> List[list]:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    return await http_get_json(f"{BINANCE}/api/v3/klines", params=params)


# =========================
# SIGNAL LOGIC (simple)
# =========================
@dataclass
class Signal:
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float
    score: int

async def build_signal(symbol: str) -> Optional[Signal]:
    # Simple 15m momentum baseline
    k = await klines(symbol, "15m", limit=60)
    closes = [float(x[4]) for x in k]
    if len(closes) < 3:
        return None

    last = closes[-1]
    prev = closes[-2]

    score = 8
    move = abs(last - prev) / last if last else 0.0
    if move > 0.002:
        score += 1

    if last > prev:
        side = "LONG"
        tp = last * (1 + TP_PCT / 100)
        sl = last * (1 - SL_PCT / 100)
    else:
        side = "SHORT"
        tp = last * (1 - TP_PCT / 100)
        sl = last * (1 + SL_PCT / 100)

    return Signal(symbol=symbol, side=side, entry=last, tp=tp, sl=sl, score=score)


# =========================
# EVALUATOR (WIN / LOSE only)
# =========================
async def evaluator_loop():
    while True:
        try:
            con = db()
            cur = con.cursor()
            cur.execute("""
                SELECT id, symbol, side, tp, sl, ts, eval_bars
                FROM signals
                WHERE status='PENDING'
                ORDER BY ts ASC
                LIMIT 80
            """)
            rows = cur.fetchall()
            con.close()

            now = int(time.time())

            for (sid, sym, side, tp, sl, ts0, bars) in rows:
                need_seconds = int(bars) * 5 * 60
                if now - int(ts0) < need_seconds:
                    continue

                try:
                    start_ms = int(ts0) * 1000
                    k = await klines(sym, EVAL_INTERVAL, start_ms=start_ms, limit=int(bars) + 6)
                    if len(k) < int(bars):
                        continue

                    window = k[:int(bars)]
                    highs = [float(x[2]) for x in window]
                    lows = [float(x[3]) for x in window]
                    last_price = float(window[-1][4])

                    hit_tp = False
                    hit_sl = False

                    if side == "LONG":
                        hit_tp = max(highs) >= float(tp)
                        hit_sl = min(lows) <= float(sl)
                    else:
                        hit_tp = min(lows) <= float(tp)
                        hit_sl = max(highs) >= float(sl)

                    # binary result:
                    # - if both hit within window -> LOSE (консервативно)
                    # - if only TP -> WIN
                    # - else -> LOSE
                    if hit_tp and not hit_sl:
                        status = "WIN"
                    else:
                        status = "LOSE"

                    con2 = db()
                    cur2 = con2.cursor()
                    cur2.execute("""
                        UPDATE signals
                        SET status=?, resolved_ts=?, resolved_price=?
                        WHERE id=?
                    """, (status, int(time.time()), float(last_price), sid))
                    con2.commit()
                    con2.close()

                except Exception:
                    continue

        except Exception:
            pass

        await asyncio.sleep(EVAL_LOOP_SECONDS)


# =========================
# ACCESS / ADMIN
# =========================
def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID

def fmt_until(ts: int) -> str:
    if ts <= 0:
        return "нет"
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))


# =========================
# KEYBOARDS
# =========================
def kb_user(user_id: int):
    u = get_user(user_id)
    active, until = is_access_active(user_id)

    kb = InlineKeyboardBuilder()

    if active:
        kb.button(text="📣 Сигнал", callback_data="sig_now")
        kb.button(text="📊 Статистика", callback_data="stats")
        kb.button(text="🪙 По монетам", callback_data="stats_coins")
        kb.button(text=f"🤖 Автосканер: {'ON' if u['autoscan'] else 'OFF'}", callback_data="toggle_auto")
    else:
        kb.button(text="📝 Запросить доступ", callback_data="request_access")
        kb.button(text="ℹ️ Как работает", callback_data="how")
    kb.adjust(1)
    return kb.as_markup()

def kb_admin_request(req_user_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ +7 дней", callback_data=f"approve:{req_user_id}:7")
    kb.button(text="✅ +15 дней", callback_data=f"approve:{req_user_id}:15")
    kb.button(text="✅ +30 дней", callback_data=f"approve:{req_user_id}:30")
    kb.button(text="❌ Отклонить", callback_data=f"reject:{req_user_id}")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()


@dp.message(F.text.in_({"/start", "start"}))
async def start(m: Message):
    db_init()
    ensure_user(m.from_user.id)

    active, until = is_access_active(m.from_user.id)
    if active:
        await m.answer(
            "✅ Доступ активен\n"
            f"До: <b>{fmt_until(until)}</b>\n\n"
            "Жми кнопки ниже.",
            reply_markup=kb_user(m.from_user.id)
        )
    else:
        await m.answer(
            "⛔️ Доступ по одобрению.\n\n"
            "Нажми «Запросить доступ». Админу придёт заявка с кнопками +7/+15/+30.\n",
            reply_markup=kb_user(m.from_user.id)
        )

@dp.message(F.text.in_({"/myid", "myid"}))
async def myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.callback_query(F.data == "how")
async def how(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer(
        "ℹ️ <b>Как работает</b>\n"
        f"• Берём Top-{TOP_N} USDT по 24h объёму (фильтр ≥ {MIN_QUOTE_VOL_USDT_24H:,.0f} USDT)\n"
        f"• Нажми «📣 Сигнал» — бот найдёт лучший сетап сейчас\n"
        f"• Результат считается автоматически через {EVAL_BARS} свечи 5m\n"
        "• Винрейт = WIN/LOSE (без нейтрали)\n",
        reply_markup=kb_user(cb.from_user.id)
    )

@dp.callback_query(F.data == "request_access")
async def request_access(cb: CallbackQuery):
    uid = cb.from_user.id
    ensure_user(uid)
    ok = create_request(uid)
    await cb.answer("Ок")

    if ok:
        await cb.message.answer("✅ Заявка отправлена. Жди одобрения.", reply_markup=kb_user(uid))
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🛂 Заявка на доступ от <code>{uid}</code>",
                    reply_markup=kb_admin_request(uid)
                )
            except Exception:
                pass
    else:
        await cb.message.answer("⏳ У тебя уже есть активная заявка. Жди.", reply_markup=kb_user(uid))

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

    ensure_user(uid)
    until = approve_user(uid, days)

    await cb.answer("Одобрено ✅")
    await cb.message.answer(f"✅ Одобрено для <code>{uid}</code> на {days} дней (до {fmt_until(until)}).")

    try:
        await bot.send_message(uid, f"✅ Доступ выдан на {days} дней.\nДо: <b>{fmt_until(until)}</b>\nНажми /start")
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
    await cb.message.answer(f"🤖 Автосканер теперь: <b>{'ON' if new_val else 'OFF'}</b>", reply_markup=kb_user(uid))

@dp.callback_query(F.data == "sig_now")
async def sig_now(cb: CallbackQuery):
    uid = cb.from_user.id
    active, until = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return

    await cb.answer("Сканирую Top-100…")

    syms = await top_symbols()
    best: Optional[Signal] = None
    errors = 0

    for s in syms:
        try:
            sig = await build_signal(s)
            if not sig:
                continue
            if (best is None) or (sig.score > best.score):
                best = sig
        except Exception:
            errors += 1
            continue

    if not best:
        await cb.message.answer(
            f"Сейчас нет сигнала.\nОшибки API: {errors}\nПопробуй позже.",
            reply_markup=kb_user(uid)
        )
        return

    db_add_signal(uid, best.symbol, best.side, best.entry, best.tp, best.sl, best.score, EVAL_BARS)

    await cb.message.answer(
        f"📣 <b>{best.symbol}</b>\n"
        f"Направление: <b>{best.side}</b>\n"
        f"Вход: <b>MARKET NOW</b> ≈ <code>{best.entry:.6f}</code>\n"
        f"TP: <code>{best.tp:.6f}</code>\n"
        f"SL: <code>{best.sl:.6f}</code>\n"
        f"Оценка результата: через <b>{EVAL_BARS}</b> свечи 5m\n"
        f"Score: <b>{best.score}</b>\n\n"
        "📊 Итог попадёт в статистику автоматически.",
        reply_markup=kb_user(uid)
    )

@dp.callback_query(F.data == "stats")
async def stats(cb: CallbackQuery):
    uid = cb.from_user.id
    active, until = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return

    w, l, done, wr, pending = stats_total(uid)
    await cb.answer()
    await cb.message.answer(
        "📊 <b>Статистика</b>\n"
        f"Доступ до: <b>{fmt_until(until)}</b>\n\n"
        f"WIN: <b>{w}</b>\n"
        f"LOSE: <b>{l}</b>\n"
        f"DONE: <b>{done}</b>\n"
        f"PENDING: <b>{pending}</b>\n"
        f"Winrate (WIN/LOSE): <b>{wr:.1f}%</b>\n\n"
        "⚠️ Смотри на дистанции 30–50+ сигналов.",
        reply_markup=kb_user(uid)
    )

@dp.callback_query(F.data == "stats_coins")
async def stats_coins(cb: CallbackQuery):
    uid = cb.from_user.id
    active, _ = is_access_active(uid)
    if not active:
        await cb.answer("Нет доступа", show_alert=True)
        return

    rows = stats_by_coin(uid, min_trades=5, limit=10)
    await cb.answer()

    if not rows:
        await cb.message.answer(
            "🪙 Пока мало данных по монетам.\nНужно минимум 5 завершённых сделок на монету.",
            reply_markup=kb_user(uid)
        )
        return

    text = "🪙 <b>Топ монет по winrate</b> (мин. 5 сделок)\n\n"
    for sym, t, wr in rows:
        text += f"• <b>{sym}</b> — {t} сделок — <b>{wr:.1f}%</b>\n"

    await cb.message.answer(text, reply_markup=kb_user(uid))


# =========================
# AUTOSCAN LOOP
# =========================
async def autoscan_loop():
    while True:
        try:
            con = db()
            cur = con.cursor()
            cur.execute("SELECT user_id FROM users WHERE status='APPROVED' AND autoscan=1")
            users = [r[0] for r in cur.fetchall()]
            con.close()

            if not users:
                await asyncio.sleep(AUTO_SCAN_EVERY_MIN * 60)
                continue

            syms = await top_symbols()
            best: Optional[Signal] = None

            for s in syms:
                try:
                    sig = await build_signal(s)
                    if not sig:
                        continue
                    if (best is None) or (sig.score > best.score):
                        best = sig
                except Exception:
                    continue

            if best:
                for uid in users:
                    active, _ = is_access_active(uid)
                    if not active:
                        continue
                    try:
                        db_add_signal(uid, best.symbol, best.side, best.entry, best.tp, best.sl, best.score, EVAL_BARS)
                        await bot.send_message(
                            uid,
                            f"🤖 <b>Автосканер</b>\n\n"
                            f"📣 <b>{best.symbol}</b>\n"
                            f"Направление: <b>{best.side}</b>\n"
                            f"Вход: <b>MARKET NOW</b> ≈ <code>{best.entry:.6f}</code>\n"
                            f"TP: <code>{best.tp:.6f}</code>\n"
                            f"SL: <code>{best.sl:.6f}</code>\n"
                            f"Оценка: через <b>{EVAL_BARS}</b> свечи 5m\n"
                            f"Score: <b>{best.score}</b>\n",
                            reply_markup=kb_user(uid)
                        )
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
