import os
import time
import asyncio
import sqlite3
import random
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

import httpx
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import CommandStart, Command

# =========================
# CONFIG
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing. Set BOT_TOKEN in Railway Variables.")

DB_PATH = "signals.db"

# BingX OpenAPI base (public market data)
BINGX_BASE = "https://open-api.bingx.com"

# Universe
TOP_N_STRICT = 50           # first pass
TOP_N_FALLBACK = 150        # fallback pass (manual button only)
MIN_QUOTE_VOL_STRICT = 50_000_000.0
MIN_QUOTE_VOL_FALLBACK = 10_000_000.0
MIN_PRICE = 0.01

# Auto scan
AUTO_SCAN_EVERY_MIN = 60
AUTO_MIN_PROB = 7
BROADCAST_COOLDOWN_SEC = 30 * 60

# Manual signal output
SHOW_TOP_K = 3
ENTRY_MIN_PROB = 7          # "можно входить" если >= 7/10 и Strict=OK

# Speed / stability
HTTP_TIMEOUT = 25
HTTP_CONCURRENCY = 4
SCAN_TIMEOUT_SECONDS = 30
TOPLIST_CACHE_TTL = 10 * 60

# Strategy thresholds (STRICT)
ATR_MIN_PCT = 0.30
VOL_RATIO_MIN = 1.10
OVERHEAT_DIST_MAX_PCT = 1.20
RSI_LONG_MIN = 55
RSI_LONG_MAX = 70
RSI_SHORT_MIN = 30
RSI_SHORT_MAX = 45

# Fallback loosen (manual button only; entry rules DO NOT change)
ATR_MIN_PCT_FALLBACK = ATR_MIN_PCT * 0.80
VOL_RATIO_MIN_FALLBACK = VOL_RATIO_MIN * 0.90
OVERHEAT_DIST_MAX_PCT_FALLBACK = 1.60

# TP/SL (for "market now" idea)
TP_PCT = 1.0
SL_PCT = 0.5

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

HTTP_SEM = asyncio.Semaphore(HTTP_CONCURRENCY)
HTTP_CLIENT: Optional[httpx.AsyncClient] = None

TOP_CACHE: Dict[str, object] = {"ts": 0.0, "strict": [], "fallback": []}
LAST_BROADCAST = {"ts": 0}

# =========================
# BRIGHT JOKES (no profanity)
# =========================
JOKES_OK = [
    "🔥 Сетап выглядит бодро. Но стоп ставим как взрослые.",
    "🧠 План есть? Есть. Тогда руки не трясём — действуем по правилам.",
    "🚀 Если это отработает — свечи официально красавчики. Если нет — стоп спасёт.",
    "🎯 Тут уже похоже на «можно». Только без геройства, ок?",
    "🐂🐻 Быки и медведи дерутся — мы просто забираем свой кусочек.",
]
JOKES_WAIT = [
    "⏳ Почти-почти… но рынок пока жмёт паузу. Ждём подтверждение.",
    "👀 Пахнет сигналом, но пока больше пахнет «не лезь».",
    "🧊 Холодная голова: лучше пропустить слабое, чем лечить депозит.",
    "🕵️‍♂️ Сетап есть, но не красавец. Смотри, не влюбись в график.",
    "🎭 Рынок как актёр: делает вид, что даёт вход. Мы не ведёмся.",
]
JOKES_NO = [
    "🍵 Сигналов нет. Рынок выдал официальную справку: «Отдыхай».",
    "🧱 Здесь входить — это как лбом стену тестить. Не надо.",
    "🦉 Сегодня мудрый день: ничего не делать — тоже стратегия.",
    "📉📈 График устроил спектакль без сюжета. Мы уходим из зала.",
    "🧯 Ничего годного — значит сохраняем депозит, а не эмоции.",
]
JOKES_ERR = [
    "📡 Биржа зависла. Свечи, видимо, грузятся по модему.",
    "🛠️ Данные не приехали. Рынок в ремонте на 30 секунд.",
    "🙃 Таймаут. BingX решил сыграть в «поймай меня, если сможешь».",
]

JOKES_FALLBACK = [
    "🧪 Включаю режим «детектив»: расширяю поиск и копаю глубже…",
    "🔍 Первый проход пустой. Делаю второй — более широкий и умный.",
    "🧹 Уберём лишний шум и посмотрим, где хоть что-то шевелится…",
]

def joke(arr: List[str]) -> str:
    return random.choice(arr)

# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def init_db():
    con = db()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'PENDING',       -- PENDING/APPROVED/BANNED
        access_until INTEGER NOT NULL DEFAULT 0,
        autoscan INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL DEFAULT 0
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS access_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING'        -- PENDING/APPROVED/REJECTED
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        user_id INTEGER NOT NULL,                     -- 0 = autoscan/system
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        entry REAL NOT NULL,
        tp REAL NOT NULL,
        sl REAL NOT NULL,
        prob INTEGER NOT NULL,                        -- 0..10
        strict_ok INTEGER NOT NULL,                   -- 0/1
        reason TEXT NOT NULL
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status, access_until);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_req_status ON access_requests(status, ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_ts ON signals_log(ts);")

    con.commit()
    con.close()

def ensure_user(uid: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (uid,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users(user_id, status, access_until, autoscan, created_ts) VALUES(?,?,?,?,?)",
            (uid, "PENDING", 0, 1, int(time.time())),
        )
    con.commit()
    con.close()

def is_admin(uid: int) -> bool:
    return ADMIN_ID != 0 and uid == ADMIN_ID

def user_active(uid: int) -> Tuple[bool, int]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT status, access_until FROM users WHERE user_id=?", (uid,))
    r = cur.fetchone()
    con.close()
    if not r:
        return False, 0
    status, until = r[0], int(r[1])
    return status == "APPROVED" and until > int(time.time()), until

def set_autoscan(uid: int, enabled: bool):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET autoscan=? WHERE user_id=?", (1 if enabled else 0, uid))
    con.commit()
    con.close()

def get_autoscan(uid: int) -> int:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT autoscan FROM users WHERE user_id=?", (uid,))
    r = cur.fetchone()
    con.close()
    return int(r[0]) if r else 1

def create_access_request(uid: int) -> bool:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT id FROM access_requests WHERE user_id=? AND status='PENDING' ORDER BY ts DESC LIMIT 1", (uid,))
    if cur.fetchone():
        con.close()
        return False
    cur.execute("INSERT INTO access_requests(ts, user_id, status) VALUES(?,?, 'PENDING')", (int(time.time()), uid))
    con.commit()
    con.close()
    return True

def approve_user(uid: int, days: int) -> int:
    until = int(time.time()) + int(days) * 86400
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET status='APPROVED', access_until=? WHERE user_id=?", (until, uid))
    cur.execute("UPDATE access_requests SET status='APPROVED' WHERE user_id=? AND status='PENDING'", (uid,))
    con.commit()
    con.close()
    return until

def reject_user(uid: int):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE access_requests SET status='REJECTED' WHERE user_id=? AND status='PENDING'", (uid,))
    con.commit()
    con.close()

def approved_users_for_broadcast() -> List[int]:
    now = int(time.time())
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT user_id FROM users
        WHERE status='APPROVED' AND access_until>? AND autoscan=1
    """, (now,))
    users = [int(r[0]) for r in cur.fetchall()]
    con.close()
    return users

def log_signal(user_id: int, symbol: str, side: str, entry: float, tp: float, sl: float, prob: int, strict_ok: int, reason: str):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO signals_log(ts, user_id, symbol, side, entry, tp, sl, prob, strict_ok, reason)
        VALUES(?,?,?,?,?,?,?,?,?,?)
    """, (int(time.time()), user_id, symbol, side, float(entry), float(tp), float(sl), int(prob), int(strict_ok), reason))
    con.commit()
    con.close()

def fmt_until(ts: int) -> str:
    if ts <= 0:
        return "нет"
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))

# =========================
# HTTP (BingX)
# =========================
async def get_client() -> httpx.AsyncClient:
    global HTTP_CLIENT
    if HTTP_CLIENT is None:
        HTTP_CLIENT = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
    return HTTP_CLIENT

def ms() -> int:
    return int(time.time() * 1000)

async def fetch_bingx(path: str, params: Optional[dict] = None) -> dict:
    if params is None:
        params = {}
    params = dict(params)
    params.setdefault("timestamp", ms())

    last_err = None
    for attempt in range(1, 4):
        try:
            async with HTTP_SEM:
                client = await get_client()
                r = await client.get(f"{BINGX_BASE}{path}", params=params)

            if r.status_code in (418, 429):
                raise httpx.HTTPStatusError(f"RateLimit {r.status_code}", request=r.request, response=r)
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(f"Server {r.status_code}", request=r.request, response=r)

            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data.get("code") not in (0, "0"):
                raise RuntimeError(f"BingX code={data.get('code')} msg={data.get('msg')}")
            return data
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.6 * attempt)

    print("BINGX FETCH ERROR:", repr(last_err))
    raise last_err

# =========================
# INDICATORS
# =========================
def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out

def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 2:
        return [50.0] * len(values)
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rs = avg_gain / avg_loss if avg_loss else 0.0
    rsi_vals = [100.0 - (100.0 / (1.0 + rs))]
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss else 0.0
        rsi_vals.append(100.0 - (100.0 / (1.0 + rs)))
    return [50.0] * (len(values) - len(rsi_vals)) + rsi_vals

def atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[float]:
    if len(close) < period + 2:
        return [0.0] * len(close)
    trs = []
    for i in range(1, len(close)):
        tr = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
        trs.append(tr)
    a = sum(trs[:period]) / period
    out = [a]
    for i in range(period, len(trs)):
        a = (a * (period - 1) + trs[i]) / period
        out.append(a)
    return [0.0] * (len(close) - len(out)) + out

def clamp_int(x: float, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(round(x))))

# =========================
# MARKET DATA (BingX Futures)
# =========================
def _get_float(d: dict, keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                continue
    return default

def symbol_allowed(sym: str) -> bool:
    if not isinstance(sym, str) or "-" not in sym:
        return False
    base, quote = sym.split("-", 1)
    if quote != "USDT":
        return False
    if base in {"USDC", "TUSD", "FDUSD", "USDP", "DAI", "BUSD"}:
        return False
    return True

async def swap_tickers_24h() -> List[dict]:
    data = await fetch_bingx("/openApi/swap/v2/quote/ticker", params={})
    d = data.get("data", [])
    return d if isinstance(d, list) else []

async def swap_klines(symbol: str, interval: str, limit: int = 210) -> List[dict]:
    data = await fetch_bingx(
        "/openApi/swap/v3/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": str(limit)},
    )
    d = data.get("data", [])
    return d if isinstance(d, list) else []

async def top_symbols(min_quote_vol: float, top_n: int, cache_key: str) -> List[str]:
    now = time.time()
    cached = TOP_CACHE.get(cache_key, [])
    if cached and (now - float(TOP_CACHE["ts"])) < TOPLIST_CACHE_TTL:
        return list(cached)  # type: ignore

    tickers = await swap_tickers_24h()
    arr = []
    for t in tickers:
        sym = t.get("symbol") or t.get("s") or ""
        if not symbol_allowed(sym):
            continue

        last = _get_float(t, ["lastPrice", "last", "close", "c"], 0.0)
        vol_quote = _get_float(t, ["quoteVolume", "quoteVol", "qv", "turnover", "volumeQuote", "amount"], 0.0)
        vol_base = _get_float(t, ["volume", "v", "baseVolume"], 0.0)
        if vol_quote <= 0 and vol_base > 0 and last > 0:
            vol_quote = vol_base * last

        if last >= MIN_PRICE and vol_quote >= min_quote_vol:
            arr.append((sym, vol_quote))

    arr.sort(key=lambda x: x[1], reverse=True)
    syms = [x[0] for x in arr[:top_n]]

    TOP_CACHE["ts"] = now
    TOP_CACHE[cache_key] = syms
    return syms

def _parse_klines_to_ohlcv(kl: List[dict]) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    o, h, l, c, v = [], [], [], [], []
    for x in kl:
        oo = _get_float(x, ["o", "open"], 0.0)
        cc = _get_float(x, ["c", "close"], 0.0)
        hh = _get_float(x, ["h", "high"], 0.0)
        ll = _get_float(x, ["l", "low"], 0.0)
        vv = _get_float(x, ["v", "volume"], 0.0)
        if oo == 0 and cc == 0:
            continue
        o.append(oo)
        c.append(cc)
        h.append(hh if hh else max(oo, cc))
        l.append(ll if ll else min(oo, cc))
        v.append(vv)
    return o, h, l, c, v

# =========================
# STRATEGY / SCORING
# =========================
@dataclass
class Candidate:
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float
    prob: int
    strict_ok: bool
    reason: str

async def analyze_symbol(
    symbol: str,
    atr_min_pct: float,
    vol_ratio_min: float,
    overheat_max_pct: float,
) -> Optional[Candidate]:
    t1 = asyncio.create_task(swap_klines(symbol, "1h", 210))
    t15 = asyncio.create_task(swap_klines(symbol, "15m", 210))
    t5 = asyncio.create_task(swap_klines(symbol, "5m", 140))
    k1, k15, k5 = await asyncio.gather(t1, t15, t5)

    _, _, _, c1, _ = _parse_klines_to_ohlcv(k1)
    _, h15, l15, c15, v15 = _parse_klines_to_ohlcv(k15)
    _, _, _, c5, _ = _parse_klines_to_ohlcv(k5)

    if len(c1) < 210 or len(c15) < 210 or len(c5) < 50 or len(v15) < 60:
        return None

    e50_1 = ema(c1, 50)[-1]
    e200_1 = ema(c1, 200)[-1]
    e50_15 = ema(c15, 50)[-1]
    e200_15 = ema(c15, 200)[-1]

    trend_up = (e50_1 > e200_1) and (e50_15 > e200_15)
    trend_down = (e50_1 < e200_1) and (e50_15 < e200_15)

    a15 = atr(h15, l15, c15, 14)[-1]
    atr_pct = (a15 / c15[-1]) * 100.0 if c15[-1] else 0.0

    last_vol = v15[-1]
    avg_vol = sum(v15[-50:]) / 50.0
    vol_ratio = (last_vol / avg_vol) if avg_vol > 0 else 0.0

    last5 = c5[-1]
    prev5 = c5[-2]
    bullish5 = last5 > prev5
    bearish5 = last5 < prev5
    r5 = rsi(c5, 14)[-1]

    e50_15_last = ema(c15, 50)[-1]
    dist_pct = abs(c15[-1] - e50_15_last) / c15[-1] * 100.0 if c15[-1] else 999.0

    side: Optional[str] = None
    if trend_up and bullish5:
        side = "LONG"
    elif trend_down and bearish5:
        side = "SHORT"
    else:
        return None

    # HARD FILTERS (adaptive for candidates)
    if atr_pct < atr_min_pct:
        return None
    if vol_ratio < vol_ratio_min:
        return None
    if dist_pct > overheat_max_pct:
        return None

    # SCORE (0..10) stays "strict-style"
    score = 0
    reasons = []

    score += 2
    reasons.append("trend(1H+15m)")

    if atr_pct >= 0.45:
        score += 2
    else:
        score += 1
    reasons.append(f"atr%={atr_pct:.2f}")

    if vol_ratio >= 1.50:
        score += 2
    else:
        score += 1
    reasons.append(f"volx{vol_ratio:.2f}")

    score += 1
    reasons.append("5m_confirm")

    rsi_ok = False
    if side == "LONG" and (RSI_LONG_MIN <= r5 <= RSI_LONG_MAX):
        score += 2
        rsi_ok = True
    elif side == "SHORT" and (RSI_SHORT_MIN <= r5 <= RSI_SHORT_MAX):
        score += 2
        rsi_ok = True
    reasons.append(f"rsi={r5:.1f}")

    if dist_pct <= 0.8:
        score += 1
    reasons.append(f"dist={dist_pct:.2f}%")

    prob = clamp_int(score, 0, 10)

    # STRICT_OK is ALWAYS computed by STRICT rules (entry rules unchanged)
    strict_ok = (
        (trend_up or trend_down)
        and (atr_pct >= ATR_MIN_PCT)
        and (vol_ratio >= VOL_RATIO_MIN)
        and (dist_pct <= OVERHEAT_DIST_MAX_PCT)
        and rsi_ok
    )

    entry = last5
    if side == "LONG":
        tp = entry * (1 + TP_PCT / 100.0)
        sl = entry * (1 - SL_PCT / 100.0)
    else:
        tp = entry * (1 - TP_PCT / 100.0)
        sl = entry * (1 + SL_PCT / 100.0)

    return Candidate(
        symbol=symbol,
        side=side,
        entry=entry,
        tp=tp,
        sl=sl,
        prob=prob,
        strict_ok=strict_ok,
        reason="; ".join(reasons),
    )

async def top_k_candidates(symbols: List[str], k: int, atr_min_pct: float, vol_ratio_min: float, overheat_max_pct: float) -> List[Candidate]:
    tasks = [asyncio.create_task(analyze_symbol(s, atr_min_pct, vol_ratio_min, overheat_max_pct)) for s in symbols]
    best: List[Candidate] = []
    try:
        for coro in asyncio.as_completed(tasks, timeout=SCAN_TIMEOUT_SECONDS):
            try:
                res = await coro
                if not res:
                    continue
                best.append(res)
                best.sort(key=lambda x: x.prob, reverse=True)
                if len(best) > k:
                    best = best[:k]
            except Exception:
                continue
    except asyncio.TimeoutError:
        pass
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
    return best

def pick_best_strict(cands: List[Candidate]) -> Optional[Candidate]:
    strict = [c for c in cands if c.strict_ok]
    if not strict:
        return None
    strict.sort(key=lambda x: x.prob, reverse=True)
    return strict[0]

# =========================
# KEYBOARDS
# =========================
def kb_user(uid: int):
    active_flag, _ = user_active(uid)
    auto = get_autoscan(uid)
    kb = InlineKeyboardBuilder()
    if active_flag:
        kb.button(text="📣 Сигнал", callback_data="sig_now")
        kb.button(text=f"🤖 Авто: {'ON' if auto else 'OFF'}", callback_data="toggle_auto")
    else:
        kb.button(text="📝 Запросить доступ", callback_data="request_access")
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
# HANDLERS
# =========================
@dp.message(CommandStart())
async def start(m: Message):
    init_db()
    ensure_user(m.from_user.id)

    if is_admin(m.from_user.id):
        active_flag, _ = user_active(m.from_user.id)
        if not active_flag:
            until = approve_user(m.from_user.id, 3650)
            await m.answer(f"✅ Админ-доступ активирован до: <b>{fmt_until(until)}</b>", reply_markup=kb_user(m.from_user.id))
            return

    active_flag, until = user_active(m.from_user.id)
    if active_flag:
        await m.answer(
            f"✅ Доступ активен до: <b>{fmt_until(until)}</b>\n"
            f"Рынок: <b>BingX Futures</b>\n"
            f"Кнопка «Сигнал»: <b>Top-{SHOW_TOP_K} кандидата</b> + умный fallback\n"
            f"Вход: только если <b>Prob ≥ {ENTRY_MIN_PROB}</b> и <b>Strict=OK</b>\n\n"
            f"😄 {joke(JOKES_OK)}",
            reply_markup=kb_user(m.from_user.id),
        )
    else:
        await m.answer(
            "⛔️ Доступ по одобрению.\nНажми «Запросить доступ» — мне придёт заявка с кнопками +7/+15/+30.",
            reply_markup=kb_user(m.from_user.id),
        )

@dp.message(Command("myid"))
async def myid(m: Message):
    await m.answer(f"ID: <code>{m.from_user.id}</code>")

@dp.callback_query(F.data == "request_access")
async def request_access(cb: CallbackQuery):
    uid = cb.from_user.id
    ensure_user(uid)
    await cb.answer("Ок")

    created = create_access_request(uid)
    if not created:
        await cb.message.answer("⏳ У тебя уже есть активная заявка. Жди.", reply_markup=kb_user(uid))
        return

    await cb.message.answer("✅ Заявка отправлена. Ожидай одобрения.", reply_markup=kb_user(uid))

    if not ADMIN_ID:
        await cb.message.answer("⚠️ ADMIN_ID не задан. Добавь ADMIN_ID в Railway Variables и перезапусти.")
        return

    try:
        await bot.send_message(
            ADMIN_ID,
            f"🛂 Заявка на доступ от <code>{uid}</code>",
            reply_markup=kb_admin_request(uid),
        )
    except Exception:
        pass

@dp.callback_query(F.data.startswith("approve:"))
async def approve_cb(cb: CallbackQuery):
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
async def reject_cb(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    if len(parts) != 2:
        await cb.answer("Ошибка", show_alert=True)
        return
    uid = int(parts[1])
    reject_user(uid)

    await cb.answer("Отклонено")
    await cb.message.answer(f"❌ Отклонено для <code>{uid}</code>.")
    try:
        await bot.send_message(uid, "❌ Доступ не одобрен.")
    except Exception:
        pass

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(cb: CallbackQuery):
    uid = cb.from_user.id
    active_flag, _ = user_active(uid)
    if not active_flag:
        await cb.answer("Нет доступа", show_alert=True)
        return

    current = get_autoscan(uid)
    new_val = 0 if current else 1
    set_autoscan(uid, bool(new_val))

    await cb.answer("Ок")
    await cb.message.answer(f"🤖 Автоанализ: <b>{'ON' if new_val else 'OFF'}</b>", reply_markup=kb_user(uid))

def format_candidate(c: Candidate, idx: int) -> str:
    ok_enter = (c.prob >= ENTRY_MIN_PROB) and c.strict_ok
    badge = "✅ <b>ВХОД</b>" if ok_enter else "⏳ <b>ЖДАТЬ</b>"
    return (
        f"<b>#{idx} {c.symbol}</b>  {badge}\n"
        f"Side: <b>{c.side}</b> | Prob: <b>{c.prob}/10</b> | Strict: <b>{'OK' if c.strict_ok else 'NO'}</b>\n"
        f"Entry: <b>MARKET NOW</b> ≈ <code>{c.entry:.6f}</code>\n"
        f"TP: <code>{c.tp:.6f}</code> | SL: <code>{c.sl:.6f}</code>\n"
        f"<i>{c.reason}</i>"
    )

@dp.callback_query(F.data == "sig_now")
async def sig_now(cb: CallbackQuery):
    uid = cb.from_user.id
    active_flag, _ = user_active(uid)
    if not active_flag:
        await cb.answer("Нет доступа", show_alert=True)
        return

    await cb.answer("Анализирую…")
    msg = await cb.message.answer("⏳ Первый проход (строго, только топ-ликвид)…")

    try:
        # Pass 1: strict universe + strict minima
        syms1 = await top_symbols(MIN_QUOTE_VOL_STRICT, TOP_N_STRICT, "strict")
        cands = await top_k_candidates(
            syms1,
            SHOW_TOP_K,
            ATR_MIN_PCT,
            VOL_RATIO_MIN,
            OVERHEAT_DIST_MAX_PCT,
        )

        # If empty -> fallback pass (wider universe + relaxed minima), BUT entry rule unchanged
        if not cands:
            await msg.edit_text(f"Пока пусто по строгому проходу.\n\n😄 {joke(JOKES_FALLBACK)}")
            syms2 = await top_symbols(MIN_QUOTE_VOL_FALLBACK, TOP_N_FALLBACK, "fallback")
            cands = await top_k_candidates(
                syms2,
                SHOW_TOP_K,
                ATR_MIN_PCT_FALLBACK,
                VOL_RATIO_MIN_FALLBACK,
                OVERHEAT_DIST_MAX_PCT_FALLBACK,
            )

        if not cands:
            await msg.edit_text(f"Сейчас нет достойных кандидатов.\n\n😄 {joke(JOKES_NO)}")
            return

        # Log + message
        best_strict = pick_best_strict(cands)

        header = (
            f"📣 <b>Лучшие кандидаты (Top-{SHOW_TOP_K})</b>\n"
            f"✅ Вход = <b>Prob ≥ {ENTRY_MIN_PROB}</b> и <b>Strict=OK</b>\n"
            f"⚠️ Если Strict=NO — это <b>наблюдение</b>, не сигнал на вход.\n\n"
        )

        blocks = []
        for i, c in enumerate(cands, 1):
            blocks.append(format_candidate(c, i))
            log_signal(uid, c.symbol, c.side, c.entry, c.tp, c.sl, c.prob, 1 if c.strict_ok else 0, c.reason)

        if best_strict and best_strict.prob >= ENTRY_MIN_PROB:
            tail = f"\n\n😄 {joke(JOKES_OK)}"
        else:
            tail = f"\n\n😄 {joke(JOKES_WAIT)}"

        await msg.edit_text(header + "\n\n".join(blocks) + tail)
        await cb.message.answer("Меню:", reply_markup=kb_user(uid))

    except Exception as e:
        print("SIGNAL ERROR:", repr(e))
        await msg.edit_text(f"Ошибка при получении данных (BingX/таймаут/лимит). Попробуй ещё раз через минуту.\n\n😄 {joke(JOKES_ERR)}")

# =========================
# AUTO SCAN LOOP (STRICT ONLY)
# =========================
async def autoscan_loop():
    while True:
        try:
            users = approved_users_for_broadcast()
            if users:
                syms = await top_symbols(MIN_QUOTE_VOL_STRICT, TOP_N_STRICT, "strict")

                # autoscan: strict minima ONLY
                cands = await top_k_candidates(
                    syms,
                    SHOW_TOP_K,
                    ATR_MIN_PCT,
                    VOL_RATIO_MIN,
                    OVERHEAT_DIST_MAX_PCT,
                )

                best = None
                if cands:
                    strict_best = pick_best_strict(cands)
                    if strict_best and strict_best.prob >= AUTO_MIN_PROB:
                        best = strict_best

                if best:
                    now = int(time.time())
                    if now - int(LAST_BROADCAST["ts"]) >= BROADCAST_COOLDOWN_SEC:
                        LAST_BROADCAST["ts"] = now
                        log_signal(0, best.symbol, best.side, best.entry, best.tp, best.sl, best.prob, 1, best.reason)

                        text = (
                            f"🤖 <b>Автоанализ (каждые {AUTO_SCAN_EVERY_MIN} минут)</b>\n\n"
                            f"📣 <b>{best.symbol}</b>\n"
                            f"Side: <b>{best.side}</b>\n"
                            f"Entry: <b>MARKET NOW</b> ≈ <code>{best.entry:.6f}</code>\n"
                            f"TP: <code>{best.tp:.6f}</code>\n"
                            f"SL: <code>{best.sl:.6f}</code>\n"
                            f"Вероятность: <b>{best.prob}/10</b>\n"
                            f"<i>{best.reason}</i>\n\n"
                            f"😄 {joke(JOKES_OK)}"
                        )

                        for uid in users:
                            try:
                                await bot.send_message(uid, text, reply_markup=kb_user(uid))
                            except Exception:
                                continue

        except Exception as e:
            print("AUTOSCAN ERROR:", repr(e))

        await asyncio.sleep(AUTO_SCAN_EVERY_MIN * 60)

# =========================
# MAIN
# =========================
async def main():
    init_db()
    asyncio.create_task(autoscan_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
