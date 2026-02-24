import os
import time
import asyncio
import random
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import httpx
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import CommandStart, Command

# =========================
# НАСТРОЙКИ
# =========================
load_dotenv()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN отсутствует. Добавь BOT_TOKEN в Railway Variables.")
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID отсутствует. Добавь ADMIN_ID (твой Telegram ID) в Railway Variables.")

BINGX_BASE = "https://open-api.bingx.com"

# Монеты и фильтры
TOP_N_STRICT = 60
TOP_N_MANUAL_MAX = 180
MIN_QUOTE_VOL_STRICT = 50_000_000.0
MIN_QUOTE_VOL_MANUAL = 10_000_000.0
MIN_PRICE = 0.01

# Выдача
SHOW_TOP_K = 3
ENTRY_MIN_PROB = 7  # ВХОД только если Prob >= 7 и Strict=OK

# Автоанализ
AUTO_SCAN_EVERY_MIN = 60
AUTO_MIN_PROB = 7
BROADCAST_COOLDOWN_SEC = 30 * 60

# HTTP
HTTP_TIMEOUT = 25
HTTP_CONCURRENCY = 4
SCAN_TIMEOUT_SECONDS = 35
TOPLIST_CACHE_TTL = 10 * 60
PRICE_CACHE_TTL = 6

# STRATEGY (ATR-стоп и RR)
ATR_PERIOD = 14
ATR_MIN_PCT = 0.30
VOL_RATIO_MIN = 1.10
OVERHEAT_DIST_MAX_PCT = 1.20
RSI_LONG_MIN = 55
RSI_LONG_MAX = 70
RSI_SHORT_MIN = 30
RSI_SHORT_MAX = 45

ATR_SL_MULT = 1.20   # SL = ATR * 1.20
RR_TP = 1.80         # TP distance = SL distance * 1.80

MAX_SL_PCT = 3.50    # ограничим стоп, чтобы не был огромным
MIN_SL_PCT = 0.25    # и не был слишком микроскопическим

# Допуск к рыночному входу (если цена уже убежала сильно - сигнал лучше не брать)
MAX_SLIPPAGE_PCT = 0.80

# =========================
# БОТ
# =========================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

HTTP_SEM = asyncio.Semaphore(HTTP_CONCURRENCY)
HTTP_CLIENT: Optional[httpx.AsyncClient] = None

TOP_CACHE: Dict[str, object] = {"ts": 0.0, "strict": [], "manual": []}
PRICE_CACHE: Dict[str, Tuple[float, float]] = {}  # symbol -> (ts, last_price)
LAST_BROADCAST_TS = 0

# =========================
# ТЕКСТ (русский, без эмодзи)
# =========================
JOKES_OK = [
    "Сетап выглядит бодро. Дисциплина и риск-менеджмент обязательны.",
    "План есть. Теперь не мешай ему сработать.",
    "Входи по правилам, а не по эмоциям.",
    "Дисциплина важнее одного сигнала.",
    "Если входишь, не забывай про риск.",
]
JOKES_WAIT = [
    "Почти. Но 'почти' не приносит прибыль. Ждем подтверждение.",
    "Идея есть, но пока слабовато. Терпение.",
    "Не угадываем. Ждем, пока рынок покажет намерение.",
    "Лучше пропустить слабое, чем лечить депозит.",
    "Рынок пока не дал нормального 'да'.",
]
JOKES_NO = [
    "Сейчас ничего достойного. Это тоже сигнал: не входить.",
    "Пусто. Лучше подождать, чем ловить шум.",
    "Сетапов нет. Сохраняем депозит.",
    "Рынок скучный. Не делаем лишних движений.",
    "Ничего годного. Ждем.",
]
JOKES_ERR = [
    "Биржа не ответила. Попробуй еще раз через минуту.",
    "Ошибка данных. Похоже на таймаут/лимит.",
    "Данные не пришли. Попробуй позже.",
]
JOKES_MANUAL = [
    "Запускаю анализ рынка. Ищу лучшие варианты.",
    "Сканирую рынок: выбираю топ кандидатов.",
    "Сейчас посмотрим, где есть движение.",
]

def joke(arr: List[str]) -> str:
    return random.choice(arr)

def only_admin(uid: int) -> bool:
    return uid == ADMIN_ID

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
# ИНДИКАТОРЫ
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

def clamp_float(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

# =========================
# РЫНОЧНЫЕ ДАННЫЕ
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

def _parse_klines(kl: List[dict]) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
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

async def get_last_price(symbol: str) -> float:
    now = time.time()
    cached = PRICE_CACHE.get(symbol)
    if cached and (now - cached[0] <= PRICE_CACHE_TTL):
        return cached[1]

    tickers = await swap_tickers_24h()
    price = 0.0
    for t in tickers:
        sym = t.get("symbol") or t.get("s") or ""
        if sym != symbol:
            continue
        price = _get_float(t, ["lastPrice", "last", "close", "c"], 0.0)
        break

    if price <= 0:
        # fallback: попробуем дернуть одиночный тикер (если доступно) через те же данные
        price = 0.0

    PRICE_CACHE[symbol] = (now, price)
    return price

# =========================
# СТРАТЕГИЯ (с ATR стопом)
# =========================
@dataclass
class Candidate:
    symbol: str
    side: str
    prob: int
    strict_ok: bool
    reason: str
    ref_price: float        # цена на момент расчета (последняя 5m)
    atr_abs_15m: float      # ATR в цене
    atr_pct_15m: float

def compute_levels_market(side: str, entry_market: float, atr_abs: float) -> Tuple[float, float, float]:
    # SL distance = ATR * mult (с ограничениями по %)
    dist = atr_abs * ATR_SL_MULT
    if entry_market > 0:
        dist_pct = dist / entry_market * 100.0
    else:
        dist_pct = 0.0

    dist_pct = clamp_float(dist_pct, MIN_SL_PCT, MAX_SL_PCT)
    dist = entry_market * (dist_pct / 100.0)

    tp_dist = dist * RR_TP

    if side == "LONG":
        sl = entry_market - dist
        tp = entry_market + tp_dist
    else:
        sl = entry_market + dist
        tp = entry_market - tp_dist

    return entry_market, tp, sl

async def analyze_symbol(symbol: str) -> Optional[Candidate]:
    t1 = asyncio.create_task(swap_klines(symbol, "1h", 210))
    t15 = asyncio.create_task(swap_klines(symbol, "15m", 210))
    t5 = asyncio.create_task(swap_klines(symbol, "5m", 140))
    k1, k15, k5 = await asyncio.gather(t1, t15, t5)

    _, _, _, c1, _ = _parse_klines(k1)
    _, h15, l15, c15, v15 = _parse_klines(k15)
    _, _, _, c5, _ = _parse_klines(k5)

    if len(c1) < 210 or len(c15) < 210 or len(c5) < 50 or len(v15) < 60:
        return None

    e50_1 = ema(c1, 50)[-1]
    e200_1 = ema(c1, 200)[-1]
    e50_15 = ema(c15, 50)[-1]
    e200_15 = ema(c15, 200)[-1]

    trend_up = (e50_1 > e200_1) and (e50_15 > e200_15)
    trend_down = (e50_1 < e200_1) and (e50_15 < e200_15)

    a15 = atr(h15, l15, c15, ATR_PERIOD)[-1]
    atr_pct = (a15 / c15[-1]) * 100.0 if c15[-1] else 0.0

    last_vol = v15[-1]
    avg_vol = sum(v15[-50:]) / 50.0
    vol_ratio = (last_vol / avg_vol) if avg_vol > 0 else 0.0

    ref_price = c5[-1]
    prev5 = c5[-2]
    bullish5 = ref_price > prev5
    bearish5 = ref_price < prev5
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

    # Скоринг (0..10)
    score = 0
    reasons = ["trend(1h+15m)"]

    if atr_pct >= 0.60:
        score += 2
    elif atr_pct >= ATR_MIN_PCT:
        score += 1
    reasons.append(f"atr%={atr_pct:.2f}")

    if vol_ratio >= 1.60:
        score += 2
    elif vol_ratio >= VOL_RATIO_MIN:
        score += 1
    reasons.append(f"volx{vol_ratio:.2f}")

    score += 1
    reasons.append("confirm(5m)")

    rsi_ok = False
    if side == "LONG" and (RSI_LONG_MIN <= r5 <= RSI_LONG_MAX):
        score += 2
        rsi_ok = True
    elif side == "SHORT" and (RSI_SHORT_MIN <= r5 <= RSI_SHORT_MAX):
        score += 2
        rsi_ok = True
    reasons.append(f"rsi={r5:.1f}")

    if dist_pct <= 0.9:
        score += 1
    elif dist_pct > OVERHEAT_DIST_MAX_PCT:
        score -= 1
    reasons.append(f"dist={dist_pct:.2f}%")

    prob = clamp_int(score, 0, 10)

    strict_ok = (
        (trend_up or trend_down)
        and (atr_pct >= ATR_MIN_PCT)
        and (vol_ratio >= VOL_RATIO_MIN)
        and (dist_pct <= OVERHEAT_DIST_MAX_PCT)
        and rsi_ok
    )

    return Candidate(
        symbol=symbol,
        side=side,
        prob=prob,
        strict_ok=strict_ok,
        reason="; ".join(reasons),
        ref_price=ref_price,
        atr_abs_15m=a15,
        atr_pct_15m=atr_pct
    )

async def top_k_candidates(symbols: List[str], k: int) -> List[Candidate]:
    tasks = [asyncio.create_task(analyze_symbol(s)) for s in symbols]
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

def slippage_pct(current: float, ref: float) -> float:
    if current <= 0 or ref <= 0:
        return 999.0
    return abs(current - ref) / ref * 100.0

# =========================
# UI
# =========================
def kb_admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="Сигнал", callback_data="sig_now")
    kb.adjust(1)
    return kb.as_markup()

def format_candidate_market(c: Candidate, idx: int, entry_market: float) -> str:
    entry, tp, sl = compute_levels_market(c.side, entry_market, c.atr_abs_15m)
    ok_enter = (c.prob >= ENTRY_MIN_PROB) and c.strict_ok

    badge = "ВХОД" if ok_enter else "ЖДАТЬ"
    note = "" if c.strict_ok else " (наблюдение)"

    # Подсказка для BingX
    hint = "Для BingX: TP/SL = рыночные, триггер = Mark Price."

    # Для SHORT SL должен быть выше входа, для LONG ниже
    if c.side == "SHORT":
        rule = "SHORT: стоп выше входа, тейк ниже входа."
    else:
        rule = "LONG: стоп ниже входа, тейк выше входа."

    return (
        f"<b>#{idx} {c.symbol}</b> - <b>{badge}</b>{note}\n"
        f"Сторона: <b>{c.side}</b> | Вероятность: <b>{c.prob}/10</b> | Strict: <b>{'OK' if c.strict_ok else 'NO'}</b>\n"
        f"Вход: <b>MARKET сейчас</b> ~= <code>{entry:.6f}</code>\n"
        f"TP: <code>{tp:.6f}</code> | SL: <code>{sl:.6f}</code>\n"
        f"<i>{rule} {hint}</i>\n"
        f"<i>{c.reason}</i>"
    )

# =========================
# HANDLERS
# =========================
@dp.message(CommandStart())
async def start_cmd(m: Message):
    if not only_admin(m.from_user.id):
        await m.answer("Этот бот приватный. Доступ запрещен.")
        return
    await m.answer(
        "Бот активен.\n"
        "Кнопка 'Сигнал' делает принудительный анализ и дает рыночный вход.\n"
        f"Автоанализ: каждые {AUTO_SCAN_EVERY_MIN} минут (если есть сильный сигнал, он придет сюда).",
        reply_markup=kb_admin_menu(),
    )

@dp.message(Command("myid"))
async def myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.callback_query(F.data == "sig_now")
async def sig_now(cb: CallbackQuery):
    if not only_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return

    await cb.answer("Анализ...")
    msg = await cb.message.answer(joke(JOKES_MANUAL))

    try:
        strict_syms = await top_symbols(MIN_QUOTE_VOL_STRICT, TOP_N_STRICT, "strict")
        manual_syms = await top_symbols(MIN_QUOTE_VOL_MANUAL, TOP_N_MANUAL_MAX, "manual")
        syms = list(dict.fromkeys(strict_syms + manual_syms))

        cands = await top_k_candidates(syms, SHOW_TOP_K)

        if not cands:
            await msg.edit_text("Сейчас нет достойных кандидатов.\n\n" + joke(JOKES_NO))
            return

        # Для каждого кандидата берем текущую цену и строим MARKET TP/SL от нее
        blocks = []
        for i, c in enumerate(cands, 1):
            cur = await get_last_price(c.symbol)
            if cur <= 0:
                continue
            slip = slippage_pct(cur, c.ref_price)
            # Если цена уже убежала слишком далеко от цены, на которой был сетап - помечаем
            warn = ""
            if slip > MAX_SLIPPAGE_PCT:
                warn = f"\n<i>Предупреждение: цена ушла на {slip:.2f}%. Лучше пересканировать позже.</i>"
            blocks.append(format_candidate_market(c, i, cur) + warn)

        if not blocks:
            await msg.edit_text("Не удалось получить текущие цены. Попробуй еще раз.\n\n" + joke(JOKES_ERR))
            return

        best_strict = pick_best_strict(cands)
        header = (
            f"<b>Топ-{SHOW_TOP_K} кандидата</b>\n"
            f"Вход только если <b>Prob >= {ENTRY_MIN_PROB}</b> и <b>Strict=OK</b>\n"
            f"Вход: рыночный (по текущей цене).\n\n"
        )

        tail = joke(JOKES_OK) if (best_strict and best_strict.prob >= ENTRY_MIN_PROB) else joke(JOKES_WAIT)
        await msg.edit_text(header + "\n\n".join(blocks) + "\n\n" + tail)

    except Exception as e:
        print("SIGNAL ERROR:", repr(e))
        await msg.edit_text("Ошибка данных (таймаут/лимит BingX). Попробуй еще раз через минуту.\n\n" + joke(JOKES_ERR))

# =========================
# AUTOSCAN (только тебе)
# =========================
async def autoscan_loop():
    global LAST_BROADCAST_TS
    while True:
        try:
            syms = await top_symbols(MIN_QUOTE_VOL_STRICT, TOP_N_STRICT, "strict")
            cands = await top_k_candidates(syms, SHOW_TOP_K)

            best = None
            if cands:
                strict_best = pick_best_strict(cands)
                if strict_best and strict_best.prob >= AUTO_MIN_PROB:
                    best = strict_best

            if best:
                now = int(time.time())
                if now - int(LAST_BROADCAST_TS) >= BROADCAST_COOLDOWN_SEC:
                    cur = await get_last_price(best.symbol)
                    if cur > 0:
                        slip = slippage_pct(cur, best.ref_price)
                        entry, tp, sl = compute_levels_market(best.side, cur, best.atr_abs_15m)

                        extra = ""
                        if slip > MAX_SLIPPAGE_PCT:
                            extra = f"\n<i>Предупреждение: цена ушла на {slip:.2f}%. Можно дождаться нового сетапа.</i>"

                        text = (
                            f"<b>Автоанализ (каждые {AUTO_SCAN_EVERY_MIN} минут)</b>\n\n"
                            f"<b>{best.symbol}</b>\n"
                            f"Сторона: <b>{best.side}</b>\n"
                            f"Вход: <b>MARKET сейчас</b> ~= <code>{entry:.6f}</code>\n"
                            f"TP: <code>{tp:.6f}</code>\n"
                            f"SL: <code>{sl:.6f}</code>\n"
                            f"Вероятность: <b>{best.prob}/10</b>\n"
                            f"<i>{best.reason}</i>\n"
                            f"<i>Для BingX: TP/SL = рыночные, триггер = Mark Price.</i>"
                            f"{extra}\n\n"
                            f"{joke(JOKES_OK)}"
                        )

                        LAST_BROADCAST_TS = now
                        try:
                            await bot.send_message(ADMIN_ID, text, reply_markup=kb_admin_menu())
                        except Exception:
                            pass

        except Exception as e:
            print("AUTOSCAN ERROR:", repr(e))

        await asyncio.sleep(AUTO_SCAN_EVERY_MIN * 60)

# =========================
# MAIN
# =========================
async def main():
    asyncio.create_task(autoscan_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
