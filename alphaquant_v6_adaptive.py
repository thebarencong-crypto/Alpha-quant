"""
ALPHAQUANT V6.0 (Adaptive / fail-closed reliability engine)
SMC Scalp 15m - Telegram signal engine (signal-only) for the top 50 OKX USDT perpetuals

Only one strategy remains: Smart Money Concept scalp on the 15m timeframe.

  1. Liquidity sweep of a recent swing (wick through, close back inside)
  2. Structure BEFORE the sweep agrees with the trade (BOS / CHOCH)
  3. Displacement candle after the sweep
  4. Order block = last opposing candle before the displacement
  5. Retest of the order block AFTER the displacement + confirmation candle
     (must be the latest candle; entry = its close)
  6. SL beyond OB / swept level / sweep wick, TP at SMC_RR

V5.3 reliability changes
- Paper entry references a live OKX last-price snapshot (not an old 15m close),
  with a maximum drift and age guard; this is NOT proof of a filled order.
- Legacy V5.3 monitored only 5m candles after signal creation; use candle end as
  exit timestamp and classify ambiguous SL+TP candles as UNRESOLVED.
- Old OPEN signals outside monitor coverage become UNRESOLVED (never fake WIN/LOSS).
- Swing structure at sweep time uses only already-confirmed fractals.
- Do not overwrite broken history files; exclude invalid old timestamps from stats.
- Optional volume/session filters, OFF by default until tested out of sample.

V5.2 changes
- Scan recap: after EVERY scan (each new 15m candle) a short recap is sent to Telegram
  (pairs scanned, new signals, filter funnel, near misses, open/closed summary).
  Env: SCAN_RECAP=0 disables it, SCAN_RECAP_NEAR=<n> sets the near-miss lines.

V5.1 changes
- Trades of the removed strategies (Breakout, Mean Rev, ...) are moved to
  h["archive"] on start-up: no more monitoring, notifications or statistics for them
- Notifications and statistics now only cover SMC scalp

V5.0 changes
- 4H setups, ICT and Adaptive removed (they failed backtests)
- SMC scans the top TOP_PAIRS pairs by 24h volume (default 50) on every new 15m candle,
  24 hours a day (there is no session filter; run the workflow every 15 minutes)
- Cost gate: SL must be >= SMC_MIN_RISK_PCT of price (default 0.4%)
- Portfolio cap: at most SMC_MAX_OPEN open scalps (default 5)
- Periodic report (every REPORT_EVERY_HOURS) with the scan funnel + near misses

Backtests of the previous BTC-only version were negative after fees. Treat every
signal as unproven until backtest.py shows an edge on the pairs you actually scan.

V6.0 controls (deliberately preset, NOT backtest-optimized):
- Verified SWAP 24h USDT notional, live-only instruments, and spread/quote freshness.
- 1m chronological, resumable paper monitoring, including the initial partial minute.
  Any touch in that first partial minute is UNRESOLVED (unknown pre-entry path).
- Direction-specific quote, adaptive target/risk by volatility and trend regime.
- Portfolio and 24-hour loss budget; durable Telegram notification outbox.
- Legacy performance kept separately from V6 (no artificial improvement claims).
- Signal-only, no orders, fills or automated leverage management.

"""

from __future__ import annotations

import os, json, time, math, logging, statistics, shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import requests

# ================= CONFIG =================

OKX = "https://www.okx.com"

def env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""

TOKEN = env_first("TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN", "BOT_TOKEN")
CHAT_ID = env_first("CHAT_ID", "TELEGRAM_CHAT_ID", "TELEGRAM_CHAT")

# --- SMC scalp ---
SMC_TF = "15m"
SMC_TF_MS = 15 * 60_000
SMC_MONITOR_TF = "1m"                 # earliest fully confirmed minute; partial minute is conservative
SMC_MONITOR_MS = 60_000
SMC_SYMBOL = "BTC-USDT-SWAP"          # reference symbol for the "new 15m candle" gate
SMC_HISTORY_BARS = 200
SMC_MIN_BARS = 80
SMC_SWING = 3
SMC_RR = 1.75
SMC_MIN_SL_ATR = 0.35
SMC_MAX_SL_ATR = 2.50
SMC_DISP_BODY_MIN = 0.55
SMC_DISP_ATR_MIN = 0.80
SMC_OB_LOOKBACK = 12
SMC_SWEEP_TOL_ATR = 0.15
SMC_CONFIRM_BARS = 1
SMC_COOLDOWN_HOURS = 4
SMC_MIN_RISK_PCT = float(os.getenv("SMC_MIN_RISK_PCT", "0.004"))   # 0 disables the cost gate
# Separate cost gate for BTC only (BTC moves little relative to its price, so the
# global 0.4% gate rejects almost every BTC signal). Negative = use SMC_MIN_RISK_PCT.
SMC_MIN_RISK_PCT_BTC = float(os.getenv("SMC_MIN_RISK_PCT_BTC", "-1"))
SMC_MAX_OPEN = int(os.getenv("SMC_MAX_OPEN", "10"))
ENGINE_VERSION = "ALPHAQUANT_V6_ADAPTIVE"
# Preset adaptive risk: these values are not tuned on the supplied 4-day log.
MAX_PORTFOLIO_RISK_PCT = float(os.getenv("MAX_PORTFOLIO_RISK_PCT", "0.04"))
LOSS_PAUSE_R_24H = float(os.getenv("LOSS_PAUSE_R_24H", "6.0"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "22"))
MAX_SPREAD_OF_RISK = float(os.getenv("MAX_SPREAD_OF_RISK", "0.12"))
MAX_TICKER_AGE_SEC = float(os.getenv("MAX_TICKER_AGE_SEC", "20"))
EST_SLIPPAGE_BPS_PER_SIDE = float(os.getenv("EST_SLIPPAGE_BPS_PER_SIDE", "3"))
MAX_MONITOR_CATCHUP_BARS = int(os.getenv("MAX_MONITOR_CATCHUP_BARS", "720"))
MAX_POSITION_AGE_HOURS = float(os.getenv("MAX_POSITION_AGE_HOURS", "72"))
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", "40"))
HISTORY_BACKUP_DAYS = int(os.getenv("HISTORY_BACKUP_DAYS", "14"))

# Experimental filters: OFF by default to avoid curve-fitting a small sample.
SMC_MIN_VOLUME_RATIO = float(os.getenv("SMC_MIN_VOLUME_RATIO", "0"))
SMC_SESSION_FILTER_WITA = os.getenv("SMC_SESSION_FILTER_WITA", "0").strip() == "1"
SMC_SESSION_START_WITA = int(os.getenv("SMC_SESSION_START_WITA", "6"))
SMC_SESSION_END_WITA = int(os.getenv("SMC_SESSION_END_WITA", "18"))  # exclusive
# Price at notification may differ from the candle close; reject stale / late signals.
SMC_MAX_SIGNAL_AGE_SEC = int(os.getenv("SMC_MAX_SIGNAL_AGE_SEC", "300"))
SMC_MAX_ENTRY_DRIFT_ATR = float(os.getenv("SMC_MAX_ENTRY_DRIFT_ATR", "0.5"))

# --- universe ---
TOP_PAIRS = int(os.getenv("TOP_PAIRS", "50"))
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "5000000"))

# --- indicators ---
RSI_PERIOD = 14
ATR_PERIOD = 14
VOL_PERIOD = 20
SL_ATR_BUFFER = 0.30

# --- risk / reporting ---
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.004"))
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", "1000"))
FEE_PER_SIDE = float(os.getenv("FEE_PER_SIDE", "0.0005"))          # taker 0.05%, for the fee estimate
REPORT_EVERY_HOURS = float(os.getenv("REPORT_EVERY_HOURS", "4"))
# Recap of every single scan (one per new 15m candle), sent to Telegram. 0 = off.
SCAN_RECAP = os.getenv("SCAN_RECAP", "1").strip() != "0"
SCAN_RECAP_NEAR = int(os.getenv("SCAN_RECAP_NEAR", "5"))           # near-miss lines per recap

MONITOR_MAX_BARS = MAX_MONITOR_CATCHUP_BARS
RR = 2.0                              # fallback when a stored trade has no "rr"

SCAN_INTERVAL = max(30, int(os.getenv("SCAN_INTERVAL", "60")))
RUN_FOREVER = os.getenv("RUN_FOREVER", "0").strip() == "1"
HISTORY_FILE = os.getenv("SIGNAL_HISTORY_FILE", "signals_history.json")
TIMEOUT = 15
RETRIES = 3
MIN_REQUEST_GAP = 0.09                # OKX public limit is 20 requests / 2 s per IP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("V6_ADAPTIVE")
S = requests.Session()
S.headers.update({"User-Agent": "ALPHAQUANT-V6"})
TELEGRAM = bool(TOKEN and CHAT_ID)

SETUP_NAME = {"SMC_SCALP": "SMC Scalp"}

# ================= HELPERS =================


def iso(ms):
    return datetime.fromtimestamp(
        ms / 1000, tz=timezone.utc
    ).isoformat()

def now():
    return datetime.now(timezone.utc).isoformat()

_LAST_REQ = [0.0]

def api(path, params=None):
    for attempt in range(RETRIES):
        wait = MIN_REQUEST_GAP - (time.monotonic() - _LAST_REQ[0])
        if wait > 0:
            time.sleep(wait)
        _LAST_REQ[0] = time.monotonic()
        try:
            r = S.get(
                OKX + path,
                params=params,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            d = r.json()
            if d.get("code") == "0":
                return d
            log.warning("OKX API response code=%s msg=%s", d.get("code"), d.get("msg"))
        except Exception as e:
            log.warning(
                "Request failed %s/%s: %s",
                attempt + 1, RETRIES, e
            )
        if attempt + 1 < RETRIES:
            time.sleep(min(8.0, 1.5 * (2 ** attempt)))
    return None


def tg(text):
    if not TELEGRAM:
        return False
    try:
        r = S.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=TIMEOUT
        )
        r.raise_for_status()
        return bool(r.json().get("ok") is True)
    except Exception as e:
        log.error("Telegram error: %s", e)
        return False

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {"open": [], "closed": [], "unresolved": []}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if not isinstance(d, dict):
            raise ValueError("history root harus object JSON")
        for key in ("open", "closed", "unresolved"):
            d.setdefault(key, [])
            if not isinstance(d[key], list):
                raise ValueError(f"history.{key} harus array")
        return d
    except (OSError, ValueError, TypeError) as e:
        # Never silently start empty: it would overwrite the only history copy.
        raise RuntimeError(f"History rusak/tidak bisa dibaca ({HISTORY_FILE}); data tidak ditimpa: {e}") from e

def queue_notification(h, key, body):
    """Durable at-least-once notification, not guaranteed exactly-once after timeouts."""
    outbox = h.setdefault("outbox", [])
    if not any(x.get("id") == key for x in outbox):
        outbox.append({"id": key, "text": body, "created_at": now(), "attempts": 0})


def flush_notifications(h, limit=15):
    if not TELEGRAM:
        return 0
    sent = 0
    for item in list(h.get("outbox", []))[:limit]:
        if not tg(item["text"]):
            item["attempts"] = int(item.get("attempts", 0)) + 1
            save_history(h)
            break
        h["outbox"].remove(item)
        save_history(h)
        sent += 1
    return sent


@contextmanager
def history_lock():
    """Avoid concurrent GitHub jobs corrupting the same local persistent history."""
    try:
        import fcntl
    except ImportError as exc:
        raise RuntimeError("Host requires POSIX fcntl locking (GitHub Ubuntu supported)") from exc
    lock_path = HISTORY_FILE + ".lock"
    lock_dir = os.path.dirname(os.path.abspath(lock_path))
    os.makedirs(lock_dir, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Bot instance already running on this filesystem") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def archive_legacy(h):
    """
    Move trades of removed strategies (Breakout, Mean Rev, ...) out of open/closed.
    They are kept under h["archive"] (not deleted) but are never monitored,
    reported or counted again. Returns how many trades were moved.
    """
    moved = 0
    for key in ("open", "closed"):
        keep, drop = [], []
        for x in h.get(key, []):
            (keep if x.get("setup") == "SMC_SCALP" else drop).append(x)
        if drop:
            h[key] = keep
            h.setdefault("archive", []).extend(dict(x, archived_from=key) for x in drop)
            moved += len(drop)
    return moved

def save_history(h):
    # Atomic update, retaining original on serialization/write failures.
    tmp = HISTORY_FILE + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(HISTORY_FILE)), exist_ok=True)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(h, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, HISTORY_FILE)
        # fsync containing directory after rename for crash-consistent commits.
        fd = os.open(os.path.dirname(os.path.abspath(HISTORY_FILE)), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)

def backup_history():
    """Keep dated pre-migration snapshots; caller holds the history lock.

    Backups are LOCAL: persistent runner/repo storage is still required.
    """
    if HISTORY_BACKUP_DAYS <= 0 or not os.path.exists(HISTORY_FILE):
        return None
    dest = HISTORY_FILE + ".bak." + datetime.now(timezone.utc).strftime("%Y%m%d")
    if not os.path.exists(dest):
        tmp = dest + ".tmp"
        try:
            with open(HISTORY_FILE, "rb") as source, open(tmp, "wb") as target:
                shutil.copyfileobj(source, target)
                target.flush()
                os.fsync(target.fileno())
            os.replace(tmp, dest)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
    prefix = os.path.basename(HISTORY_FILE) + ".bak."
    root = os.path.dirname(os.path.abspath(HISTORY_FILE))
    backups = sorted((x for x in os.listdir(root) if x.startswith(prefix)
                      and len(x) == len(prefix) + 8 and x[-8:].isdigit()))
    for name in backups[:-HISTORY_BACKUP_DAYS]:
        os.remove(os.path.join(root, name))
    return dest


# ================= MARKET DATA =================

def usdt_notional_ticker(t):
    """OKX SWAP volCcy24h is in BASE currency, while last is quote USDT/base.

    Official OKX V5 market/tickers API. Fail closed for incomplete/stale tickers.
    """
    try:
        volume_base = float(t["volCcy24h"])
        last = float(t["last"])
        if not all(math.isfinite(v) and v > 0 for v in (volume_base, last)):
            return 0.0
        return volume_base * last
    except (KeyError, TypeError, ValueError):
        return 0.0


def top_pairs(meta=None):
    d = api("/api/v5/market/tickers", {"instType": "SWAP"})
    if not d:
        return []
    a = []
    for t in d.get("data", []):
        inst = t.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        if meta is not None and (inst not in meta or meta[inst].get("state") != "live"):
            continue
        value_usdt = usdt_notional_ticker(t)
        if value_usdt >= MIN_VOLUME_USDT:
            a.append((inst, value_usdt))
    a.sort(key=lambda x: x[1], reverse=True)
    return [inst for inst, _ in a[:TOP_PAIRS]]


def instruments():
    d = api("/api/v5/public/instruments", {"instType": "SWAP"})
    out = {}
    if not d:
        return out
    for x in d.get("data", []):
        inst = x.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        try:
            tick = float(x["tickSz"])
            if tick <= 0 or not math.isfinite(tick):
                continue
            out[inst] = {"tick": tick, "state": x.get("state", "")}
        except (KeyError, TypeError, ValueError):
            continue
    return out


def get_quote(inst):
    """Fresh bid/ask ticker for conservative signal-only entry estimation."""
    d = api("/api/v5/market/ticker", {"instId": inst})
    try:
        t = d["data"][0]
        bid, ask, last = (float(t[k]) for k in ("bidPx", "askPx", "last"))
        ts = int(t["ts"])
        if not (0 < bid <= ask and last > 0 and all(map(math.isfinite, (bid, ask, last)))):
            return None
        return {"bid": bid, "ask": ask, "last": last, "ts": ts}
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def price(inst):
    quote = get_quote(inst)
    return quote["last"] if quote else None


def candles(inst, bar, needed):
    """
    OKX pagination using `after`.
    Returns confirmed candles oldest -> newest.
    """
    found = {}
    cursor = None
    max_requests = math.ceil(needed / 100) + 6

    for _ in range(max_requests):
        p = {
            "instId": inst,
            "bar": bar,
            "limit": "100"
        }
        if cursor is not None:
            p["after"] = str(cursor)

        d = api("/api/v5/market/candles", p)
        if not d:
            break

        rows = d.get("data", [])
        if not rows:
            break

        oldest = None

        for x in rows:
            try:
                ts = int(x[0])
                oldest = ts if oldest is None else min(oldest, ts)

                # x[8] == 1 means confirmed candle.
                if int(x[8]) != 1:
                    continue

                found[ts] = [
                    ts,
                    float(x[1]),  # open
                    float(x[2]),  # high
                    float(x[3]),  # low
                    float(x[4]),  # close
                    float(x[5])   # volume
                ]
            except:
                continue

        if len(found) >= needed:
            break

        if oldest is None or oldest == cursor:
            break

        cursor = oldest
        time.sleep(0.12)

    out = sorted(found.values(), key=lambda x: x[0])
    return out[-needed:]

def latest_candle_ts(bar=SMC_TF, inst=SMC_SYMBOL):
    """Open time of the newest confirmed candle (BTC as the reference clock)."""
    cs = candles(inst, bar, 3)
    return cs[-1][0] if cs else None

# ================= INDICATORS =================

def rma(v, n):
    out = [None] * len(v)
    if len(v) < n:
        return out
    x = sum(v[:n]) / n
    out[n - 1] = x
    for i in range(n, len(v)):
        x = ((x * (n - 1)) + v[i]) / n
        out[i] = x
    return out

def atr(cs, n=14):
    tr = []
    for i, c in enumerate(cs):
        if i == 0:
            tr.append(c[2] - c[3])
        else:
            pc = cs[i-1][4]
            tr.append(max(
                c[2] - c[3],
                abs(c[2] - pc),
                abs(c[3] - pc)
            ))
    return rma(tr, n)

def rsi(cs, n=14):
    closes = [x[4] for x in cs]
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0))
        losses.append(max(-ch, 0))
    ag, al = rma(gains, n), rma(losses, n)
    out = [None] * len(cs)
    for i in range(len(cs)):
        if ag[i] is None or al[i] is None:
            continue
        if al[i] == 0:
            out[i] = 100.0
        else:
            rs = ag[i] / al[i]
            out[i] = 100 - 100 / (1 + rs)
    return out

def volume_ratio(cs, n=20):
    """Latest confirmed volume divided by the PREVIOUS n confirmed candles."""
    if len(cs) < n + 1:
        return None
    avg = sum(x[5] for x in cs[-n-1:-1]) / n
    return cs[-1][5] / avg if avg > 0 else None

def tick_round(x, tick):
    if tick <= 0:
        return x
    return round(round(x/tick)*tick, 12)

def ema_last(cs, period):
    """EMA warm-up via first close (using available 200 bars)."""
    values = [c[4] for c in cs]
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e += k * (v - e)
    return e


def adaptive_profile(cs, atr_series, side):
    """Predefined, bounded profile. No online win-rate chasing / overfitting."""
    close = cs[-1][4]
    current_pct = atr_series[-1] / close
    past = [a / c[4] for a, c in zip(atr_series[-65:-1], cs[-65:-1])
            if a is not None and c[4] > 0]
    median_pct = statistics.median(past) if past else current_pct
    vol_mult = max(0.2, min(5.0, current_pct / max(median_pct, 1e-12)))
    e50, e200 = ema_last(cs, 50), ema_last(cs, 200)
    aligned = (e50 >= e200 and side == "LONG") or (e50 < e200 and side == "SHORT")
    trend_strength = abs(e50 - e200) / max(atr_series[-1], 1e-12)
    trending = trend_strength >= 1.0
    extreme = vol_mult >= 1.8
    low_vol = vol_mult <= 0.7
    # Reduce size when volatility shocks, or when direction opposes EMA trend.
    risk_multiplier = (0.60 if extreme else 0.85 if low_vol else 1.0)
    if trending and not aligned:
        risk_multiplier *= 0.75
    # RR remains bounded; this is a *hypothesis*, not an estimated optimal RR.
    rr = max(1.55, min(2.0, SMC_RR + (0.18 if trending and aligned else -0.12)
                           - (0.10 if extreme else 0.0)))
    regime = "VOLATILE" if extreme else "QUIET" if low_vol else "NORMAL"
    return {
        "volatility_ratio": round(vol_mult, 3),
        "market_regime": regime,
        "trend_aligned": aligned,
        "trend_strength_atr": round(trend_strength, 3),
        "risk_multiplier": round(max(0.35, min(1.0, risk_multiplier)), 3),
        "adaptive_rr": round(rr, 3),
        "ema50": e50, "ema200": e200,
    }


def round_tick(value, tick, mode="nearest"):
    if tick <= 0:
        raise ValueError("missing or invalid tick size")
    ratio = Decimal(str(value)) / Decimal(str(tick))
    if mode == "down":
        q = ratio.to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "up":
        q = ratio.to_integral_value(rounding=ROUND_CEILING)
    else:
        q = Decimal(round(ratio))
    return float(q * Decimal(str(tick)))


def conservative_levels(side, entry, stop, rr, tick):
    """Round SL outward and TP inward so target is never overstated by rounding."""
    sl = round_tick(stop, tick, "down" if side == "LONG" else "up")
    risk = entry - sl if side == "LONG" else sl - entry
    if risk <= 0:
        return None
    raw_tp = entry + rr * risk if side == "LONG" else entry - rr * risk
    tp = round_tick(raw_tp, tick, "down" if side == "LONG" else "up")
    if tp <= 0 or (side == "LONG" and tp <= entry) or (side == "SHORT" and tp >= entry):
        return None
    actual_rr = abs(tp - entry) / risk
    return sl, tp, actual_rr


# ================= REJECT TRACKING =================

SMC_STAGES = [
    "Data", "Volume", "Sesi", "Struktur", "Sweep", "Displacement",
    "OrderBlock", "Retest", "Konfirmasi", "SL/RR", "Diblokir"
]

CUR_SETUP = "SMC_SCALP"

# (setup, stage) -> [(pair, detail)], reset at the start of every scan.
REJECTS = {}

def _p(x):
    return f"{x:.8g}"


def _safe(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _rej(inst, stage, detail="", setup=None):
    key = (setup or CUR_SETUP, stage)
    REJECTS.setdefault(key, []).append((inst, _safe(detail)))
    return None

# ================= SMC SCALP (15m) =================

def _smc_swings(cs, swing=SMC_SWING):
    """Fractal swing highs/lows. Returns lists of (index, price)."""
    highs, lows = [], []
    for i in range(swing, len(cs) - swing):
        h, l = cs[i][2], cs[i][3]
        if all(h >= cs[i - j][2] for j in range(1, swing + 1)) and \
           all(h > cs[i + j][2] for j in range(1, swing + 1)):
            highs.append((i, h))
        if all(l <= cs[i - j][3] for j in range(1, swing + 1)) and \
           all(l < cs[i + j][3] for j in range(1, swing + 1)):
            lows.append((i, l))
    return highs, lows

def _smc_structure(highs, lows):
    """
    Determine last structural bias from swing sequence.
    Returns: bias ('BULL'|'BEAR'|None), event ('BOS'|'CHOCH'|None),
             last_sh, last_sl
    """
    if len(highs) < 2 or len(lows) < 2:
        return None, None, None, None

    sh1, sh0 = highs[-2], highs[-1]
    sl1, sl0 = lows[-2], lows[-1]
    last_sh, last_sl = sh0, sl0

    # Most recent swing event determines bias.
    # Higher high + higher low = bull structure; opposite = bear.
    hh = sh0[1] > sh1[1]
    hl = sl0[1] > sl1[1]
    lh = sh0[1] < sh1[1]
    ll = sl0[1] < sl1[1]

    if hh and hl:
        return "BULL", "BOS", last_sh, last_sl
    if lh and ll:
        return "BEAR", "BOS", last_sh, last_sl
    # CHOCH: only one side of structure flips
    if hh and ll:
        # conflict – use the more recent swing
        if sh0[0] > sl0[0]:
            return "BULL", "CHOCH", last_sh, last_sl
        return "BEAR", "CHOCH", last_sh, last_sl
    if lh and hl:
        if sl0[0] > sh0[0]:
            return "BEAR", "CHOCH", last_sh, last_sl
        return "BULL", "CHOCH", last_sh, last_sl
    return None, None, last_sh, last_sl

def _find_order_block(cs, side, start_i, end_i):
    """
    Order block = last opposing candle before the impulse that caused the
    structure break / sweep recovery.
    LONG  → last bearish candle (close < open) in [start_i, end_i)
    SHORT → last bullish candle
    Returns (index, high, low) or None.
    """
    if start_i < 0:
        start_i = 0
    end_i = min(end_i, len(cs))
    for i in range(end_i - 1, start_i - 1, -1):
        o, cl = cs[i][1], cs[i][4]
        if side == "LONG" and cl < o:
            return i, cs[i][2], cs[i][3]
        if side == "SHORT" and cl > o:
            return i, cs[i][2], cs[i][3]
    return None

def _is_displacement(c, atr_v, side):
    """Strong impulse candle in the trade direction."""
    o, hi, lo, cl = c[1], c[2], c[3], c[4]
    rng = hi - lo
    if rng <= 0 or not atr_v:
        return False
    body = abs(cl - o) / rng
    atr_mult = rng / atr_v
    if body < SMC_DISP_BODY_MIN or atr_mult < SMC_DISP_ATR_MIN:
        return False
    if side == "LONG":
        return cl > o
    return cl < o

def _smc_attempt(inst, cs, meta, a, rs, highs, lows, side, sweep_idx, swept_level):
    """
    Run steps 2-6 for ONE sweep candidate.
    Returns a signal dict, or (stage, detail) describing why it failed.
    """
    # Structure is judged on swings that existed BEFORE the sweep. The sweep
    # candle's own wick later becomes a swing low/high and must not flip the bias.
    # Each fractal needs SMC_SWING RIGHT-side candles to be knowable.
    # A swing whose right side was formed after the sweep is future leakage.
    pre_h = [x for x in highs if x[0] + SMC_SWING < sweep_idx]
    pre_l = [x for x in lows if x[0] + SMC_SWING < sweep_idx]
    bias, event, _, _ = _smc_structure(pre_h, pre_l)
    if bias is None:
        return "Struktur", "bias struktur tidak jelas"

    expected_bias = "BULL" if side == "LONG" else "BEAR"
    reversal = bias != expected_bias
    if reversal and event != "CHOCH":
        return "Struktur", f"sweep {side} melawan bias {bias} tanpa perubahan struktur"

    # --- Displacement after the sweep ---
    disp_idx = None
    for i in range(sweep_idx, min(sweep_idx + 6, len(cs))):
        if _is_displacement(cs[i], a, side):
            disp_idx = i
            break
    if disp_idx is None:
        return "Displacement", f"{side} tidak ada displacement setelah sweep"
    if reversal:
        # Aggressive reversal is allowed ONLY after price closes outside the
        # swept candle's opposite extreme (not just a permissive CHOCH label).
        broke = (cs[disp_idx][4] > cs[sweep_idx][2] if side == "LONG"
                 else cs[disp_idx][4] < cs[sweep_idx][3])
        if disp_idx <= sweep_idx or not broke:
            return "Struktur", "reversal tanpa directional break yang terkonfirmasi"

    # --- Order block: last opposing candle before displacement ---
    ob = _find_order_block(cs, side, max(0, disp_idx - SMC_OB_LOOKBACK), disp_idx)
    if ob is None:
        return "OrderBlock", f"{side} order block tidak ditemukan"
    ob_i, ob_hi, ob_lo = ob

    # Check *all* bars after displacement (not only the last 8). A historical
    # invalidation cannot be forgotten just because 8 bars passed.
    mid_ob = (ob_hi + ob_lo) / 2
    confirm_idx = None
    for i in range(disp_idx + 1, len(cs)):
        c = cs[i]
        if side == "LONG" and c[4] < ob_lo:
            return "Retest", "LONG order block pernah ditutup di bawah batas"
        if side == "SHORT" and c[4] > ob_hi:
            return "Retest", "SHORT order block pernah ditutup di atas batas"
        tapped = c[3] <= ob_hi and c[2] >= ob_lo
        if tapped and ((side == "LONG" and c[4] > mid_ob and c[4] > c[1]) or
                       (side == "SHORT" and c[4] < mid_ob and c[4] < c[1])):
            confirm_idx = i

    if confirm_idx is None:
        return "Retest", f"{side} belum ada retest + konfirmasi"
    if confirm_idx < len(cs) - SMC_CONFIRM_BARS:
        return "Konfirmasi", "konfirmasi lama; perlu retest/konfirmasi terkini"

    close = cs[-1][4]

    # SL: beyond the OB extreme, the swept level and the sweep wick
    if side == "LONG":
        sweep_ext = cs[sweep_idx][3]
        sl_raw = min(ob_lo, swept_level, sweep_ext) - SL_ATR_BUFFER * a
        risk = close - sl_raw
    else:
        sweep_ext = cs[sweep_idx][2]
        sl_raw = max(ob_hi, swept_level, sweep_ext) + SL_ATR_BUFFER * a
        risk = sl_raw - close

    if risk <= 0:
        return "SL/RR", f"{side} SL tidak valid"

    sl_atr = risk / a
    if not SMC_MIN_SL_ATR <= sl_atr <= SMC_MAX_SL_ATR:
        return (
            "SL/RR",
            f"{side} SL {sl_atr:.2f} ATR (perlu {SMC_MIN_SL_ATR}-{SMC_MAX_SL_ATR})"
        )

    # Cost gate: with ~0.10% round-trip taker fees, a tiny SL makes fees eat the R.
    risk_pct = risk / close
    min_risk = SMC_MIN_RISK_PCT_BTC if (inst == SMC_SYMBOL and SMC_MIN_RISK_PCT_BTC >= 0) \
        else SMC_MIN_RISK_PCT
    if risk_pct < min_risk:
        return (
            "SL/RR",
            f"{side} SL {risk_pct * 100:.2f}% (min {min_risk * 100:.2f}%, fee memakan R)"
        )

    profile = adaptive_profile(cs, atr(cs, ATR_PERIOD), side)
    tick = meta.get(inst, {}).get("tick")
    if not tick:
        return "Data", "tick size exchange tidak diketahui"
    entry = round_tick(close, tick)
    levels = conservative_levels(side, entry, sl_raw, profile["adaptive_rr"], tick)
    if levels is None:
        return "SL/RR", "SL/TP tidak valid setelah tick rounding"
    sl, tp, rr_actual = levels
    risk = abs(entry - sl)
    risk_pct = risk / entry
    # Score is an explainable, heuristic quality measure, NOT a win probability.
    vr = volume_ratio(cs, VOL_PERIOD) or 0.0
    score = 45 + (14 if not reversal else -6) + (9 if vr >= 1.0 else 0) + \
            (8 if profile["trend_aligned"] else 0) + \
            (7 if profile["market_regime"] == "NORMAL" else 0)
    score = min(100, max(0, score))
    if score < MIN_QUALITY_SCORE:
        return "Konfirmasi", f"quality {score} < {MIN_QUALITY_SCORE:.0f}"

    note = (
        f"SMC {event or 'BOS'} {bias} | "
        f"Sweep {'low' if side == 'LONG' else 'high'} @ {swept_level:.8g} | "
        f"OB [{ob_lo:.8g}-{ob_hi:.8g}] | "
        f"Disp bar {disp_idx} | RSI {rs[-1]:.1f}"
    )

    return {
        "signal_id": f"{inst}|{side}|{cs[-1][0]}|SMC",
        "symbol": inst,
        "direction": side,
        "timeframe": SMC_TF,
        "setup": "SMC_SCALP",
        "note": note,
        "signal_candle_time": cs[-1][0],
        "signal_candle_iso": iso(cs[-1][0]),
        "created_at": now(),
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr_actual,
        "atr": a,
        "sl_atr": risk / a,
        "ema50": profile["ema50"],
        "ema200": profile["ema200"],
        "rsi": rs[-1],
        "adx": None,
        "volume_ratio": vr,
        "regime": bias or "-",
        "signal_type": "REVERSAL" if reversal else "CONTINUATION",
        "market_regime": profile["market_regime"],
        "volatility_ratio": profile["volatility_ratio"],
        "trend_aligned": profile["trend_aligned"],
        "risk_multiplier": profile["risk_multiplier"],
        "engine_version": ENGINE_VERSION,
        "structure": event or "-",
        "bos": event or "NONE",
        "pullback": None,
        "score": score,
        "score_reasons": ["price action heuristic", profile["market_regime"]],
        "risk_usdt": ACCOUNT_EQUITY * RISK_PER_TRADE * profile["risk_multiplier"],
        "status": "OPEN",
        "exit_price": None,
        "exit_time": None,
        "result_r": None,
        "result": None,
        "monitor_tf_ms": SMC_MONITOR_MS,
    }

def build_smc_scalp(inst, cs, meta):
    """
    15m Smart Money Concept scalp, any USDT perpetual (long; short is mirrored):
      1. Liquidity sweep of a recent swing (wick through, close back inside)
      2. Structure BEFORE the sweep agrees with the trade (BOS/CHOCH)
      3. Displacement candle after the sweep
      4. Order block = last opposing candle before the displacement
      5. Price retests the OB AFTER the displacement and prints a
         confirmation candle (must be the latest candle)
      6. Entry at close, SL beyond OB / swept level / sweep wick, TP at SMC_RR
    Both directions are evaluated; the first valid one wins.
    """
    global CUR_SETUP
    CUR_SETUP = "SMC_SCALP"

    if len(cs) < SMC_MIN_BARS:
        return _rej(inst, "Data", f"hanya {len(cs)} candle (min {SMC_MIN_BARS})")

    at = atr(cs, ATR_PERIOD)
    rs = rsi(cs, RSI_PERIOD)
    if at[-1] is None or rs[-1] is None:
        return _rej(inst, "Data", "indikator belum siap")

    a = at[-1]
    if a <= 0:
        return _rej(inst, "Data", "ATR nol")

    vr = volume_ratio(cs, VOL_PERIOD)
    if SMC_MIN_VOLUME_RATIO > 0 and (vr is None or vr < SMC_MIN_VOLUME_RATIO):
        return _rej(inst, "Volume", f"{vr or 0:.2f}x < {SMC_MIN_VOLUME_RATIO:.2f}x")

    if SMC_SESSION_FILTER_WITA:
        # The signal candle is timestamped at its OPEN, so use its CLOSE time.
        hour_wita = int((cs[-1][0] + SMC_TF_MS + 8 * 3_600_000)
                        // 3_600_000 % 24)
        start, end = SMC_SESSION_START_WITA, SMC_SESSION_END_WITA
        allowed = (start <= hour_wita < end) if start < end else (
            hour_wita >= start or hour_wita < end)
        if not allowed:
            return _rej(inst, "Sesi", f"{hour_wita:02d}:00 WITA (di luar sesi)")

    highs, lows = _smc_swings(cs, SMC_SWING)
    if len(highs) < 2 or len(lows) < 2:
        return _rej(inst, "Struktur", "swing high/low kurang dari 2")

    # --- Sweep candidates (last 15 bars), newest first, both directions ---
    cands = []
    for side in ("LONG", "SHORT"):
        pool = lows if side == "LONG" else highs
        for abs_i in range(len(cs) - 1, max(0, len(cs) - 15) - 1, -1):
            c = cs[abs_i]
            for si, p in pool:
                if si + SMC_SWING >= abs_i or abs_i - si > 30:
                    continue
                if side == "LONG":
                    ok = (p - c[3]) / a >= SMC_SWEEP_TOL_ATR and c[4] > p
                else:
                    ok = (c[2] - p) / a >= SMC_SWEEP_TOL_ATR and c[4] < p
                if ok:
                    cands.append((side, abs_i, p))
                    break

    if not cands:
        return _rej(inst, "Sweep", "tidak ada liquidity sweep terkini")

    best = None
    valid = []
    for side, sweep_idx, swept in sorted(cands, key=lambda c: c[1], reverse=True):
        res = _smc_attempt(inst, cs, meta, a, rs, highs, lows, side, sweep_idx, swept)
        if isinstance(res, dict):
            valid.append((sweep_idx, res.get("score", 0), res))
            continue
        stage, detail = res
        depth = SMC_STAGES.index(stage)
        if best is None or depth > best[0]:
            best = (depth, stage, detail)

    if valid:
        # Most recent sweep wins, then quality score if equal time.
        return max(valid, key=lambda x: (x[0], x[1]))[2]
    return _rej(inst, best[1], best[2])

# ================= LIVE SIGNAL / DEDUPE =================

def _timestamp_ms(value):
    try:
        if value is None:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None  # explicit timezone is mandatory
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def activate_signal(sig, observed_quote, tick, timestamp_ms=None):
    """Conservative paper reference at executable-side quote, never a real fill."""
    tms = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
    closed_at = int(sig["signal_candle_time"]) + SMC_TF_MS
    age = (tms - closed_at) / 1000
    if age < -3 or age > SMC_MAX_SIGNAL_AGE_SEC:
        return "Konfirmasi", f"candle age {age:.0f} s (max {SMC_MAX_SIGNAL_AGE_SEC})"
    if not isinstance(observed_quote, dict):
        return "Data", "bid/ask ticker snapshot wajib tersedia"
    try:
        bid, ask, last = (float(observed_quote[x]) for x in ("bid", "ask", "last"))
        quote_ts = int(observed_quote["ts"])
    except (ValueError, KeyError, TypeError):
        return "Data", "ticker tidak lengkap"
    if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask and last > 0):
        return "Data", "bid/ask harga tidak valid"
    quote_age = (tms - quote_ts) / 1000
    if quote_age < -3 or quote_age > MAX_TICKER_AGE_SEC:
        return "Data", f"ticker stale/skew {quote_age:.1f}s"
    side = sig["direction"]
    observed_price = ask if side == "LONG" else bid
    spread_bps = (ask - bid) / ((ask + bid) / 2) * 10_000
    if spread_bps > MAX_SPREAD_BPS:
        return "SL/RR", f"spread {spread_bps:.1f}bps > {MAX_SPREAD_BPS:.1f}bps"
    ref = float(sig["entry"])
    av = float(sig.get("atr") or 0)
    drift_atr = abs(observed_price - ref) / av if av > 0 else float("inf")
    if drift_atr > SMC_MAX_ENTRY_DRIFT_ATR:
        return "Konfirmasi", f"quote bergeser {drift_atr:.2f} ATR"
    # Ask is rounded UP, bid DOWN: conservative worse-case reference.
    entry = round_tick(observed_price, tick, "up" if side == "LONG" else "down")
    raw_sl = float(sig["sl"])
    planned_rr = float(sig.get("rr") or SMC_RR)
    levels = conservative_levels(side, entry, raw_sl, planned_rr, tick)
    if levels is None:
        return "SL/RR", "SL/TP tidak valid di quote aktual"
    sl, tp, real_rr = levels
    risk = abs(entry - sl)
    sl_atr = risk / av if av > 0 else float("inf")
    if not SMC_MIN_SL_ATR <= sl_atr <= SMC_MAX_SL_ATR:
        return "SL/RR", f"SL quote {sl_atr:.2f} ATR di luar batas"
    min_risk = (SMC_MIN_RISK_PCT_BTC if sig["symbol"] == SMC_SYMBOL
                and SMC_MIN_RISK_PCT_BTC >= 0 else SMC_MIN_RISK_PCT)
    if risk / entry < min_risk:
        return "SL/RR", "SL terlalu dekat relatif fee"
    if (ask - bid) / risk > MAX_SPREAD_OF_RISK:
        return "SL/RR", f"spread memakan {(ask-bid)/risk:.1%} jarak SL"
    estimated_cost_r = (2 * FEE_PER_SIDE +
                        2 * EST_SLIPPAGE_BPS_PER_SIDE / 10000) * entry / risk
    if real_rr - estimated_cost_r < 1.20:
        return "SL/RR", f"net RR setelah biaya terlalu kecil ({real_rr-estimated_cost_r:.2f})"
    actual_at = iso(tms)
    updated = dict(sig)
    updated.update({
        "entry_reference": ref,
        "entry": entry, "sl": sl, "tp": tp, "rr": real_rr,
        "sl_atr": sl_atr,
        "created_at": actual_at, "activated_at": actual_at,
        "entry_source": "OKX_ASK_BID_PAPER_ESTIMATE_NOT_FILL",
        "quote_ts": quote_ts, "quote_age_sec": round(quote_age, 3),
        "spread_bps": round(spread_bps, 3),
        "fee_slippage_r_estimate": round(estimated_cost_r, 4),
        "entry_drift_atr": round(drift_atr, 4),
        "monitor_tf_ms": SMC_MONITOR_MS,
        "engine_version": ENGINE_VERSION,
    })
    return updated


def current_open_risk_usdt(h):
    return sum(max(0.0, float(x.get("risk_usdt", 0) or 0)) for x in h["open"])


def in_loss_pause(h, now_ms=None):
    if LOSS_PAUSE_R_24H <= 0:
        return False
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff = now_ms - 24 * 3600 * 1000
    recent = [x for x in h.get("closed", [])
              if x.get("engine_version") == ENGINE_VERSION
              and closed_time_valid(x)
              and (_timestamp_ms(x.get("exit_time")) or 0) >= cutoff]
    net_r = sum(float(t.get("result_r", 0)) - fee_r_of(t) -
                2 * EST_SLIPPAGE_BPS_PER_SIDE / 10000 *
                float(t["entry"]) / max(abs(float(t["entry"])-float(t["sl"])), 1e-12)
                for t in recent)
    return net_r <= -LOSS_PAUSE_R_24H


def symbol_blocked(h, inst):
    """Block open position and post-close cooldown, but not unresolved records."""
    if any(x.get("symbol") == inst for x in h["open"]):
        return True
    cutoff = time.time() - SMC_COOLDOWN_HOURS * 3600
    for x in h["closed"]:
        if x.get("symbol") != inst:
            continue
        try:
            if datetime.fromisoformat(x["exit_time"]).timestamp() > cutoff:
                return True
        except (KeyError, TypeError, ValueError):
            pass
    return False


def blocked(h, sig):
    sid = sig["signal_id"]
    known = h["open"] + h["closed"] + h.get("unresolved", [])
    if any(x.get("signal_id") == sid for x in known):
        return True
    return symbol_blocked(h, sig["symbol"])


def n_open(h):
    return len(h["open"])

# ================= MONITOR =================

def monitor_start_ms(trade, monitor_ms=SMC_MONITOR_MS):
    """Include first *partial* 1m bar: if it touches a level, unresolved."""
    candle_close = int(trade.get("signal_candle_time", 0)) + SMC_TF_MS
    activation = _timestamp_ms(trade.get("activated_at") or trade.get("created_at"))
    if activation is None:
        raise ValueError("trade tidak punya timestamp aktivasi timezone-aware")
    effective = max(candle_close, activation)
    return effective // monitor_ms * monitor_ms


def _unresolved(trade, reason, candle=None):
    t = dict(trade)
    t.update({"status": "UNRESOLVED", "result": None, "result_r": None,
              "unresolved_reason": reason, "unresolved_at": now()})
    if candle is not None:
        t["ambiguous_candle_open"] = iso(candle[0])
    return t


def monitor(trade, now_ms=None):
    """Chronological 1m paper monitor with a durable cursor and no skipped bars.

    A partial first minute can contain pre-activation extremes, so ANY level
    touch there is unresolved. Later 1m candles can still be SL+TP ambiguous.
    Data gaps, large downtime and exceeded TTL are unresolved, never WIN/LOSS.
    """
    mon_ms = SMC_MONITOR_MS
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    try:
        first = monitor_start_ms(trade, mon_ms)
    except ValueError as exc:
        return _unresolved(trade, str(exc))
    activated = _timestamp_ms(trade.get("activated_at") or trade.get("created_at"))
    effective_activation = max(int(trade.get("signal_candle_time", 0)) + SMC_TF_MS,
                               activated)
    if current - effective_activation > MAX_POSITION_AGE_HOURS * 3_600_000:
        return _unresolved(trade, f"monitor TTL {MAX_POSITION_AGE_HOURS:g}h terlampaui; rekonsiliasi manual")
    start = int(trade.get("monitor_next_ms", first))
    if start < first:
        return _unresolved(trade, "cursor monitoring mundur/corrupt")
    if current < start + mon_ms:
        return None
    bars_due = max(0, (current - start) // mon_ms)
    if bars_due > MONITOR_MAX_BARS:
        return _unresolved(trade, "monitor offline melebihi batas catch-up 1m")
    needed = min(MONITOR_MAX_BARS, max(10, bars_due + 2))
    cs = candles(trade["symbol"], SMC_MONITOR_TF, needed)
    if not cs:
        trade["monitor_api_failures"] = int(trade.get("monitor_api_failures", 0)) + 1
        return None  # transient API error: leave cursor untouched
    trade["monitor_api_failures"] = 0
    expected = start
    advanced = False
    for c in cs:
        if c[0] < start:
            continue
        if c[0] != expected:
            if c[0] > expected:
                return _unresolved(trade, f"missing 1m candle {iso(expected)} (data tidak kontinu)")
            continue
        if c[0] + mon_ms > current:
            break
        sl, tp = float(trade["sl"]), float(trade["tp"])
        if trade["direction"] == "LONG":
            hit_sl, hit_tp = c[3] <= sl, c[2] >= tp
        else:
            hit_sl, hit_tp = c[2] >= sl, c[3] <= tp
        if hit_sl or hit_tp:
            if c[0] == first and effective_activation > first:
                return _unresolved(trade, "level tersentuh pada menit aktivasi parsial; urutan tidak diketahui", c)
            if hit_sl and hit_tp:
                return _unresolved(trade, "SL dan TP pada 1m yang sama; urutan tidak diketahui", c)
            exit_price, result = (tp, "WIN") if hit_tp else (sl, "LOSS")
            r_value = ((abs(tp-float(trade["entry"])) /
                       abs(float(trade["entry"])-sl)) if hit_tp else -1.0)
            t = dict(trade)
            t.update({"status": "CLOSED", "exit_price": exit_price,
                      "exit_time": iso(c[0] + mon_ms),
                      "exit_candle_open": iso(c[0]),
                      "result_r": r_value, "result": result,
                      "monitor_tf_ms": mon_ms})
            return t
        expected += mon_ms
        advanced = True
    if advanced:
        trade["monitor_next_ms"] = expected
    elif cs[-1][0] > start and current >= start + 2 * mon_ms:
        return _unresolved(trade, "awal interval monitor hilang dari data API")
    return None


def check_open(h):
    if not h["open"]:
        return 0

    remain, closed, unresolved = [], [], []
    for t in h["open"]:
        try:
            result = monitor(t)
            if result is None:
                remain.append(t)
            elif result["status"] == "UNRESOLVED":
                unresolved.append(result)
            else:
                closed.append(result)
        except Exception as e:
            log.error("Monitor %s failed: %s", t.get("symbol", "-"), e)
            remain.append(t)

    h["open"] = remain
    if closed:
        h["closed"].extend(closed)
    if unresolved:
        h.setdefault("unresolved", []).extend(unresolved)
    for t in closed:
        emoji = "✅" if t["result"] == "WIN" else "❌"
        queue_notification(h, "closed:" + str(t.get("signal_id")),

            f"<b>{emoji} PAPER SIGNAL CLOSED</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>{t['symbol']}</b> {t['direction']}\n"
            f"Entry: <code>{t['entry']}</code>\n"
            f"Exit: <code>{t['exit_price']}</code>\n"
            f"Result: <b>{t['result_r']:+.2f}R</b>\n"
            f"Closed: {t['exit_time']}\n"
            "⚠️ 1m OHLC paper estimate; not an executed order"
        )
    if unresolved:
        log.warning("%s signals moved to UNRESOLVED (excluded from P/L).", len(unresolved))
        for item in unresolved:
            queue_notification(
                h, "unresolved:" + str(item.get("signal_id")),
                f"<b>⚠️ PAPER SIGNAL UNRESOLVED</b>\n"
                f"{_safe(item['symbol'])}: {_safe(item['unresolved_reason'])}\n"
                "Tidak dihitung WIN/LOSS; perlu rekonsiliasi historis."
            )
    # Save even if no trade closed: progress cursor and delivery outbox matter.
    save_history(h)
    flush_notifications(h)
    return len(closed)

# ================= REPORT =================


def float_r(t, p):
    risk = abs(t["entry"]-t["sl"])
    if risk <= 0:
        return 0
    return ((p-t["entry"])/risk
            if t["direction"]=="LONG"
            else (t["entry"]-p)/risk)

def running_report(h):
    if not h["open"]:
        return

    blocks, rs = [], []

    for t in h["open"]:
        p = price(t["symbol"])
        if p is None:
            continue
        r = float_r(t, p)
        rs.append(r)
        emoji = "🟢" if r > 0 else "🔴" if r < 0 else "⚪"
        blocks.append(
            f"{emoji} <b>{t['symbol']}</b> · {t['direction']}\n"
            f"Entry <code>{t['entry']}</code> | Now <code>{p}</code>\n"
            f"SL <code>{t['sl']}</code> | TP <code>{t['tp']}</code>\n"
            f"Float <b>{r:+.2f}R</b>"
        )

    if not rs:
        return

    total = sum(rs)
    avg = total / len(rs)

    tg(
        "<b>📡 RUNNING POSITIONS</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n" +
        "\n\n".join(blocks) +
        "\n\n━━━━━━━━━━━━━━━━━━\n"
        f"Open Positions : {len(h['open'])}\n"
        f"Profitable     : {sum(x > 0 for x in rs)}\n"
        f"Negative       : {sum(x < 0 for x in rs)}\n"
        f"Floating R     : <b>{total:+.2f}R</b>\n"
        f"Average Float  : {avg:+.2f}R\n"
        f"Best           : {max(rs):+.2f}R\n"
        f"Worst          : {min(rs):+.2f}R\n\n"
        f"TP Potential   : +{sum(float(x.get('rr', RR)) for x in h['open']):.2f}R\n"
        f"SL Exposure    : -{len(h['open']):.2f}R\n\n"
        "⚠️ Floating R is unrealized."
    )

def fee_r_of(x):
    """Estimated round-trip taker fee of a trade, in R."""
    try:
        entry = float(x["entry"])
        risk = abs(entry - float(x["sl"]))
        return (2 * FEE_PER_SIDE * entry / risk) if risk > 0 else 0.0
    except Exception:
        return 0.0

def closed_time_valid(t):
    """Only trust results whose recorded exit did not predate the notification."""
    opened = _timestamp_ms(t.get("activated_at") or t.get("created_at"))
    exited = _timestamp_ms(t.get("exit_time"))
    return opened is not None and exited is not None and exited >= opened


def stats(h):
    versioned = [t for t in h["closed"] if t.get("engine_version") == ENGINE_VERSION]
    ts = sorted((t for t in versioned if closed_time_valid(t)),
                key=lambda t: _timestamp_ms(t["exit_time"]))
    invalid_n = len(versioned) - len(ts)
    legacy_n = len(h["closed"]) - len(versioned)
    if not ts:
        log.info("No V6 closed trades yet; %s legacy results excluded.", legacy_n)
        return

    wins = [x for x in ts if x.get("result") == "WIN"]
    losses = [x for x in ts if x.get("result") == "LOSS"]
    total_r = sum(float(x.get("result_r", 0)) for x in ts)
    gp = sum(max(float(x.get("result_r", 0)), 0) for x in ts)
    gl = abs(sum(min(float(x.get("result_r", 0)), 0) for x in ts))
    pf = gp / gl if gl else float("inf")

    net_per_trade = [float(x.get("result_r", 0)) - fee_r_of(x) -
                     2 * EST_SLIPPAGE_BPS_PER_SIDE / 10000 *
                     float(x["entry"]) / max(abs(float(x["entry"])-float(x["sl"])), 1e-12)
                     for x in ts]
    eq = peak = dd = 0.0
    for net in net_per_trade:
        eq += net
        peak = max(peak, eq)
        dd = max(dd, peak - eq)

    pf_text = "∞" if math.isinf(pf) else f"{pf:.2f}"

    rrs = [float(x.get("rr", RR)) for x in ts]
    avg_rr = sum(rrs) / len(rrs)
    bep = 100 / (1 + avg_rr)
    fees = [fee_r_of(x) for x in ts]
    net_r = sum(net_per_trade)
    gp_net = sum(max(x, 0.0) for x in net_per_trade)
    gl_net = -sum(min(x, 0.0) for x in net_per_trade)
    pf_net = gp_net / gl_net if gl_net else float("inf")
    cost_r = (total_r - net_r) / len(ts)
    bep_net = 100 * (1 + cost_r) / (1 + avg_rr)

    note = ""
    if invalid_n:
        note += f"\n⚠️ {invalid_n} V6 records have invalid timestamps."
    if legacy_n:
        note += f"\n📁 {legacy_n} legacy CLOSED tidak dicampur ke V6."
    if h.get("unresolved"):
        note += f"\n⚠️ {len(h['unresolved'])} unresolved (tidak dihitung WIN/LOSS)."
    if len(ts) < 30:
        note += "\n\n<i>Kurang dari 30 trade: belum bisa disimpulkan.</i>"

    tg(
        "<b>📊 V6 PAPER SIGNAL PERFORMANCE</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Trades        : {len(ts)}\n"
        f"Wins          : {len(wins)}\n"
        f"Losses        : {len(losses)}\n"
        f"Win Rate      : <b>{len(wins) / len(ts) * 100:.2f}%</b>\n"
        f"Total R       : <b>{total_r:+.2f}R</b> (gross)\n"
        f"Total R net   : <b>{net_r:+.2f}R</b> (setelah fee ≈)\n"
        f"Avg R/Trade   : {total_r / len(ts):+.3f}R gross | {net_r / len(ts):+.3f}R net\n"
        f"Profit Factor : {pf_text} gross | {pf_net:.2f} net\n"
        f"Max Drawdown  : -{dd:.2f}R net (realized, exit-order)\n"
        f"Avg RR Target : 1:{avg_rr:.2f}\n"
        f"BEP WR        : {bep:.2f}% gross | <b>{bep_net:.2f}%</b> net" + note
    )

# ================= SIGNAL MESSAGE =================

def signal_message(s):
    setup = s.get("setup", "SMC_SCALP")
    risk = abs(s["entry"] - s["sl"])
    fee_r = (2 * FEE_PER_SIDE * s["entry"] / risk) if risk > 0 else 0.0
    risk_pct = (risk / s["entry"] * 100) if s["entry"] else 0.0

    head = (
        f"{s['timeframe']} | <b>{SETUP_NAME.get(setup, setup)}</b>\n"
        f"Structure: {s.get('structure', '-')} · Regime: {s.get('regime', '-')}\n"
        f"{_safe(s.get('note', ''))}\n\n"
    )

    return (
        "<b>🚨 ALPHAQUANT V6 ADAPTIVE SIGNAL</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"<b>{s['symbol']}</b> · <b>{s['direction']}</b>\n" +
        head +
        "<b>TRADE PLAN</b>\n"
        f"Entry: <code>{s['entry']}</code>\n"
        f"SL: <code>{s['sl']}</code>\n"
        f"TP: <code>{s['tp']}</code>\n"
        f"RR: <b>1:{float(s.get('rr', RR)):.2f}</b>\n"
        f"SL: {s['sl_atr']:.2f} ATR · {risk_pct:.2f}% harga\n"
        f"Fee ≈ {fee_r:.2f}R (taker {FEE_PER_SIDE * 100:.2f}%/sisi)\n\n"
        f"RSI: {s['rsi']:.2f} | Volume: {s['volume_ratio']:.2f}x\n"
        f"Risk: {s['risk_usdt']:.2f} USDT · Quality {s.get('score','-')}/100\n"
        f"Regime: {s.get('market_regime','-')} · Vol {s.get('volatility_ratio', 1):.2f}x\n"
        f"Spread: {s.get('spread_bps',0):.1f}bps · Cost ≈ {s.get('fee_slippage_r_estimate',0):.2f}R\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ SIGNAL ONLY — NO EXECUTION; entry = ASK(LONG)/BID(SHORT) estimate\n"
        f"Candle: {s['signal_candle_iso']} | Activated: {s.get('activated_at', '-')}"
    )

# ================= SCAN =================

def _tally(h, scanned, created, rows):
    """Accumulate the scan funnel in the history file until the next report."""
    d = h.get("smc_rej")
    if not d or "pairs" not in d:           # empty, or left over from V4.x (different format)
        d = {"scans": 0, "pairs": 0, "signals": 0, "stages": {}, "near": []}
    d["scans"] += 1
    d["pairs"] += scanned
    d["signals"] += created

    for (_, stage), items in rows.items():
        d["stages"][stage] = d["stages"].get(stage, 0) + len(items)
        if stage in SMC_STAGES and stage not in ("Data", "Struktur", "Sweep"):
            depth = SMC_STAGES.index(stage)
            for inst, detail in items:
                d["near"].append({"depth": depth, "text": f"{inst} → {stage}: {detail}"})

    d["near"] = sorted(d["near"], key=lambda x: -x["depth"])[:6]
    h["smc_rej"] = d

def smc_report_text(h):
    d = h.get("smc_rej")
    if not d or "pairs" not in d or not d.get("scans"):
        return ""
    parts_ = [f"{st} {d['stages'][st]}" for st in SMC_STAGES + ["Error"] if d["stages"].get(st)]
    text = (
        "<b>⚡ SMC SCALP · SCAN REPORT</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Scan         : {d['scans']} candle 15m\n"
        f"Pair dicek   : {d['pairs']}\n"
        f"Sinyal baru  : {d['signals']}\n\n"
        "<b>Gugur di filter</b> (filter pertama yang gagal)\n" +
        (" · ".join(parts_) if parts_ else "-")
    )
    if d.get("near"):
        text += "\n\n<b>Paling dekat lolos</b>\n" + "\n".join(x["text"] for x in d["near"][:4])
    return text

def scan_recap_text(ref, scanned, skipped, created, signals, rows, h,
                    note="", elapsed=None):
    """Recap of ONE scan (sent after every scan). `rows` = REJECTS of this scan."""
    candle = iso(ref)[:16].replace("T", " ") + " UTC" if ref else "-"
    emoji = "🚨" if created else "🔍"

    stages = {}
    near = []
    for (_, stage), items in rows.items():
        stages[stage] = stages.get(stage, 0) + len(items)
        if stage in SMC_STAGES and stage not in ("Data", "Struktur", "Sweep"):
            depth = SMC_STAGES.index(stage)
            for inst, detail in items:
                near.append((depth, f"{inst} → {stage}: {detail}"))
    near.sort(key=lambda x: -x[0])

    text = (
        f"<b>{emoji} SCAN RECAP · SMC Scalp {SMC_TF}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Candle       : {candle}\n"
        f"Pair dicek   : {scanned}\n"
        f"Dilewati     : {skipped} (posisi open/cooldown)\n"
        f"Sinyal baru  : <b>{created}</b>\n"
    )
    if elapsed is not None:
        text += f"Durasi scan  : {elapsed:.0f} dtk\n"

    if note:
        text += f"\n<i>{_safe(note)}</i>\n"

    if signals:
        text += "\n<b>Sinyal baru</b>\n"
        for s in signals:
            text += (
                f"• <b>{s['symbol']}</b> {s['direction']} | "
                f"E <code>{s['entry']}</code> SL <code>{s['sl']}</code> "
                f"TP <code>{s['tp']}</code>\n"
            )

    if stages:
        parts_ = [f"{st} {stages[st]}" for st in SMC_STAGES + ["Error"] if stages.get(st)]
        text += "\n<b>Gugur di filter</b>\n" + " · ".join(parts_) + "\n"

    if near and SCAN_RECAP_NEAR > 0:
        text += "\n<b>Paling dekat lolos</b>\n" + "\n".join(
            x[1] for x in near[:SCAN_RECAP_NEAR]
        ) + "\n"

    # Open positions snapshot
    opens = h.get("open", [])
    closed = [x for x in h.get("closed", []) if x.get("engine_version") == ENGINE_VERSION
              and closed_time_valid(x)]
    wins = sum(1 for x in closed if x.get("result") == "WIN")
    total_r = sum(float(x.get("result_r", 0) or 0) - fee_r_of(x) for x in closed)
    text += (
        "\n━━━━━━━━━━━━━━━━━━\n"
        f"Posisi open  : {len(opens)}/{SMC_MAX_OPEN}\n"
        f"Unresolved   : {len(h.get('unresolved', []))} (tanpa P/L)\n"
    )
    if closed:
        text += (
            f"Closed total : {len(closed)} ({wins}W/{len(closed) - wins}L) · "
            f"<b>{total_r:+.2f}R net-fee</b>"
        )
    else:
        text += "Closed total : 0"
    return text

def send_scan_recap(*args, **kwargs):
    if not SCAN_RECAP:
        return
    try:
        tg(scan_recap_text(*args, **kwargs))
    except Exception as e:
        log.error("Scan recap failed: %s", e)

def scan_smc(h):
    """
    Scan the top pairs for an SMC scalp on every NEW 15m candle.
    Returns the number of new signals.
    """
    global CUR_SETUP

    ref = latest_candle_ts()
    if ref is None:
        log.warning("Reference candle unavailable; scan skipped to avoid stale signals.")
        return 0
    if (time.time() * 1000 - ref - SMC_TF_MS) > SMC_MAX_SIGNAL_AGE_SEC * 1000:
        log.warning("Reference candle is stale; no trade scan on old candles.")
        return 0
    if ref is not None and h.get("last_smc_candle") == ref:
        log.info("No new %s candle since %s - scan skipped.", SMC_TF, iso(ref))
        return 0

    if any(int(x.get("monitor_api_failures", 0)) >= 2 for x in h["open"]):
        log.warning("Monitoring unhealthy; new entries paused until candle feed recovers.")
        send_scan_recap(ref, 0, 0, 0, [], {}, h,
                        note="Monitoring data belum sehat; entry baru dijeda.")
        return 0
    if in_loss_pause(h):
        log.warning("24h net loss circuit breaker triggered. New signals paused.")
        send_scan_recap(ref, 0, 0, 0, [], {}, h,
                        note="Loss circuit breaker aktif (24h). Monitoring tetap berjalan.")
        h["last_smc_candle"] = ref
        return 0
    if n_open(h) >= SMC_MAX_OPEN:
        log.info("SMC scan skipped: %s open scalps (cap %s).", n_open(h), SMC_MAX_OPEN)
        if ref is not None:
            h["last_smc_candle"] = ref
        send_scan_recap(
            ref, 0, 0, 0, [], {}, h,
            note=f"Scan dilewati: {n_open(h)} posisi open (batas {SMC_MAX_OPEN})."
        )
        return 0

    meta = instruments()
    if not meta:
        log.warning("Instrument metadata unavailable; scan aborted fail-closed.")
        return 0
    pairs = top_pairs(meta)
    if not pairs:
        log.warning("No pairs returned by OKX; scan aborted.")
        send_scan_recap(ref, 0, 0, 0, [], {}, h,
                        note="Scan gagal: OKX tidak mengembalikan daftar pair.")
        return 0

    log.info("SMC scan: %s pairs on %s...", len(pairs), SMC_TF)
    REJECTS.clear()
    CUR_SETUP = "SMC_SCALP"
    created, scanned, skipped = 0, 0, 0
    new_signals = []
    cap_hit = False
    t0 = time.time()

    for inst in pairs:
        if n_open(h) >= SMC_MAX_OPEN:
            log.info("Open scalp cap (%s) reached - stopping the scan.", SMC_MAX_OPEN)
            cap_hit = True
            break

        # Skip pairs that already have a position / are cooling down BEFORE
        # spending API calls on them.
        if symbol_blocked(h, inst):
            skipped += 1
            continue

        scanned += 1
        try:
            cs = candles(inst, SMC_TF, SMC_HISTORY_BARS)
            if len(cs) < SMC_MIN_BARS:
                _rej(inst, "Data", f"hanya {len(cs)} candle")
                continue

            # Stale data (no trades in the latest bar): never signal on an old candle.
            if ref is not None and cs[-1][0] != ref:
                _rej(inst, "Data", "candle terakhir tidak sinkron")
                continue

            s = build_smc_scalp(inst, cs, meta)
            if not s:
                continue

            if blocked(h, s):
                _rej(inst, "Diblokir", f"{s['direction']} duplikat/cooldown/posisi open")
                continue

            # The candle CLOSED before this GitHub run; do not mark that older
            # close as a live fill. Quote the latest OKX price before notifying.
            quote = get_quote(inst)
            activated = activate_signal(s, quote, meta[inst]["tick"])
            if not isinstance(activated, dict):
                stage, detail = activated
                _rej(inst, stage, detail)
                continue
            s = activated
            # Reduce risk after recent V6 stop-out cluster, without disabling signals.
            recent_losses = sorted((x for x in h.get("closed", [])
                                    if x.get("engine_version") == ENGINE_VERSION
                                    and closed_time_valid(x)),
                                   key=lambda x: x["exit_time"], reverse=True)
            if len(recent_losses) >= 3 and all(x.get("result") == "LOSS" for x in recent_losses[:3]):
                s["risk_usdt"] *= 0.65
                s["risk_multiplier"] = round(s.get("risk_multiplier", 1.0) * 0.65, 3)
            budget_usdt = ACCOUNT_EQUITY * MAX_PORTFOLIO_RISK_PCT
            if current_open_risk_usdt(h) + s["risk_usdt"] > budget_usdt + 1e-9:
                _rej(inst, "Diblokir", "batas risiko portofolio tercapai")
                continue
            h["open"].append(s)
            queue_notification(h, "signal:" + s["signal_id"], signal_message(s))
            save_history(h)
            created += 1
            new_signals.append(s)
            log.info("NEW SIGNAL %s %s rr=%.2f", s["symbol"], s["direction"], s["rr"])
            flush_notifications(h)
            time.sleep(0.2)

        except Exception as e:
            log.exception("Scan error %s: %s", inst, e)
            _rej(inst, "Error", str(e)[:60])

    log.info("SMC scan complete. Pairs: %s | New signals: %s", scanned, created)
    rows = dict(REJECTS)
    _tally(h, scanned, created, rows)
    REJECTS.clear()

    send_scan_recap(
        ref, scanned, skipped, created, new_signals, rows, h,
        note=f"Scan berhenti: batas {SMC_MAX_OPEN} posisi open tercapai." if cap_hit else "",
        elapsed=time.time() - t0
    )

    if ref is not None:
        h["last_smc_candle"] = ref
    return created

# ================= MAIN =================

def validate_config():
    if not (ACCOUNT_EQUITY > 0 and 0 < RISK_PER_TRADE <= 0.02):
        raise ValueError("ACCOUNT_EQUITY harus >0 dan RISK_PER_TRADE dalam (0, 0.02]")
    if not (0 < MAX_PORTFOLIO_RISK_PCT <= 0.10 and 0 < SMC_MAX_OPEN <= 50):
        raise ValueError("MAX_PORTFOLIO_RISK_PCT harus (0,0.10], SMC_MAX_OPEN (0,50]")
    if not (0 < TOP_PAIRS <= 100 and MIN_VOLUME_USDT > 0):
        raise ValueError("TOP_PAIRS (0,100], MIN_VOLUME_USDT >0")
    if (SMC_MAX_SIGNAL_AGE_SEC <= 0 or MAX_TICKER_AGE_SEC <= 0 or
            MAX_MONITOR_CATCHUP_BARS < 20 or FEE_PER_SIDE < 0 or
            HISTORY_BACKUP_DAYS < 0):
        raise ValueError("Konfigurasi waktu, monitoring atau fee tidak valid")


def _main():
    global TELEGRAM
    validate_config()

    log.info("==========================================")
    log.info("ALPHAQUANT V6 ADAPTIVE STARTING")
    log.info("==========================================")

    if not TOKEN or not CHAT_ID:
        TELEGRAM = False
        missing = []
        if not TOKEN:
            missing.append("TELEGRAM_TOKEN")
        if not CHAT_ID:
            missing.append("CHAT_ID")
        log.warning(
            "Telegram disabled. Missing Secret/Environment variable: %s",
            ", ".join(missing)
        )
        log.warning("GitHub: Settings > Secrets and variables > Actions")
    else:
        TELEGRAM = True
        log.info("Telegram credentials detected. Notifications ENABLED.")

    h = load_history()
    snapshot = backup_history()
    if snapshot:
        log.info("History pre-run backup: %s", snapshot)
    h.setdefault("outbox", [])
    moved = archive_legacy(h)
    if moved:
        log.info("Archived %s trades of removed strategies (Breakout / Mean Rev / ...).", moved)
        save_history(h)

    flush_notifications(h)
    log.info("Open trades: %s | Closed trades: %s | Unresolved: %s",
             len(h["open"]), len(h["closed"]), len(h.get("unresolved", [])))
    log.info("Optional filters | volume >= %.2fx (%s) | WITA session %02d-%02d (%s) | max age=%ss | max drift=%.2f ATR",
             SMC_MIN_VOLUME_RATIO, "ON" if SMC_MIN_VOLUME_RATIO > 0 else "OFF",
             SMC_SESSION_START_WITA, SMC_SESSION_END_WITA,
             "ON" if SMC_SESSION_FILTER_WITA else "OFF",
             SMC_MAX_SIGNAL_AGE_SEC, SMC_MAX_ENTRY_DRIFT_ATR)
    log.info(
        "Config | SMC@%s RR=1:%.2f | Pairs=%s | MaxOpen=%s | MinRisk=%.2f%% (BTC %s) | "
        "Monitor=%s | History=%s | Telegram=%s | RunForever=%s",
        SMC_TF, SMC_RR, TOP_PAIRS, SMC_MAX_OPEN, SMC_MIN_RISK_PCT * 100,
        f"{SMC_MIN_RISK_PCT_BTC * 100:.2f}%" if SMC_MIN_RISK_PCT_BTC >= 0 else "= sama",
        SMC_MONITOR_TF, HISTORY_FILE, "ON" if TELEGRAM else "OFF", RUN_FOREVER
    )

    today = datetime.now(timezone.utc).date().isoformat()
    if TELEGRAM and h.get("last_online_day") != today:
        h["last_online_day"] = today
        tg(
            "<b>🟢 ALPHAQUANT V6 ADAPTIVE ONLINE</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"SMC Scalp 15m · top {TOP_PAIRS} pair\n"
            "Sweep · Order Block · Displacement · Retest\n"
            f"Maks {SMC_MAX_OPEN} posisi · SL min {SMC_MIN_RISK_PCT * 100:.2f}% harga\n"
            "Signal-only"
        )

    while True:
        started = time.time()

        try:
            log.info("Starting engine cycle...")
            flush_notifications(h)
            closed_n = check_open(h)

            # Periodic report: every REPORT_EVERY_HOURS, or right after a trade closed.
            due = (time.time() - float(h.get("last_report_ts", 0))) >= REPORT_EVERY_HOURS * 3600
            if due:
                running_report(h)
                stats(h)
                text = smc_report_text(h)
                if text:
                    tg(text)
                    h["smc_rej"] = None
                h["last_report_ts"] = time.time()
            elif closed_n:
                stats(h)

            scan_smc(h)
            save_history(h)
        except KeyboardInterrupt:
            log.info("Engine stopped.")
            break
        except Exception as e:
            log.exception("MAIN LOOP ERROR: %s", e)

        elapsed = time.time() - started
        log.info(
            "Cycle finished in %.1fs. History: %s | Open: %s | Closed: %s",
            elapsed, HISTORY_FILE, len(h["open"]), len(h["closed"])
        )

        if not RUN_FOREVER:
            log.info("RUN_ONCE mode: exiting for GitHub Actions.")
            break

        sleep_for = max(10, SCAN_INTERVAL - elapsed)
        log.info("Cycle finished. Sleeping %.1f seconds.", sleep_for)
        time.sleep(sleep_for)

def main():
    with history_lock():
        _main()


if __name__ == "__main__":
    main()

