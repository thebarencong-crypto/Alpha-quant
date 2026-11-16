"""ALPHAQUANT V8 - Book Technical Research + SMC on OKX USDT-SWAP.
Paper signals only; all five Technical Research stages from Crypto Trading Guide.
"""
from __future__ import annotations

import os, json, time, math, logging, statistics, shutil, re, html
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

DISCORD_WEBHOOK_URL = env_first("DISCORD_WEBHOOK_URL")

# --- SMC scalp ---
SMC_TF = "15m"
SMC_TF_MS = 15 * 60_000
SMC_MONITOR_TF = "1m"                 # earliest fully confirmed minute; partial minute is conservative
SMC_MONITOR_MS = 60_000
SMC_SYMBOL = "BTC-USDT-SWAP"          # reference symbol for the "new 15m candle" gate
SMC_HISTORY_BARS = 200
SMC_MIN_BARS = 80
SMC_SWING = 3
SMC_RR = 2.10
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
MAX_OPEN_PER_DIRECTION = int(os.getenv("MAX_OPEN_PER_DIRECTION", "3"))
MAX_SIGNALS_PER_CANDLE = int(os.getenv("MAX_SIGNALS_PER_CANDLE", "3"))
ENGINE_VERSION = "ALPHAQUANT_V8_BOOK_PROTOCOL"
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
# Recap of every single scan (one per new 15m candle), sent to Discord. 0 = off.
SCAN_RECAP = os.getenv("SCAN_RECAP", "1").strip() != "0"
SCAN_RECAP_NEAR = int(os.getenv("SCAN_RECAP_NEAR", "5"))           # near-miss lines per recap

MONITOR_MAX_BARS = MAX_MONITOR_CATCHUP_BARS
RR = 2.0                              # fallback when a stored trade has no "rr"

SCAN_INTERVAL = max(30, int(os.getenv("SCAN_INTERVAL", "60")))
RUN_FOREVER = os.getenv("RUN_FOREVER", "0").strip() == "1"
# GitHub Actions run-once rescue: if an invocation lands just BEFORE a new
# 15m close, wait for the NEXT fully confirmed candle rather than scanning
# an old candle. A delayed exchange feed is also retried briefly AFTER close.
# Never extends SMC_MAX_SIGNAL_AGE_SEC or trades unconfirmed candles.
GITHUB_CANDLE_RESCUE = os.getenv("GITHUB_CANDLE_RESCUE", "1").strip() == "1"
GITHUB_PRE_CLOSE_WAIT_SEC = int(os.getenv("GITHUB_PRE_CLOSE_WAIT_SEC", "75"))
GITHUB_POST_CLOSE_POLL_SEC = int(os.getenv("GITHUB_POST_CLOSE_POLL_SEC", "40"))
HISTORY_FILE = os.getenv("SIGNAL_HISTORY_FILE", "signals_history.json")
TIMEOUT = 15
RETRIES = 3
MIN_REQUEST_GAP = 0.09                # OKX public limit is 20 requests / 2 s per IP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("V8_BOOK_PROTOCOL")
S = requests.Session()
S.headers.update({"User-Agent": "ALPHAQUANT-V8"})
DISCORD = bool(DISCORD_WEBHOOK_URL)

SETUP_NAME = {"SMC_SCALP": "SMC Scalp Legacy", "SMC_BOOK": "SMC + Akademi Crypto", "BOOK_CONTINUATION": "Book Trend/Continuation", "BOOK_REVERSAL": "Book Reversal"}

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


def discord_format(text):
    """Convert the existing Telegram HTML messages to Discord Markdown."""
    for tag, marker in (("b", "**"), ("i", "*"), ("code", "`")):
        text = re.sub(rf"</?{tag}>", marker, text, flags=re.IGNORECASE)
    return html.unescape(text)


def discord_chunks(text, max_length=1900):
    """Discord content is limited to 2000 characters per message."""
    while text:
        if len(text) <= max_length:
            yield text
            break
        split_at = text.rfind("\n", 0, max_length + 1)
        if split_at < max_length // 2:
            split_at = max_length
        yield text[:split_at]
        text = text[split_at:].lstrip("\n")


def notify(text):
    if not DISCORD:
        return False
    for chunk in discord_chunks(discord_format(text)):
        for attempt in range(3):
            try:
                response = S.post(
                    DISCORD_WEBHOOK_URL,
                    params={"wait": "true"},
                    json={
                        "content": chunk,
                        "username": "AlphaQuant V8",
                        "allowed_mentions": {"parse": []}
                    },
                    timeout=TIMEOUT
                )
                if response.status_code == 429:
                    try:
                        delay = float(response.json().get("retry_after", 1))
                    except (ValueError, TypeError):
                        delay = 1.0
                    log.warning("Discord rate limit. Retrying in %.1fs", delay)
                    time.sleep(min(max(delay, 0.5), 15))
                    continue
                response.raise_for_status()
                # wait=true means Discord confirms and returns the created message.
                if not response.json().get("id"):
                    log.error("Discord webhook did not confirm the posted message")
                    return False
                break
            except (requests.RequestException, ValueError) as exc:
                # Avoid logging a secret webhook URL from exception messages.
                log.error("Discord webhook delivery failed (%s)", type(exc).__name__)
                return False
        else:
            log.error("Discord rate limit: retries exhausted")
            return False
    return True

def load_history():
    if not os.path.exists(HISTORY_FILE):
        # V8 starts fresh ONLY through the explicit bootstrap step in state_manager.py.
        # A disappeared previously-persisted history must never become a silent reset.
        raise RuntimeError(
            f"V8 state missing: {HISTORY_FILE}. Run state_manager.py bootstrap first; "
            "do not restore legacy V7 history and do not auto-reset."
        )
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
    if not DISCORD:
        return 0
    sent = 0
    for item in list(h.get("outbox", []))[:limit]:
        if not notify(item["text"]):
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
            (keep if x.get("setup") in ("SMC_SCALP", "SMC_BOOK", "BOOK_CONTINUATION", "BOOK_REVERSAL") else drop).append(x)
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


def reference_candle_diagnostics(ref, now_ms=None):
    """Explain why a confirmed reference candle can/cannot be traded.

    The 15m reference is in UTC epoch milliseconds. This deliberately does
    NOT extend the 5m signal-age gate if GitHub starts late. A delayed feed
    is distinguished from a late scheduled workflow.
    """
    at_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    current_bar_open = (at_ms // SMC_TF_MS) * SMC_TF_MS
    expected_ref_open = current_bar_open - SMC_TF_MS
    next_close = current_bar_open + SMC_TF_MS
    status = {
        "now_ms": at_ms,
        "expected_ref_open_ms": expected_ref_open,
        "next_close_ms": next_close,
        "ref_open_ms": ref,
        "age_sec": None,
        "state": "NO_DATA",
    }
    if ref is None:
        return status
    ref_open = int(ref)
    status["age_sec"] = (at_ms - (ref_open + SMC_TF_MS)) / 1000
    if ref_open > expected_ref_open:
        status["state"] = "FUTURE_REFERENCE"
    elif ref_open < expected_ref_open:
        status["state"] = "FEED_BEHIND"
    elif status["age_sec"] < -3:
        status["state"] = "CLOCK_SKEW"
    elif status["age_sec"] > SMC_MAX_SIGNAL_AGE_SEC:
        status["state"] = "SCHEDULE_LATE"
    else:
        status["state"] = "READY"
    return status


def reference_for_scan():
    """Fetch a closed 15m reference and rescue near-boundary GitHub runs.

    Not a substitute for reliable scheduling: never accept a candle which
    has not been confirmed by the OKX API, never extend the 5m entry window,
    and never wait a full 15m in a one-shot CI run.
    """
    ref = latest_candle_ts()
    state = reference_candle_diagnostics(ref)
    if RUN_FOREVER or not GITHUB_CANDLE_RESCUE:
        return ref, state

    next_in_sec = (state["next_close_ms"] - state["now_ms"]) / 1000.0
    secs_since_boundary = (state["now_ms"] % SMC_TF_MS) / 1000.0
    pre_close_rescue = (state["state"] == "SCHEDULE_LATE"
                        and 0 < next_in_sec <= GITHUB_PRE_CLOSE_WAIT_SEC)
    # If this run began a few seconds after the boundary, the exchange may
    # not have marked the newest candle as confirmed yet.
    just_closed_feed_lag = (state["state"] == "FEED_BEHIND"
                            and secs_since_boundary < GITHUB_POST_CLOSE_POLL_SEC)

    if pre_close_rescue:
        log.info(
            "GitHub timing rescue: next 15m close in %.1fs; "
            "waiting for a CONFIRMED candle, not trading the old one.",
            next_in_sec,
        )
        time.sleep(next_in_sec + 1.0)
    elif not just_closed_feed_lag:
        return ref, state

    if GITHUB_POST_CLOSE_POLL_SEC <= 0:
        ref = latest_candle_ts()
        return ref, reference_candle_diagnostics(ref)

    # Bounded time. A stalled OKX API must not make a CI job hang forever.
    post_elapsed = 0.0 if pre_close_rescue else secs_since_boundary
    deadline = time.monotonic() + max(0.0, GITHUB_POST_CLOSE_POLL_SEC - post_elapsed)
    while True:
        ref = latest_candle_ts()
        state = reference_candle_diagnostics(ref)
        if state["state"] == "READY":
            log.info("GitHub timing rescue OK: confirmed candle=%s UTC, age=%.1fs",
                     iso(ref), state["age_sec"])
            return ref, state
        remaining = deadline - time.monotonic()
        if state["state"] not in ("FEED_BEHIND", "NO_DATA") or remaining <= 0:
            log.warning("GitHub timing rescue ended safely: reference state=%s", state["state"])
            return ref, state
        time.sleep(min(2.0, remaining))

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

# ================= AKADEMI CRYPTO TECHNICAL RESEARCH =================
# Book mapping: Chapter 3 / Technical Research (five stages).
# ALL named technical concepts in the book are represented as detectors/evidence.
# Algorithms are explicit, conservative INTERPRETATIONS (the book is not executable code).
# The VPFR/POC computation below is an OHLCV approximation; true VPFR needs intrabar trades.
# Sources: Crypto Trading Guide, Akademi Crypto, first ed. 2023, pages 65-139.
# Never use future candles or unconfirmed bar data. No execution or leverage management.

BOOK_MIN_SCORE = float(os.getenv("BOOK_MIN_SCORE", "57"))
BOOK_MIN_RR = max(2.0, float(os.getenv("BOOK_MIN_RR", "2.0")))
BOOK_HTF = os.getenv("BOOK_HTF", "1H").strip()
BOOK_ALLOW_SMC = os.getenv("BOOK_ALLOW_SMC", "1").strip() != "0"
BOOK_VOLUME_BREAKOUT_MIN = float(os.getenv("BOOK_VOLUME_BREAKOUT_MIN", "1.05"))
BOOK_SWING_LOOKBACK = max(40, int(os.getenv("BOOK_SWING_LOOKBACK", "90")))
# The book explicitly uses RSI 30/70; V7 used 35/65 heuristics.
BOOK_RSI_OVERSOLD = float(os.getenv("BOOK_RSI_OVERSOLD", "30"))
BOOK_RSI_OVERBOUGHT = float(os.getenv("BOOK_RSI_OVERBOUGHT", "70"))
# 20/80 is an explicit programmable convention for Stochastic 5/3/3.
BOOK_STOCH_OVERSOLD = float(os.getenv("BOOK_STOCH_OVERSOLD", "20"))
BOOK_STOCH_OVERBOUGHT = float(os.getenv("BOOK_STOCH_OVERBOUGHT", "80"))
BOOK_PROTOCOL_STRICT = os.getenv("BOOK_PROTOCOL_STRICT", "1").strip() != "0"

BOOK_FAMILIES = {
    "trend": ["market_programming_HH_HL_LH_LL", "primary_secondary_trend",
              "trendline", "MA_9_13_21_33_55_90", "EMA13_21_money_magnet_cross",
              "bollinger20", "supertrend"],
    "area": ["horizontal_SR", "supply_demand", "area_flip", "liquidity_grab",
             "dynamic_trendline_SR", "fib_golden_pocket_050_0618",
             "fibonacci_extension_1618"],
    "chart_pattern": ["bull_pennant", "bear_pennant", "bull_flag", "bear_flag",
                      "ascending_triangle", "descending_triangle", "head_shoulders",
                      "inverse_head_shoulders", "double_top", "triple_top",
                      "double_bottom", "triple_bottom", "rising_wedge", "falling_wedge"],
    "candlestick": ["hammer", "inverted_hammer", "bull_engulfing", "tweezer_bottom",
                    "hanging_man", "shooting_star", "bear_engulfing", "tweezer_top"],
    "indicators": ["RSI14_70_30", "stochastic_5_3_3", "MACD_12_26_9",
                   "MACD_divergence", "RSI_divergence", "volume_bar_breakout", "VPFR_POC_proxy",
                   "OBV", "Bollinger_volatility", "Supertrend_ATR"],
    "entry_exit": ["market_structure_entry_exit", "psychological_levels", "ATR_stop_buffer", "RR_min_2", "scale_in_out_plan_only",
                   "fee_slippage_guard", "position_risk_budget"],
}


def _avg(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _series_ema(values, span):
    if not values:
        return []
    k = 2.0 / (span + 1)
    out = [float(values[0])]
    for x in values[1:]:
        out.append(out[-1] + k * (float(x) - out[-1]))
    return out


def _linear(values):
    """Slope and value at next index; only uses already-known values."""
    n = len(values)
    if n < 2:
        return (0.0, float(values[-1]) if values else 0.0)
    sx = n * (n - 1) / 2
    sxx = n * (n - 1) * (2 * n - 1) / 6
    sy = sum(values)
    sxy = sum(i * v for i, v in enumerate(values))
    den = n * sxx - sx * sx
    slope = (n * sxy - sx * sy) / den if den else 0.0
    return slope, (sy - slope * sx) / n + slope * n


def _pivots(cs, width=2):
    """Confirmed fractals strictly before latest candle; no look-ahead."""
    highs, lows = [], []
    for i in range(width, len(cs) - width):
        if all(cs[i][2] > cs[j][2] for j in range(i - width, i + width + 1) if j != i):
            highs.append((i, cs[i][2]))
        if all(cs[i][3] < cs[j][3] for j in range(i - width, i + width + 1) if j != i):
            lows.append((i, cs[i][3]))
    return highs, lows


def _line_at(points, idx):
    if len(points) < 2:
        return None
    (x0, y0), (x1, y1) = points[-2:]
    return y1 + (y1 - y0) / max(1, x1 - x0) * (idx - x1)


def _supertrend(cs, period=10, multiplier=3.0):
    if len(cs) < period + 3:
        return 0, None
    avs = atr(cs, period)
    last_upper = last_lower = None
    direction = 0
    line = None
    for i in range(period + 1, len(cs)):
        av = avs[i]
        if av is None or not av > 0:
            continue
        high, low, close = cs[i][2:5]
        mid = (high + low) / 2
        basic_upper = mid + multiplier * av
        basic_lower = mid - multiplier * av
        prev_close = cs[i-1][4]
        upper = (basic_upper if last_upper is None or basic_upper < last_upper or prev_close > last_upper
                 else last_upper)
        lower = (basic_lower if last_lower is None or basic_lower > last_lower or prev_close < last_lower
                 else last_lower)
        if direction == 0:
            direction = 1 if close >= mid else -1
        elif direction == 1 and close < lower:
            direction = -1
        elif direction == -1 and close > upper:
            direction = 1
        line = lower if direction == 1 else upper
        last_upper, last_lower = upper, lower
    return direction, line


def _stochastic(cs):
    """Stochastic 5/3/3, returns previous/current K and D, range [0,100]."""
    if len(cs) < 14:
        return None
    raw = []
    for i in range(4, len(cs)):
        lo = min(c[3] for c in cs[i-4:i+1]); hi = max(c[2] for c in cs[i-4:i+1])
        raw.append(50.0 if hi == lo else 100 * (cs[i][4]-lo)/(hi-lo))
    sm = [_avg(raw[i-2:i+1]) for i in range(2, len(raw))]
    ds = [_avg(sm[i-2:i+1]) for i in range(2, len(sm))]
    if len(ds) < 2:
        return None
    return (sm[-2], sm[-1], ds[-2], ds[-1])


def _macd(cs):
    if len(cs) < 35:
        return None
    close = [c[4] for c in cs]
    e12 = _series_ema(close, 12); e26 = _series_ema(close, 26)
    m = [a-b for a,b in zip(e12,e26)]
    signal = _series_ema(m, 9)
    return (m[-2]-signal[-2], m[-1]-signal[-1], m[-1], signal[-1])


def _macd_hist_series(cs):
    """Causal MACD (12,26,9) histogram, all bars; no lookahead."""
    if len(cs) < 35:
        return []
    closes = [c[4] for c in cs]
    ema12 = _series_ema(closes, 12)
    ema26 = _series_ema(closes, 26)
    fast_slow = [a - b for a, b in zip(ema12, ema26)]
    signal = _series_ema(fast_slow, 9)
    return [a - b for a, b in zip(fast_slow, signal)]


def _psychological_levels(price):
    """Round-number areas, a bot-defined approximation of psychological SR."""
    if not (math.isfinite(price) and price > 0):
        return None
    magnitude = 10 ** math.floor(math.log10(price))
    step = magnitude / 10
    floor = math.floor(price / step) * step
    ceil = floor + step
    return {"below": floor, "above": ceil, "step": step}


def _scale_plan(side, entry, stop, target):
    """Informational paper plan ONLY, never a claim of partially executed fills."""
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    direction = 1 if side == "LONG" else -1
    return {"mode": "ADVISORY_ONLY_NO_ORDERS_NO_PARTIAL_PNL",
            "entry_1": entry,
            "entry_2_optional": entry - direction * min(.25 * risk, .50 * risk),
            "take_profit_1_optional": entry + direction * risk,
            "take_profit_2": target,
            "stop": stop,
            "allocation_note": "No second entry/partial exit is executed or booked in performance stats"}


def _obv(cs):
    score = 0.0
    out = [0.0]
    for prev, curr in zip(cs, cs[1:]):
        score += curr[5] * (1 if curr[4] > prev[4] else -1 if curr[4] < prev[4] else 0)
        out.append(score)
    return out


def _poc_proxy(cs, bins=30, lookback=75):
    """Candle-volume distributed uniformly over each high/low bin.

    This is NOT trade/tick accurate Volume Profile Fixed Range (VPFR).
    """
    window = cs[-lookback:]
    lo = min(c[3] for c in window); hi = max(c[2] for c in window)
    if hi <= lo:
        return None
    step = (hi-lo)/bins
    volumes = [0.0]*bins
    for c in window:
        a = max(0,min(bins-1,int((c[3]-lo)/step)))
        b = max(0,min(bins-1,int((c[2]-lo)/step)))
        qty = max(0.0,c[5])/(b-a+1)
        for j in range(a,b+1):
            volumes[j] += qty
    idx = max(range(bins), key=lambda i: volumes[i])
    return lo + (idx+0.5)*step


def _candle_triggers(cs, atr_value):
    a,b = cs[-2],cs[-1]
    tol = max(atr_value*.13, b[4]*.00018)
    o,h,l,c=b[1:5]; ao,ah,al,ac=a[1:5]
    rg=max(h-l,1e-12); body=max(abs(c-o), rg*.035)
    upper=h-max(o,c); lower=min(o,c)-l
    triggers={"LONG":[],"SHORT":[]}
    # Same silhouette has opposing meanings depending on what PRECEDED it.
    # Use already closed bars only, and do not label both directions at once.
    context_prev = _avg([x[4] for x in cs[-11:-6]])
    context_recent = _avg([x[4] for x in cs[-6:-1]])
    local_up = context_recent > context_prev
    local_down = context_recent < context_prev
    if lower>=2*body and upper<=body*.8 and (c-l)/rg>=.60:
        if local_down: triggers["LONG"].append("hammer")
        if local_up: triggers["SHORT"].append("hanging_man")
    if upper>=2*body and lower<=body*.8:
        if local_down: triggers["LONG"].append("inverted_hammer")
        if local_up: triggers["SHORT"].append("shooting_star")
    if ac<ao and c>o and o<=ac and c>=ao and abs(c-o)>.65*abs(ac-ao):
        triggers["LONG"].append("bull_engulfing")
    if ac>ao and c<o and o>=ac and c<=ao and abs(c-o)>.65*abs(ac-ao):
        triggers["SHORT"].append("bear_engulfing")
    if abs(l-al)<=tol and c>o and c>ac:
        triggers["LONG"].append("tweezer_bottom")
    if abs(h-ah)<=tol and c<o and c<ac:
        triggers["SHORT"].append("tweezer_top")
    return triggers


def _book_chart_patterns(cs, av):
    """Test 14 patterns with price-only causal rules; last bar is breakout/trigger.

    The textbook diagrams are discretionary; these operational definitions are
    deliberately documented approximations, not a claim of exact visual recognition.
    """
    if len(cs)<62 or av<=0:
        return {"LONG":[],"SHORT":[]}
    out={"LONG":[],"SHORT":[]}; n=len(cs); prev=cs[-2]; last=cs[-1]
    highs,lows=_pivots(cs[:-1],2)
    highs=[p for p in highs if p[0]>=n-55]
    lows=[p for p in lows if p[0]>=n-55]
    # Confirmed double/triple reversal: similar peaks/valleys, then neckline break.
    for side,points,opposites in (("SHORT",highs,lows),("LONG",lows,highs)):
        for count in (3,2):
            if len(points)<count: continue
            xs=points[-count:]
            if xs[-1][0]-xs[0][0]<count*3: continue
            if max(y for _,y in xs)-min(y for _,y in xs)>.70*av: continue
            mids=[p[1] for p in opposites if xs[0][0]<p[0]<xs[-1][0]]
            if not mids: continue
            neckline=(min(mids) if side=="SHORT" else max(mids))
            crossed = (prev[4]>=neckline and last[4]<neckline-.05*av) if side=="SHORT" else (prev[4]<=neckline and last[4]>neckline+.05*av)
            if crossed:
                out[side].append(("triple_" if count==3 else "double_")+("top" if side=="SHORT" else "bottom"))
                break
    # Head and shoulders, both directions, only breakout of neckline after right shoulder.
    for side,pts,opps,name in (("SHORT",highs,lows,"head_shoulders"),("LONG",lows,highs,"inverse_head_shoulders")):
        if len(pts)>=3:
            p0,p1,p2=pts[-3:]
            shoulders=abs(p0[1]-p2[1])<=1.25*av
            head=(p1[1]>max(p0[1],p2[1])+.35*av) if side=="SHORT" else (p1[1]<min(p0[1],p2[1])-.35*av)
            valleys=[p[1] for p in opps if p0[0]<p[0]<p2[0]]
            if shoulders and head and valleys and p2[0] < n-2:
                neck = min(valleys) if side=="SHORT" else max(valleys)
                crossed=(prev[4]>=neck and last[4]<neck) if side=="SHORT" else (prev[4]<=neck and last[4]>neck)
                if crossed: out[side].append(name)
    # Flag/pennant and ascending/descending triangles; pole is confirmed old momentum.
    consolidation=cs[-11:-1]
    pole=cs[-22:-11]
    if len(consolidation)==10 and len(pole)==11:
        hi=[c[2] for c in consolidation]; lo=[c[3] for c in consolidation]
        hs,ht=_linear(hi); ls,lt=_linear(lo)
        pole_move=pole[-1][4]-pole[0][1]
        width=_avg([h-l for h,l in zip(hi,lo)])
        converges=(ht-lt)<(hi[0]-lo[0])*.85 and ht>lt
        new_up=last[4]>ht+.05*av and prev[4]<=max(hi[-1],ht)+.15*av
        new_dn=last[4]<lt-.05*av and prev[4]>=min(lo[-1],lt)-.15*av
        if pole_move>2*av and width<2.8*av:
            if hs<-.02*av and ls<-.02*av and abs(hs-ls)<.12*av and new_up:
                out["LONG"].append("bull_flag")
            if hs<-.015*av and ls>.01*av and converges and new_up:
                out["LONG"].append("bull_pennant")
        if pole_move<-2*av and width<2.8*av:
            if hs>.02*av and ls>.02*av and abs(hs-ls)<.12*av and new_dn:
                out["SHORT"].append("bear_flag")
            if hs<-.01*av and ls>.015*av and converges and new_dn:
                out["SHORT"].append("bear_pennant")
        if abs(hs)<.07*av and ls>.045*av and new_up and converges:
            out["LONG"].append("ascending_triangle")
        if abs(ls)<.07*av and hs<-.045*av and new_dn and converges:
            out["SHORT"].append("descending_triangle")
        if hs>.025*av and ls>hs+.025*av and converges and new_dn:
            out["SHORT"].append("rising_wedge")
        if ls<-.025*av and hs<ls-.025*av and converges and new_up:
            out["LONG"].append("falling_wedge")
    return out


def _book_features(cs, htf=None):
    if len(cs)<90:
        return None
    av=atr(cs,14)[-1]
    if not av or av<=0:
        return None
    close=cs[-1][4]
    prices=[c[4] for c in cs]
    mas={n:_avg(prices[-n:]) for n in (9,13,21,33,55,90)}
    ema13=_series_ema(prices,13); ema21=_series_ema(prices,21)
    slow_st,st_line=_supertrend(cs)
    rsi_seq=rsi(cs,14)
    stoch=_stochastic(cs); macd=_macd(cs); obv=_obv(cs)
    highs,lows=_pivots(cs[:-1],3)
    highs=[x for x in highs if x[0]>=len(cs)-BOOK_SWING_LOOKBACK]
    lows=[x for x in lows if x[0]>=len(cs)-BOOK_SWING_LOOKBACK]
    struct="RANGE"
    if len(highs)>=2 and len(lows)>=2:
        if highs[-1][1]>highs[-2][1] and lows[-1][1]>lows[-2][1]: struct="BULL"
        elif highs[-1][1]<highs[-2][1] and lows[-1][1]<lows[-2][1]: struct="BEAR"
    htf_bias="RANGE"
    if htf and len(htf)>=55:
        hp=[c[4] for c in htf]
        he13=_series_ema(hp,13)[-1]; he21=_series_ema(hp,21)[-1]
        if he13>he21 and hp[-1]>he21: htf_bias="BULL"
        elif he13<he21 and hp[-1]<he21: htf_bias="BEAR"
    res=sorted([v for _,v in highs if v>close]); sup=sorted([v for _,v in lows if v<close],reverse=True)
    resistance=res[0] if res else max(c[2] for c in cs[-80:-1])
    support=sup[0] if sup else min(c[3] for c in cs[-80:-1])
    near_support=abs(close-support)<=.9*av or any(abs(close-p[1])<=.9*av for p in lows[-4:])
    near_resistance=abs(close-resistance)<=.9*av or any(abs(close-p[1])<=.9*av for p in highs[-4:])
    line_sup=_line_at(lows,len(cs)-1); line_res=_line_at(highs,len(cs)-1)
    near_line_sup=line_sup is not None and abs(close-line_sup)<=.8*av
    near_line_res=line_res is not None and abs(close-line_res)<=.8*av
    pre=cs[-66:-1]; last=cs[-1]
    prior_hi=max(c[2] for c in pre); prior_lo=min(c[3] for c in pre)
    sweep_low=(last[3]<prior_lo-.10*av and last[4]>prior_lo)
    sweep_high=(last[2]>prior_hi+.10*av and last[4]<prior_hi)
    # Structural flip when close crosses yesterday's resistance/support and retests it.
    prev_hi=max(c[2] for c in cs[-32:-2]); prev_lo=min(c[3] for c in cs[-32:-2])
    flip_long=cs[-2][4]>prev_hi and last[3]<=prev_hi+.35*av and last[4]>prev_hi
    flip_short=cs[-2][4]<prev_lo and last[2]>=prev_lo-.35*av and last[4]<prev_lo
    # Fibonacci golden pocket from the previous completed 70-bar swing, no current candle.
    prior=cs[-76:-1]
    swing_hi=max(c[2] for c in prior); swing_lo=min(c[3] for c in prior)
    span=swing_hi-swing_lo
    long_fib=(swing_hi-.618*span,swing_hi-.5*span)
    short_fib=(swing_lo+.5*span,swing_lo+.618*span)
    fib_long=span>3*av and long_fib[0]-.25*av <=close<=long_fib[1]+.25*av
    fib_short=span>3*av and short_fib[0]-.25*av<=close<=short_fib[1]+.25*av
    # 1.618 extension is a measured target reference, never a standalone entry.
    fib_ext_up=swing_lo+1.618*span
    fib_ext_down=swing_hi-1.618*span
    boll_mid=_avg(prices[-20:]); std=(sum((x-boll_mid)**2 for x in prices[-20:])/20)**.5
    bb_upper=boll_mid+2*std; bb_lower=boll_mid-2*std
    bb_lower_touch=last[3]<=bb_lower and last[4]>bb_lower
    bb_upper_touch=last[2]>=bb_upper and last[4]<bb_upper
    poc=_poc_proxy(cs)
    near_poc=poc is not None and abs(close-poc)<=.85*av
    vol=volume_ratio(cs,20) or 0.0
    obv_up=obv[-1]>obv[-6] if len(obv)>6 else False
    obv_dn=obv[-1]<obv[-6] if len(obv)>6 else False
    rsi_now=rsi_seq[-1] if rsi_seq[-1] is not None else 50
    divergences={"LONG":False,"SHORT":False}
    if len(lows)>=2:
        i,j=lows[-2][0],lows[-1][0]
        if lows[-1][1]<lows[-2][1] and rsi_seq[i] is not None and rsi_seq[j] is not None:
            divergences["LONG"]=rsi_seq[j]>rsi_seq[i]+2.0
    if len(highs)>=2:
        i,j=highs[-2][0],highs[-1][0]
        if highs[-1][1]>highs[-2][1] and rsi_seq[i] is not None and rsi_seq[j] is not None:
            divergences["SHORT"]=rsi_seq[j]<rsi_seq[i]-2.0
    macd_hist = _macd_hist_series(cs)
    macd_divergences = {"LONG": False, "SHORT": False}
    if macd_hist and len(lows) >= 2:
        i, j = lows[-2][0], lows[-1][0]
        # Lower low in price, higher low in MACD histogram.
        macd_divergences["LONG"] = (lows[-1][1] < lows[-2][1]
            and macd_hist[j] > macd_hist[i] and j < len(cs) - 1)
    if macd_hist and len(highs) >= 2:
        i, j = highs[-2][0], highs[-1][0]
        macd_divergences["SHORT"] = (highs[-1][1] > highs[-2][1]
            and macd_hist[j] < macd_hist[i] and j < len(cs) - 1)
    psych = _psychological_levels(close)
    return {
        "atr":av,"close":close,"structure":struct,"htf":htf_bias,
        "ma":mas,"ema13":ema13[-1],"ema21":ema21[-1],
        "ema_cross_up":ema13[-2]<=ema21[-2] and ema13[-1]>ema21[-1],
        "ema_cross_down":ema13[-2]>=ema21[-2] and ema13[-1]<ema21[-1],
        "supertrend":slow_st,"supertrend_line":st_line,
        "support":support,"resistance":resistance,
        "near_support":near_support,"near_resistance":near_resistance,
        "near_line_sup":near_line_sup,"near_line_res":near_line_res,
        "sweep_low":sweep_low,"sweep_high":sweep_high,
        "flip_long":flip_long,"flip_short":flip_short,
        "fibonacci_long":fib_long,"fibonacci_short":fib_short,
        "fibonacci_extension_up":fib_ext_up,"fibonacci_extension_down":fib_ext_down,
        "boll_mid":boll_mid,"boll_lower":bb_lower,"boll_upper":bb_upper,
        "bb_lower_touch":bb_lower_touch,"bb_upper_touch":bb_upper_touch,
        "poc_proxy":poc,"near_poc":near_poc,
        "rsi":rsi_now,"stoch":stoch,"macd":macd,
        "obv_up":obv_up,"obv_down":obv_dn,
        "divergence":divergences,"macd_divergence":macd_divergences,
        "psychological_levels": psych, "volume_ratio":vol,
        "candles":_candle_triggers(cs,av),"patterns":_book_chart_patterns(cs,av),
    }


def _book_score(f,side):
    """Independent evidence buckets prevent pretending all opposing methods agree."""
    long=side=="LONG"; expected="BULL" if long else "BEAR"
    items={}
    is_trend=f["htf"]==expected and f["structure"] in (expected,"RANGE")
    ema_ok=(f["ema13"]>f["ema21"] and f["close"]>=f["ema21"]) if long else (f["ema13"]<f["ema21"] and f["close"]<=f["ema21"])
    st_ok=(f["supertrend"]==1 if long else f["supertrend"]==-1)
    mas=f["ma"]
    ma_ok=(mas[9]>mas[21]>mas[55] if long else mas[9]<mas[21]<mas[55])
    trend_count=int(is_trend)+int(ema_ok)+int(st_ok)+int(ma_ok)
    if trend_count>=3:items["trend"]=(23,["HTF/EMA/Supertrend/MA9-21-55"])
    elif trend_count>=2:items["trend"]=(17,["trend-confluence"])
    elif trend_count==1:items["trend"]=(8,["trend-partial"])
    if "trend" in items and (f["ema_cross_up"] if long else f["ema_cross_down"]):
        items["trend"][1].append("EMA13/21 crossover")
    area=[]
    if f["near_support"] if long else f["near_resistance"]:area.append("support/resistance")
    if f["near_line_sup"] if long else f["near_line_res"]:area.append("trendline area")
    if f["fibonacci_long"] if long else f["fibonacci_short"]:area.append("fibonacci 0.5-0.618")
    if f["flip_long"] if long else f["flip_short"]:area.append("area flip")
    if f["sweep_low"] if long else f["sweep_high"]:area.append("liquidity grab")
    if f["bb_lower_touch"] if long else f["bb_upper_touch"]:area.append("Bollinger band")
    if f["near_poc"]:area.append("POC proxy")
    if ema_ok and abs(f["close"]-f["ema21"])<f["atr"]*.8:area.append("EMA13/21 money magnet")
    if area: items["area"]=(21,area)
    chart=f["patterns"][side]
    if chart:items["chart"]=(19,chart)
    cand=f["candles"][side]
    if cand: items["candle"]=(16,cand)
    oscillator=[]
    rsi_now=f["rsi"]
    if long and rsi_now<=BOOK_RSI_OVERSOLD:oscillator.append("RSI oversold")
    if not long and rsi_now>=BOOK_RSI_OVERBOUGHT:oscillator.append("RSI overbought")
    st=f["stoch"]
    if st:
        k0,k1,d0,d1=st
        if long and k0<=d0 and k1>d1 and min(k0,d0)<BOOK_STOCH_OVERSOLD:oscillator.append("stoch 5/3/3 bull cross")
        if not long and k0>=d0 and k1<d1 and max(k0,d0)>BOOK_STOCH_OVERBOUGHT:oscillator.append("stoch 5/3/3 bear cross")
    m=f["macd"]
    if m and ((long and m[0]<=0<m[1]) or (not long and m[0]>=0>m[1])):
        oscillator.append("MACD signal cross")
    if f["divergence"][side]:oscillator.append("RSI bullish divergence" if long else "RSI bearish divergence")
    if f.get("macd_divergence", {}).get(side):oscillator.append("MACD bullish divergence" if long else "MACD bearish divergence")
    if oscillator:items["oscillator"]=(13,oscillator)
    volume=[]
    if f["volume_ratio"]>=1.05:volume.append("volume confirmation")
    if f["obv_up"] if long else f["obv_down"]:volume.append("OBV directional")
    if volume:items["volume"]=(8,volume)
    total=sum(weight for weight,_ in items.values())
    labels=[label for _,names in items.values() for label in names]
    return total,items,labels


def build_book_signal(inst,cs,meta,htf=None):
    """Book 5-stage system; continuation and reversal may trigger independently."""
    f=_book_features(cs,htf)
    if f is None:
        return None
    best=None
    for side in ("LONG","SHORT"):
        expected="BULL" if side=="LONG" else "BEAR"
        score,groups,reasons=_book_score(f,side)
        chart=f["patterns"][side]
        is_reversal=any(x in chart for x in ("head_shoulders","inverse_head_shoulders",
                            "double_top","triple_top","double_bottom","triple_bottom",
                            "rising_wedge","falling_wedge")) or f["divergence"][side] or f.get("macd_divergence", {}).get(side, False) or (f["sweep_low"] if side=="LONG" else f["sweep_high"])
        is_continuation=bool(any(x in chart for x in ("bull_pennant","bear_pennant","bull_flag",
                           "bear_flag","ascending_triangle","descending_triangle"))) or (
                           "trend" in groups and "area" in groups)
        if not (is_reversal or is_continuation):continue
        if "area" not in groups:continue
        # Price POC proxy is weak by itself; require a genuine price-level anchor
        # or completed continuation chart breakout (which defines its own level).
        anchor=(f["near_support"] if side=="LONG" else f["near_resistance"]) or (
            f["near_line_sup"] if side=="LONG" else f["near_line_res"]) or (
            f["fibonacci_long"] if side=="LONG" else f["fibonacci_short"]) or (
            f["flip_long"] if side=="LONG" else f["flip_short"]) or (
            f["sweep_low"] if side=="LONG" else f["sweep_high"])
        if not anchor and not chart: continue
        # Without a chart pattern, reversal requires direct candle/oscillator trigger.
        if is_reversal and not ("candle" in groups or "oscillator" in groups or chart):continue
        if is_continuation and not ("candle" in groups or "oscillator" in groups or "chart" in groups):continue
        if score<BOOK_MIN_SCORE:continue
        # Require stronger confluence for countertrend signals; avoid fighting HTF.
        against_htf=f["htf"] not in (expected,"RANGE")
        if against_htf and (not is_reversal or score<BOOK_MIN_SCORE+12):continue
        # Breakout continuation requires participation to avoid unsupported fakeouts.
        breakout=any(any(token in pattern for token in ("triangle", "flag", "pennant"))
                     for pattern in chart)
        if breakout and f["volume_ratio"]<BOOK_VOLUME_BREAKOUT_MIN:continue
        a=f["atr"];close=f["close"]
        raw_stop = (min(c[3] for c in cs[-9:]) - .22*a if side=="LONG"
                    else max(c[2] for c in cs[-9:]) + .22*a)
        # A tight structure-based stop is never widened just to force a signal.
        risk=(close-raw_stop) if side=="LONG" else (raw_stop-close)
        if risk<=0 or risk/a<SMC_MIN_SL_ATR or risk/a>SMC_MAX_SL_ATR:continue
        tick=meta.get(inst,{}).get("tick")
        if not tick:continue
        profile=adaptive_profile(cs,atr(cs,ATR_PERIOD),side)
        rr=BOOK_MIN_RR + .10 + .12*(score>=76) + .15*(score>=91)
        entry=round_tick(close,tick)
        levels=conservative_levels(side,entry,raw_stop,rr,tick)
        if levels is None:continue
        sl,tp,rr_actual=levels
        if rr_actual + 1e-9 < BOOK_MIN_RR: continue
        if not SMC_MIN_SL_ATR<=abs(entry-sl)/a<=SMC_MAX_SL_ATR:continue
        # Keep away from micropenny scalp where estimated fees dwarf stop distance.
        min_risk=(SMC_MIN_RISK_PCT_BTC if inst==SMC_SYMBOL and SMC_MIN_RISK_PCT_BTC>=0
                  else SMC_MIN_RISK_PCT)
        if abs(entry-sl)/entry<min_risk:continue
        group="BOOK_REVERSAL" if is_reversal and (not is_continuation or against_htf) else "BOOK_CONTINUATION"
        pattern=chart[0] if chart else ("liquidity_grab" if is_reversal else "EMA13_21_pullback")
        sig={
            "signal_id":f"{inst}|{side}|{cs[-1][0]}|{group}",
            "symbol":inst,"direction":side,"timeframe":SMC_TF,"setup":group,
            "note":f"BOOK {group} | {pattern} | {' / '.join(reasons[:5])}",
            "signal_candle_time":cs[-1][0],"signal_candle_iso":iso(cs[-1][0]),
            "created_at":now(),"entry":entry,"sl":sl,"tp":tp,"rr":rr_actual,
            "atr":a,"sl_atr":abs(entry-sl)/a,
            "ema50":profile["ema50"],"ema200":profile["ema200"],"rsi":f["rsi"],
            "adx":None,"volume_ratio":f["volume_ratio"],
            "regime":f["structure"],"htf_regime":f["htf"],"structure":f["structure"],
            "bos":"NONE","pullback":None,"score":score,
            "score_reasons":reasons,
            "quality_buckets":list(groups),"chart_patterns":chart,
            "candlestick_patterns":f["candles"][side],
            "indicators_snapshot":{"ema13":f["ema13"],"ema21":f["ema21"],
                "supertrend":f["supertrend"],"bollinger20_mid":f["boll_mid"],
                "poc_proxy":f["poc_proxy"],"obv_up":f["obv_up"],
                "obv_down":f["obv_down"],"fib_extension_up":f["fibonacci_extension_up"],
                "fib_extension_down":f["fibonacci_extension_down"],
                "stochastic_5_3_3":f["stoch"],"macd":f["macd"],
                "vpfr_precision":"OHLCV_APPROXIMATION_NOT_TICK_VPFR",
                "macd_divergence":f.get("macd_divergence"),
                "psychological_levels":f.get("psychological_levels")},
            "scale_in_out_plan":_scale_plan(side,entry,sl,tp),
            "risk_usdt":ACCOUNT_EQUITY*RISK_PER_TRADE*profile["risk_multiplier"]*
                        (.80 if against_htf else 1.0),
            "risk_multiplier":profile["risk_multiplier"]*(.80 if against_htf else 1.0),
            "signal_type":"REVERSAL" if is_reversal else "CONTINUATION",
            "market_regime":profile["market_regime"],
            "volatility_ratio":profile["volatility_ratio"],
            "trend_aligned":profile["trend_aligned"],
            "engine_version":ENGINE_VERSION,"status":"OPEN","exit_price":None,
            "exit_time":None,"result_r":None,"result":None,
            "monitor_tf_ms":SMC_MONITOR_MS}
        if best is None or score>best["score"]:
            best=sig
    return best


def build_multi_technical(inst,cs,meta,htf=None):
    """All book methods are computed. SMC also remains one separate, testable branch."""
    book=build_book_signal(inst,cs,meta,htf)
    smc=build_smc_scalp(inst,cs,meta) if BOOK_ALLOW_SMC else None
    if smc:
        smc["rr"]=max(BOOK_MIN_RR+.1,float(smc.get("rr",BOOK_MIN_RR)))
        smc["engine_version"]=ENGINE_VERSION
        smc["note"]="SMC + BOOK context | "+smc["note"]
        smc["setup"]="SMC_BOOK"
        smc["signal_id"]+= "|BOOK"
        f=_book_features(cs,htf)
        if f:
            score,groups,reasons=_book_score(f,smc["direction"])
            smc["book_score"]=score
            smc["book_evidence"]=reasons
            smc["quality_buckets"]=list(groups)
        # Recalculate TP to satisfy book minimum RR, using same SL and closed-bar entry.
        lv=conservative_levels(smc["direction"],smc["entry"],smc["sl"],smc["rr"],meta[inst]["tick"])
        if lv and lv[2]+1e-9>=BOOK_MIN_RR: smc["sl"],smc["tp"],smc["rr"]=lv
        else: smc=None
    if book or smc:
        # A successful book signal should not be reported as a SMC rejection.
        REJECTS.pop(inst, None)
    if book and smc:
        return book if book["score"] >= float(smc.get("score") or 0) else smc
    return book or smc

def book_protocol_audit(sig, cs, htf=None):
    """Independent five-stage protocol. Every stage is evaluated explicitly.

    The stages follow Technical Research in the book. Thresholds, pivot geometry
    and exact entry rules are *engineering interpretations*, not quotations.
    """
    f = _book_features(cs, htf)
    if f is None:
        return {"eligible": False, "reason": "insufficient confirmed technical data",
                "stages": {}}
    side = sig.get("direction")
    if side not in ("LONG", "SHORT"):
        return {"eligible": False, "reason": "invalid direction", "stages": {}}
    bull = side == "LONG"
    expected = "BULL" if bull else "BEAR"
    trend = (f["structure"] == expected and f["htf"] in (expected, "RANGE"))
    area_candidates = {
        "horizontal_support_resistance": f["near_support"] if bull else f["near_resistance"],
        "trendline": f["near_line_sup"] if bull else f["near_line_res"],
        "fibonacci_golden_pocket": f["fibonacci_long"] if bull else f["fibonacci_short"],
        "area_flip": f["flip_long"] if bull else f["flip_short"],
        "liquidity_grab": f["sweep_low"] if bull else f["sweep_high"],
    }
    chart = list(f["patterns"][side])
    candles = list(f["candles"][side])
    chart_breakout = any(p in (
        "bull_pennant", "bear_pennant", "bull_flag", "bear_flag",
        "ascending_triangle", "descending_triangle", "head_shoulders",
        "inverse_head_shoulders", "double_top", "double_bottom",
        "triple_top", "triple_bottom", "rising_wedge", "falling_wedge"
    ) for p in chart)
    has_area = any(area_candidates.values()) or chart_breakout
    rsi_trigger = (f["rsi"] <= BOOK_RSI_OVERSOLD if bull else f["rsi"] >= BOOK_RSI_OVERBOUGHT)
    stochastic = f.get("stoch")
    stoch_trigger = False
    if stochastic:
        k0, k1, d0, d1 = stochastic
        stoch_trigger = (k0 <= d0 and k1 > d1 and min(k0,d0) < BOOK_STOCH_OVERSOLD) if bull else (
            k0 >= d0 and k1 < d1 and max(k0,d0) > BOOK_STOCH_OVERBOUGHT)
    macd = f.get("macd")
    macd_trigger = bool(macd and ((macd[0] <= 0 < macd[1]) if bull else (macd[0] >= 0 > macd[1])))
    trigger = bool(chart or candles or rsi_trigger or stoch_trigger or macd_trigger or
                   f["divergence"][side] or f.get("macd_divergence", {}).get(side, False))
    entry = float(sig.get("entry") or 0)
    sl = float(sig.get("sl") or 0)
    tp = float(sig.get("tp") or 0)
    rr = abs(tp-entry)/abs(entry-sl) if entry != sl and entry > 0 else 0.0
    entry_ok = bool(entry > 0 and sl > 0 and tp > 0 and rr + 1e-9 >= BOOK_MIN_RR
        and (sl < entry < tp if bull else tp < entry < sl))
    stages = {
        "trend": {"pass": bool(trend), "structure": f["structure"], "higher_tf": f["htf"]},
        "area": {"pass": bool(has_area), "anchors": [k for k,v in area_candidates.items() if v],
                 "breakout_chart_level": bool(chart_breakout)},
        "pattern": {"pass": True, "detected": chart, "note": "No pattern also allowed when a candle/indicator trigger exists"},
        "trigger": {"pass": trigger, "candles": candles, "rsi": round(f["rsi"], 2),
                    "rsi_extreme": bool(rsi_trigger), "stoch_cross": bool(stoch_trigger),
                    "macd_cross": bool(macd_trigger), "macd_divergence": bool(f.get("macd_divergence", {}).get(side, False)),
                    "rsi_divergence": bool(f["divergence"][side])},
        "entry_exit": {"pass": bool(entry_ok), "rr_gross": round(rr, 3), "minimum_rr": BOOK_MIN_RR,
                       "note": "paper target, not a filled trade"},
    }
    eligible = all(d["pass"] for d in stages.values())
    reason = ", ".join(k for k,v in stages.items() if not v["pass"]) or "five stages passed"
    return {"eligible": eligible, "reason": reason, "stages": stages,
            "source_provenance": "AKADEMI_CRYPTO_CH3_STAGES+BOT_DESIGNED_NUMERIC_THRESHOLDS"}


def needs_hanging_man_confirmation(sig, protocol):
    if sig["setup"] != "BOOK_CONTINUATION" or sig["direction"] != "SHORT":
        return False
    stages = protocol.get("stages", {})
    trigger = stages.get("trigger", {})
    return (
        trigger.get("candles") == ["hanging_man"]
        and not stages.get("pattern", {}).get("detected")
        and not any(trigger.get(key) for key in (
            "rsi_extreme", "stoch_cross", "macd_cross", "macd_divergence", "rsi_divergence",
        ))
    )


def confirm_hanging_man(pending, cs):
    sig = pending["signal"]
    setup_ms = sig["signal_candle_time"]
    bar_ms = cs[-1][0]
    close = cs[-1][4]
    if bar_ms != setup_ms + SMC_TF_MS or not close < pending["low"]:
        return None

    atr_series = atr(cs, ATR_PERIOD)
    av = atr_series[-1]
    if not av or av <= 0:
        return None
    profile = adaptive_profile(cs, atr_series, "SHORT")
    confirmed = dict(sig)
    confirmed.update({
        "signal_id": f"{sig['symbol']}|SHORT|{bar_ms}|{sig['setup']}",
        "signal_candle_time": bar_ms,
        "signal_candle_iso": iso(bar_ms),
        "status": "OPEN",
        "entry": close,
        "sl": max(c[2] for c in cs[-9:]) + .22 * av,
        "atr": av,
        "ema50": profile["ema50"],
        "ema200": profile["ema200"],
        "risk_usdt": ACCOUNT_EQUITY * RISK_PER_TRADE * profile["risk_multiplier"],
        "risk_multiplier": profile["risk_multiplier"],
        "market_regime": profile["market_regime"],
        "volatility_ratio": profile["volatility_ratio"],
        "trend_aligned": profile["trend_aligned"],
        "note": sig["note"] + " | hanging man confirmed",
        "hanging_man_confirmation": {
            "setup_candle_time": setup_ms,
            "setup_low": pending["low"],
            "confirmation_close": close,
        },
    })
    return confirmed


# ================= END BOOK ENGINE =================


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
    if sig.get("setup") in ("SMC_BOOK", "BOOK_CONTINUATION", "BOOK_REVERSAL") and real_rr + 1e-9 < BOOK_MIN_RR:
        return "SL/RR", f"RR aktual {real_rr:.3f} < target buku {BOOK_MIN_RR:.2f}"
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
        "scale_in_out_plan": _scale_plan(side, entry, sl, tp),
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

    notify(
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
        log.info("No V8 closed trades yet; %s legacy results excluded.", legacy_n)
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

    notify(
        "<b>📊 V8 PAPER SIGNAL PERFORMANCE</b>\n"
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

    book_audit = s.get("book_protocol") or {}
    stages = book_audit.get("stages") or {}
    stage_flags = " / ".join(
        f"{key}:{'OK' if info.get('pass') else 'NO'}"
        for key, info in stages.items()
    )
    protocol_line = (
        f"Book protocol: <b>{_safe(stage_flags)}</b>\n"
        if stages else "Book protocol: not audited (legacy/paper test)\n"
    )
    head = (
        f"{s['timeframe']} | <b>{SETUP_NAME.get(setup, setup)}</b>\n"
        f"Structure: {s.get('structure', '-')} · Regime: {s.get('regime', '-')}\n"
        f"{_safe(s.get('note', ''))}\n"
        + protocol_line + "\n"
    )

    return (
        "<b>🚨 ALPHAQUANT V8 BOOK PROTOCOL SIGNAL</b>\n"
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
        notify(scan_recap_text(*args, **kwargs))
    except Exception as e:
        log.error("Scan recap failed: %s", e)

def scan_smc(h):
    """
    Scan the top pairs for an SMC scalp on every NEW 15m candle.
    Returns the number of new signals.
    """
    global CUR_SETUP

    ref, health = reference_for_scan()
    if health["state"] != "READY":
        next_close = iso(health["next_close_ms"])
        if health["state"] == "SCHEDULE_LATE":
            log.warning(
                "Reference candle is stale because run started too late: "
                "confirmed_open=%s UTC | candle_age=%.0fs > max=%ss | "
                "next_close=%s UTC. Scheduling issue, not necessarily OKX error.",
                iso(ref), health["age_sec"], SMC_MAX_SIGNAL_AGE_SEC,
                next_close,
            )
        elif health["state"] == "FEED_BEHIND":
            log.warning(
                "OKX reference feed behind: latest_confirmed=%s UTC | "
                "expected_confirmed=%s UTC | now=%s UTC. Scan skipped.",
                iso(ref), iso(health["expected_ref_open_ms"]), iso(health["now_ms"]),
            )
        else:
            log.warning(
                "Reference candle not ready (%s) | now=%s UTC | "
                "next_close=%s UTC. Scan skipped.",
                health["state"], iso(health["now_ms"]), next_close,
            )
        return 0
    if h.get("last_smc_candle") == ref:
        log.info("No new %s candle since %s - scan skipped.", SMC_TF, iso(ref))
        return 0

    waiting = h.get("pending_hanging_man", {})
    pending = {
        inst: item for inst, item in waiting.items() if item["confirm_candle_ms"] >= ref
    }
    if "pending_hanging_man" in h:
        h["pending_hanging_man"] = pending

    signals_on_candle = sum(
        trade.get("signal_candle_time") == ref
        for status in ("open", "closed", "unresolved", "archive")
        for trade in h.get(status, [])
    )
    if signals_on_candle >= MAX_SIGNALS_PER_CANDLE:
        log.info("Candle signal cap reached for %s (%s/%s).",
                 iso(ref), signals_on_candle, MAX_SIGNALS_PER_CANDLE)
        h["last_smc_candle"] = ref
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

    log.info("BOOK TECHNICAL scan: %s pairs on %s...", len(pairs), SMC_TF)
    REJECTS.clear()
    CUR_SETUP = "SMC_SCALP"
    created, scanned, skipped = 0, 0, 0
    new_signals = []
    stop_note = ""
    t0 = time.time()

    for inst in pairs:
        if n_open(h) >= SMC_MAX_OPEN:
            log.info("Open scalp cap (%s) reached - stopping the scan.", SMC_MAX_OPEN)
            stop_note = f"Scan berhenti: batas {SMC_MAX_OPEN} posisi open tercapai."
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

            htf = candles(inst, BOOK_HTF, 120) if BOOK_HTF else []
            if BOOK_HTF and len(htf) < 55:
                _rej(inst, "Data", "HTF trend data kurang; fail-closed")
                continue
            s = None
            watch = pending.get(inst)
            if watch and watch["confirm_candle_ms"] == ref:
                s = confirm_hanging_man(watch, cs)
                if s is None:
                    _rej(inst, "Konfirmasi", "hanging man: konfirmasi harga atau data ATR belum valid",
                         setup="BOOK_CONTINUATION")

            if s is None:
                s = build_multi_technical(inst, cs, meta, htf)
                if not s:
                    continue
                protocol = book_protocol_audit(s, cs, htf)
                s["book_protocol"] = protocol
                if BOOK_PROTOCOL_STRICT and not protocol["eligible"]:
                    _rej(inst, "Konfirmasi", "book 5-stage: " + protocol["reason"][:95])
                    continue
                if needs_hanging_man_confirmation(s, protocol):
                    if not blocked(h, s):
                        pending[inst] = {
                            "signal": dict(s, status="PENDING_CONFIRMATION"),
                            "low": cs[-1][3],
                            "confirm_candle_ms": ref + SMC_TF_MS,
                        }
                        h["pending_hanging_man"] = pending
                        _rej(inst, "Konfirmasi", "hanging man: menunggu satu candle 15m",
                             setup=s["setup"])
                    continue

            if blocked(h, s):
                _rej(inst, "Diblokir", f"{s['direction']} duplikat/cooldown/posisi open")
                continue

            open_in_direction = sum(trade.get("direction") == s["direction"] for trade in h["open"])
            if open_in_direction >= MAX_OPEN_PER_DIRECTION:
                _rej(inst, "Diblokir",
                     f"batas {MAX_OPEN_PER_DIRECTION} posisi {s['direction']} bersamaan",
                     setup=s["setup"])
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
            if s.get("hanging_man_confirmation"):
                audit = s["book_protocol"]
                stages = audit["stages"]
                s["book_protocol"] = dict(audit, stages=dict(
                    stages,
                    trigger=dict(stages["trigger"], hanging_man_confirmed=True),
                    entry_exit=dict(stages["entry_exit"], rr_gross=round(s["rr"], 3)),
                ))
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
            if s.get("hanging_man_confirmation"):
                pending.pop(inst, None)
            signals_on_candle += 1
            queue_notification(h, "signal:" + s["signal_id"], signal_message(s))
            save_history(h)
            created += 1
            new_signals.append(s)
            log.info("NEW SIGNAL %s %s rr=%.2f", s["symbol"], s["direction"], s["rr"])
            flush_notifications(h)

        except Exception as e:
            log.exception("Scan error %s: %s", inst, e)
            _rej(inst, "Error", str(e)[:60])

        if signals_on_candle >= MAX_SIGNALS_PER_CANDLE:
            stop_note = f"Scan berhenti: batas {MAX_SIGNALS_PER_CANDLE} sinyal per candle tercapai."
            log.info("Candle signal cap reached for %s (%s/%s).",
                     iso(ref), signals_on_candle, MAX_SIGNALS_PER_CANDLE)
            break
        time.sleep(0.2)

    log.info("BOOK TECHNICAL scan complete. Pairs: %s | New signals: %s", scanned, created)
    rows = dict(REJECTS)
    _tally(h, scanned, created, rows)
    REJECTS.clear()

    send_scan_recap(
        ref, scanned, skipped, created, new_signals, rows, h,
        note=stop_note,
        elapsed=time.time() - t0
    )

    if ref is not None:
        h["last_smc_candle"] = ref
        if "pending_hanging_man" in h:
            h["pending_hanging_man"] = {
                inst: item for inst, item in pending.items() if item["confirm_candle_ms"] > ref
            }
    return created

# ================= MAIN =================

def validate_config():
    if not (ACCOUNT_EQUITY > 0 and 0 < RISK_PER_TRADE <= 0.02):
        raise ValueError("ACCOUNT_EQUITY harus >0 dan RISK_PER_TRADE dalam (0, 0.02]")
    if not (0 < MAX_PORTFOLIO_RISK_PCT <= 0.10 and 0 < SMC_MAX_OPEN <= 50):
        raise ValueError("MAX_PORTFOLIO_RISK_PCT harus (0,0.10], SMC_MAX_OPEN (0,50]")
    if not (1 <= MAX_OPEN_PER_DIRECTION <= 50 and 1 <= MAX_SIGNALS_PER_CANDLE <= 50):
        raise ValueError("MAX_OPEN_PER_DIRECTION dan MAX_SIGNALS_PER_CANDLE harus 1..50")
    if not (0 < TOP_PAIRS <= 100 and MIN_VOLUME_USDT > 0):
        raise ValueError("TOP_PAIRS (0,100], MIN_VOLUME_USDT >0")
    if not (2.0 <= BOOK_MIN_RR <= 5.0 and 20 <= BOOK_MIN_SCORE <= 100):
        raise ValueError("BOOK_MIN_RR 2..5 dan BOOK_MIN_SCORE 20..100")
    if not (0 < BOOK_RSI_OVERSOLD < 50 < BOOK_RSI_OVERBOUGHT < 100):
        raise ValueError("RSI bounds must straddle 50")
    if not (0 < BOOK_STOCH_OVERSOLD < 50 < BOOK_STOCH_OVERBOUGHT < 100):
        raise ValueError("Stoch bounds must straddle 50")
    if not (0 <= GITHUB_PRE_CLOSE_WAIT_SEC <= 90 and
            0 <= GITHUB_POST_CLOSE_POLL_SEC <= 60):
        raise ValueError("GITHUB_PRE_CLOSE_WAIT_SEC 0..90; GITHUB_POST_CLOSE_POLL_SEC 0..60")
    if (SMC_MAX_SIGNAL_AGE_SEC <= 0 or MAX_TICKER_AGE_SEC <= 0 or
            MAX_MONITOR_CATCHUP_BARS < 20 or FEE_PER_SIDE < 0 or
            HISTORY_BACKUP_DAYS < 0):
        raise ValueError("Konfigurasi waktu, monitoring atau fee tidak valid")


def _main():
    global DISCORD
    validate_config()

    log.info("==========================================")
    log.info("ALPHAQUANT V8 BOOK PROTOCOL STARTING")
    log.info("==========================================")

    if not DISCORD_WEBHOOK_URL:
        DISCORD = False
        log.warning("Discord disabled. Missing Secret/Environment variable: DISCORD_WEBHOOK_URL")
        log.warning("GitHub: Settings > Secrets and variables > Actions")
    else:
        DISCORD = True
        log.info("Discord webhook configured. Notifications ENABLED.")

    h = load_history()
    from state_manager import verify_state_pair
    verify_state_pair(HISTORY_FILE, os.getenv("STATE_GENESIS_FILE", "alphaquant_v8_genesis.json"))
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
        "Config | Book+SMC@%s RR baseline=1:%.2f | Pairs=%s | MaxOpen=%s | MinRisk=%.2f%% (BTC %s) | "
        "Monitor=%s | History=%s | Discord=%s | RunForever=%s",
        SMC_TF, SMC_RR, TOP_PAIRS, SMC_MAX_OPEN, SMC_MIN_RISK_PCT * 100,
        f"{SMC_MIN_RISK_PCT_BTC * 100:.2f}%" if SMC_MIN_RISK_PCT_BTC >= 0 else "= sama",
        SMC_MONITOR_TF, HISTORY_FILE, "ON" if DISCORD else "OFF", RUN_FOREVER
    )
    log.info("Exposure limits | Open per direction=%s | Signals per 15m candle=%s",
             MAX_OPEN_PER_DIRECTION, MAX_SIGNALS_PER_CANDLE)

    today = datetime.now(timezone.utc).date().isoformat()
    if DISCORD and h.get("last_online_day") != today:
        h["last_online_day"] = today
        notify(
            "<b>🟢 ALPHAQUANT V8 BOOK PROTOCOL ONLINE</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"Book Technical+SMC 15m · top {TOP_PAIRS} pair\n"
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
                    notify(text)
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
            if os.getenv("CI_FAIL_ON_ERROR", "1") == "1":
                raise

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

