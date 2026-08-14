import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()

ASSET = os.getenv("ASSET", "AUDUSD_otc").strip()
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))

TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "2500"))
EXPIRY_SECONDS = int(os.getenv("EXPIRY_SECONDS", "15"))

LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", "50"))

# ------------------------------------------------------------
# STRATEGY PARAMETERS
# ------------------------------------------------------------

TREND_CANDLES = 3

# 3 candles on either side of a pivot.
PIVOT_STRENGTH = int(os.getenv("PIVOT_STRENGTH", "3"))

# 0.75 x average candle range.
ZONE_TOLERANCE_MULTIPLIER = float(
    os.getenv("ZONE_TOLERANCE_MULTIPLIER", "0.75")
)

MIN_ZONE_TOUCHES = int(os.getenv("MIN_ZONE_TOUCHES", "2"))
MAX_ZONE_TOUCHES = int(os.getenv("MAX_ZONE_TOUCHES", "4"))

DOJI_BODY_MAX_PCT = float(
    os.getenv("DOJI_BODY_MAX_PCT", "0.10")
)

DOJI_WICK_MIN_PCT = float(
    os.getenv("DOJI_WICK_MIN_PCT", "0.60")
)

STAR_HAMMER_BODY_MAX_PCT = float(
    os.getenv("STAR_HAMMER_BODY_MAX_PCT", "0.30")
)

STAR_HAMMER_WICK_MULTIPLIER = float(
    os.getenv("STAR_HAMMER_WICK_MULTIPLIER", "2.0")
)

MIN_RANGE_FILTER = float(
    os.getenv("MIN_RANGE_FILTER", "0.50")
)

# ------------------------------------------------------------
# SAFETY
# ------------------------------------------------------------

ALLOW_REAL_TRADING = (
    os.getenv("ALLOW_REAL_TRADING", "false").lower()
    in ("1", "true", "yes")
)

# Only one open trade at a time.
ONE_TRADE_AT_A_TIME = True

# ------------------------------------------------------------
# STREAM / STARTUP
# ------------------------------------------------------------

INITIAL_HISTORY_CANDLES = max(
    LOOKBACK_WINDOW + PIVOT_STRENGTH + 5,
    60
)

SERVER_TIME_SYNC_INTERVAL = 60


# ============================================================
# GLOBAL STATE
# ============================================================

running = True
client = None

candles = []

last_processed_candle_timestamp = None

# Active zone lock.
#
# Format:
# {
#     "type": "support" / "resistance",
#     "low": float,
#     "high": float,
#     "center": float
# }
#
# The lock is removed only after price exits the zone.
locked_zone = None

trade_in_progress = False

server_time_offset = 0


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float

    @property
    def range(self):
        return self.high - self.low

    @property
    def body(self):
        return abs(self.close - self.open)

    @property
    def upper_wick(self):
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self):
        return min(self.open, self.close) - self.low

    @property
    def bullish(self):
        return self.close > self.open

    @property
    def bearish(self):
        return self.close < self.open


@dataclass
class Zone:
    zone_low: float
    zone_high: float
    touch_count: int
    zone_type: str

    @property
    def center(self):
        return (self.zone_low + self.zone_high) / 2


# ============================================================
# LOGGING
# ============================================================

def log(message: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def print_header():
    print()
    print("=" * 80)
    print("POCKET OPTION 15s REVERSAL BOT")
    print("=" * 80)
    print(f"Asset              : {ASSET}")
    print(f"Timeframe          : {TIMEFRAME}s")
    print(f"Trade amount       : ${TRADE_AMOUNT:,.2f}")
    print(f"Expiry             : {EXPIRY_SECONDS}s")
    print(f"S/R lookback       : {LOOKBACK_WINDOW} candles")
    print(f"Pivot strength     : {PIVOT_STRENGTH}")
    print(f"Zone tolerance     : {ZONE_TOLERANCE_MULTIPLIER}x avg range")
    print(f"Zone touches       : {MIN_ZONE_TOUCHES}-{MAX_ZONE_TOUCHES}")
    print(f"Min range filter   : {MIN_RANGE_FILTER}x avg range")
    print(
        f"Real trading       : "
        f"{'ENABLED' if ALLOW_REAL_TRADING else 'DISABLED - DEMO ONLY'}"
    )
    print("=" * 80)
    print()


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(signum, frame):
    global running

    log("Shutdown signal received.")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# IMPORT LIBRARY
# ============================================================

def import_library():

    try:
        from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

        return PocketOptionAsync

    except ImportError as exc:

        print()
        print("=" * 80)
        print("ERROR: Could not import BinaryOptionsToolsV2")
        print("=" * 80)
        print(f"Import error: {exc}")
        print()
        print(
            "Expected package:"
            " BinaryOptionsToolsV2.pocketoption"
        )
        print("=" * 80)

        raise


# ============================================================
# NORMALIZE CANDLE
# ============================================================

def normalize_candle(raw: Any) -> Optional[Candle]:

    try:

        if isinstance(raw, Candle):
            return raw

        if isinstance(raw, str):

            try:
                raw = json.loads(raw)
            except Exception:
                return None

        if isinstance(raw, dict):

            timestamp = raw.get(
                "timestamp",
                raw.get(
                    "time",
                    raw.get(
                        "from",
                        raw.get(
                            "at",
                            raw.get("aligned_time")
                        )
                    )
                )
            )

            open_price = raw.get(
                "open",
                raw.get("o")
            )

            high_price = raw.get(
                "high",
                raw.get("h")
            )

            low_price = raw.get(
                "low",
                raw.get("l")
            )

            close_price = raw.get(
                "close",
                raw.get("c")
            )

            # Some stream messages may contain price only.
            #
            # Those are not OHLC candles, so don't treat them as
            # completed candles.
            if None in (
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price
            ):
                return None

            return Candle(
                timestamp=int(float(timestamp)),
                open=float(open_price),
                high=float(high_price),
                low=float(low_price),
                close=float(close_price)
            )

        # Some library versions may expose object attributes.
        timestamp = getattr(
            raw,
            "timestamp",
            getattr(raw, "time", None)
        )

        open_price = getattr(
            raw,
            "open",
            getattr(raw, "o", None)
        )

        high_price = getattr(
            raw,
            "high",
            getattr(raw, "h", None)
        )

        low_price = getattr(
            raw,
            "low",
            getattr(raw, "l", None)
        )

        close_price = getattr(
            raw,
            "close",
            getattr(raw, "c", None)
        )

        if None in (
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price
        ):
            return None

        return Candle(
            timestamp=int(float(timestamp)),
            open=float(open_price),
            high=float(high_price),
            low=float(low_price),
            close=float(close_price)
        )

    except Exception as exc:

        log(
            f"Could not normalize candle: "
            f"{type(exc).__name__}: {exc}"
        )

        return None


# ============================================================
# CANDLE DISPLAY
# ============================================================

def print_candle(candle: Candle):

    direction = "DOJI"

    if candle.close > candle.open:
        direction = "BULLISH"

    elif candle.close < candle.open:
        direction = "BEARISH"

    change = candle.close - candle.open

    candle_time = datetime.fromtimestamp(
        candle.timestamp
    ).strftime("%H:%M:%S")

    print()
    print("-" * 80)
    print(
        f"CLOSED {TIMEFRAME}s CANDLE | "
        f"{candle_time}"
    )
    print(
        f"O: {candle.open:.6f} | "
        f"H: {candle.high:.6f} | "
        f"L: {candle.low:.6f} | "
        f"C: {candle.close:.6f}"
    )
    print(
        f"Direction: {direction:<8} | "
        f"Change: {change:+.6f}"
    )
    print("-" * 80)


# ============================================================
# CANDLE PATTERNS
# ============================================================

def is_gravestone_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (
        candle.body
        <= DOJI_BODY_MAX_PCT * candle.range

        and candle.upper_wick
        >= DOJI_WICK_MIN_PCT * candle.range

        and candle.lower_wick
        <= DOJI_BODY_MAX_PCT * candle.range
    )


def is_dragonfly_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (
        candle.body
        <= DOJI_BODY_MAX_PCT * candle.range

        and candle.lower_wick
        >= DOJI_WICK_MIN_PCT * candle.range

        and candle.upper_wick
        <= DOJI_BODY_MAX_PCT * candle.range
    )


def is_shooting_star(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    body = candle.body

    if body <= 0:
        return False

    body_in_lower_third = (
        max(candle.open, candle.close)
        <= candle.low
        + candle.range * (1 / 3)
    )

    return (
        body
        <= STAR_HAMMER_BODY_MAX_PCT * candle.range

        and candle.upper_wick
        >= STAR_HAMMER_WICK_MULTIPLIER * body

        and body_in_lower_third
    )


def is_hammer(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    body = candle.body

    if body <= 0:
        return False

    body_in_upper_third = (
        min(candle.open, candle.close)
        >= candle.low
        + candle.range * (2 / 3)
    )

    return (
        body
        <= STAR_HAMMER_BODY_MAX_PCT * candle.range

        and candle.lower_wick
        >= STAR_HAMMER_WICK_MULTIPLIER * body

        and body_in_upper_third
    )


# ============================================================
# TREND GATES
# ============================================================

def buy_trend_gate(history):

    if len(history) < 4:
        return False

    # C0 = latest closed candle
    # C-1 = candle before C0
    # C-2 = candle before C-1
    # C-3 = candle before C-2

    c1 = history[-2]
    c2 = history[-3]
    c3 = history[-4]

    # STRICT inequality is intentional.
    return (
        c1.low < c2.low
        and c2.low < c3.low
    )


def sell_trend_gate(history):

    if len(history) < 4:
        return False

    c1 = history[-2]
    c2 = history[-3]
    c3 = history[-4]

    # STRICT inequality is intentional.
    return (
        c1.high > c2.high
        and c2.high > c3.high
    )


# ============================================================
# PIVOT DETECTION
# ============================================================

def find_pivot_high(
    data,
    index,
    strength=PIVOT_STRENGTH
):

    if index - strength < 0:
        return False

    if index + strength >= len(data):
        return False

    value = data[index].high

    for i in range(
        index - strength,
        index + strength + 1
    ):

        if i == index:
            continue

        if data[i].high >= value:
            return False

    return True


def find_pivot_low(
    data,
    index,
    strength=PIVOT_STRENGTH
):

    if index - strength < 0:
        return False

    if index + strength >= len(data):
        return False

    value = data[index].low

    for i in range(
        index - strength,
        index + strength + 1
    ):

        if i == index:
            continue

        if data[i].low <= value:
            return False

    return True


# ============================================================
# RANGE
# ============================================================

def average_range(data):

    valid = [
        c.range
        for c in data
        if c.range > 0
    ]

    if not valid:
        return 0.0

    return sum(valid) / len(valid)


# ============================================================
# ZONE CLUSTERING
# ============================================================

def cluster_prices(
    prices,
    tolerance,
    zone_type
):

    if not prices:
        return []

    prices = sorted(prices)

    clusters = []

    current = [prices[0]]

    for price in prices[1:]:

        current_center = (
            sum(current) / len(current)
        )

        if abs(price - current_center) <= tolerance:

            current.append(price)

        else:

            clusters.append(current)
            current = [price]

    clusters.append(current)

    zones = []

    for cluster in clusters:

        touch_count = len(cluster)

        if touch_count < MIN_ZONE_TOUCHES:
            continue

        if touch_count > MAX_ZONE_TOUCHES:
            continue

        center = sum(cluster) / touch_count

        zones.append(
            Zone(
                zone_low=center - tolerance,
                zone_high=center + tolerance,
                touch_count=touch_count,
                zone_type=zone_type
            )
        )

    return zones


# ============================================================
# BUILD SUPPORT / RESISTANCE ZONES
# ============================================================

def build_zones(data):

    if len(data) < LOOKBACK_WINDOW:
        return [], []

    data = data[-LOOKBACK_WINDOW:]

    avg_range = average_range(data)

    if avg_range <= 0:
        return [], []

    tolerance = (
        avg_range
        * ZONE_TOLERANCE_MULTIPLIER
    )

    resistance_pivots = []
    support_pivots = []

    # IMPORTANT:
    #
    # We do NOT allow the most recent PIVOT_STRENGTH
    # candles to be used as pivots because a fractal
    # requires candles on the right side to confirm it.
    #
    # This prevents look-ahead/repainting.

    last_confirmable_index = (
        len(data) - 1 - PIVOT_STRENGTH
    )

    for i in range(
        PIVOT_STRENGTH,
        last_confirmable_index + 1
    ):

        if find_pivot_high(
            data,
            i,
            PIVOT_STRENGTH
        ):
            resistance_pivots.append(
                data[i].high
            )

        if find_pivot_low(
            data,
            i,
            PIVOT_STRENGTH
        ):
            support_pivots.append(
                data[i].low
            )

    resistance_zones = cluster_prices(
        resistance_pivots,
        tolerance,
        "resistance"
    )

    support_zones = cluster_prices(
        support_pivots,
        tolerance,
        "support"
    )

    return (
        support_zones,
        resistance_zones
    )


# ============================================================
# ZONE MATCHING
# ============================================================

def price_in_zone(
    price,
    zone
):

    return (
        zone.zone_low
        <= price
        <= zone.zone_high
    )


def find_support_zone(
    price,
    zones
):

    candidates = [
        z
        for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z:
        abs(z.center - price)
    )


def find_resistance_zone(
    price,
    zones
):

    candidates = [
        z
        for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z:
        abs(z.center - price)
    )


# ============================================================
# ZONE LOCK MANAGEMENT
# ============================================================

def update_zone_lock(price):

    global locked_zone

    if locked_zone is None:
        return

    # Once price completely exits the zone,
    # the zone may become eligible again when
    # price later re-enters it.
    if (
        price < locked_zone["low"]
        or price > locked_zone["high"]
    ):

        log(
            "ZONE UNLOCKED | "
            f"{locked_zone['type']} | "
            f"{locked_zone['low']:.6f} - "
            f"{locked_zone['high']:.6f}"
        )

        locked_zone = None


def zone_is_locked(zone):

    if locked_zone is None:
        return False

    if (
        locked_zone["type"]
        != zone.zone_type
    ):
        return False

    # Compare zone centers rather than requiring
    # identical floating-point boundaries.
    tolerance = (
        zone.zone_high
        - zone.zone_low
    )

    return (
        abs(
            locked_zone["center"]
            - zone.center
        )
        <= tolerance
    )


def lock_zone(zone):

    global locked_zone

    locked_zone = {
        "type": zone.zone_type,
        "low": zone.zone_low,
        "high": zone.zone_high,
        "center": zone.center
    }

    log(
        "ZONE LOCKED | "
        f"{zone.zone_type.upper()} | "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f} | "
        f"touches={zone.touch_count}"
    )


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def evaluate_signal(data):

    if len(data) < LOOKBACK_WINDOW:
        return None

    c0 = data[-1]

    # --------------------------------------------------------
    # TREND GATES COME FIRST.
    #
    # These are hard gates.
    # --------------------------------------------------------

    buy_trend = buy_trend_gate(data)

    sell_trend = sell_trend_gate(data)

    # --------------------------------------------------------
    # Minimum range filter.
    # --------------------------------------------------------

    avg_range = average_range(
        data[-LOOKBACK_WINDOW:]
    )

    if avg_range <= 0:
        return None

    if c0.range < (
        MIN_RANGE_FILTER
        * avg_range
    ):

        log(
            "Signal rejected | "
            f"C0 range {c0.range:.8f} < "
            f"{MIN_RANGE_FILTER}x avg "
            f"{avg_range:.8f}"
        )

        return None

    # --------------------------------------------------------
    # Build zones ONLY from closed candle history.
    # --------------------------------------------------------

    support_zones, resistance_zones = (
        build_zones(data)
    )

    # ========================================================
    # BUY
    #
    # Trend:
    # C-1.low < C-2.low < C-3.low
    #
    # Zone:
    # C0.low inside support
    #
    # Pattern:
    # Dragonfly OR Hammer
    # ========================================================

    if buy_trend:

        support_zone = find_support_zone(
            c0.low,
            support_zones
        )

        if support_zone:

            dragonfly = is_dragonfly_doji(c0)
            hammer = is_hammer(c0)

            if dragonfly or hammer:

                pattern = (
                    "DRAGONFLY_DOJI"
                    if dragonfly
                    else "HAMMER"
                )

                return {
                    "direction": "BUY",
                    "pattern": pattern,
                    "zone": support_zone,
                    "candle": c0
                }

    # ========================================================
    # SELL
    #
    # Trend:
    # C-1.high > C-2.high > C-3.high
    #
    # Zone:
    # C0.high inside resistance
    #
    # Pattern:
    # Gravestone OR Shooting Star
    # ========================================================

    if sell_trend:

        resistance_zone = (
            find_resistance_zone(
                c0.high,
                resistance_zones
            )
        )

        if resistance_zone:

            gravestone = (
                is_gravestone_doji(c0)
            )

            shooting_star = (
                is_shooting_star(c0)
            )

            if gravestone or shooting_star:

                pattern = (
                    "GRAVESTONE_DOJI"
                    if gravestone
                    else "SHOOTING_STAR"
                )

                return {
                    "direction": "SELL",
                    "pattern": pattern,
                    "zone": resistance_zone,
                    "candle": c0
                }

    return None


# ============================================================
# TRADE RESULT
# ============================================================

async def monitor_trade_result(
    trade_id,
    direction,
    pattern
):

    global trade_in_progress

    try:

        # Give the broker enough time to settle
        # the 15-second option.
        await asyncio.sleep(
            EXPIRY_SECONDS + 3
        )

        result = await client.check_win(
            trade_id
        )

        profit = float(
            result.get("profit", 0)
        )

        result_name = str(
            result.get(
                "result",
                "unknown"
            )
        ).upper()

        if profit > 0:
            outcome = "WIN"

        elif profit < 0:
            outcome = "LOSS"

        else:
            outcome = "DRAW"

        print()
        print("=" * 80)
        print("TRADE RESULT")
        print("=" * 80)
        print(f"Direction : {direction}")
        print(f"Pattern   : {pattern}")
        print(f"Trade ID  : {trade_id}")
        print(f"Result    : {outcome}")
        print(f"Broker    : {result_name}")
        print(f"P/L       : ${profit:+,.2f}")
        print("=" * 80)
        print()

        try:

            balance = await client.balance()

            log(
                f"Current balance: "
                f"${float(balance):,.2f}"
            )

        except Exception as exc:

            log(
                f"Could not update balance: "
                f"{exc}"
            )

    except Exception as exc:

        log(
            f"Trade result error: "
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        trade_in_progress = False


# ============================================================
# TRADE EXECUTION
# ============================================================

async def execute_trade(signal_data):

    global trade_in_progress

    direction = signal_data["direction"]
    pattern = signal_data["pattern"]
    zone = signal_data["zone"]
    candle = signal_data["candle"]

    if ONE_TRADE_AT_A_TIME and trade_in_progress:

        log(
            "TRADE BLOCKED | "
            "Another trade is still active."
        )

        return None

    print()
    print("=" * 80)
    print("VALID 15s REVERSAL SIGNAL")
    print("=" * 80)

    print(f"Direction       : {direction}")
    print(f"Pattern         : {pattern}")

    print(
        f"Signal candle   : "
        f"{datetime.fromtimestamp(candle.timestamp).strftime('%H:%M:%S')}"
    )

    print(
        f"Zone            : "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f}"
    )

    print(
        f"Zone touches    : "
        f"{zone.touch_count}"
    )

    print(
        f"Amount          : "
        f"${TRADE_AMOUNT:,.2f}"
    )

    print(
        f"Expiry          : "
        f"{EXPIRY_SECONDS}s"
    )

    print(
        "Entry timing    : "
        "Immediately after C0 closes / C+1 begins"
    )

    print("=" * 80)

    # --------------------------------------------------------
    # REAL ACCOUNT PROTECTION
    # --------------------------------------------------------

    try:

        is_demo = bool(
            client.is_demo()
        )

    except Exception as exc:

        log(
            f"Could not determine account type: "
            f"{exc}"
        )

        return None

    if not is_demo and not ALLOW_REAL_TRADING:

        log(
            "TRADE BLOCKED | "
            "Real account detected and "
            "ALLOW_REAL_TRADING is false."
        )

        return None

    # --------------------------------------------------------
    # SUBMIT TRADE
    # --------------------------------------------------------

    try:

        trade_in_progress = True

        if direction == "BUY":

            trade_id, deal = await client.buy(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                time=EXPIRY_SECONDS
            )

        else:

            trade_id, deal = await client.sell(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                time=EXPIRY_SECONDS
            )

        log(
            f"TRADE SUBMITTED | "
            f"{direction} | "
            f"ID={trade_id}"
        )

        log(
            f"Deal: {deal}"
        )

        asyncio.create_task(
            monitor_trade_result(
                trade_id,
                direction,
                pattern
            )
        )

        return trade_id

    except Exception as exc:

        trade_in_progress = False

        log(
            f"TRADE ERROR | "
            f"{type(exc).__name__}: {exc}"
        )

        return None


# ============================================================
# CLOSED CANDLE PROCESSOR
# ============================================================

async def process_closed_candle(candle):

    global last_processed_candle_timestamp

    if (
        last_processed_candle_timestamp
        == candle.timestamp
    ):
        return

    last_processed_candle_timestamp = (
        candle.timestamp
    )

    # --------------------------------------------------------
    # Update zone lock BEFORE evaluating new signal.
    # --------------------------------------------------------

    update_zone_lock(candle.close)

    candles.append(candle)

    # Keep enough history without allowing unlimited memory.
    max_history = (
        LOOKBACK_WINDOW
        + PIVOT_STRENGTH
        + 20
    )

    if len(candles) > max_history:

        del candles[
            :-max_history
        ]

    print_candle(candle)

    if len(candles) < LOOKBACK_WINDOW:

        log(
            f"Building strategy history: "
            f"{len(candles)}/"
            f"{LOOKBACK_WINDOW}"
        )

        return

    # --------------------------------------------------------
    # Evaluate only CLOSED C0.
    # --------------------------------------------------------

    signal_data = evaluate_signal(
        candles
    )

    if not signal_data:
        return

    zone = signal_data["zone"]

    # --------------------------------------------------------
    # One trade per zone touch.
    # --------------------------------------------------------

    if zone_is_locked(zone):

        log(
            "SIGNAL BLOCKED | "
            "Zone already traded during "
            "this touch."
        )

        return

    # --------------------------------------------------------
    # Lock zone before submitting.
    # --------------------------------------------------------

    lock_zone(zone)

    await execute_trade(
        signal_data
    )


# ============================================================
# INITIAL HISTORY
# ============================================================

async def load_initial_history():

    log(
        f"Loading {INITIAL_HISTORY_CANDLES} "
        f"historical {TIMEFRAME}s candles..."
    )

    try:

        raw_candles = await client.get_candles(
            ASSET,
            TIMEFRAME,
            INITIAL_HISTORY_CANDLES
        )

    except Exception as exc:

        log(
            f"get_candles failed: "
            f"{type(exc).__name__}: {exc}"
        )

        # Fallback to compile_candles.
        #
        # This is useful because 15 seconds can be
        # a custom timeframe depending on the feed.

        try:

            lookback_seconds = (
                INITIAL_HISTORY_CANDLES
                * TIMEFRAME
            )

            log(
                "Trying compile_candles fallback..."
            )

            raw_candles = (
                await client.compile_candles(
                    ASSET,
                    TIMEFRAME,
                    lookback_seconds
                )
            )

        except Exception as fallback_exc:

            raise RuntimeError(
                "Unable to obtain initial "
                "15-second candle history. "
                f"get_candles error: {exc}; "
                f"compile_candles error: "
                f"{fallback_exc}"
            )

    if isinstance(raw_candles, str):

        try:
            raw_candles = json.loads(
                raw_candles
            )
        except Exception:
            pass

    if not isinstance(
        raw_candles,
        (list, tuple)
    ):

        raise RuntimeError(
            "Historical candle response "
            "was not a list."
        )

    normalized = []

    for raw in raw_candles:

        candle = normalize_candle(raw)

        if candle is not None:

            normalized.append(candle)

    # Sort oldest → newest.
    normalized.sort(
        key=lambda c: c.timestamp
    )

    # Remove duplicate timestamps.
    unique = {}

    for candle in normalized:

        unique[candle.timestamp] = candle

    normalized = list(
        unique.values()
    )

    normalized.sort(
        key=lambda c: c.timestamp
    )

    # Keep enough history.
    candles.clear()

    candles.extend(
        normalized[-INITIAL_HISTORY_CANDLES:]
    )

    if candles:

        # The newest historical candle may still be
        # the currently-forming candle.
        #
        # We DO NOT evaluate it.
        #
        # It is only retained for reference/history.
        log(
            f"Loaded {len(candles)} candles."
        )

        latest = candles[-1]

        log(
            "Latest historical candle: "
            f"{datetime.fromtimestamp(latest.timestamp).strftime('%H:%M:%S')} "
            f"O={latest.open:.6f} "
            f"H={latest.high:.6f} "
            f"L={latest.low:.6f} "
            f"C={latest.close:.6f}"
        )

    else:

        raise RuntimeError(
            "No usable historical candles "
            f"received for {ASSET}."
        )


# ============================================================
# SERVER TIME
# ============================================================

async def sync_server_time():

    global server_time_offset

    try:

        server_time = await (
            client.get_server_time()
        )

        local_time = int(
            time.time()
        )

        server_time_offset = (
            server_time
            - local_time
        )

        log(
            f"Server time sync | "
            f"server={server_time} | "
            f"local={local_time} | "
            f"offset={server_time_offset}s"
        )

        return server_time

    except Exception as exc:

        log(
            f"Server time unavailable: "
            f"{type(exc).__name__}: {exc}"
        )

        return None


async def server_time_loop():

    while running:

        try:

            await sync_server_time()

        except Exception:
            pass

        for _ in range(
            SERVER_TIME_SYNC_INTERVAL
        ):

            if not running:
                break

            await asyncio.sleep(1)


# ============================================================
# ACCOUNT
# ============================================================

async def show_account():

    print()
    print("=" * 80)
    print("ACCOUNT")
    print("=" * 80)

    try:

        is_demo = bool(
            client.is_demo()
        )

        log(
            "Account type: "
            + (
                "DEMO"
                if is_demo
                else "REAL"
            )
        )

        if not is_demo:

            if ALLOW_REAL_TRADING:

                log(
                    "WARNING: REAL TRADING "
                    "IS ENABLED."
                )

            else:

                log(
                    "REAL ACCOUNT DETECTED. "
                    "Trading is BLOCKED."
                )

    except Exception as exc:

        log(
            f"Could not determine account type: "
            f"{exc}"
        )

    try:

        balance = await client.balance()

        log(
            f"Balance: "
            f"${float(balance):,.2f}"
        )

    except Exception as exc:

        log(
            f"Could not retrieve balance: "
            f"{type(exc).__name__}: {exc}"
        )

    print("=" * 80)
    print()


# ============================================================
# ASSET CHECK
# ============================================================

async def check_asset():

    log(
        f"Checking availability of "
        f"{ASSET}..."
    )

    try:

        assets = await client.active_assets()

        if isinstance(assets, str):

            assets = json.loads(
                assets
            )

        if isinstance(assets, dict):

            asset_list = list(
                assets.values()
            )

        else:

            asset_list = assets

        matches = []

        for asset in asset_list:

            if not isinstance(
                asset,
                dict
            ):
                continue

            symbol = str(
                asset.get(
                    "symbol",
                    asset.get(
                        "name",
                        ""
                    )
                )
            )

            if symbol.lower() == ASSET.lower():

                matches.append(asset)

        if matches:

            for match in matches:

                log(
                    f"Asset available: "
                    f"{match}"
                )

            return True

        # Some asset responses use the symbol as
        # a dictionary key or have unusual formatting.
        for asset in asset_list:

            if str(
                asset.get(
                    "symbol",
                    ""
                )
            ).lower() == ASSET.lower():

                return True

        log(
            f"WARNING: Could not find "
            f"{ASSET} in active_assets()."
        )

        log(
            "The subscription will be attempted "
            "anyway."
        )

        return True

    except Exception as exc:

        log(
            f"Asset check failed: "
            f"{type(exc).__name__}: {exc}"
        )

        return True


# ============================================================
# REAL-TIME 15 SECOND SUBSCRIPTION
# ============================================================

async def handle_stream():

    log(
        f"Subscribing to {ASSET} "
        f"using aligned {TIMEFRAME}s stream..."
    )

    # --------------------------------------------------------
    # BinaryOptionsToolsV2 provides:
    #
    # subscribe_symbol_time_aligned(
    #     asset,
    #     timedelta(seconds=15)
    # )
    #
    # This is the correct API for aligned real-time
    # candle windows.
    # --------------------------------------------------------

    subscription = (
        await client.subscribe_symbol_time_aligned(
            ASSET,
            timedelta(
                seconds=TIMEFRAME
            )
        )
    )

    log(
        "15-second subscription established."
    )

    log(
        "Waiting for CLOSED candles..."
    )

    async for raw in subscription:

        if not running:
            break

        # ----------------------------------------------------
        # Debug only when the library returns a structure
        # that cannot be interpreted as OHLC.
        # ----------------------------------------------------

        candle = normalize_candle(raw)

        if candle is None:

            # Some stream implementations may emit
            # price-only updates. They are intentionally
            # ignored here because the strategy operates
            # exclusively on CLOSED OHLC candles.

            continue

        # ----------------------------------------------------
        # Determine whether this candle is actually closed.
        #
        # The stream is time-aligned. A new candle timestamp
        # indicates the previous candle has completed.
        #
        # We therefore process the previous candle when the
        # stream advances.
        # ----------------------------------------------------

        if (
            last_processed_candle_timestamp
            is None
        ):

            # Seed the stream.
            #
            # Do not immediately treat the first streamed
            # candle as closed.
            last_processed_candle_timestamp = (
                candle.timestamp - TIMEFRAME
            )

        if (
            candle.timestamp
            > last_processed_candle_timestamp
        ):

            # If there is an explicit OHLC candle at the
            # current timestamp, it represents the current
            # aligned window. Therefore process the candle
            # immediately preceding it as the closed C0
            # when it exists in our stream history.
            #
            # However, if the stream itself emits completed
            # candle objects, process the object directly.
            #
            # We identify this by checking whether the
            # timestamp is already in our history.

            existing = None

            for item in reversed(candles):

                if (
                    item.timestamp
                    == candle.timestamp
                ):

                    existing = item
                    break

            if existing is not None:

                await process_closed_candle(
                    existing
                )

            # Current candle becomes the forming candle.
            #
            # It is NOT evaluated until the next timestamp.

            if not any(
                c.timestamp
                == candle.timestamp
                for c in candles
            ):

                # Keep the current candle in memory
                # only as a provisional object.
                candles.append(candle)

                max_history = (
                    LOOKBACK_WINDOW
                    + PIVOT_STRENGTH
                    + 20
                )

                if len(candles) > max_history:

                    del candles[
                        :-max_history
                    ]

        # ----------------------------------------------------
        # Fallback:
        #
        # Some versions of the stream can deliver a candle
        # that is already closed. If the timestamp is older
        # than the current aligned interval, process it.
        # ----------------------------------------------------

        current_server_time = (
            int(time.time())
            + server_time_offset
        )

        candle_end = (
            candle.timestamp
            + TIMEFRAME
        )

        if candle_end <= current_server_time:

            # Remove any provisional copy.
            candles[:] = [
                c
                for c in candles
                if c.timestamp
                != candle.timestamp
            ]

            await process_closed_candle(
                candle
            )


# ============================================================
# MORE RELIABLE TICK/CANDLE LOOP
# ============================================================

async def handle_stream_safe():

    """
    Primary 15-second aligned stream.

    If the stream closes unexpectedly, reconnect the
    subscription instead of killing the entire Railway
    process.
    """

    while running:

        try:

            await handle_stream()

            if running:
                log(
                    "15s stream ended. "
                    "Reconnecting..."
                )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            log(
                f"Stream error: "
                f"{type(exc).__name__}: {exc}"
            )

            if running:

                log(
                    "Retrying subscription in 3 seconds..."
                )

                await asyncio.sleep(3)


# ============================================================
# CLIENT STARTUP
# ============================================================

async def start_client(
    PocketOptionAsync
):

    global client

    if not SSID:

        raise RuntimeError(
            "POCKET_OPTION_SSID is not configured."
        )

    log(
        "Creating PocketOptionAsync client..."
    )

    # The actual library accepts the SSID directly.
    client = PocketOptionAsync(
        ssid=SSID
    )

    log(
        "PocketOptionAsync client created."
    )

    # Wait for websocket/asset initialization.
    await asyncio.sleep(5)

    try:

        await client.wait_for_assets(
            timeout=30
        )

        log(
            "Pocket Option assets loaded."
        )

    except Exception as exc:

        log(
            f"wait_for_assets warning: "
            f"{type(exc).__name__}: {exc}"
        )

    return client


# ============================================================
# MAIN
# ============================================================

async def main():

    print_header()

    PocketOptionAsync = (
        import_library()
    )

    await start_client(
        PocketOptionAsync
    )

    await show_account()

    # --------------------------------------------------------
    # HARD REAL-ACCOUNT CHECK
    # --------------------------------------------------------

    try:

        is_demo = bool(
            client.is_demo()
        )

    except Exception:

        is_demo = True

    if (
        not is_demo
        and not ALLOW_REAL_TRADING
    ):

        raise RuntimeError(
            "Real account detected. "
            "Bot is configured DEMO ONLY. "
            "Set ALLOW_REAL_TRADING=true "
            "only if you intentionally want "
            "to enable real trading."
        )

    # --------------------------------------------------------
    # SERVER TIME
    # --------------------------------------------------------

    await sync_server_time()

    # --------------------------------------------------------
    # ASSET
    # --------------------------------------------------------

    await check_asset()

    # --------------------------------------------------------
    # INITIAL HISTORY
    # --------------------------------------------------------

    await load_initial_history()

    if len(candles) < LOOKBACK_WINDOW:

        raise RuntimeError(
            f"Insufficient historical candles. "
            f"Need {LOOKBACK_WINDOW}, "
            f"received {len(candles)}."
        )

    log(
        f"Strategy history ready: "
        f"{len(candles)} candles."
    )

    print()
    print("=" * 80)
    print("STRATEGY ARMED")
    print("=" * 80)
    print(
        "BUY  : Dragonfly Doji / Hammer "
        "at confirmed support"
    )
    print(
        "SELL : Gravestone Doji / Shooting Star "
        "at confirmed resistance"
    )
    print(
        "Trend gate : STRICT 3-candle "
        "descending/ascending structure"
    )
    print(
        "Entry      : Immediately after "
        "signal candle closes"
    )
    print(
        "Expiry     : 15 seconds"
    )
    print("=" * 80)
    print()

    # --------------------------------------------------------
    # Run server-time synchronization and market stream
    # concurrently.
    # --------------------------------------------------------

    time_task = asyncio.create_task(
        server_time_loop()
    )

    try:

        await handle_stream_safe()

    finally:

        time_task.cancel()

        try:
            await time_task
        except asyncio.CancelledError:
            pass


# ============================================================
# ENTRY POINT
# ============================================================

async def shutdown_client():

    global client

    if client is None:
        return

    try:

        shutdown = getattr(
            client,
            "shutdown",
            None
        )

        if shutdown:

            result = shutdown()

            if asyncio.iscoroutine(result):

                await result

    except Exception as exc:

        log(
            f"Shutdown warning: "
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        client = None


def run():

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        pass

    except Exception as exc:

        print()
        print("=" * 80)
        print("BOT STOPPED")
        print("=" * 80)
        print(
            f"Error type : "
            f"{type(exc).__name__}"
        )
        print(
            f"Error      : {exc}"
        )
        print("=" * 80)

        sys.exit(1)

    finally:

        # A new event loop is required here because
        # asyncio.run() has already closed the previous one.
        try:

            asyncio.run(
                shutdown_client()
            )

        except Exception:

            pass

        print()
        print(
            "Bot shutdown complete."
        )


if __name__ == "__main__":
    run()
