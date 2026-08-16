import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()

ASSET = os.getenv("ASSET", "AUDUSD_otc")

TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))
EXPIRY_SECONDS = int(os.getenv("EXPIRY_SECONDS", "15"))

TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "2500"))

LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", "50"))

TREND_CANDLES = int(os.getenv("TREND_CANDLES", "3"))

ZONE_TOLERANCE_MULTIPLIER = float(
    os.getenv("ZONE_TOLERANCE_MULTIPLIER", "0.75")
)

MIN_ZONE_TOUCHES = int(
    os.getenv("MIN_ZONE_TOUCHES", "2")
)

MAX_ZONE_TOUCHES = int(
    os.getenv("MAX_ZONE_TOUCHES", "4")
)

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

PIVOT_STRENGTH = int(
    os.getenv("PIVOT_STRENGTH", "3")
)

ONE_TRADE_PER_ZONE = True


# ============================================================
# HARD SAFETY VALIDATION
# ============================================================

if TIMEFRAME != 15:
    raise RuntimeError(
        "This bot is specifically configured for a 15-second strategy. "
        "TIMEFRAME must be 15."
    )

if EXPIRY_SECONDS != 15:
    raise RuntimeError(
        "This bot is specifically configured for a 15-second expiry. "
        "EXPIRY_SECONDS must be 15."
    )

if LOOKBACK_WINDOW < 4:
    raise RuntimeError(
        "LOOKBACK_WINDOW must be at least 4 because the strategy "
        "requires C-1, C-2 and C-3."
    )

if TREND_CANDLES != 3:
    raise RuntimeError(
        "This strategy requires exactly 3 trend-precondition candles."
    )


# ============================================================
# GLOBAL STATE
# ============================================================

running = True
client = None

candles = []

# Stable zone locks.
#
# Each entry:
# (
#     zone_type,
#     zone_low,
#     zone_high
# )
#
# We deliberately store the actual price region instead of a
# recalculated floating center.
active_zone_locks = []

last_processed_candle_timestamp = None

trade_tasks = set()


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


@dataclass
class Zone:
    zone_low: float
    zone_high: float
    touch_count: int
    zone_type: str

    @property
    def center(self):
        return (self.zone_low + self.zone_high) / 2.0


# ============================================================
# LOGGING
# ============================================================

def log(message: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def print_header():

    print("=" * 80)
    print("POCKET OPTION 15s REVERSAL BOT")
    print("=" * 80)

    print(f"Asset          : {ASSET}")
    print(f"Timeframe      : {TIMEFRAME}s")
    print(f"Trade amount   : ${TRADE_AMOUNT:,.2f}")
    print(f"Expiry         : {EXPIRY_SECONDS}s")
    print(f"S/R lookback   : {LOOKBACK_WINDOW} candles")

    print("-" * 80)
    print("15s MODE       : LOCAL / TIME-ALIGNED CANDLE ENGINE")
    print("Native 15s     : NOT REQUIRED")
    print("=" * 80)


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(signum, frame):

    global running

    if not running:
        return

    log(
        f"Shutdown signal received "
        f"({signal.Signals(signum).name})."
    )

    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# IMPORT POCKET OPTION LIBRARY
# ============================================================

def import_library():

    try:

        from BinaryOptionsToolsV2.pocketoption import (
            PocketOptionAsync
        )

        log(
            "BinaryOptionsToolsV2.PocketOptionAsync imported successfully."
        )

        return PocketOptionAsync

    except ImportError as exc:

        print()
        print("=" * 80)
        print("ERROR: Could not import BinaryOptionsToolsV2")
        print("=" * 80)
        print()
        print("Expected import:")
        print(
            "from BinaryOptionsToolsV2.pocketoption "
            "import PocketOptionAsync"
        )
        print()
        print(f"Original error: {exc}")
        print()

        raise


# ============================================================
# GENERIC VALUE EXTRACTION
# ============================================================

def get_value(obj: Any, *names):

    if isinstance(obj, dict):

        for name in names:

            if name in obj:
                return obj[name]

        return None

    for name in names:

        if hasattr(obj, name):

            return getattr(obj, name)

    return None


# ============================================================
# NORMALIZE CANDLE
# ============================================================

def normalize_candle(raw: Any) -> Optional[Candle]:

    try:

        if isinstance(raw, Candle):
            return raw

        timestamp = get_value(
            raw,
            "timestamp",
            "time",
            "from",
            "at",
            "open_time",
            "start"
        )

        open_price = get_value(
            raw,
            "open",
            "o"
        )

        high_price = get_value(
            raw,
            "high",
            "h"
        )

        low_price = get_value(
            raw,
            "low",
            "l"
        )

        close_price = get_value(
            raw,
            "close",
            "c"
        )

        if None in (
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price
        ):

            return None

        timestamp = int(float(timestamp))

        # Some feeds can return milliseconds.
        if timestamp > 10_000_000_000:
            timestamp //= 1000

        return Candle(
            timestamp=timestamp,
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
# CANDLE DEDUPLICATION
# ============================================================

def append_closed_candle(candle: Candle):

    global candles

    # Existing timestamp:
    for index, existing in enumerate(candles):

        if existing.timestamp == candle.timestamp:

            # Replace only if the incoming candle is considered
            # the same closed candle.
            candles[index] = candle
            return False

    candles.append(candle)

    candles.sort(
        key=lambda c: c.timestamp
    )

    # Keep enough history for lookback + pivots.
    max_history = LOOKBACK_WINDOW + 20

    if len(candles) > max_history:

        candles = candles[-max_history:]

    return True


# ============================================================
# CANDLE PATTERNS
# ============================================================

def is_gravestone_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (

        candle.body
        <= DOJI_BODY_MAX_PCT * candle.range

        and

        candle.upper_wick
        >= DOJI_WICK_MIN_PCT * candle.range

        and

        candle.lower_wick
        <= DOJI_BODY_MAX_PCT * candle.range
    )


def is_dragonfly_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (

        candle.body
        <= DOJI_BODY_MAX_PCT * candle.range

        and

        candle.lower_wick
        >= DOJI_WICK_MIN_PCT * candle.range

        and

        candle.upper_wick
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

        and

        candle.upper_wick
        >= STAR_HAMMER_WICK_MULTIPLIER * body

        and

        body_in_lower_third
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

        and

        candle.lower_wick
        >= STAR_HAMMER_WICK_MULTIPLIER * body

        and

        body_in_upper_third
    )


# ============================================================
# TREND GATES
# ============================================================

def buy_trend_gate(history):

    if len(history) < 4:
        return False

    # C0 = latest closed candle
    #
    # C-1 = history[-2]
    # C-2 = history[-3]
    # C-3 = history[-4]

    c1 = history[-2]
    c2 = history[-3]
    c3 = history[-4]

    return (
        c1.low < c2.low
        and
        c2.low < c3.low
    )


def sell_trend_gate(history):

    if len(history) < 4:
        return False

    c1 = history[-2]
    c2 = history[-3]
    c3 = history[-4]

    return (
        c1.high > c2.high
        and
        c2.high > c3.high
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

        center = sum(current) / len(current)

        if abs(price - center) <= tolerance:

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
# BUILD SUPPORT / RESISTANCE
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

    for i in range(len(data)):

        if find_pivot_high(data, i):

            resistance_pivots.append(
                data[i].high
            )

        if find_pivot_low(data, i):

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

    return support_zones, resistance_zones


# ============================================================
# ZONE MATCHING
# ============================================================

def price_in_zone(price, zone):

    return (
        zone.zone_low
        <= price
        <= zone.zone_high
    )


def find_support_zone(price, zones):

    candidates = [
        z
        for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z: abs(
            z.center - price
        )
    )


def find_resistance_zone(price, zones):

    candidates = [
        z
        for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z: abs(
            z.center - price
        )
    )


# ============================================================
# STABLE ZONE LOCKS
# ============================================================

def zone_lock_matches(
    zone_type,
    zone_low,
    zone_high
):

    for locked_type, locked_low, locked_high in active_zone_locks:

        if locked_type != zone_type:
            continue

        # Overlap means we are still in the same price region.
        if (
            zone_low <= locked_high
            and
            zone_high >= locked_low
        ):

            return True

    return False


def lock_zone(zone: Zone):

    if not ONE_TRADE_PER_ZONE:
        return

    if zone_lock_matches(
        zone.zone_type,
        zone.zone_low,
        zone.zone_high
    ):
        return

    active_zone_locks.append(
        (
            zone.zone_type,
            zone.zone_low,
            zone.zone_high
        )
    )

    log(
        f"Zone locked | "
        f"{zone.zone_type.upper()} | "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f}"
    )


def unlock_zones_if_price_exited(price):

    global active_zone_locks

    remaining = []

    for zone_type, zone_low, zone_high in active_zone_locks:

        # Keep lock while price remains inside.
        if zone_low <= price <= zone_high:

            remaining.append(
                (
                    zone_type,
                    zone_low,
                    zone_high
                )
            )

    removed = len(active_zone_locks) - len(remaining)

    active_zone_locks = remaining

    if removed:

        log(
            f"Zone lock(s) released: {removed}"
        )


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def evaluate_signal(data):

    # Explicit lower-bound safety.
    if LOOKBACK_WINDOW < 4:
        log(
            "Signal evaluation disabled: "
            "LOOKBACK_WINDOW < 4."
        )
        return None

    if len(data) < LOOKBACK_WINDOW:
        return None

    if len(data) < 4:
        return None

    c0 = data[-1]

    # --------------------------------------------------------
    # HARD TREND GATE FIRST
    # --------------------------------------------------------

    buy_trend = buy_trend_gate(data)
    sell_trend = sell_trend_gate(data)

    # --------------------------------------------------------
    # RANGE FILTER
    # --------------------------------------------------------

    avg_range = average_range(
        data[-LOOKBACK_WINDOW:]
    )

    if avg_range <= 0:
        return None

    if (
        c0.range
        < MIN_RANGE_FILTER * avg_range
    ):

        log(
            "Signal rejected | "
            "C0 range below minimum range filter."
        )

        return None

    # --------------------------------------------------------
    # BUILD ZONES FROM CLOSED CANDLES
    # --------------------------------------------------------

    support_zones, resistance_zones = build_zones(data)

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SELL
    # --------------------------------------------------------

    if sell_trend:

        resistance_zone = find_resistance_zone(
            c0.high,
            resistance_zones
        )

        if resistance_zone:

            gravestone = is_gravestone_doji(c0)
            shooting_star = is_shooting_star(c0)

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
# TRADE EXECUTION
# ============================================================

async def execute_trade(signal_data):

    direction = signal_data["direction"]
    pattern = signal_data["pattern"]
    zone = signal_data["zone"]
    candle = signal_data["candle"]

    log("")
    log("=" * 70)
    log("VALID TRADE SIGNAL")
    log("=" * 70)

    log(f"Direction : {direction}")
    log(f"Pattern   : {pattern}")

    log(
        f"Signal C0 : "
        f"{datetime.fromtimestamp(candle.timestamp).strftime('%H:%M:%S')}"
    )

    log(
        f"Zone      : "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f}"
    )

    log(
        f"Touches   : {zone.touch_count}"
    )

    log(
        f"Amount    : ${TRADE_AMOUNT:,.2f}"
    )

    log(
        f"Expiry    : {EXPIRY_SECONDS}s"
    )

    log("=" * 70)

    try:

        # IMPORTANT:
        #
        # The library's documented async interface is:
        #
        # await client.buy(asset, amount, duration)
        # await client.sell(asset, amount, duration)

        if direction == "BUY":

            result = await client.buy(
                ASSET,
                TRADE_AMOUNT,
                EXPIRY_SECONDS
            )

        else:

            result = await client.sell(
                ASSET,
                TRADE_AMOUNT,
                EXPIRY_SECONDS
            )

        log(
            f"TRADE SUBMITTED | "
            f"{direction} | "
            f"{result}"
        )

        return result

    except Exception as exc:

        log(
            f"TRADE ERROR | "
            f"{type(exc).__name__}: {exc}"
        )

        return None


# ============================================================
# NON-BLOCKING TRADE TASK
# ============================================================

async def trade_task(signal_data):

    try:

        await execute_trade(signal_data)

    except asyncio.CancelledError:

        log("Trade task cancelled.")

    except Exception as exc:

        log(
            f"Trade task error | "
            f"{type(exc).__name__}: {exc}"
        )


def launch_trade(signal_data):

    task = asyncio.create_task(
        trade_task(signal_data)
    )

    trade_tasks.add(task)

    def done_callback(completed_task):

        trade_tasks.discard(
            completed_task
        )

        try:
            completed_task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log(
                f"Background trade task failed | "
                f"{type(exc).__name__}: {exc}"
            )

    task.add_done_callback(done_callback)


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

async def process_closed_candle(candle):

    global last_processed_candle_timestamp

    # Never process the same timestamp twice.
    if (
        last_processed_candle_timestamp
        is not None
        and
        candle.timestamp
        <= last_processed_candle_timestamp
    ):

        return

    last_processed_candle_timestamp = (
        candle.timestamp
    )

    added = append_closed_candle(candle)

    if not added:
        return

    candle_time = datetime.fromtimestamp(
        candle.timestamp
    ).strftime("%H:%M:%S")

    log(
        f"CANDLE CLOSED | "
        f"{candle_time} | "
        f"O={candle.open:.6f} "
        f"H={candle.high:.6f} "
        f"L={candle.low:.6f} "
        f"C={candle.close:.6f}"
    )

    log(
        f"History: "
        f"{len(candles)}/{LOOKBACK_WINDOW}"
    )

    # Release locks only after price has actually exited.
    unlock_zones_if_price_exited(
        candle.close
    )

    if len(candles) < LOOKBACK_WINDOW:

        return

    # --------------------------------------------------------
    # Evaluate only AFTER C0 is completely closed.
    # --------------------------------------------------------

    signal_data = evaluate_signal(
        candles
    )

    if not signal_data:
        return

    zone = signal_data["zone"]

    # --------------------------------------------------------
    # Stable one-trade-per-zone lock.
    # --------------------------------------------------------

    if (
        ONE_TRADE_PER_ZONE
        and
        zone_lock_matches(
            zone.zone_type,
            zone.zone_low,
            zone.zone_high
        )
    ):

        log(
            "SIGNAL BLOCKED | "
            "same S/R zone is already locked."
        )

        return

    # Lock before scheduling the trade.
    lock_zone(zone)

    # Do NOT await here.
    #
    # This prevents order submission from blocking the
    # live market stream.
    launch_trade(signal_data)


# ============================================================
# INITIAL HISTORY
# ============================================================

async def load_initial_history():

    global candles

    log(
        f"Loading initial "
        f"{LOOKBACK_WINDOW}-candle "
        f"{TIMEFRAME}s history..."
    )

    # The library explicitly supports custom-period candles.
    #
    # custom_period = 15 seconds
    # lookback_period = number of seconds to inspect
    #
    # We request substantially more than the minimum because
    # pivot detection needs candles on both sides.

    lookback_seconds = (
        (LOOKBACK_WINDOW + 20)
        * TIMEFRAME
    )

    try:

        raw_history = await client.compile_candles(
            ASSET,
            TIMEFRAME,
            lookback_seconds
        )

    except Exception as exc:

        log(
            f"compile_candles() failed | "
            f"{type(exc).__name__}: {exc}"
        )

        raise

    normalized = []

    for raw in raw_history:

        candle = normalize_candle(raw)

        if candle is None:
            continue

        normalized.append(candle)

    normalized.sort(
        key=lambda c: c.timestamp
    )

    # Deduplicate.
    unique = {}

    for candle in normalized:

        unique[candle.timestamp] = candle

    normalized = list(
        unique.values()
    )

    normalized.sort(
        key=lambda c: c.timestamp
    )

    # IMPORTANT:
    #
    # We do NOT intentionally feed the most recent possibly
    # forming candle into the strategy.
    #
    # Determine the current 15s boundary using server time if
    # possible.

    server_now = None

    try:

        server_now = await client.get_server_time()

    except Exception:

        try:
            server_now = int(time.time())
        except Exception:
            server_now = None

    if server_now is not None:

        current_bucket = (
            int(server_now)
            // TIMEFRAME
        ) * TIMEFRAME

        normalized = [
            c
            for c in normalized
            if c.timestamp < current_bucket
        ]

    if len(normalized) > LOOKBACK_WINDOW + 20:

        normalized = normalized[
            -(LOOKBACK_WINDOW + 20):
        ]

    candles = normalized

    if candles:

        last_processed_candle_timestamp = (
            candles[-1].timestamp
        )

    log(
        f"Initial closed candles loaded: "
        f"{len(candles)}"
    )

    if candles:

        latest = candles[-1]

        log(
            f"Latest closed candle: "
            f"{datetime.fromtimestamp(latest.timestamp).strftime('%H:%M:%S')} "
            f"C={latest.close:.6f}"
        )

    if len(candles) < LOOKBACK_WINDOW:

        raise RuntimeError(
            f"Only {len(candles)} closed candles "
            f"were loaded; "
            f"{LOOKBACK_WINDOW} are required."
        )


# ============================================================
# ASSET VALIDATION
# ============================================================

async def validate_asset():

    log(
        f"Checking asset: {ASSET}"
    )

    assets = await client.active_assets()

    # Depending on the version, active_assets may return a
    # decoded list or a JSON string.
    if isinstance(assets, str):

        import json

        assets = json.loads(assets)

    if not isinstance(assets, list):

        raise RuntimeError(
            "Unexpected active_assets() response."
        )

    matches = [
        asset
        for asset in assets
        if asset.get("symbol") == ASSET
    ]

    if not matches:

        raise RuntimeError(
            f"{ASSET} was not found in active assets."
        )

    asset = matches[0]

    is_active = asset.get(
        "is_active",
        True
    )

    if not is_active:

        raise RuntimeError(
            f"{ASSET} is not active."
        )

    log(
        f"Asset verified | "
        f"{ASSET} | "
        f"OTC={asset.get('is_otc')} | "
        f"active={is_active} | "
        f"payout={asset.get('payout')}%"
    )

    allowed = asset.get(
        "allowed_candles",
        []
    )

    if TIMEFRAME not in allowed:

        log(
            f"INFO | Native {TIMEFRAME}s candle "
            f"is NOT advertised by asset metadata."
        )

        log(
            f"INFO | Allowed native candle periods: "
            f"{allowed}"
        )

        log(
            f"INFO | This is NOT fatal. "
            f"We will construct {TIMEFRAME}s candles "
            f"using BinaryOptionsToolsV2 custom "
            f"candle/timed-stream functionality."
        )

    else:

        log(
            f"Native {TIMEFRAME}s candle "
            f"is advertised."
        )

    return asset


# ============================================================
# ACCOUNT INFORMATION
# ============================================================

async def show_account():

    try:

        demo_result = client.is_demo()

        if asyncio.iscoroutine(demo_result):

            demo_result = await demo_result

        log(
            "Account type: "
            + (
                "DEMO"
                if demo_result
                else "REAL"
            )
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


# ============================================================
# LIVE 15-SECOND STREAM
# ============================================================

async def handle_live_stream():

    log("")
    log("=" * 80)
    log(
        f"SUBSCRIBING TO {ASSET} "
        f"15-SECOND LIVE STREAM"
    )
    log("=" * 80)

    log(
        "Using time-aligned 15-second candles."
    )

    log(
        "Native asset candle support is NOT "
        "required for this mode."
    )

    stream = None

    try:

        # BinaryOptionsToolsV2 provides this specifically for
        # timed, clock-aligned candle windows.

        stream = await client.subscribe_symbol_time_aligned(
            ASSET,
            timedelta(seconds=TIMEFRAME)
        )

    except AttributeError:

        log(
            "subscribe_symbol_time_aligned() "
            "not available."
        )

        log(
            "Falling back to subscribe_symbol_timed()."
        )

        stream = await client.subscribe_symbol_timed(
            ASSET,
            timedelta(seconds=TIMEFRAME)
        )

    log(
        "15-second live subscription established."
    )

    async for raw in stream:

        if not running:
            break

        candle = normalize_candle(raw)

        if candle is None:

            # Some library versions can expose a price-only
            # update through a timed subscription. Ignore those
            # here because this strategy only evaluates CLOSED
            # OHLC candles.
            continue

        # ----------------------------------------------------
        # Closed-candle safety
        # ----------------------------------------------------
        #
        # We do not blindly process every object returned.
        #
        # A candle whose bucket is still current is ignored.
        #

        try:

            server_time = await client.get_server_time()

        except Exception:

            server_time = int(time.time())

        current_bucket = (
            server_time // TIMEFRAME
        ) * TIMEFRAME

        candle_end = (
            candle.timestamp
            + TIMEFRAME
        )

        if candle_end > current_bucket:

            # Still forming.
            continue

        await process_closed_candle(
            candle
        )


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

    client = PocketOptionAsync(
        ssid=SSID
    )

    log(
        "Client created."
    )

    # The repo examples/documentation use a short initialization
    # wait before account/market operations.

    await asyncio.sleep(5)

    try:

        await client.wait_for_assets(
            timeout=60
        )

    except Exception as exc:

        log(
            f"Asset wait returned: "
            f"{type(exc).__name__}: {exc}"
        )

    log(
        "Asset list loaded."
    )


# ============================================================
# BOT MAIN
# ============================================================

async def main():

    print_header()

    PocketOptionAsync = import_library()

    await start_client(
        PocketOptionAsync
    )

    await show_account()

    await validate_asset()

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # This is where the old bot would have failed because
    # AUDUSD_otc did not advertise 15 seconds.
    #
    # We deliberately allow it.
    # --------------------------------------------------------

    await load_initial_history()

    log("")
    log(
        f"Starting {TIMEFRAME}s "
        f"{ASSET} reversal strategy..."
    )

    log(
        "Strategy is now waiting for "
        "the next CLOSED 15s candle."
    )

    await handle_live_stream()


# ============================================================
# CLEANUP
# ============================================================

async def shutdown_client():

    global client

    # Cancel background trade tasks first.
    if trade_tasks:

        log(
            f"Cancelling "
            f"{len(trade_tasks)} background "
            f"trade task(s)..."
        )

        for task in list(trade_tasks):

            task.cancel()

        await asyncio.gather(
            *trade_tasks,
            return_exceptions=True
        )

        trade_tasks.clear()

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

            log(
                "Pocket Option client shutdown successfully."
            )

    except Exception as exc:

        log(
            f"Shutdown error: "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

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
            f"Error      : "
            f"{exc}"
        )

        print("=" * 80)

        sys.exit(1)

    finally:

        # asyncio.run() has already closed the event loop,
        # so cleanup must be handled inside the running loop.
        #
        # If the main coroutine exits normally, there is no
        # active loop here. This final block is intentionally
        # lightweight.

        print(
            "Bot shutdown complete."
        )
