import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT / CONFIGURATION
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()

ASSET = os.getenv("ASSET", "AUDUSD_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))

TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "2500"))
EXPIRY_SECONDS = int(os.getenv("EXPIRY_SECONDS", "15"))

LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", "50"))

# Strategy requires C0 + C-1 + C-2 + C-3.
TREND_CANDLES = 3

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

ONE_TRADE_PER_ZONE = (
    os.getenv("ONE_TRADE_PER_ZONE", "true").lower()
    in ("1", "true", "yes", "on")
)

# Historical data window used during startup.
HISTORY_HOURS = float(
    os.getenv("HISTORY_HOURS", "2")
)

# Maximum number of completed candles kept in memory.
MAX_CANDLE_BUFFER = max(
    LOOKBACK_WINDOW + 20,
    100
)


# ============================================================
# GLOBAL STATE
# ============================================================

running = True

client = None

candles: list["Candle"] = []

# Persistent zone locks.
#
# IMPORTANT:
# These are NOT keyed by the dynamically recalculated zone
# bounds. Each lock represents the actual price area that
# triggered a trade.
active_zone_locks: list["ZoneLock"] = []

# All outstanding trade tasks.
trade_tasks: set[asyncio.Task] = set()

# Prevent duplicate processing of the same closed candle.
last_processed_candle_timestamp: Optional[int] = None

# Async shutdown event.
shutdown_event: Optional[asyncio.Event] = None

# Main stream task.
stream_task: Optional[asyncio.Task] = None


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
    def range(self) -> float:
        return self.high - self.low

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low


@dataclass
class Zone:
    zone_low: float
    zone_high: float
    touch_count: int
    zone_type: str

    @property
    def center(self) -> float:
        return (self.zone_low + self.zone_high) / 2


@dataclass
class ZoneLock:
    """
    Persistent lock created when a trade fires.

    The lock is deliberately independent of the currently
    recalculated S/R zones.
    """

    zone_type: str
    zone_low: float
    zone_high: float
    created_at: int
    signal_candle_timestamp: int

    @property
    def center(self) -> float:
        return (self.zone_low + self.zone_high) / 2


# ============================================================
# LOGGING
# ============================================================

def log(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def print_header() -> None:
    print("=" * 80)
    print("POCKET OPTION 15s REVERSAL BOT")
    print("=" * 80)
    print(f"Asset             : {ASSET}")
    print(f"Timeframe         : {TIMEFRAME}s")
    print(f"Trade amount      : ${TRADE_AMOUNT:,.2f}")
    print(f"Expiry            : {EXPIRY_SECONDS}s")
    print(f"S/R lookback      : {LOOKBACK_WINDOW} candles")
    print(f"Zone tolerance    : {ZONE_TOLERANCE_MULTIPLIER}x avg range")
    print(f"Zone touches      : {MIN_ZONE_TOUCHES}-{MAX_ZONE_TOUCHES}")
    print(f"Min range filter  : {MIN_RANGE_FILTER}x avg range")
    print("=" * 80)


# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

def validate_configuration() -> None:

    if not SSID:
        raise RuntimeError(
            "POCKET_OPTION_SSID is not configured."
        )

    if TIMEFRAME != 15:
        raise ValueError(
            "This strategy is configured for a 15-second timeframe. "
            f"TIMEFRAME={TIMEFRAME} is invalid."
        )

    if EXPIRY_SECONDS != 15:
        raise ValueError(
            "This strategy uses a one-candle 15-second expiry. "
            f"EXPIRY_SECONDS={EXPIRY_SECONDS} is invalid."
        )

    if TRADE_AMOUNT <= 0:
        raise ValueError(
            "TRADE_AMOUNT must be greater than zero."
        )

    if LOOKBACK_WINDOW < TREND_CANDLES + 1:
        raise ValueError(
            f"LOOKBACK_WINDOW must be at least "
            f"{TREND_CANDLES + 1}. "
            f"Current value: {LOOKBACK_WINDOW}"
        )

    if MIN_ZONE_TOUCHES < 1:
        raise ValueError(
            "MIN_ZONE_TOUCHES must be at least 1."
        )

    if MAX_ZONE_TOUCHES < MIN_ZONE_TOUCHES:
        raise ValueError(
            "MAX_ZONE_TOUCHES cannot be smaller than "
            "MIN_ZONE_TOUCHES."
        )

    if ZONE_TOLERANCE_MULTIPLIER <= 0:
        raise ValueError(
            "ZONE_TOLERANCE_MULTIPLIER must be greater than zero."
        )

    if DOJI_BODY_MAX_PCT <= 0:
        raise ValueError(
            "DOJI_BODY_MAX_PCT must be greater than zero."
        )

    if DOJI_WICK_MIN_PCT <= 0:
        raise ValueError(
            "DOJI_WICK_MIN_PCT must be greater than zero."
        )

    if STAR_HAMMER_BODY_MAX_PCT <= 0:
        raise ValueError(
            "STAR_HAMMER_BODY_MAX_PCT must be greater than zero."
        )

    if STAR_HAMMER_WICK_MULTIPLIER <= 0:
        raise ValueError(
            "STAR_HAMMER_WICK_MULTIPLIER must be greater than zero."
        )

    if MIN_RANGE_FILTER < 0:
        raise ValueError(
            "MIN_RANGE_FILTER cannot be negative."
        )


# ============================================================
# SHUTDOWN
# ============================================================

def request_shutdown(reason: str = "shutdown signal") -> None:
    """
    Thread/signal-safe shutdown request.

    The signal handler itself does NOT perform async work.
    It only tells the running event loop to shut down.
    """

    global running

    if not running:
        return

    running = False

    print()
    log(f"Shutdown requested: {reason}")

    if shutdown_event is not None:

        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(
                shutdown_event.set
            )
        except RuntimeError:
            # No running loop.
            pass


def shutdown_handler(signum, frame) -> None:
    signame = signal.Signals(signum).name

    request_shutdown(
        f"{signame}"
    )


def install_signal_handlers() -> None:

    try:
        signal.signal(
            signal.SIGINT,
            shutdown_handler
        )

        signal.signal(
            signal.SIGTERM,
            shutdown_handler
        )

        log("SIGINT/SIGTERM handlers installed.")

    except Exception as exc:
        log(
            f"Could not install signal handlers: "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# IMPORT POCKET OPTION LIBRARY
# ============================================================

def import_library():

    try:
        from BinaryOptionsToolsV2.pocketoption import (
            PocketOptionAsync
        )

        return PocketOptionAsync

    except ImportError as exc:

        print()
        print("=" * 80)
        print("ERROR: Could not import BinaryOptionsToolsV2")
        print("=" * 80)
        print(f"Original error: {exc}")
        print()
        print(
            "Expected import:"
        )
        print(
            "from BinaryOptionsToolsV2.pocketoption "
            "import PocketOptionAsync"
        )
        print("=" * 80)

        raise


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_candle(raw: Any) -> Optional[Candle]:

    try:

        if isinstance(raw, Candle):
            return raw

        # ----------------------------------------------------
        # Dictionary
        # ----------------------------------------------------

        if isinstance(raw, dict):

            timestamp = raw.get(
                "timestamp",
                raw.get(
                    "time",
                    raw.get(
                        "from",
                        raw.get("at")
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

        # ----------------------------------------------------
        # JSON string
        # ----------------------------------------------------

        if isinstance(raw, str):

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None

            return normalize_candle(parsed)

        # ----------------------------------------------------
        # Object with attributes
        # ----------------------------------------------------

        timestamp = getattr(
            raw,
            "timestamp",
            getattr(
                raw,
                "time",
                None
            )
        )

        open_price = getattr(
            raw,
            "open",
            None
        )

        high_price = getattr(
            raw,
            "high",
            None
        )

        low_price = getattr(
            raw,
            "low",
            None
        )

        close_price = getattr(
            raw,
            "close",
            None
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

    # Body must be in lower third.
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

    # Body must be in upper third.
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

def buy_trend_gate(history: list[Candle]) -> bool:

    if len(history) < TREND_CANDLES + 1:
        return False

    # C0 = history[-1]
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


def sell_trend_gate(history: list[Candle]) -> bool:

    if len(history) < TREND_CANDLES + 1:
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
    data: list[Candle],
    index: int,
    strength: int = 3
) -> bool:

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
    data: list[Candle],
    index: int,
    strength: int = 3
) -> bool:

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

def average_range(data: list[Candle]) -> float:

    valid = [
        candle.range
        for candle in data
        if candle.range > 0
    ]

    if not valid:
        return 0.0

    return sum(valid) / len(valid)


# ============================================================
# ZONE CLUSTERING
# ============================================================

def cluster_prices(
    prices: list[float],
    tolerance: float,
    zone_type: str
) -> list[Zone]:

    if not prices:
        return []

    prices = sorted(prices)

    clusters: list[list[float]] = []

    current = [prices[0]]

    for price in prices[1:]:

        center = sum(current) / len(current)

        if abs(price - center) <= tolerance:
            current.append(price)

        else:
            clusters.append(current)
            current = [price]

    clusters.append(current)

    zones: list[Zone] = []

    for cluster in clusters:

        touch_count = len(cluster)

        if touch_count < MIN_ZONE_TOUCHES:
            continue

        if touch_count > MAX_ZONE_TOUCHES:
            continue

        center = sum(cluster) / len(cluster)

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
# BUILD S/R ZONES
# ============================================================

def build_zones(
    data: list[Candle]
) -> tuple[list[Zone], list[Zone]]:

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

    resistance_pivots: list[float] = []
    support_pivots: list[float] = []

    # Strength 3 = three candles on each side.
    for i in range(len(data)):

        if find_pivot_high(data, i, 3):
            resistance_pivots.append(
                data[i].high
            )

        if find_pivot_low(data, i, 3):
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
    price: float,
    zone: Zone
) -> bool:

    return (
        zone.zone_low
        <= price
        <= zone.zone_high
    )


def find_support_zone(
    price: float,
    zones: list[Zone]
) -> Optional[Zone]:

    candidates = [
        zone
        for zone in zones
        if price_in_zone(price, zone)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda zone:
        abs(zone.center - price)
    )


def find_resistance_zone(
    price: float,
    zones: list[Zone]
) -> Optional[Zone]:

    candidates = [
        zone
        for zone in zones
        if price_in_zone(price, zone)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda zone:
        abs(zone.center - price)
    )


# ============================================================
# STABLE ZONE LOCKING
# ============================================================

def zones_overlap(
    low_a: float,
    high_a: float,
    low_b: float,
    high_b: float
) -> bool:

    return (
        max(low_a, low_b)
        <= min(high_a, high_b)
    )


def zone_is_locked(
    zone: Zone
) -> bool:

    if not ONE_TRADE_PER_ZONE:
        return False

    for lock in active_zone_locks:

        if lock.zone_type != zone.zone_type:
            continue

        if zones_overlap(
            zone.zone_low,
            zone.zone_high,
            lock.zone_low,
            lock.zone_high
        ):
            return True

    return False


def lock_zone(
    zone: Zone,
    signal_candle_timestamp: int
) -> None:

    if not ONE_TRADE_PER_ZONE:
        return

    active_zone_locks.append(
        ZoneLock(
            zone_type=zone.zone_type,
            zone_low=zone.zone_low,
            zone_high=zone.zone_high,
            created_at=int(
                datetime.now(
                    timezone.utc
                ).timestamp()
            ),
            signal_candle_timestamp=(
                signal_candle_timestamp
            )
        )
    )

    log(
        "ZONE LOCKED | "
        f"{zone.zone_type.upper()} | "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f} | "
        f"touches={zone.touch_count}"
    )


def unlock_zones_if_price_exited(
    price: float
) -> None:

    if not active_zone_locks:
        return

    remaining: list[ZoneLock] = []

    for lock in active_zone_locks:

        # Keep lock while price remains inside the
        # exact region that generated the signal.
        if (
            lock.zone_low
            <= price
            <= lock.zone_high
        ):

            remaining.append(lock)

        else:

            log(
                "ZONE UNLOCKED | "
                f"{lock.zone_type.upper()} | "
                f"{lock.zone_low:.6f} - "
                f"{lock.zone_high:.6f} | "
                f"price={price:.6f}"
            )

    active_zone_locks.clear()
    active_zone_locks.extend(remaining)


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def evaluate_signal(
    data: list[Candle]
) -> Optional[dict]:

    # --------------------------------------------------------
    # HARD DATA REQUIREMENT
    # --------------------------------------------------------

    required_candles = max(
        LOOKBACK_WINDOW,
        TREND_CANDLES + 1
    )

    if len(data) < required_candles:
        return None

    # --------------------------------------------------------
    # C0 = most recently closed candle.
    # --------------------------------------------------------

    c0 = data[-1]

    # --------------------------------------------------------
    # Minimum range filter.
    # --------------------------------------------------------

    recent = data[-LOOKBACK_WINDOW:]

    avg_range = average_range(recent)

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
    # BUILD ZONES FROM CLOSED CANDLES ONLY
    # --------------------------------------------------------

    support_zones, resistance_zones = (
        build_zones(data)
    )

    # --------------------------------------------------------
    # BUY
    #
    # Gate order:
    #
    # 1. Trend
    # 2. Support zone
    # 3. Pattern
    # --------------------------------------------------------

    if buy_trend_gate(data):

        support_zone = find_support_zone(
            c0.low,
            support_zones
        )

        if support_zone is not None:

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
    #
    # Gate order:
    #
    # 1. Trend
    # 2. Resistance zone
    # 3. Pattern
    # --------------------------------------------------------

    if sell_trend_gate(data):

        resistance_zone = (
            find_resistance_zone(
                c0.high,
                resistance_zones
            )
        )

        if resistance_zone is not None:

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
# TRADE TASK CLEANUP
# ============================================================

def trade_task_done(
    task: asyncio.Task
) -> None:

    trade_tasks.discard(task)

    try:
        task.result()

    except asyncio.CancelledError:
        log("Trade task cancelled.")

    except Exception as exc:
        log(
            "Trade task failed | "
            f"{type(exc).__name__}: {exc}"
        )


def create_trade_task(
    signal_data: dict
) -> None:

    task = asyncio.create_task(
        execute_trade(signal_data)
    )

    trade_tasks.add(task)

    task.add_done_callback(
        trade_task_done
    )


# ============================================================
# TRADE EXECUTION
# ============================================================

async def execute_trade(
    signal_data: dict
):

    direction = signal_data["direction"]
    pattern = signal_data["pattern"]
    zone: Zone = signal_data["zone"]
    candle: Candle = signal_data["candle"]

    log("")
    log("=" * 70)
    log("VALID TRADE SIGNAL")
    log("=" * 70)

    log(f"Direction : {direction}")
    log(f"Pattern   : {pattern}")

    log(
        f"Zone      : "
        f"{zone.zone_low:.6f} - "
        f"{zone.zone_high:.6f}"
    )

    log(
        f"Signal C0 : "
        f"{candle.timestamp}"
    )

    log(
        f"Amount    : "
        f"${TRADE_AMOUNT:,.2f}"
    )

    log(
        f"Expiry    : "
        f"{EXPIRY_SECONDS}s"
    )

    log("=" * 70)

    try:

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # BinaryOptionsTools-v2 uses:
        #
        # buy(asset, amount, time)
        # sell(asset, amount, time)
        #
        # NOT duration=...
        # ----------------------------------------------------

        if direction == "BUY":

            result = await client.buy(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                time=EXPIRY_SECONDS,
                check_win=False
            )

        else:

            result = await client.sell(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                time=EXPIRY_SECONDS,
                check_win=False
            )

        log(
            f"TRADE SUBMITTED | "
            f"{direction} | "
            f"{result}"
        )

        # ----------------------------------------------------
        # Optional result check after expiry.
        #
        # This happens in the trade task, NOT in the
        # candle-stream task, so it cannot block candle
        # processing.
        # ----------------------------------------------------

        await asyncio.sleep(
            EXPIRY_SECONDS + 1
        )

        if (
            isinstance(result, tuple)
            and len(result) >= 1
        ):

            trade_id = result[0]

            try:

                result_data = (
                    await client.check_win(
                        trade_id
                    )
                )

                log(
                    f"TRADE RESULT | "
                    f"{trade_id} | "
                    f"{result_data}"
                )

            except Exception as exc:

                log(
                    "Could not retrieve trade result | "
                    f"{type(exc).__name__}: {exc}"
                )

    except asyncio.CancelledError:

        log(
            f"Trade task cancelled | "
            f"{direction}"
        )

        raise

    except Exception as exc:

        log(
            f"TRADE ERROR | "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# CLOSED CANDLE PROCESSOR
# ============================================================

async def process_closed_candle(
    candle: Candle
) -> None:

    global last_processed_candle_timestamp

    # --------------------------------------------------------
    # DUPLICATE CANDLE PROTECTION
    # --------------------------------------------------------

    if (
        last_processed_candle_timestamp
        == candle.timestamp
    ):
        return

    # --------------------------------------------------------
    # A candle timestamp older than the last processed candle
    # must never be inserted into the live history.
    # --------------------------------------------------------

    if (
        last_processed_candle_timestamp is not None
        and
        candle.timestamp
        < last_processed_candle_timestamp
    ):

        log(
            "Ignoring out-of-order candle | "
            f"{candle.timestamp}"
        )

        return

    last_processed_candle_timestamp = (
        candle.timestamp
    )

    # --------------------------------------------------------
    # Update candle history exactly once.
    # --------------------------------------------------------

    # Extra protection against accidental duplicate
    # timestamps from the API.
    if candles:

        if candles[-1].timestamp == candle.timestamp:
            candles[-1] = candle

        else:
            candles.append(candle)

    else:
        candles.append(candle)

    # Keep memory bounded.
    if len(candles) > MAX_CANDLE_BUFFER:

        del candles[
            :-MAX_CANDLE_BUFFER
        ]

    # --------------------------------------------------------
    # Log closed candle.
    # --------------------------------------------------------

    candle_time = datetime.fromtimestamp(
        candle.timestamp
    ).strftime("%H:%M:%S")

    log(
        "CANDLE CLOSED | "
        f"{candle_time} | "
        f"O={candle.open:.6f} "
        f"H={candle.high:.6f} "
        f"L={candle.low:.6f} "
        f"C={candle.close:.6f}"
    )

    # --------------------------------------------------------
    # Unlock a previous zone if price has exited it.
    #
    # We use the CLOSED candle close here. This keeps zone
    # locking based on closed-candle data and avoids using
    # live prices to alter strategy state.
    # --------------------------------------------------------

    unlock_zones_if_price_exited(
        candle.close
    )

    # --------------------------------------------------------
    # Build history.
    # --------------------------------------------------------

    if len(candles) < LOOKBACK_WINDOW:

        log(
            "Building history | "
            f"{len(candles)}/"
            f"{LOOKBACK_WINDOW}"
        )

        return

    # --------------------------------------------------------
    # Evaluate C0.
    #
    # C0 is ALWAYS the candle that just closed.
    # --------------------------------------------------------

    signal_data = evaluate_signal(
        candles
    )

    if not signal_data:
        return

    zone: Zone = signal_data["zone"]

    # --------------------------------------------------------
    # Stable zone lock check.
    # --------------------------------------------------------

    if zone_is_locked(zone):

        log(
            "SIGNAL BLOCKED | "
            f"{zone.zone_type.upper()} zone "
            "already traded during current touch."
        )

        return

    # --------------------------------------------------------
    # Lock BEFORE scheduling the trade.
    #
    # This prevents another candle arriving immediately
    # afterwards from creating another order.
    # --------------------------------------------------------

    lock_zone(
        zone,
        candle.timestamp
    )

    # --------------------------------------------------------
    # DO NOT await execute_trade().
    #
    # The live candle stream must continue running.
    # --------------------------------------------------------

    create_trade_task(
        signal_data
    )


# ============================================================
# INITIAL HISTORICAL DATA
# ============================================================

async def load_initial_history() -> None:

    global candles
    global last_processed_candle_timestamp

    log(
        f"Loading approximately "
        f"{HISTORY_HOURS}h of closed "
        f"{TIMEFRAME}s candles..."
    )

    try:

        generator = client.get_candles_live(
            ASSET,
            TIMEFRAME,
            hours=HISTORY_HOURS,
            max_rows=max(
                LOOKBACK_WINDOW + 20,
                100
            )
        )

        closed_candles, forming_candle = (
            await anext(generator)
        )

        normalized: list[Candle] = []

        for raw in closed_candles:

            candle = normalize_candle(raw)

            if candle is None:
                continue

            normalized.append(candle)

        # ----------------------------------------------------
        # Sort and deduplicate.
        # ----------------------------------------------------

        normalized.sort(
            key=lambda candle:
            candle.timestamp
        )

        deduped: list[Candle] = []

        seen_timestamps = set()

        for candle in normalized:

            if candle.timestamp in seen_timestamps:
                continue

            seen_timestamps.add(
                candle.timestamp
            )

            deduped.append(candle)

        candles = deduped[
            -MAX_CANDLE_BUFFER:
        :]

        if candles:

            last_processed_candle_timestamp = (
                candles[-1].timestamp
            )

        log(
            f"Loaded {len(candles)} CLOSED candles."
        )

        if forming_candle is not None:

            log(
                "Current forming candle was "
                "intentionally NOT added to strategy history."
            )

        if len(candles) < LOOKBACK_WINDOW:

            raise RuntimeError(
                "Insufficient historical candles. "
                f"Required {LOOKBACK_WINDOW}, "
                f"received {len(candles)}."
            )

    except Exception as exc:

        log(
            "Historical data error | "
            f"{type(exc).__name__}: {exc}"
        )

        raise


# ============================================================
# LIVE 15-SECOND STREAM
# ============================================================

async def handle_stream() -> None:

    log(
        f"Subscribing to "
        f"{ASSET} "
        f"with TIME-ALIGNED "
        f"{TIMEFRAME}s candles..."
    )

    # --------------------------------------------------------
    # BinaryOptionsTools-v2 provides:
    #
    # subscribe_symbol_time_aligned(
    #     asset,
    #     timedelta(seconds=15)
    # )
    #
    # TimeAligned candles are aligned to clock boundaries.
    # --------------------------------------------------------

    stream = await (
        client.subscribe_symbol_time_aligned(
            ASSET,
            timedelta(
                seconds=TIMEFRAME
            )
        )
    )

    log(
        "15-second time-aligned subscription active."
    )

    async for raw_candle in stream:

        if not running:
            break

        candle = normalize_candle(
            raw_candle
        )

        if candle is None:

            # Some library versions may produce a raw
            # price object rather than a candle object.
            # We intentionally do NOT evaluate it as C0.
            continue

        # ----------------------------------------------------
        # Every item from this subscription is treated as a
        # completed time-aligned candle.
        #
        # Never evaluate an unclosed candle.
        # ----------------------------------------------------

        await process_closed_candle(
            candle
        )

    log("Live candle stream ended.")


# ============================================================
# ACCOUNT
# ============================================================

async def show_account() -> None:

    try:

        is_demo = client.is_demo()

        log(
            "Account type: "
            + (
                "DEMO"
                if is_demo
                else "REAL"
            )
        )

    except Exception as exc:

        log(
            "Could not determine account type | "
            f"{type(exc).__name__}: {exc}"
        )

    try:

        balance = await client.balance()

        log(
            f"Balance: "
            f"${float(balance):,.2f}"
        )

    except Exception as exc:

        log(
            "Could not retrieve balance | "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# CLIENT STARTUP
# ============================================================

async def start_client(
    PocketOptionAsync
):

    global client

    log(
        "Creating PocketOptionAsync client..."
    )

    client = PocketOptionAsync(
        SSID
    )

    log(
        "Client created."
    )

    # Give the WebSocket/client time to initialize.
    await asyncio.sleep(5)

    # Wait for asset information if available.
    try:

        await client.wait_for_assets(
            timeout_secs=30.0
        )

        log(
            "Asset list loaded."
        )

    except TypeError:

        # Some versions use timeout rather than
        # timeout_secs.
        try:

            await client.wait_for_assets(
                timeout=30.0
            )

            log(
                "Asset list loaded."
            )

        except Exception as exc:

            log(
                "Asset wait failed | "
                f"{type(exc).__name__}: {exc}"
            )

    except Exception as exc:

        log(
            "Asset wait failed | "
            f"{type(exc).__name__}: {exc}"
        )

    return client


# ============================================================
# ASSET VALIDATION
# ============================================================

async def validate_asset() -> None:

    try:

        assets = await client.active_assets()

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

        log(
            f"Asset verified | "
            f"{asset.get('symbol')} | "
            f"OTC={asset.get('is_otc')} | "
            f"active={asset.get('is_active')} | "
            f"payout={asset.get('payout')}%"
        )

        allowed = asset.get(
            "allowed_candles",
            []
        )

        allowed_times = []

        for item in allowed:

            if isinstance(item, dict):
                value = item.get("time")

                if value is not None:
                    allowed_times.append(
                        int(value)
                    )

        if (
            allowed_times
            and
            TIMEFRAME not in allowed_times
        ):

            raise RuntimeError(
                f"{ASSET} does not advertise "
                f"{TIMEFRAME}s candles. "
                f"Allowed: {allowed_times}"
            )

    except Exception as exc:

        log(
            "Asset validation failed | "
            f"{type(exc).__name__}: {exc}"
        )

        raise


# ============================================================
# CLEANUP
# ============================================================

async def cancel_stream_task() -> None:

    global stream_task

    if stream_task is None:
        return

    if stream_task.done():
        return

    log(
        "Cancelling live stream task..."
    )

    stream_task.cancel()

    try:

        await stream_task

    except asyncio.CancelledError:
        pass

    except Exception as exc:

        log(
            "Stream task shutdown error | "
            f"{type(exc).__name__}: {exc}"
        )


async def cleanup_trade_tasks() -> None:

    if not trade_tasks:
        return

    log(
        f"Cleaning up "
        f"{len(trade_tasks)} trade task(s)..."
    )

    tasks = list(
        trade_tasks
    )

    # Give currently submitted trades a moment to finish
    # their API call. Do NOT cancel immediately.
    try:

        await asyncio.wait(
            tasks,
            timeout=5
        )

    except Exception:
        pass

    # Cancel anything still running.
    for task in tasks:

        if not task.done():
            task.cancel()

    if tasks:

        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    trade_tasks.clear()


async def shutdown_client() -> None:

    global client

    if client is None:
        return

    log(
        "Shutting down Pocket Option client..."
    )

    try:

        shutdown_method = getattr(
            client,
            "shutdown",
            None
        )

        if shutdown_method is not None:

            result = shutdown_method()

            if asyncio.iscoroutine(result):

                await result

            log(
                "Pocket Option client shutdown."
            )

            return

    except Exception as exc:

        log(
            "Client shutdown() failed | "
            f"{type(exc).__name__}: {exc}"
        )

    # Fallback to disconnect if available.
    try:

        disconnect_method = getattr(
            client,
            "disconnect",
            None
        )

        if disconnect_method is not None:

            result = disconnect_method()

            if asyncio.iscoroutine(result):
                await result

            log(
                "Pocket Option client disconnected."
            )

    except Exception as exc:

        log(
            "Client disconnect failed | "
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# MAIN
# ============================================================

async def main() -> None:

    global shutdown_event
    global stream_task

    shutdown_event = asyncio.Event()

    print_header()

    validate_configuration()

    PocketOptionAsync = import_library()

    await start_client(
        PocketOptionAsync
    )

    await show_account()

    await validate_asset()

    # --------------------------------------------------------
    # Load historical CLOSED candles.
    #
    # The current forming candle is intentionally discarded.
    # --------------------------------------------------------

    await load_initial_history()

    log("")
    log("=" * 80)
    log("STRATEGY READY")
    log("=" * 80)

    log(
        f"Asset       : {ASSET}"
    )

    log(
        f"Timeframe   : {TIMEFRAME}s"
    )

    log(
        f"Expiry      : {EXPIRY_SECONDS}s"
    )

    log(
        f"Lookback    : {LOOKBACK_WINDOW}"
    )

    log(
        "C0          : most recently CLOSED candle"
    )

    log(
        "Entry       : immediately after C0 closes"
    )

    log(
        "BUY         : Dragonfly Doji / Hammer at support"
    )

    log(
        "SELL        : Gravestone Doji / Shooting Star at resistance"
    )

    log("=" * 80)

    # --------------------------------------------------------
    # Start live stream.
    # --------------------------------------------------------

    stream_task = asyncio.create_task(
        handle_stream()
    )

    shutdown_wait_task = asyncio.create_task(
        shutdown_event.wait()
    )

    try:

        done, pending = await asyncio.wait(
            [
                stream_task,
                shutdown_wait_task
            ],
            return_when=asyncio.FIRST_COMPLETED
        )

        # ----------------------------------------------------
        # If stream stopped unexpectedly while bot is still
        # running, surface its exception.
        # ----------------------------------------------------

        if stream_task in done:

            try:
                await stream_task

            except asyncio.CancelledError:
                pass

            except Exception as exc:

                log(
                    "Live stream stopped with error | "
                    f"{type(exc).__name__}: {exc}"
                )

                raise

        # ----------------------------------------------------
        # If shutdown was requested, terminate stream.
        # ----------------------------------------------------

        if shutdown_wait_task in done:

            log(
                "Shutdown event received."
            )

            if (
                stream_task is not None
                and
                not stream_task.done()
            ):

                stream_task.cancel()

                try:
                    await stream_task

                except asyncio.CancelledError:
                    pass

    finally:

        shutdown_wait_task.cancel()

        try:
            await shutdown_wait_task

        except asyncio.CancelledError:
            pass

        await cancel_stream_task()

        await cleanup_trade_tasks()

        await shutdown_client()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    install_signal_handlers()

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        request_shutdown(
            "KeyboardInterrupt"
        )

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

        print(
            "Bot shutdown complete.",
            flush=True
        )
