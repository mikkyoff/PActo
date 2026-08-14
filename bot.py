import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from dotenv import load_dotenv

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()

ASSET = os.getenv("ASSET", "AUDUSD_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))

TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "2500"))
EXPIRY_SECONDS = int(os.getenv("EXPIRY_SECONDS", "15"))

LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", "50"))

TREND_CANDLES = 3

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

ONE_TRADE_PER_ZONE = True

# ============================================================
# GLOBAL STATE
# ============================================================

running = True
client = None

candles = []
active_zone_locks = set()

last_processed_candle_timestamp = None


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
        return (self.zone_low + self.zone_high) / 2


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
    print("=" * 80)


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
# IMPORT POCKET OPTION LIBRARY
# ============================================================

def import_library():
    """
    Import the async Pocket Option client.

    The exact import is intentionally kept here so that if the
    package exposes a slightly different import path in a future
    library update, only this section needs changing.
    """

    try:
        from binary_options_tools.async_client import PocketOptionAsync

        return PocketOptionAsync

    except ImportError as exc:
        print()
        print("ERROR: Could not import PocketOptionAsync.")
        print()
        print("Installed BinaryOptionsTools-v2 may expose the")
        print("client through a different import path.")
        print()
        print(f"Original error: {exc}")
        print()
        raise


# ============================================================
# NORMALIZE CANDLE
# ============================================================

def normalize_candle(raw: Any) -> Optional[Candle]:
    """
    Convert different possible candle structures returned by the
    library into our internal Candle structure.
    """

    try:

        if isinstance(raw, Candle):
            return raw

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

        return None

    except Exception as exc:

        log(f"Could not normalize candle: {exc}")
        return None


# ============================================================
# CANDLE PATTERNS
# ============================================================

def is_gravestone_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (
        candle.body <= DOJI_BODY_MAX_PCT * candle.range
        and
        candle.upper_wick >= DOJI_WICK_MIN_PCT * candle.range
        and
        candle.lower_wick <= DOJI_BODY_MAX_PCT * candle.range
    )


def is_dragonfly_doji(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    return (
        candle.body <= DOJI_BODY_MAX_PCT * candle.range
        and
        candle.lower_wick >= DOJI_WICK_MIN_PCT * candle.range
        and
        candle.upper_wick <= DOJI_BODY_MAX_PCT * candle.range
    )


def is_shooting_star(candle: Candle) -> bool:

    if candle.range <= 0:
        return False

    body = candle.body

    if body <= 0:
        return False

    body_in_lower_third = (
        max(candle.open, candle.close)
        <= candle.low + (candle.range * (1 / 3))
    )

    return (
        body <= STAR_HAMMER_BODY_MAX_PCT * candle.range
        and
        candle.upper_wick >= STAR_HAMMER_WICK_MULTIPLIER * body
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
        >= candle.low + (candle.range * (2 / 3))
    )

    return (
        body <= STAR_HAMMER_BODY_MAX_PCT * candle.range
        and
        candle.lower_wick >= STAR_HAMMER_WICK_MULTIPLIER * body
        and
        body_in_upper_third
    )


# ============================================================
# TREND GATES
# ============================================================

def buy_trend_gate(history):

    if len(history) < 4:
        return False

    c0 = history[-1]
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

    c0 = history[-1]
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

def find_pivot_high(data, index, strength=3):

    if index - strength < 0:
        return False

    if index + strength >= len(data):
        return False

    value = data[index].high

    for i in range(index - strength, index + strength + 1):

        if i == index:
            continue

        if data[i].high >= value:
            return False

    return True


def find_pivot_low(data, index, strength=3):

    if index - strength < 0:
        return False

    if index + strength >= len(data):
        return False

    value = data[index].low

    for i in range(index - strength, index + strength + 1):

        if i == index:
            continue

        if data[i].low <= value:
            return False

    return True


# ============================================================
# ZONE CLUSTERING
# ============================================================

def average_range(data):

    valid = [c.range for c in data if c.range > 0]

    if not valid:
        return 0

    return sum(valid) / len(valid)


def build_zones(data):

    if len(data) < LOOKBACK_WINDOW:
        return [], []

    data = data[-LOOKBACK_WINDOW:]

    avg_range = average_range(data)

    if avg_range <= 0:
        return [], []

    tolerance = avg_range * ZONE_TOLERANCE_MULTIPLIER

    resistance_pivots = []
    support_pivots = []

    # Strength 3 means 3 candles on either side.
    for i in range(len(data)):

        if find_pivot_high(data, i, 3):
            resistance_pivots.append(data[i].high)

        if find_pivot_low(data, i, 3):
            support_pivots.append(data[i].low)

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


def cluster_prices(prices, tolerance, zone_type):

    if not prices:
        return []

    prices = sorted(prices)

    clusters = []

    current = [prices[0]]

    for price in prices[1:]:

        if abs(price - sum(current) / len(current)) <= tolerance:

            current.append(price)

        else:

            clusters.append(current)
            current = [price]

    clusters.append(current)

    zones = []

    for cluster in clusters:

        if len(cluster) < MIN_ZONE_TOUCHES:
            continue

        if len(cluster) > MAX_ZONE_TOUCHES:
            continue

        center = sum(cluster) / len(cluster)

        zones.append(
            Zone(
                zone_low=center - tolerance,
                zone_high=center + tolerance,
                touch_count=len(cluster),
                zone_type=zone_type
            )
        )

    return zones


# ============================================================
# ZONE MATCHING
# ============================================================

def price_in_zone(price, zone):

    return zone.zone_low <= price <= zone.zone_high


def find_support_zone(price, zones):

    candidates = [
        z for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z: abs(z.center - price)
    )


def find_resistance_zone(price, zones):

    candidates = [
        z for z in zones
        if price_in_zone(price, z)
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda z: abs(z.center - price)
    )


# ============================================================
# SIGNAL EVALUATION
# ============================================================

def evaluate_signal(data):

    if len(data) < LOOKBACK_WINDOW:
        return None

    c0 = data[-1]

    # --------------------------------------------------------
    # Minimum range filter
    # --------------------------------------------------------

    avg_range = average_range(
        data[-LOOKBACK_WINDOW:]
    )

    if c0.range < MIN_RANGE_FILTER * avg_range:

        log(
            "Signal rejected: candle range below "
            "minimum range filter."
        )

        return None

    # --------------------------------------------------------
    # Build zones from CLOSED candles only
    # --------------------------------------------------------

    support_zones, resistance_zones = build_zones(data)

    # --------------------------------------------------------
    # BUY
    # Trend gate MUST happen before pattern/zone evaluation.
    # --------------------------------------------------------

    if buy_trend_gate(data):

        support_zone = find_support_zone(
            c0.low,
            support_zones
        )

        if support_zone:

            if (
                is_dragonfly_doji(c0)
                or
                is_hammer(c0)
            ):

                pattern = (
                    "DRAGONFLY_DOJI"
                    if is_dragonfly_doji(c0)
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
    # Trend gate MUST happen before pattern/zone evaluation.
    # --------------------------------------------------------

    if sell_trend_gate(data):

        resistance_zone = find_resistance_zone(
            c0.high,
            resistance_zones
        )

        if resistance_zone:

            if (
                is_gravestone_doji(c0)
                or
                is_shooting_star(c0)
            ):

                pattern = (
                    "GRAVESTONE_DOJI"
                    if is_gravestone_doji(c0)
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

    log("")
    log("=" * 60)
    log("VALID TRADE SIGNAL")
    log("=" * 60)

    log(f"Direction : {direction}")
    log(f"Pattern   : {pattern}")

    log(
        f"Zone      : "
        f"{zone.zone_low:.6f} - {zone.zone_high:.6f}"
    )

    log(
        f"Amount    : ${TRADE_AMOUNT:,.2f}"
    )

    log(
        f"Expiry    : {EXPIRY_SECONDS}s"
    )

    log("=" * 60)

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # The exact order method used by BinaryOptionsTools-v2
    # depends on the installed version's async API.
    #
    # This section intentionally isolates order submission.
    # --------------------------------------------------------

    try:

        if direction == "BUY":

            result = await client.buy(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                duration=EXPIRY_SECONDS
            )

        else:

            result = await client.sell(
                asset=ASSET,
                amount=TRADE_AMOUNT,
                duration=EXPIRY_SECONDS
            )

        log(f"TRADE SUBMITTED: {result}")

        return result

    except Exception as exc:

        log(f"TRADE ERROR: {type(exc).__name__}: {exc}")

        return None


# ============================================================
# CANDLE PROCESSOR
# ============================================================

async def process_closed_candle(candle):

    global last_processed_candle_timestamp

    if (
        last_processed_candle_timestamp
        == candle.timestamp
    ):
        return

    last_processed_candle_timestamp = candle.timestamp

    candles.append(candle)

    if len(candles) > LOOKBACK_WINDOW + 20:
        del candles[:-LOOKBACK_WINDOW - 20]

    log(
        f"CANDLE CLOSED | "
        f"{datetime.fromtimestamp(candle.timestamp).strftime('%H:%M:%S')} | "
        f"O={candle.open:.6f} "
        f"H={candle.high:.6f} "
        f"L={candle.low:.6f} "
        f"C={candle.close:.6f}"
    )

    if len(candles) < LOOKBACK_WINDOW:
        log(
            f"Building history: "
            f"{len(candles)}/{LOOKBACK_WINDOW}"
        )

        return

    signal_data = evaluate_signal(candles)

    if not signal_data:
        return

    zone = signal_data["zone"]

    zone_key = (
        zone.zone_type,
        round(zone.center, 6)
    )

    if (
        ONE_TRADE_PER_ZONE
        and
        zone_key in active_zone_locks
    ):

        log(
            "Signal blocked: zone already traded "
            "during current touch."
        )

        return

    # Lock before execution to prevent duplicate submissions.
    active_zone_locks.add(zone_key)

    await execute_trade(signal_data)


# ============================================================
# STREAM HANDLING
# ============================================================

async def handle_tick_stream():

    """
    Main live-stream loop.

    The exact subscription/stream method names can vary between
    BinaryOptionsTools-v2 revisions. This function is kept
    isolated so the rest of the strategy remains unchanged.
    """

    log(
        f"Subscribing to {ASSET} "
        f"at {TIMEFRAME}s..."
    )

    try:

        # ----------------------------------------------------
        # Attempt the async subscription API.
        # ----------------------------------------------------

        stream = await client.subscribe_candles(
            asset=ASSET,
            timeframe=TIMEFRAME
        )

        log("Subscription successful.")

        async for raw_candle in stream:

            if not running:
                break

            candle = normalize_candle(raw_candle)

            if candle is None:
                continue

            await process_closed_candle(candle)

    except AttributeError:

        log(
            "The installed BinaryOptionsTools-v2 version "
            "does not expose subscribe_candles() under "
            "that exact method name."
        )

        raise

    except Exception as exc:

        log(
            f"Stream error: "
            f"{type(exc).__name__}: {exc}"
        )

        raise


# ============================================================
# CLIENT STARTUP
# ============================================================

async def start_client(PocketOptionAsync):

    global client

    if not SSID:

        raise RuntimeError(
            "POCKET_OPTION_SSID is not configured."
        )

    log("Creating PocketOptionAsync client...")

    client = PocketOptionAsync(
        ssid=SSID
    )

    log("Client created.")

    # Allow connection initialization.
    await asyncio.sleep(5)

    return client


# ============================================================
# ACCOUNT INFORMATION
# ============================================================

async def show_account():

    try:

        is_demo = await client.is_demo()

        log(
            "Account type: "
            + ("DEMO" if is_demo else "REAL")
        )

    except Exception as exc:

        log(
            f"Could not determine account type: {exc}"
        )

    try:

        balance = await client.get_balance()

        log(
            f"Balance: ${float(balance):,.2f}"
        )

    except Exception as exc:

        log(
            f"Could not retrieve balance: {exc}"
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    print_header()

    PocketOptionAsync = import_library()

    await start_client(
        PocketOptionAsync
    )

    await show_account()

    log(
        f"Starting live {TIMEFRAME}s "
        f"{ASSET} strategy..."
    )

    await handle_tick_stream()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        pass

    except Exception as exc:

        print()
        print("=" * 80)
        print("BOT STOPPED")
        print("=" * 80)
        print(
            f"Error type : {type(exc).__name__}"
        )
        print(
            f"Error      : {exc}"
        )
        print("=" * 80)

        sys.exit(1)

    finally:

        if client is not None:

            try:

                shutdown = getattr(
                    client,
                    "shutdown",
                    None
                )

                if shutdown:

                    result = shutdown()

                    if asyncio.iscoroutine(result):

                        try:
                            asyncio.run(result)
                        except RuntimeError:
                            pass

            except Exception:

                pass

        print("Bot shutdown complete.")
