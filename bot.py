import asyncio
import os
import signal
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

from dotenv import load_dotenv

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID")

ASSET = "EURGBP_otc"
TIMEFRAME_SECONDS = 15

# How long this diagnostic test should run.
# Set to 300 for 5 minutes, 600 for 10 minutes, etc.
TEST_DURATION_SECONDS = int(
    os.getenv("TEST_DURATION_SECONDS", "300")
)


# ============================================================
# GLOBAL STATE
# ============================================================

shutdown_requested = False


# ============================================================
# LOGGING
# ============================================================

def log(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# SHUTDOWN
# ============================================================

def request_shutdown(signum=None, frame=None):
    global shutdown_requested

    if not shutdown_requested:
        shutdown_requested = True

        if signum is not None:
            log(f"Shutdown signal received: {signum}")

        log("Stopping candle alignment test...")


# ============================================================
# NUMBER CONVERSION
# ============================================================

def to_float(value):
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ============================================================
# CLOCK ALIGNMENT
# ============================================================

def aligned_bucket_timestamp(timestamp, interval_seconds):
    """
    Convert any Unix timestamp into the beginning of its
    wall-clock interval.

    For 15 seconds:

        xx:xx:00 -> xx:xx:00
        xx:xx:01 -> xx:xx:00
        ...
        xx:xx:14 -> xx:xx:00

        xx:xx:15 -> xx:xx:15
        ...
        xx:xx:29 -> xx:xx:15

        xx:xx:30 -> xx:xx:30
        ...
        xx:xx:44 -> xx:xx:30

        xx:xx:45 -> xx:xx:45
        ...
        xx:xx:59 -> xx:xx:45
    """

    return timestamp - (timestamp % interval_seconds)


def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    )


def format_timestamp(timestamp):
    dt = timestamp_to_datetime(timestamp)

    return dt.strftime("%H:%M:%S")


# ============================================================
# CANDLE OBJECT
# ============================================================

class AlignedCandle:

    def __init__(self, timestamp, price_data):
        self.timestamp = timestamp

        self.open = price_data
        self.high = price_data
        self.low = price_data
        self.close = price_data

    def update(self, price):
        if price > self.high:
            self.high = price

        if price < self.low:
            self.low = price

        self.close = price

    def as_dict(self):
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }


# ============================================================
# PRINT CANDLE
# ============================================================

def print_closed_candle(candle):
    start = candle.timestamp
    end = start + TIMEFRAME_SECONDS

    change = candle.close - candle.open

    if candle.close > candle.open:
        direction = "BULLISH"
    elif candle.close < candle.open:
        direction = "BEARISH"
    else:
        direction = "DOJI"

    range_value = candle.high - candle.low

    print()
    print("=" * 78)
    print("CLOSED ALIGNED 15-SECOND CANDLE")
    print("=" * 78)

    print(
        f"Interval : "
        f"{format_timestamp(start)} → {format_timestamp(end)}"
    )

    print(
        f"O: {candle.open:.6f}    "
        f"H: {candle.high:.6f}    "
        f"L: {candle.low:.6f}    "
        f"C: {candle.close:.6f}"
    )

    print(
        f"Direction: {direction:<8} "
        f"Change: {change:+.6f}    "
        f"Range: {range_value:.6f}"
    )

    print(
        f"Clock alignment: "
        f"{format_timestamp(start)} "
        f"(second={start % 60:02d})"
    )

    print("=" * 78)
    print()


# ============================================================
# MAIN STREAM TEST
# ============================================================

async def run_test(client):

    global shutdown_requested

    log("=" * 78)
    log("EURGBP OTC — ALIGNED 15 SECOND CANDLE TEST")
    log("=" * 78)

    log(f"Asset      : {ASSET}")
    log(f"Timeframe  : {TIMEFRAME_SECONDS}s")
    log(f"Test time  : {TEST_DURATION_SECONDS}s")
    log("")
    log("IMPORTANT:")
    log("The subscription's initial timestamp is NOT used as")
    log("the candle clock anchor.")
    log("")
    log("Required candle boundaries:")
    log("00 → 15 → 30 → 45 → 00")
    log("=" * 78)

    # --------------------------------------------------------
    # Subscribe
    # --------------------------------------------------------

    log("Subscribing to timed EURGBP_otc stream...")

    subscription = await client.subscribe_symbol_timed(
        ASSET,
        timedelta(seconds=TIMEFRAME_SECONDS)
    )

    log("Subscription established.")
    log("Waiting for live market data...")
    log("")

    # --------------------------------------------------------
    # Candle state
    # --------------------------------------------------------

    current_candle = None

    first_stream_timestamp = None
    aligned_start_timestamp = None

    stream_start = time.monotonic()

    received_updates = 0
    completed_candles = 0

    # --------------------------------------------------------
    # Process stream
    # --------------------------------------------------------

    async for raw_candle in subscription:

        if shutdown_requested:
            break

        if time.monotonic() - stream_start >= TEST_DURATION_SECONDS:
            log("Test duration reached.")
            break

        received_updates += 1

        # ----------------------------------------------------
        # Extract timestamp
        # ----------------------------------------------------

        timestamp = (
            raw_candle.get("timestamp")
            or raw_candle.get("time")
        )

        if timestamp is None:
            log("WARNING: stream update has no timestamp.")
            continue

        try:
            timestamp = int(timestamp)
        except (ValueError, TypeError):
            log(
                f"WARNING: invalid timestamp: {timestamp}"
            )
            continue

        # ----------------------------------------------------
        # Extract price
        # ----------------------------------------------------

        close_price = to_float(
            raw_candle.get("close")
        )

        if close_price is None:

            # Some stream versions may provide last price
            close_price = to_float(
                raw_candle.get("price")
            )

        if close_price is None:
            log(
                "WARNING: stream update contains no usable price."
            )
            continue

        # ----------------------------------------------------
        # FIRST STREAM UPDATE
        # ----------------------------------------------------

        if first_stream_timestamp is None:

            first_stream_timestamp = timestamp

            aligned_start_timestamp = (
                aligned_bucket_timestamp(
                    timestamp,
                    TIMEFRAME_SECONDS
                )
            )

            log("=" * 78)
            log("FIRST STREAM UPDATE")
            log("=" * 78)

            log(
                f"Raw timestamp : "
                f"{format_timestamp(timestamp)}"
            )

            log(
                f"Aligned bucket: "
                f"{format_timestamp(aligned_start_timestamp)}"
            )

            log(
                f"Raw second    : "
                f"{timestamp % 60:02d}"
            )

            log(
                f"Aligned second: "
                f"{aligned_start_timestamp % 60:02d}"
            )

            log("")
            log(
                "The first received partial candle will NOT be "
                "treated as a trading candle."
            )

            log("=" * 78)

        # ----------------------------------------------------
        # DETERMINE CORRECT WALL-CLOCK BUCKET
        # ----------------------------------------------------

        bucket_timestamp = aligned_bucket_timestamp(
            timestamp,
            TIMEFRAME_SECONDS
        )

        # ----------------------------------------------------
        # INITIAL CANDLE
        # ----------------------------------------------------

        if current_candle is None:

            current_candle = AlignedCandle(
                bucket_timestamp,
                close_price
            )

            log(
                f"[ALIGN] Started candle "
                f"{format_timestamp(bucket_timestamp)} → "
                f"{format_timestamp(bucket_timestamp + TIMEFRAME_SECONDS)}"
            )

            continue

        # ----------------------------------------------------
        # SAME CANDLE
        # ----------------------------------------------------

        if bucket_timestamp == current_candle.timestamp:

            current_candle.update(close_price)

            # Show live update
            print(
                f"[LIVE] "
                f"{format_timestamp(timestamp)} | "
                f"{ASSET} | "
                f"{close_price:.6f}",
                flush=True
            )

            continue

        # ----------------------------------------------------
        # NEW CLOCK BUCKET
        # ----------------------------------------------------

        if bucket_timestamp > current_candle.timestamp:

            # ------------------------------------------------
            # CLOSE PREVIOUS CANDLE
            # ------------------------------------------------

            print_closed_candle(current_candle)

            completed_candles += 1

            # ------------------------------------------------
            # Detect skipped intervals
            # ------------------------------------------------

            expected_next = (
                current_candle.timestamp
                + TIMEFRAME_SECONDS
            )

            if bucket_timestamp > expected_next:

                missed = (
                    bucket_timestamp - expected_next
                ) // TIMEFRAME_SECONDS

                log(
                    f"WARNING: {missed} aligned candle "
                    f"interval(s) had no stream update."
                )

            # ------------------------------------------------
            # Start new aligned candle
            # ------------------------------------------------

            current_candle = AlignedCandle(
                bucket_timestamp,
                close_price
            )

            log(
                f"[ALIGN] New candle "
                f"{format_timestamp(bucket_timestamp)} → "
                f"{format_timestamp(bucket_timestamp + TIMEFRAME_SECONDS)}"
            )

            continue

        # ----------------------------------------------------
        # OLD / OUT-OF-ORDER UPDATE
        # ----------------------------------------------------

        log(
            f"[WARNING] Ignoring out-of-order update: "
            f"{format_timestamp(timestamp)} "
            f"(bucket {format_timestamp(bucket_timestamp)})"
        )

    # ========================================================
    # END
    # ========================================================

    log("")
    log("=" * 78)
    log("CANDLE ALIGNMENT TEST COMPLETE")
    log("=" * 78)

    log(f"Stream updates received : {received_updates}")
    log(f"Completed candles        : {completed_candles}")

    if current_candle is not None:

        log(
            f"Current forming candle  : "
            f"{format_timestamp(current_candle.timestamp)} → "
            f"{format_timestamp(current_candle.timestamp + TIMEFRAME_SECONDS)}"
        )

    log("=" * 78)


# ============================================================
# MAIN
# ============================================================

async def main():

    global shutdown_requested

    if not SSID:
        raise RuntimeError(
            "POCKET_OPTION_SSID environment variable is missing."
        )

    print()
    print("=" * 78)
    print("POCKET OPTION — 15 SECOND CLOCK ALIGNMENT TEST")
    print("=" * 78)
    print(f"Asset     : {ASSET}")
    print(f"Timeframe : {TIMEFRAME_SECONDS}s")
    print(f"Duration  : {TEST_DURATION_SECONDS}s")
    print("=" * 78)
    print()

    client = None

    try:

        # ----------------------------------------------------
        # Create client
        # ----------------------------------------------------

        log("Creating PocketOptionAsync client...")

        client = PocketOptionAsync(
            ssid=SSID
        )

        log("Client created.")

        # ----------------------------------------------------
        # Allow connection initialization
        # ----------------------------------------------------

        await asyncio.sleep(5)

        # ----------------------------------------------------
        # Run stream test
        # ----------------------------------------------------

        await run_test(client)

    except asyncio.CancelledError:

        log("Async task cancelled.")

    except Exception as exc:

        log("=" * 78)
        log("TEST FAILED")
        log("=" * 78)

        log(
            f"Error type : {type(exc).__name__}"
        )

        log(
            f"Error      : {exc}"
        )

        import traceback

        traceback.print_exc()

    finally:

        if client is not None:

            log("Shutting down PocketOption client...")

            try:
                await client.shutdown()
            except Exception as exc:
                log(
                    f"Shutdown warning: {type(exc).__name__}: {exc}"
                )

            log("Client shutdown complete.")

        print()
        print("=" * 78)
        print("TEST FINISHED")
        print("=" * 78)


# ============================================================
# SIGNAL HANDLERS
# ============================================================

if __name__ == "__main__":

    signal.signal(
        signal.SIGINT,
        request_shutdown
    )

    signal.signal(
        signal.SIGTERM,
        request_shutdown
    )

    asyncio.run(main())
