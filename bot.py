import asyncio
import os
import signal
from datetime import timedelta, datetime, timezone

from dotenv import load_dotenv

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID")

ASSET = "EURGBP_otc"
TIMEFRAME_SECONDS = 15

TRADE_AMOUNT = 2500.0
EXPIRY_SECONDS = 15

LOOKBACK_WINDOW = 50


# ============================================================
# GLOBAL STATE
# ============================================================

shutdown_event = asyncio.Event()


# ============================================================
# LOGGING
# ============================================================

def log(message: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# SHUTDOWN
# ============================================================

def request_shutdown():
    if not shutdown_event.is_set():
        log("Shutdown requested.")
        shutdown_event.set()


def install_signal_handlers():
    try:
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_shutdown)
            except (NotImplementedError, RuntimeError):
                pass

    except Exception as e:
        log(f"Signal handler setup warning: {e}")


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_candle(raw):
    """
    BinaryOptionsToolsV2 returns candle dictionaries.

    This function normalizes the common field names into:

        {
            timestamp,
            open,
            high,
            low,
            close
        }

    We intentionally keep this defensive because the exact
    candle dictionary representation can vary between streams.
    """

    if raw is None:
        return None

    if not isinstance(raw, dict):
        log(f"Unexpected candle type: {type(raw).__name__}")
        return None

    timestamp = (
        raw.get("timestamp")
        or raw.get("time")
        or raw.get("at")
        or raw.get("from")
    )

    open_price = (
        raw.get("open")
        or raw.get("o")
    )

    high_price = (
        raw.get("high")
        or raw.get("h")
    )

    low_price = (
        raw.get("low")
        or raw.get("l")
    )

    close_price = (
        raw.get("close")
        or raw.get("c")
    )

    try:
        if timestamp is not None:
            timestamp = float(timestamp)

        if open_price is not None:
            open_price = float(open_price)

        if high_price is not None:
            high_price = float(high_price)

        if low_price is not None:
            low_price = float(low_price)

        if close_price is not None:
            close_price = float(close_price)

    except (TypeError, ValueError):
        return None

    if any(
        value is None
        for value in (
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        )
    ):
        return None

    return {
        "timestamp": timestamp,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
    }


# ============================================================
# CANDLE DISPLAY
# ============================================================

def candle_time(timestamp):
    try:
        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(timestamp)


def display_candle(candle, number=None):

    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    change = c - o

    if c > o:
        direction = "BULLISH"
    elif c < o:
        direction = "BEARISH"
    else:
        direction = "DOJI"

    label = f"CANDLE #{number}" if number else "CANDLE"

    print()
    print("=" * 80)
    print(f"{label} | {ASSET} | {TIMEFRAME_SECONDS}s")
    print("=" * 80)

    print(f"Time      : {candle_time(candle['timestamp'])}")
    print(f"Open      : {o:.6f}")
    print(f"High      : {h:.6f}")
    print(f"Low       : {l:.6f}")
    print(f"Close     : {c:.6f}")
    print(f"Direction : {direction}")
    print(f"Change    : {change:+.6f}")

    print("=" * 80)


# ============================================================
# MAIN STREAM
# ============================================================

async def run():

    if not SSID:
        raise RuntimeError(
            "POCKET_OPTION_SSID is missing from Railway environment variables."
        )

    print("=" * 80)
    print("POCKET OPTION 15s STREAM TEST")
    print("=" * 80)

    print(f"Asset          : {ASSET}")
    print(f"Requested TF   : {TIMEFRAME_SECONDS}s")
    print(f"Trade amount   : ${TRADE_AMOUNT:,.2f}")
    print(f"Expiry         : {EXPIRY_SECONDS}s")
    print(f"History target : {LOOKBACK_WINDOW} candles")
    print("=" * 80)

    install_signal_handlers()

    api = None

    try:

        # --------------------------------------------------------
        # CREATE CLIENT
        # --------------------------------------------------------

        log("Creating PocketOptionAsync client...")

        api = PocketOptionAsync(SSID)

        log("Client created.")

        # --------------------------------------------------------
        # WAIT FOR INITIALIZATION
        # --------------------------------------------------------

        log("Waiting for Pocket Option connection initialization...")

        await asyncio.sleep(5)

        # --------------------------------------------------------
        # ACCOUNT
        # --------------------------------------------------------

        try:
            balance = await api.get_balance()

            log(f"Balance: ${float(balance):,.2f}")

        except Exception as e:
            log(f"Balance check warning: {type(e).__name__}: {e}")

        # --------------------------------------------------------
        # ASSET CHECK
        # --------------------------------------------------------

        log(f"Checking asset: {ASSET}")

        try:

            assets = await api.get_assets()

            matching = []

            if isinstance(assets, list):

                for asset in assets:

                    if not isinstance(asset, dict):
                        continue

                    symbol = asset.get("symbol")

                    if symbol == ASSET:
                        matching.append(asset)

            if matching:

                asset = matching[0]

                log(
                    f"Asset verified | "
                    f"{asset.get('symbol')} | "
                    f"OTC={asset.get('is_otc')} | "
                    f"active={asset.get('is_active')} | "
                    f"payout={asset.get('payout')}%"
                )

                log(
                    "Native candle metadata: "
                    f"{asset.get('allowed_candles')}"
                )

            else:

                log(
                    f"WARNING: {ASSET} was not found in the returned "
                    "asset list."
                )

        except Exception as e:

            log(
                f"Asset check warning | "
                f"{type(e).__name__}: {e}"
            )

        # --------------------------------------------------------
        # IMPORTANT:
        #
        # DO NOT CHECK allowed_candles FOR 15s.
        #
        # The library provides a timed subscription which combines
        # candle data into the requested time range.
        # --------------------------------------------------------

        print()
        print("=" * 80)
        print("SUBSCRIBING TO LIBRARY TIMED 15s STREAM")
        print("=" * 80)

        log(
            f"Calling subscribe_symbol_timed("
            f"{ASSET}, timedelta(seconds={TIMEFRAME_SECONDS})"
            f")"
        )

        stream = await api.subscribe_symbol_timed(
            ASSET,
            timedelta(seconds=TIMEFRAME_SECONDS)
        )

        log("15-second stream subscription returned successfully.")
        log("Waiting for candles...")

        # --------------------------------------------------------
        # STREAM
        # --------------------------------------------------------

        candle_count = 0

        history = []

        async for raw_candle in stream:

            if shutdown_event.is_set():
                break

            log(
                f"RAW STREAM DATA RECEIVED: "
                f"{raw_candle!r}"
            )

            candle = normalize_candle(raw_candle)

            if candle is None:

                log(
                    "WARNING: Could not normalize received "
                    "stream item."
                )

                continue

            candle_count += 1

            history.append(candle)

            if len(history) > LOOKBACK_WINDOW:
                history.pop(0)

            display_candle(
                candle,
                candle_count
            )

            log(
                f"15s history: "
                f"{len(history)}/{LOOKBACK_WINDOW}"
            )

            # ----------------------------------------------------
            # STRATEGY WILL GO HERE
            #
            # We are intentionally NOT placing trades in this
            # diagnostic version.
            #
            # Once we confirm that EURGBP_otc is continuously
            # producing the correct 15-second candles, we put
            # the rejection strategy back here.
            # ----------------------------------------------------

    except asyncio.CancelledError:

        log("Stream task cancelled.")

    except Exception as e:

        print()
        print("=" * 80)
        print("STREAM ERROR")
        print("=" * 80)

        print(f"Error type : {type(e).__name__}")
        print(f"Error      : {e}")

        import traceback
        traceback.print_exc()

        print("=" * 80)

    finally:

        log("Shutting down PocketOption client...")

        if api is not None:

            try:

                close_method = getattr(api, "close", None)

                if close_method:

                    result = close_method()

                    if asyncio.iscoroutine(result):
                        await result

                    log("Client closed.")

                else:

                    log(
                        "No async close() method exposed by client."
                    )

            except Exception as e:

                log(
                    f"Client shutdown warning: "
                    f"{type(e).__name__}: {e}"
                )


# ============================================================
# ENTRY POINT
# ============================================================

async def main():

    try:
        await run()

    except KeyboardInterrupt:
        request_shutdown()

    finally:
        log("BOT STOPPED")


if __name__ == "__main__":
    asyncio.run(main())
