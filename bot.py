import asyncio
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


load_dotenv()

SSID = os.getenv("POCKET_OPTION_SSID")

ASSET = "EURGBP_otc"
TIMEFRAME = 15


async def main():
    print("=" * 80)
    print("POCKET OPTION — TIME-ALIGNED 15s STREAM TEST")
    print("=" * 80)
    print(f"Asset     : {ASSET}")
    print("Timeframe : 15 seconds")
    print("=" * 80)

    if not SSID:
        raise RuntimeError(
            "POCKET_OPTION_SSID environment variable is not set."
        )

    client = None

    try:
        print("Creating PocketOptionAsync client...")
        client = PocketOptionAsync(SSID)
        print("Client created.")

        # Give the client time to establish/load its connection.
        await asyncio.sleep(5)

        print("Subscribing using native clock-aligned subscription...")
        print(
            "Method: subscribe_symbol_time_aligned("
            f"{ASSET}, timedelta(seconds={TIMEFRAME})"
            ")"
        )

        subscription = await client.subscribe_symbol_time_aligned(
            ASSET,
            timedelta(seconds=TIMEFRAME),
        )

        print("[OK] Subscription established.")
        print()
        print("EXPECTED CANDLE ALIGNMENT")
        print("  :00 -> :15")
        print("  :15 -> :30")
        print("  :30 -> :45")
        print("  :45 -> :00")
        print()
        print("Waiting for candles...")
        print()

        candle_number = 0

        async for candle in subscription:
            candle_number += 1

            print("=" * 80)
            print(f"ALIGNED 15-SECOND CANDLE #{candle_number}")
            print("=" * 80)

            print(f"RAW CANDLE: {candle}")
            print()

            # ---------------------------------------------------------
            # Extract timestamp
            # ---------------------------------------------------------
            timestamp = None

            if isinstance(candle, dict):
                timestamp = (
                    candle.get("timestamp")
                    or candle.get("time")
                    or candle.get("openTime")
                    or candle.get("open_time")
                )

            if timestamp is not None:
                try:
                    timestamp = float(timestamp)

                    # Handle milliseconds if returned by the API.
                    if timestamp > 10_000_000_000:
                        timestamp /= 1000

                    dt = datetime.fromtimestamp(
                        timestamp,
                        tz=timezone.utc,
                    )

                    print(
                        "Timestamp : "
                        f"{dt.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                    print(f"Second    : {dt.second:02d}")

                    if dt.second % 15 == 0:
                        print("ALIGNMENT : OK")
                    else:
                        print("ALIGNMENT : *** ERROR ***")

                except Exception as exc:
                    print(f"Timestamp parse error: {exc}")

            # ---------------------------------------------------------
            # OHLC
            # ---------------------------------------------------------
            if isinstance(candle, dict):
                open_price = candle.get("open")
                high_price = candle.get("high")
                low_price = candle.get("low")
                close_price = candle.get("close")

                print()
                print(f"Open      : {open_price}")
                print(f"High      : {high_price}")
                print(f"Low       : {low_price}")
                print(f"Close     : {close_price}")

                # Calculate candle direction/range when possible.
                try:
                    o = float(open_price)
                    h = float(high_price)
                    l = float(low_price)
                    c = float(close_price)

                    change = c - o
                    candle_range = h - l

                    if c > o:
                        direction = "BULLISH"
                    elif c < o:
                        direction = "BEARISH"
                    else:
                        direction = "DOJI"

                    print(f"Direction : {direction}")
                    print(f"Change    : {change:+.6f}")
                    print(f"Range     : {candle_range:.6f}")

                except (TypeError, ValueError):
                    pass

            print()

    except asyncio.CancelledError:
        print()
        print("Stream cancelled.")

    except KeyboardInterrupt:
        print()
        print("Keyboard interrupt received.")

    except Exception as exc:
        print()
        print("=" * 80)
        print("TEST FAILED")
        print("=" * 80)
        print(f"Error type : {type(exc).__name__}")
        print(f"Error      : {exc}")
        print("=" * 80)

    finally:
        print()
        print("Shutting down client...")

        if client is not None:
            try:
                await client.shutdown()
                print("[OK] Client shutdown successfully.")
            except Exception as exc:
                print(f"Shutdown warning: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
