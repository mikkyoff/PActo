import asyncio
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

    async with PocketOptionAsync(SSID) as api:

        print("Client connected.")
        print("Subscribing using subscribe_symbol_time_aligned()...")

        stream = await api.subscribe_symbol_time_aligned(
            ASSET,
            timedelta(seconds=TIMEFRAME)
        )

        print("Subscription successful.")
        print("Waiting for aligned candles...")
        print()

        candle_number = 0

        async for candle in stream:

            candle_number += 1

            print("=" * 80)
            print(f"ALIGNED 15s CANDLE #{candle_number}")
            print("=" * 80)

            print(f"RAW: {candle}")

            # Extract values safely
            timestamp = candle.get("timestamp", candle.get("time"))

            try:
                ts = int(timestamp)

                # Pocket Option timestamps are normally Unix seconds.
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)

                print(f"Timestamp : {dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                print(f"Second    : {dt.second:02d}")

                if dt.second % 15 == 0:
                    print("ALIGNMENT : OK")
                else:
                    print("ALIGNMENT : ERROR")

            except Exception as e:
                print(f"Timestamp parse error: {e}")

            print(f"Open      : {candle.get('open')}")
            print(f"High      : {candle.get('high')}")
            print(f"Low       : {candle.get('low')}")
            print(f"Close     : {candle.get('close')}")
            print(f"Closed    : {candle.get('is_closed')}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
