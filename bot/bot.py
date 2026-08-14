import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass(frozen=True)
class Config:
    # Trading
    asset: str = os.getenv("ASSET", "AUDUSD_otc")
    timeframe_seconds: int = int(
        os.getenv("TIMEFRAME_SECONDS", "15")
    )
    trade_amount: float = float(
        os.getenv("TRADE_AMOUNT", "2500")
    )
    expiry_seconds: int = int(
        os.getenv("EXPIRY_SECONDS", "15")
    )

    # S/R
    lookback_window: int = int(
        os.getenv("LOOKBACK_WINDOW", "50")
    )
    pivot_strength: int = int(
        os.getenv("PIVOT_STRENGTH", "3")
    )
    zone_tolerance_avg_range: float = float(
        os.getenv("ZONE_TOLERANCE_AVG_RANGE", "0.75")
    )
    min_zone_touches: int = int(
        os.getenv("MIN_ZONE_TOUCHES", "2")
    )
    max_zone_touches: int = int(
        os.getenv("MAX_ZONE_TOUCHES", "4")
    )

    # Candlestick patterns
    doji_body_max_pct: float = float(
        os.getenv("DOJI_BODY_MAX_PCT", "0.10")
    )
    doji_wick_min_pct: float = float(
        os.getenv("DOJI_WICK_MIN_PCT", "0.60")
    )
    star_hammer_body_max_pct: float = float(
        os.getenv("STAR_HAMMER_BODY_MAX_PCT", "0.30")
    )
    star_hammer_wick_multiplier: float = float(
        os.getenv("STAR_HAMMER_WICK_MULTIPLIER", "2.0")
    )

    # Filters
    min_range_avg_multiplier: float = float(
        os.getenv("MIN_RANGE_AVG_MULTIPLIER", "0.5")
    )
    trend_candles: int = int(
        os.getenv("TREND_CANDLES", "3")
    )

    # Safety
    live_trading: bool = (
        os.getenv("LIVE_TRADING", "false").lower() == "true"
    )

    # Testing controls
    max_trades: int = int(
        os.getenv("MAX_TRADES", "0")
    )
    max_consecutive_trades: int = int(
        os.getenv("MAX_CONSECUTIVE_TRADES", "0")
    )


CFG = Config()

LOG = logging.getLogger("pocket-option-bot")

STOP_EVENT = asyncio.Event()


# ============================================================
# CANDLE MODEL
# ============================================================

@dataclass
class Candle:
    time: float
    open: float
    high: float
    low: float
    close: float

    @property
    def range(self) -> float:
        return max(0.0, self.high - self.low)


# ============================================================
# CANDLE PARSER
# ============================================================

def first_number(
    data: dict,
    keys: tuple[str, ...]
) -> Optional[float]:

    for key in keys:

        if key not in data:
            continue

        value = data[key]

        if value is None:
            continue

        try:
            return float(value)
        except (TypeError, ValueError):
            continue

    return None


def parse_candle(raw: Any) -> Candle:

    if not isinstance(raw, dict):
        raise ValueError(
            f"Unexpected candle type: "
            f"{type(raw).__name__}: {raw!r}"
        )

    timestamp = first_number(
        raw,
        (
            "time",
            "timestamp",
            "ts",
            "openTime",
            "open_timestamp",
        ),
    )

    open_price = first_number(
        raw,
        ("open", "o"),
    )

    high_price = first_number(
        raw,
        ("high", "h"),
    )

    low_price = first_number(
        raw,
        ("low", "l"),
    )

    close_price = first_number(
        raw,
        ("close", "c"),
    )

    if None in (
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
    ):
        raise ValueError(
            f"Could not parse candle: {raw!r}"
        )

    # Convert milliseconds to seconds if necessary.
    if timestamp > 10_000_000_000:
        timestamp /= 1000.0

    return Candle(
        time=timestamp,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
    )


# ============================================================
# CANDLE PATTERN DETECTION
# ============================================================

def candle_patterns(candle: Candle) -> set[str]:

    candle_range = candle.range

    if candle_range <= 0:
        return set()

    body = abs(
        candle.close - candle.open
    )

    upper_wick = (
        candle.high -
        max(candle.open, candle.close)
    )

    lower_wick = (
        min(candle.open, candle.close) -
        candle.low
    )

    patterns = set()

    # --------------------------------------------------------
    # GRAVESTONE DOJI
    # --------------------------------------------------------

    if (
        body <= CFG.doji_body_max_pct * candle_range
        and
        upper_wick >= CFG.doji_wick_min_pct * candle_range
        and
        lower_wick <= CFG.doji_body_max_pct * candle_range
    ):
        patterns.add("GRAVESTONE_DOJI")

    # --------------------------------------------------------
    # SHOOTING STAR
    # --------------------------------------------------------

    body_in_lower_third = (
        max(candle.open, candle.close)
        <= candle.low + candle_range / 3.0
    )

    if (
        body <= CFG.star_hammer_body_max_pct * candle_range
        and
        upper_wick
        >= CFG.star_hammer_wick_multiplier * body
        and
        body_in_lower_third
    ):
        patterns.add("SHOOTING_STAR")

    # --------------------------------------------------------
    # DRAGONFLY DOJI
    # --------------------------------------------------------

    if (
        body <= CFG.doji_body_max_pct * candle_range
        and
        lower_wick >= CFG.doji_wick_min_pct * candle_range
        and
        upper_wick <= CFG.doji_body_max_pct * candle_range
    ):
        patterns.add("DRAGONFLY_DOJI")

    # --------------------------------------------------------
    # HAMMER
    # --------------------------------------------------------

    body_in_upper_third = (
        min(candle.open, candle.close)
        >= candle.low + 2.0 * candle_range / 3.0
    )

    if (
        body <= CFG.star_hammer_body_max_pct * candle_range
        and
        lower_wick
        >= CFG.star_hammer_wick_multiplier * body
        and
        body_in_upper_third
    ):
        patterns.add("HAMMER")

    return patterns


# ============================================================
# RANGE CALCULATION
# ============================================================

def average_range(
    candles: list[Candle]
) -> float:

    ranges = [
        candle.range
        for candle in candles
        if candle.range > 0
    ]

    if not ranges:
        return 0.0

    return sum(ranges) / len(ranges)


# ============================================================
# S/R ZONE
# ============================================================

@dataclass
class Zone:
    kind: str

    low: float

    high: float

    touch_count: int

    center: float

    def contains(
        self,
        price: float
    ) -> bool:

        return (
            self.low
            <= price
            <= self.high
        )


# ============================================================
# SUPPORT / RESISTANCE DETECTION
# ============================================================

def detect_zones(
    candles: list[Candle]
) -> list[Zone]:

    if len(candles) < (
        2 * CFG.pivot_strength + 1
    ):
        return []

    avg_range = average_range(candles)

    if avg_range <= 0:
        return []

    tolerance = (
        CFG.zone_tolerance_avg_range
        * avg_range
    )

    pivot_strength = CFG.pivot_strength

    support_points: list[float] = []

    resistance_points: list[float] = []

    for index in range(
        pivot_strength,
        len(candles) - pivot_strength,
    ):

        current = candles[index]

        surrounding = (
            candles[
                index - pivot_strength:
                index + pivot_strength + 1
            ]
        )

        neighbors = [
            candle
            for candle in surrounding
            if candle is not current
        ]

        # ----------------------------------------------------
        # RESISTANCE PIVOT
        # ----------------------------------------------------

        if all(
            current.high > candle.high
            for candle in neighbors
        ):
            resistance_points.append(
                current.high
            )

        # ----------------------------------------------------
        # SUPPORT PIVOT
        # ----------------------------------------------------

        if all(
            current.low < candle.low
            for candle in neighbors
        ):
            support_points.append(
                current.low
            )

    def cluster_points(
        points: list[float],
        kind: str,
    ) -> list[Zone]:

        if not points:
            return []

        points = sorted(points)

        groups: list[list[float]] = []

        for price in points:

            if not groups:
                groups.append([price])
                continue

            current_center = (
                sum(groups[-1])
                / len(groups[-1])
            )

            if (
                abs(price - current_center)
                <= tolerance
            ):
                groups[-1].append(price)

            else:
                groups.append([price])

        zones: list[Zone] = []

        for group in groups:

            touch_count = len(group)

            if not (
                CFG.min_zone_touches
                <= touch_count
                <= CFG.max_zone_touches
            ):
                continue

            center = (
                sum(group)
                / len(group)
            )

            zones.append(
                Zone(
                    kind=kind,
                    low=center - tolerance,
                    high=center + tolerance,
                    touch_count=touch_count,
                    center=center,
                )
            )

        return zones

    return (
        cluster_points(
            support_points,
            "support",
        )
        +
        cluster_points(
            resistance_points,
            "resistance",
        )
    )


# ============================================================
# TREND PRECONDITION
# ============================================================

def trend_gate(
    prior_candles: list[Candle],
    direction: str,
) -> bool:

    if len(prior_candles) < 3:
        return False

    # Most recent prior candle = C-1
    c1 = prior_candles[-1]

    # Previous = C-2
    c2 = prior_candles[-2]

    # Previous = C-3
    c3 = prior_candles[-3]

    # BUY:
    #
    # C-1.low < C-2.low < C-3.low
    #
    # Strict inequality is intentional.

    if direction == "BUY":

        return (
            c1.low
            < c2.low
            < c3.low
        )

    # SELL:
    #
    # C-1.high > C-2.high > C-3.high
    #
    # Strict inequality is intentional.

    if direction == "SELL":

        return (
            c1.high
            > c2.high
            > c3.high
        )

    return False


# ============================================================
# SIGNAL DETECTION
# ============================================================

def find_signal(
    closed_candles: list[Candle]
) -> Optional[dict]:

    required = (
        CFG.lookback_window + 4
    )

    if len(closed_candles) < required:
        return None

    # --------------------------------------------------------
    # C0 = most recently closed candle
    # --------------------------------------------------------

    c0 = closed_candles[-1]

    # --------------------------------------------------------
    # Previous candles used for S/R
    #
    # IMPORTANT:
    # C0 is deliberately excluded.
    # --------------------------------------------------------

    zone_candles = closed_candles[
        -(CFG.lookback_window + 1):-1
    ]

    # --------------------------------------------------------
    # RANGE FILTER
    # --------------------------------------------------------

    avg_range = average_range(
        zone_candles
    )

    if avg_range <= 0:
        return None

    if (
        c0.range
        < CFG.min_range_avg_multiplier
        * avg_range
    ):
        return None

    # --------------------------------------------------------
    # PATTERN DETECTION
    # --------------------------------------------------------

    patterns = candle_patterns(c0)

    if not patterns:
        return None

    # --------------------------------------------------------
    # BUILD S/R
    # --------------------------------------------------------

    zones = detect_zones(
        zone_candles
    )

    # --------------------------------------------------------
    # BUY SETUP
    # --------------------------------------------------------

    # HARD TREND GATE FIRST.

    if trend_gate(
        closed_candles[:-1],
        "BUY",
    ):

        if (
            "DRAGONFLY_DOJI" in patterns
            or
            "HAMMER" in patterns
        ):

            support_zones = [
                zone
                for zone in zones
                if (
                    zone.kind == "support"
                    and zone.contains(c0.low)
                )
            ]

            if support_zones:

                selected_zone = max(
                    support_zones,
                    key=lambda zone:
                    zone.touch_count,
                )

                matching_patterns = (
                    patterns
                    &
                    {
                        "DRAGONFLY_DOJI",
                        "HAMMER",
                    }
                )

                return {
                    "direction": "BUY",
                    "pattern": sorted(
                        matching_patterns
                    )[0],
                    "zone": selected_zone,
                    "candle": c0,
                }

    # --------------------------------------------------------
    # SELL SETUP
    # --------------------------------------------------------

    # HARD TREND GATE FIRST.

    if trend_gate(
        closed_candles[:-1],
        "SELL",
    ):

        if (
            "GRAVESTONE_DOJI" in patterns
            or
            "SHOOTING_STAR" in patterns
        ):

            resistance_zones = [
                zone
                for zone in zones
                if (
                    zone.kind == "resistance"
                    and zone.contains(c0.high)
                )
            ]

            if resistance_zones:

                selected_zone = max(
                    resistance_zones,
                    key=lambda zone:
                    zone.touch_count,
                )

                matching_patterns = (
                    patterns
                    &
                    {
                        "GRAVESTONE_DOJI",
                        "SHOOTING_STAR",
                    }
                )

                return {
                    "direction": "SELL",
                    "pattern": sorted(
                        matching_patterns
                    )[0],
                    "zone": selected_zone,
                    "candle": c0,
                }

    return None


# ============================================================
# TRADE MANAGER
# ============================================================

class TradeManager:

    def __init__(self):

        self.trade_count = 0

        self.consecutive_signals = 0

        # Each entry:
        #
        # (zone_kind, zone_low, zone_high)
        self.locked_zones = []

    def can_trade(self) -> bool:

        if (
            CFG.max_trades > 0
            and
            self.trade_count
            >= CFG.max_trades
        ):
            return False

        if (
            CFG.max_consecutive_trades > 0
            and
            self.consecutive_signals
            >= CFG.max_consecutive_trades
        ):
            return False

        return True

    def zone_is_locked(
        self,
        zone: Zone,
        current_price: float,
    ) -> bool:

        new_locked_zones = []

        locked = False

        for (
            zone_kind,
            zone_low,
            zone_high,
        ) in self.locked_zones:

            # Price is still inside the zone.
            if (
                zone_low
                <= current_price
                <= zone_high
            ):

                new_locked_zones.append(
                    (
                        zone_kind,
                        zone_low,
                        zone_high,
                    )
                )

                if (
                    zone_kind == zone.kind
                    and
                    zone_low
                    <= zone.center
                    <= zone_high
                ):
                    locked = True

        # A zone disappears from the lock once
        # price has left it.

        self.locked_zones = (
            new_locked_zones
        )

        return locked

    def lock_zone(
        self,
        zone: Zone,
    ) -> None:

        self.locked_zones.append(
            (
                zone.kind,
                zone.low,
                zone.high,
            )
        )


# ============================================================
# TRADE EXECUTION
# ============================================================

async def place_trade(
    api: PocketOptionAsync,
    signal_data: dict,
    manager: TradeManager,
):

    if not manager.can_trade():

        LOG.warning(
            "Trade limit reached. "
            "Signal ignored."
        )

        return None

    direction = signal_data[
        "direction"
    ]

    pattern = signal_data[
        "pattern"
    ]

    zone: Zone = signal_data[
        "zone"
    ]

    candle: Candle = signal_data[
        "candle"
    ]

    # --------------------------------------------------------
    # SAFETY CHECK
    # --------------------------------------------------------

    demo_account = api.is_demo()

    if (
        not CFG.live_trading
        and
        not demo_account
    ):

        raise RuntimeError(
            "SAFETY STOP: "
            "LIVE_TRADING=false but "
            "the connected account is LIVE."
        )

    LOG.warning(
        "================================================"
    )

    LOG.warning(
        "VALID SIGNAL"
    )

    LOG.warning(
        "Direction : %s",
        direction,
    )

    LOG.warning(
        "Pattern   : %s",
        pattern,
    )

    LOG.warning(
        "C0 Open   : %.6f",
        candle.open,
    )

    LOG.warning(
        "C0 High   : %.6f",
        candle.high,
    )

    LOG.warning(
        "C0 Low    : %.6f",
        candle.low,
    )

    LOG.warning(
        "C0 Close  : %.6f",
        candle.close,
    )

    LOG.warning(
        "Zone      : %s",
        zone.kind,
    )

    LOG.warning(
        "Zone      : %.6f - %.6f",
        zone.low,
        zone.high,
    )

    LOG.warning(
        "Touches   : %d",
        zone.touch_count,
    )

    LOG.warning(
        "Amount    : $%.2f",
        CFG.trade_amount,
    )

    LOG.warning(
        "Expiry    : %d seconds",
        CFG.expiry_seconds,
    )

    LOG.warning(
        "================================================"
    )

    # --------------------------------------------------------
    # EXECUTE
    # --------------------------------------------------------

    if direction == "BUY":

        trade_id, deal = await api.buy(
            asset=CFG.asset,
            amount=CFG.trade_amount,
            time=CFG.expiry_seconds,
            check_win=False,
        )

    else:

        trade_id, deal = await api.sell(
            asset=CFG.asset,
            amount=CFG.trade_amount,
            time=CFG.expiry_seconds,
            check_win=False,
        )

    manager.trade_count += 1

    manager.consecutive_signals += 1

    manager.lock_zone(zone)

    LOG.warning(
        "TRADE SUBMITTED"
    )

    LOG.warning(
        "Trade ID: %s",
        trade_id,
    )

    LOG.warning(
        "Deal: %s",
        deal,
    )

    # Don't block the market stream while waiting
    # for the result.

    asyncio.create_task(
        report_trade_result(
            api,
            trade_id,
        )
    )

    return trade_id


# ============================================================
# TRADE RESULT
# ============================================================

async def report_trade_result(
    api: PocketOptionAsync,
    trade_id: str,
):

    try:

        result = await api.check_win(
            trade_id
        )

        LOG.warning(
            "TRADE RESULT | %s | %s",
            trade_id,
            result,
        )

    except Exception:

        LOG.exception(
            "Unable to retrieve result "
            "for trade %s",
            trade_id,
        )


# ============================================================
# MAIN BOT
# ============================================================

async def run_bot():

    ssid = os.getenv(
        "POCKET_OPTION_SSID"
    )

    if not ssid:

        raise RuntimeError(
            "POCKET_OPTION_SSID "
            "environment variable is required."
        )

    LOG.info(
        "================================================"
    )

    LOG.info(
        "POCKET OPTION 15s S/R REJECTION BOT"
    )

    LOG.info(
        "================================================"
    )

    LOG.info(
        "Asset          : %s",
        CFG.asset,
    )

    LOG.info(
        "Timeframe      : %ss",
        CFG.timeframe_seconds,
    )

    LOG.info(
        "Trade amount   : $%.2f",
        CFG.trade_amount,
    )

    LOG.info(
        "Expiry         : %ss",
        CFG.expiry_seconds,
    )

    LOG.info(
        "S/R lookback   : %d candles",
        CFG.lookback_window,
    )

    LOG.info(
        "LIVE_TRADING   : %s",
        CFG.live_trading,
    )

    LOG.info(
        "================================================"
    )

    # --------------------------------------------------------
    # CREATE CLIENT
    # --------------------------------------------------------

    api = PocketOptionAsync(
        ssid,
        config={
            "terminal_logging": False,
            "log_level": "WARNING",
        },
    )

    try:

        # Give the websocket time to initialize.

        await asyncio.sleep(5)

        # ----------------------------------------------------
        # ACCOUNT
        # ----------------------------------------------------

        demo = api.is_demo()

        balance = await api.balance()

        LOG.info(
            "Account type : %s",
            "DEMO" if demo else "LIVE",
        )

        LOG.info(
            "Balance      : $%.2f",
            balance,
        )

        # ----------------------------------------------------
        # SAFETY
        # ----------------------------------------------------

        if (
            not CFG.live_trading
            and
            not demo
        ):

            raise RuntimeError(
                "Connected account is LIVE. "
                "LIVE_TRADING=false prevents "
                "live trading."
            )

        # ----------------------------------------------------
        # ASSET
        # ----------------------------------------------------

        assets = await api.active_assets()

        matching_assets = [
            asset
            for asset in assets
            if asset.get("symbol")
            == CFG.asset
        ]

        if not matching_assets:

            raise RuntimeError(
                f"{CFG.asset} was not found "
                "in active_assets()."
            )

        LOG.info(
            "Asset found: %s",
            matching_assets[0],
        )

        # ----------------------------------------------------
        # INITIAL HISTORY
        # ----------------------------------------------------

        history_count = max(
            CFG.lookback_window + 20,
            80,
        )

        LOG.info(
            "Downloading %d historical "
            "15-second candles...",
            history_count,
        )

        raw_history = await api.get_candles(
            CFG.asset,
            CFG.timeframe_seconds,
            history_count,
        )

        history = sorted(
            [
                parse_candle(candle)
                for candle in raw_history
            ],
            key=lambda candle:
            candle.time,
        )

        LOG.info(
            "Historical candles loaded: %d",
            len(history),
        )

        if len(history) < (
            CFG.lookback_window + 4
        ):

            raise RuntimeError(
                "Not enough historical candles. "
                f"Received {len(history)}, "
                f"need at least "
                f"{CFG.lookback_window + 4}."
            )

        # ----------------------------------------------------
        # SUBSCRIBE
        # ----------------------------------------------------

        LOG.info(
            "Subscribing to %s "
            "at %d-second timeframe...",
            CFG.asset,
            CFG.timeframe_seconds,
        )

        stream = await (
            api.subscribe_symbol_timed(
                CFG.asset,
                timedelta(
                    seconds=CFG.timeframe_seconds
                ),
            )
        )

        LOG.info(
            "15-second candle stream active."
        )

        # ----------------------------------------------------
        # LOOP
        # ----------------------------------------------------

        manager = TradeManager()

        last_candle_timestamp = None

        async for raw_candle in stream:

            if STOP_EVENT.is_set():
                break

            try:

                candle = parse_candle(
                    raw_candle
                )

            except Exception:

                LOG.exception(
                    "Unable to parse "
                    "incoming candle: %r",
                    raw_candle,
                )

                continue

            # ------------------------------------------------
            # DUPLICATE PROTECTION
            # ------------------------------------------------

            if (
                last_candle_timestamp
                == candle.time
            ):
                continue

            last_candle_timestamp = (
                candle.time
            )

            # ------------------------------------------------
            # UPDATE HISTORY
            # ------------------------------------------------

            if history:

                # Replace overlapping candle.

                if (
                    abs(
                        history[-1].time
                        - candle.time
                    )
                    < CFG.timeframe_seconds
                ):

                    history[-1] = candle

                else:

                    history.append(candle)

            else:

                history.append(candle)

            # Keep memory bounded.

            history = history[
                -(
                    CFG.lookback_window
                    + 80
                ):
            ]

            # ------------------------------------------------
            # DISPLAY CLOSED CANDLE
            # ------------------------------------------------

            timestamp = datetime.fromtimestamp(
                candle.time,
                tz=timezone.utc,
            ).strftime("%H:%M:%S")

            signal_data = find_signal(
                history
            )

            if signal_data:

                signal_text = (
                    signal_data["direction"]
                    + "/"
                    + signal_data["pattern"]
                )

            else:

                signal_text = "NONE"

            LOG.info(
                "CLOSED %s | "
                "O %.6f | "
                "H %.6f | "
                "L %.6f | "
                "C %.6f | "
                "SIGNAL=%s",
                timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                signal_text,
            )

            # ------------------------------------------------
            # SIGNAL
            # ------------------------------------------------

            if not signal_data:
                continue

            zone = signal_data[
                "zone"
            ]

            # ------------------------------------------------
            # ONE TRADE PER ZONE TOUCH
            # ------------------------------------------------

            if manager.zone_is_locked(
                zone,
                candle.close,
            ):

                LOG.info(
                    "Signal blocked: "
                    "zone is still locked."
                )

                continue

            # ------------------------------------------------
            # TRADE
            # ------------------------------------------------

            await place_trade(
                api,
                signal_data,
                manager,
            )

    finally:

        LOG.info(
            "Shutting down Pocket Option client..."
        )

        try:

            await api.shutdown()

        except Exception:

            LOG.exception(
                "Error during shutdown."
            )


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def install_signal_handlers():

    def stop_handler(
        *_args
    ):

        LOG.info(
            "Shutdown signal received."
        )

        STOP_EVENT.set()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):

        try:

            signal.signal(
                sig,
                stop_handler,
            )

        except (
            ValueError,
            OSError,
        ):

            pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=os.getenv(
            "LOG_LEVEL",
            "INFO",
        ),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    install_signal_handlers()

    try:

        asyncio.run(
            run_bot()
        )

    except KeyboardInterrupt:

        pass

    except Exception:

        LOG.exception(
            "BOT FAILED"
        )

        raise
