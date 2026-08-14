# Pocket Option 15s Reversal Bot

Automated Pocket Option trading bot using BinaryOptionsTools-v2.

The bot is designed around a 15-second reversal strategy using:

- Support/resistance zones
- Fractal/pivot detection
- Dragonfly Doji
- Hammer
- Gravestone Doji
- Shooting Star
- Strict preceding trend gate
- 50-candle lookback
- One-candle / 15-second expiry

## Installation

The bot installs BinaryOptionsTools-v2 directly from the official ChipaDevOrg GitLab repository:

https://gitlab.chipatrade.com/chipadevorg/BinaryOptionsTools-v2

The dependency is defined in `requirements.txt`.

## Configuration

Copy:

`.env.example`

to:

`.env`

and add the Pocket Option SSID.

Example:

POCKET_OPTION_SSID=YOUR_SSID

The default configuration is:

ASSET=AUDUSD_otc
TIMEFRAME=15
TRADE_AMOUNT=2500
EXPIRY_SECONDS=15
LOOKBACK_WINDOW=50

## Strategy

### BUY

A BUY requires:

1. Three preceding candles have strictly descending lows.

C-1.low < C-2.low < C-3.low

2. C0's low enters an active support zone.

3. C0 is either:

- Dragonfly Doji
- Hammer

4. The trade is opened at the beginning of C+1.

5. Expiry is one 15-second candle.

### SELL

A SELL requires:

1. Three preceding candles have strictly ascending highs.

C-1.high > C-2.high > C-3.high

2. C0's high enters an active resistance zone.

3. C0 is either:

- Gravestone Doji
- Shooting Star

4. The trade is opened at the beginning of C+1.

5. Expiry is one 15-second candle.

## Support / Resistance

The bot uses approximately 50 closed candles.

At 15 seconds this represents approximately 12.5 minutes of market data.

Fractal pivots are identified using three candles on either side.

Pivots are clustered using:

0.75 × average candle range

A zone must have at least two touches.

Zones with more than four touches are ignored.

## Candle Filters

### Gravestone Doji

Body <= 10% of range

Upper wick >= 60% of range

Lower wick <= 10% of range

### Dragonfly Doji

Body <= 10% of range

Lower wick >= 60% of range

Upper wick <= 10% of range

### Shooting Star

Body <= 30% of range

Upper wick >= 2 × body

Body located in lower third

### Hammer

Body <= 30% of range

Lower wick >= 2 × body

Body located in upper third

## Minimum Range Filter

The signal candle must have a range of at least:

0.5 × average candle range

This helps reject extremely small candles caused by stagnant price movement or tick noise.

## Duplicate Signal Protection

A zone is locked after a trade signal fires.

This prevents multiple trades from being opened repeatedly from the same zone touch.

## Railway Deployment

Create a Railway service connected to this GitHub repository.

Railway should run:

python bot.py

Add the following environment variable in Railway:

POCKET_OPTION_SSID

Do NOT commit the real SSID to GitHub.

Other variables can be configured through Railway environment variables.

## Important

This bot should initially be tested on a DEMO Pocket Option account.

The configured trade amount is $2,500.

Do not use a real-money account until the strategy and execution layer have been thoroughly tested.
