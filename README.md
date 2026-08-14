# Pocket Option 15-Second S/R Rejection Bot

Pocket Option automated trading test bot using:

- BinaryOptionsToolsV2
- PocketOptionAsync
- AUDUSD OTC
- 15-second candles
- 15-second expiry
- Support/resistance zones
- Fractal pivot detection
- Gravestone Doji
- Shooting Star
- Dragonfly Doji
- Hammer
- Strict trend precondition

The project is designed to run locally on Windows and on Railway.

---

# IMPORTANT SAFETY NOTICE

The bot is configured for DEMO trading by default.

The default setting is:

LIVE_TRADING=false

When LIVE_TRADING=false, the bot will refuse to trade if the connected Pocket Option account is a LIVE account.

Do not change this to true until the strategy has been tested and verified.

---

# STRATEGY

## Asset

AUDUSD OTC

## Timeframe

15 seconds

## Expiry

15 seconds

## Trade Amount

$2,500

Each valid signal attempts one $2,500 trade with a 15-second expiry.

---

# CANDLE INDEXING

The strategy uses:

C-3
C-2
C-1
C0
C+1

Where:

C0 = most recently CLOSED candle.

C+1 = candle that begins after C0 closes.

The bot evaluates only closed candles.

---

# BUY STRATEGY

A BUY requires all gates to pass.

## Gate 1 — Trend

Strictly descending lows:

C-1.low < C-2.low < C-3.low

If this fails, the BUY setup is rejected immediately.

---

## Gate 2 — Support

C0.low must fall inside an active support zone.

The wick is used for the zone test.

---

## Gate 3 — Pattern

C0 must be:

Dragonfly Doji

OR

Hammer

---

## Gate 4 — Range

C0 must have a range of at least:

0.5 x average range

The average is calculated from the S/R lookback candles.

---

## BUY ENTRY

After C0 closes:

C0 closes
      ↓
C+1 begins
      ↓
BUY $2,500
      ↓
15-second expiry

---

# SELL STRATEGY

A SELL requires all gates to pass.

## Gate 1 — Trend

Strictly ascending highs:

C-1.high > C-2.high > C-3.high

If this fails, the SELL setup is rejected immediately.

---

## Gate 2 — Resistance

C0.high must fall inside an active resistance zone.

The wick is used for the zone test.

---

## Gate 3 — Pattern

C0 must be:

Gravestone Doji

OR

Shooting Star

---

## Gate 4 — Range

C0 must have a range of at least:

0.5 x average range

---

## SELL ENTRY

After C0 closes:

C0 closes
      ↓
C+1 begins
      ↓
SELL $2,500
      ↓
15-second expiry

---

# SUPPORT / RESISTANCE

The default lookback is:

50 closed candles.

Pivot strength:

3 candles.

A resistance pivot is a candle whose high is strictly higher than the highs of the surrounding pivot-strength candles.

A support pivot is a candle whose low is strictly lower than the lows of the surrounding pivot-strength candles.

---

# ZONE CLUSTERING

Nearby pivots are grouped into zones.

Default tolerance:

0.75 x average candle range.

A zone must have:

Minimum touches = 2

Maximum touches = 4

Zones outside those limits are not tradeable.

---

# IMPORTANT — C0 IS NOT USED TO BUILD THE ZONE

The signal candle C0 is deliberately excluded from S/R zone construction.

The sequence is:

Previous candles
      ↓
Build S/R zones
      ↓
C0 closes
      ↓
Check C0 against existing zones
      ↓
Generate signal

This prevents C0 from redefining the zone that it is supposed to reject.

---

# CANDLE PATTERNS

## Gravestone Doji

Body:

<= 10% of candle range

Upper wick:

>= 60% of candle range

Lower wick:

<= 10% of candle range

---

## Shooting Star

Body:

<= 30% of candle range

Upper wick:

>= 2 x body

Body:

located in lower third of candle range

---

## Dragonfly Doji

Body:

<= 10% of candle range

Lower wick:

>= 60% of candle range

Upper wick:

<= 10% of candle range

---

## Hammer

Body:

<= 30% of candle range

Lower wick:

>= 2 x body

Body:

located in upper third of candle range

---

# ONE TRADE PER ZONE TOUCH

After a trade is placed on a zone, that zone is locked.

The bot will not fire another trade from the same zone while price remains inside that zone.

The zone becomes eligible again only after price exits the zone and subsequently returns.

---

# FILE STRUCTURE

bot.py
requirements.txt
railway.toml
Dockerfile
.env.example
.gitignore
README.md

---

# PYTHON VERSION

The Dockerfile explicitly uses:

Python 3.13

Base image:

python:3.13-slim

This prevents the Railway runtime from accidentally using another Python version.

---

# LOCAL WINDOWS INSTALLATION

Open Command Prompt inside the project folder.

Create the virtual environment:

python -m venv venv

Activate it:

venv\Scripts\activate

Verify:

python --version

It should show Python 3.13.x.

Install dependencies:

python -m pip install --upgrade pip

pip install -r requirements.txt

---

# LOCAL ENVIRONMENT VARIABLE

Create a file:

.env

Do NOT upload it to GitHub.

Put your Pocket Option SSID inside:

POCKET_OPTION_SSID=YOUR_SSID

For the first test:

LIVE_TRADING=false

---

# LOCAL RUN

After activating the virtual environment:

python bot.py

---

# GITHUB

Create a repository and upload:

bot.py
requirements.txt
railway.toml
Dockerfile
.env.example
.gitignore
README.md

DO NOT upload:

.env

Your real Pocket Option SSID must never be committed to GitHub.

---

# RAILWAY

Connect the GitHub repository to Railway.

Railway will detect the Dockerfile.

The Dockerfile uses Python 3.13.

The container starts:

python bot.py

---

# RAILWAY VARIABLES

In Railway, add:

POCKET_OPTION_SSID

Set its value to your Pocket Option SSID.

Also set:

LIVE_TRADING=false

The other strategy parameters have defaults in bot.py.

---

# OPTIONAL RAILWAY VARIABLES

ASSET=AUDUSD_otc

TIMEFRAME_SECONDS=15

TRADE_AMOUNT=2500

EXPIRY_SECONDS=15

LOOKBACK_WINDOW=50

PIVOT_STRENGTH=3

ZONE_TOLERANCE_AVG_RANGE=0.75

MIN_ZONE_TOUCHES=2

MAX_ZONE_TOUCHES=4

DOJI_BODY_MAX_PCT=0.10

DOJI_WICK_MIN_PCT=0.60

STAR_HAMMER_BODY_MAX_PCT=0.30

STAR_HAMMER_WICK_MULTIPLIER=2.0

MIN_RANGE_AVG_MULTIPLIER=0.5

TREND_CANDLES=3

LIVE_TRADING=false

MAX_TRADES=0

MAX_CONSECUTIVE_TRADES=0

LOG_LEVEL=INFO

---

# INITIAL TEST

Keep:

LIVE_TRADING=false

The bot should:

1. Connect to Pocket Option.
2. Confirm the account type.
3. Display the balance.
4. Verify AUDUSD OTC.
5. Download historical candles.
6. Subscribe to 15-second candles.
7. Process each CLOSED candle.
8. Build S/R zones.
9. Apply the trend gate.
10. Detect rejection patterns.
11. Display valid signals.
12. Refuse live execution while LIVE_TRADING=false.

---

# LOG EXAMPLE

A normal candle:

CLOSED 12:31:15 |
O 0.654210 |
H 0.654390 |
L 0.654180 |
C 0.654220 |
SIGNAL=NONE

A valid signal:

VALID SIGNAL

Direction : SELL
Pattern   : SHOOTING_STAR
C0 Open   : 0.654210
C0 High   : 0.654390
C0 Low    : 0.654180
C0 Close  : 0.654220
Zone      : resistance
Touches   : 3
Amount    : $2500.00
Expiry    : 15 seconds

---

# CURRENT VERSION

This version intentionally contains only the:

S/R
+
Trend
+
Rejection candle

strategy.

It does NOT yet include:

RSI
Bollinger Bands
EMA
MACD
SMC
ICT
Fibonacci
Consensus scoring

Those should be added only after this core strategy has been tested independently.
