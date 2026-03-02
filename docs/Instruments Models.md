# Instrument Models

The instrument thet can be trades in trhe system are the following:

## General

One representation across all instruments is the tick data:

### Tick Data

#### FOREX


Each record has the following structure:


| Field             | Type                  | Description                                          |
|-------------------|-----------------------|------------------------------------------------------|
| `timestamp_recv`  | `datetime[ns, UTC]`   | When your system received the tick                   |
| `timestamp_exch`  | `datetime[ns, UTC]`   | Exchange/provider timestamp (if available)           |
| `symbol`          | `string`              | e.g. `"EUR/USD"`                                     |
| `bid`             | `decimal(10,5)`       | Best bid price                                       |
| `ask`             | `decimal(10,5)`       | Best ask price                                       |
| `bid_size`        | `decimal(18,2)`       | Bid size in base currency lots (often 1M units)      |
| `ask_size`        | `decimal(18,2)`       | Ask size in base currency lots                       |
| `provider`        | `string`              | e.g. `"FXCM"`, `"Dukascopy"`, `"EBS"`               |
| `mid_price`       | computed              | `(bid + ask) / 2`                                    |
| `spread`          | computed              | `ask - bid` in price units                           |
| `spread_pips`     | computed              | `spread × pip_multiplier` (e.g. `×10000` for most pairs) |

For each currency pair it is necessary to keep track of:

| Field                | Type      | Example     | Description                                      |
|----------------------|-----------|-------------|--------------------------------------------------|
| `symbol`             | string    | `"EUR/USD"` | Instrument identifier                            |
| `base_currency_code` | string    | `"EUR"`     | Base currency of the pair                        |
| `quote_currency_code`| string    | `"USD"`     | Quote (counter) currency of the pair             |
| `pip_size`           | decimal   | `0.0001`    | Size of one pip (e.g. `0.01` for JPY pairs)      |
| `display_decimals`   | integer   | `5`         | Number of decimal places to display the price    |
| `standard_lot_size`  | integer   | `100000`    | Standard lot size in base currency units         |
| `min_trade_size`     | integer   | `1000`      | Minimum tradeable size in base currency units    |
| `spot_settlement_days`| integer  | `2`         | Number of business days to spot settlement (T+N) |
| `is_ndf`             | boolean   | `false`     | Whether the instrument is a Non-Deliverable Forward |
| `is_active`          | boolean   | `true`      | Whether the instrument is currently tradeable    |
