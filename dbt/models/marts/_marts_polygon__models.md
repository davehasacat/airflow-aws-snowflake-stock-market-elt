# marts_polygon__stocks_options_joined

**Model Type:** Mart  
**Tags:** `["marts", "polygon", "joined", "options", "stocks"]`  
**Materialization:** Incremental (expected)  

---

## 📘 Model Description

The `marts_polygon__stocks_options_joined` model is the **final analytical mart** that joins enriched options data with their corresponding underlying stock metrics from Polygon.io.  
It provides a unified dataset for **options analytics**, **volatility modeling**, and **cross-asset insights**, combining option-level OHLCV measures with stock-level trend indicators (moving averages).

This table serves as the **primary query surface** for dashboards and downstream analysis.

---

## 🧱 Columns

| Column | Description | Example / Notes |
|--------|--------------|-----------------|
| **option_bar_id** | Unique identifier for a single day's option bar. | `O:GOOGL251017P00247500_2025-10-21` |
| **option_symbol** | OCC-formatted option contract symbol. | `O:GOOGL251017P00247500` |
| **underlying_ticker** | Underlying stock ticker symbol. | `GOOGL` |
| **polygon_trade_date** | Trading date (UTC) for the aggregated daily bar. | `2025-10-21` |
| **expiration_date** | Option contract expiration date. | `2025-10-17` |
| **strike_price** | Strike price of the option contract. | `2475.00` |
| **option_type** | Indicates whether the contract is a call or put. | `"call"`, `"put"` |
| **option_open** | Opening price for the option on the trade date. | `4.32` |
| **option_high** | Highest traded price for the option during the day. | `5.10` |
| **option_low** | Lowest traded price for the option during the day. | `3.95` |
| **option_close** | Closing price for the option on the trade date. | `4.85` |
| **option_vwap** | Volume-weighted average price for the day. | `4.70` |
| **option_volume** | Total option contracts traded for the day. | `820` |
| **option_transactions** | Number of trades contributing to the option bar. | `68` |
| **days_to_expiration** | Days between trade date and expiration date. | `-4` |
| **underlying_close_price** | Closing price of the underlying stock. | `2500.00` |
| **underlying_moving_avg_20d** | 20-day simple moving average of the underlying close price. | `2487.2300000` |
| **underlying_moving_avg_50d** | 50-day simple moving average of the underlying close price. | `2472.9900000` |
| **underlying_moving_avg_120d** | 120-day simple moving average of the underlying close price. | `2438.0100000` |
| **moneyness** | Categorical indicator of relative option value vs. underlying. | `"in_the_money"`, `"at_the_money"`, `"out_of_the_money"` |

---

## 🧪 Tests

### Column-level tests

| Column | Test | Description |
|--------|------|--------------|
| `option_bar_id` | `unique`, `not_null` | Ensures each record is unique and populated. |
| `option_symbol` | `not_null`, `expect_column_values_to_match_regex` | Validates presence and OCC pattern. |
| `underlying_ticker` | `not_null` | Must match a valid ticker from the underlying stock dataset. |
| `polygon_trade_date`, `expiration_date` | `not_null` | Required temporal keys for modeling and aging. |
| `strike_price`, price and volume fields | `expect_column_values_to_be_between (min_value: 0)` | Ensures valid numeric ranges (no negatives). |
| `option_type` | `accepted_values: ["call", "put"]` | Standardized classification. |
| `moneyness` | `accepted_values: ["in_the_money", "at_the_money", "out_of_the_money"]` | Ensures consistent categorization. |

---

## 🔗 Upstream Dependencies

| Source | Dependency Type |
|---------|-----------------|
| `int_polygon__options` | Provides normalized and enriched option bars. |
| `int_polygon__stocks` | Provides derived stock features and technical indicators. |

---

## 🧭 Downstream Use Cases

- **Options analytics dashboards** (Plotly Dash / BI tools)
- **Historical trend studies** on option liquidity and moneyness
- **Cross-asset correlation models** for volatility and momentum research

---

## ⚙️ Maintenance Notes

- The mart should refresh after upstream `int_polygon__stocks` and `int_polygon__options` finish successfully.
- Range/regex validations are handled upstream; this model enforces only **referential and categorical integrity**.
- Future enhancements may include implied volatility and Greeks once real-time feeds are integrated.

---

**Owner:** Data Engineering  
**PII:** False  
**Last Updated:** 2025-10-21
