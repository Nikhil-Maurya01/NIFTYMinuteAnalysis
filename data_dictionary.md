# Data Dictionary – NIFTY 50 Minute-Level Data

| Column Name | Description |
|-------------|-------------|
| `date`      | Timestamp in `dd-MM-yyyy HH:mm` format |
| `open`      | Opening price for the minute |
| `high`      | Highest price during the minute |
| `low`       | Lowest price during the minute |
| `close`     | Closing price for the minute |
| `volume`    | Trading volume during the minute |
| `timestamp` | Parsed timestamp column |
| `hour`      | Hour extracted from timestamp |
| `date_only` | Date part of the timestamp |
| `volatility`| Difference between high and low prices |

Derived columns are used for analysis and visualization.
