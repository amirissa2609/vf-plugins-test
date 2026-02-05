# Moving Average Downsampler Plugin

This README explains how to use the moving average functionality in the downsampler plugin for both HTTP and scheduled triggers.

## Overview

The moving average feature calculates rolling averages over a specified window of data points, partitioned by tags (e.g., location, sensor_id). This is useful for smoothing time series data and identifying trends.

## Key Parameters

- **`calculations`**: Use `"moving_avg"` as the aggregation type
- **`moving_avg_window`**: Number of data points to include in the rolling window (default: 7)
- **`interval`**: Time bucket size for grouping data (e.g., "1min", "5min", "1h")
- **`window`**: Time range to process for scheduled triggers (e.g., "1h", "1d")
- **`source_measurement`**: Input measurement to process
- **`target_measurement`**: Output measurement for results

### Parameter Units Explained

**Why different units?**
- **`moving_avg_window`**: Uses **data point count** (e.g., `3` = 3-point moving average) following statistical conventions
- **`interval`/`window`**: Use **time duration** (e.g., `"1min"`, `"1h"`) for temporal operations

This allows flexible time resolutions with consistent statistical windows, regardless of data gaps.

## HTTP Trigger

Use HTTP triggers for on-demand processing of historical data with flexible time ranges.

### 1. Create HTTP Trigger

```bash
curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
  --header "Authorization: Bearer $ADMIN_TOKEN" \
  --header "Content-Type: application/json" \
  -d '{
    "db": "sensors",
    "disabled": false,
    "plugin_filename": "gh:downsampler/downsampler.py",
    "trigger_name": "moving_avg_test_http_trigger",
    "trigger_specification": "request:moving-avg-test",
    "trigger_settings": {
      "run_async": false,
      "error_behavior": "log"
    }
  }'
```

### 2. Execute HTTP Request

```bash
curl -X POST http://localhost:8181/api/v3/engine/moving-avg-test \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "source_measurement": "sensors",
    "target_measurement": "sensors_ma_test",
    "interval": "1min",
    "batch_size": "1h",
    "calculations": [["temperature", "moving_avg"]],
    "moving_avg_window": 3,
    "backfill_start": "2024-01-01T00:00:00+00:00",
    "backfill_end": "2024-01-01T00:15:00+00:00"
  }'
```

**HTTP Parameters:**
- **`batch_size`**: Processing batch size (e.g., "1h", "1d")
- **`backfill_start/end`**: Time range to process (ISO 8601 format with timezone)
- **`calculations`**: List format `[["field_name", "moving_avg"]]`

## Scheduled Trigger

Use scheduled triggers for continuous, real-time processing of recent data.

### Create Scheduled Trigger

```bash
curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
  --header "Authorization: Bearer $ADMIN_TOKEN" \
  --header "Content-Type: application/json" \
  -d '{
    "db": "sensors",
    "disabled": false,
    "plugin_filename": "gh:downsampler/downsampler.py",
    "trigger_name": "moving_avg_test_schedule_trigger",
    "trigger_specification": "every:10s",
    "trigger_arguments": {
      "source_measurement": "sensors",
      "target_measurement": "sensors_ma_scheduled",
      "interval": "1min",
      "window": "1h",
      "moving_avg_window": "3",
      "calculations": "temperature:moving_avg"
    },
    "trigger_settings": {
      "run_async": false,
      "error_behavior": "log"
    }
  }'
```

**Scheduled Parameters:**
- **`trigger_specification`**: Schedule (e.g., "every:10s", "every:5min", "every:1h")
- **`window`**: Time window to process each run (e.g., "1h" = last hour)
- **`calculations`**: String format `"field_name:moving_avg"`

## Viewing Results

Check the processed moving average data:

```bash
# For HTTP trigger results
curl --get http://localhost:8181/api/v3/query_sql \
  --header "Authorization: Token $ADMIN_TOKEN" \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT * FROM sensors_ma_test ORDER BY time, location"

# For scheduled trigger results  
curl --get http://localhost:8181/api/v3/query_sql \
  --header "Authorization: Token $ADMIN_TOKEN" \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT * FROM sensors_ma_scheduled ORDER BY time, location"
```

## Result Format

The output includes:
- **`_time`**: Time bucket timestamp
- **`{field}_moving_avg`**: Calculated moving average value
- **`record_count`**: Number of records processed
- **`time_from/time_to`**: Time range metadata
- **Tag columns**: All tags from source data (e.g., `location`, `sensor_id`)

## Example Output

```json
[
  {
    "location": "kitchen",
    "sensor_id": "temp_01",
    "temperature_moving_avg": 22.0,
    "time": "2024-01-01T00:02:00",
    "record_count": 1,
    "time_from": 1704067320000000000,
    "time_to": 1704067320000000000
  }
]
```

## Moving Average Calculation

For a 3-point moving average (`moving_avg_window: 3`):
- **Point 1**: `avg(18.0) = 18.0` (only 1 point available)
- **Point 2**: `avg(18.0, 19.0) = 18.5` (only 2 points available)
- **Point 3**: `avg(18.0, 19.0, 20.0) = 19.0` (full 3-point window)
- **Point 4**: `avg(19.0, 20.0, 21.0) = 20.0` (sliding window)

## Important Notes

- **Window Function**: Moving averages use SQL window functions with `PARTITION BY` tags
- **Partitioning**: Calculations are performed separately for each unique combination of tags
- **Window Behavior**: At the start of time series, fewer points may be available than the specified window size
- **Real-time vs Historical**: Scheduled triggers process recent data; HTTP triggers can process any time range
- **Performance**: Use appropriate `batch_size` for HTTP requests to manage memory usage

## Troubleshooting

- **No data processed**: Ensure timestamps fall within the processing window
- **Incorrect results**: Verify `moving_avg_window` size and tag partitioning
- **Time zone issues**: Use explicit timezone offsets in ISO 8601 timestamps (e.g., `+00:00`)
- **Scheduled trigger not running**: Check that data timestamps are recent (within the window timeframe)

## Differences Between HTTP and Scheduled

| Aspect | HTTP Trigger | Scheduled Trigger |
|--------|-------------|------------------|
| **Data Processing** | Historical data with custom time ranges | Recent data within sliding time windows |
| **Calculations Format** | `[["field", "moving_avg"]]` | `"field:moving_avg"` |
| **Time Control** | `backfill_start/end` parameters | `window` parameter (e.g., "1h") |
| **Execution** | On-demand via API calls | Automatic based on schedule |
| **Use Case** | Bulk historical analysis | Real-time continuous processing |

