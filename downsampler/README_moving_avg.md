# Moving Average Downsampler Plugin

This README explains how to use the moving average functionality in the downsampler plugin for both HTTP and scheduled triggers.

## Overview

The moving average feature calculates rolling averages over a specified window of data points, partitioned by tags (e.g., location, sensor_id). This is useful for smoothing time series data and identifying trends.

## Key Parameters

- **`calculations`**: Use `"moving_avg"` as the aggregation type
- **`moving_avg_window`**: Number of data points to include in the rolling window (default: 7)
- **`partition_by_tags`**: **(Required for moving_avg)** Tags to use in the PARTITION BY clause for window calculations
- **`interval`**: Time bucket size for grouping data (e.g., "1sec", "1min", "5min", "1h")
- **`window`**: Time range to process for scheduled triggers (e.g., "1h", "1d")
- **`source_measurement`**: Input measurement to process
- **`target_measurement`**: Output measurement for results

### partition_by_tags Parameter

The `partition_by_tags` parameter controls which tags are used in the SQL `PARTITION BY` clause for moving average calculations. This is **required** when using `moving_avg` and is critical for query performance.

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
    "partition_by_tags": ["location"],
    "backfill_start": "2024-01-01T00:00:00+00:00",
    "backfill_end": "2024-01-01T00:15:00+00:00"
  }'
```

**HTTP Parameters:**
- **`batch_size`**: Processing batch size (e.g., "1h", "1d")
- **`backfill_start/end`**: Time range to process (ISO 8601 format with timezone)
- **`calculations`**: List format `[["field_name", "moving_avg"]]`
- **`partition_by_tags`**: List of tag names for PARTITION BY (e.g., `["location"]`) or `"none"`

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
      "partition_by_tags": "location",
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
- **`partition_by_tags`**: Dot-separated tag names for PARTITION BY (e.g., `"location"`) or `"none"`

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

## SUM Aggregation Example

The downsampler plugin supports various aggregation functions including `sum`. Here's how to use it:

### HTTP Trigger for SUM

```bash
# 1. Create HTTP Trigger
curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
  --header "Authorization: Bearer $ADMIN_TOKEN" \
  --header "Content-Type: application/json" \
  -d '{
    "db": "power",
    "disabled": false,
    "plugin_filename": "gh:downsampler/downsampler.py",
    "trigger_name": "power_sum_http_trigger",
    "trigger_specification": "request:power-sum",
    "trigger_settings": {
      "run_async": false,
      "error_behavior": "log"
    }
  }'

# 2. Execute HTTP Request
curl -X POST http://localhost:8181/api/v3/engine/power-sum \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "source_measurement": "power_datacenter1",
    "target_measurement": "power_datacenter1_sum",
    "interval": "1min",
    "batch_size": "1h",
    "calculations": [["metric_value", "sum"]],
    "partition_by_tags": ["primary_lineup"],
    "backfill_start": "2024-01-01T00:00:00+00:00",
    "backfill_end": "2024-01-02T00:00:00+00:00"
  }'
```

### Scheduled Trigger for SUM

```bash
curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
  --header "Authorization: Bearer $ADMIN_TOKEN" \
  --header "Content-Type: application/json" \
  -d '{
    "db": "power",
    "disabled": false,
    "plugin_filename": "gh:downsampler/downsampler.py",
    "trigger_name": "power_sum_scheduled_trigger",
    "trigger_specification": "every:1min",
    "trigger_arguments": {
      "source_measurement": "power_datacenter1",
      "target_measurement": "power_datacenter1_sum",
      "interval": "1min",
      "window": "1h",
      "partition_by_tags": "primary_lineup",
      "calculations": "metric_value:sum"
    },
    "trigger_settings": {
      "run_async": false,
      "error_behavior": "log"
    }
  }'
```

### Generated Query

The plugin generates a query similar to:

```sql
SELECT
    DATE_BIN(INTERVAL '1 minutes', time, '1970-01-01T00:00:00Z') AS _time,
    count(*) AS record_count,
    MIN(time) AS time_from,
    MAX(time) AS time_to,
    sum("metric_value") as "metric_value_sum",
    "primary_lineup"
FROM 'power_datacenter1'
WHERE time >= '2024-01-01T00:00:00Z' AND time < '2024-01-02T00:00:00Z'
GROUP BY _time, primary_lineup
```

### Available Aggregation Functions

The `calculations` parameter supports the following aggregation functions:

| Function | Description |
|----------|-------------|
| `avg` | Average of values |
| `sum` | Sum of values |
| `min` | Minimum value |
| `max` | Maximum value |
| `median` | Median value |
| `count` | Count of records |
| `stddev` | Standard deviation |
| `first_value` | First value (ordered by time) |
| `last_value` | Last value (ordered by time) |
| `var` | Variance |
| `approx_median` | Approximate median |
| `moving_avg` | Moving average (requires `moving_avg_window` and `partition_by_tags`) |

### Multiple Aggregations Example

You can apply different aggregations to different fields:

**HTTP (list format):**
```json
{
  "calculations": [
    ["temperature", "avg"],
    ["humidity", "sum"],
    ["pressure", "max"]
  ]
}
```

**Scheduled (dot-separated format):**
```json
{
  "calculations": "temperature:avg.humidity:sum.pressure:max"
}
```

## Second-Level AVG Aggregation Example

For high-resolution downsampling at 1-second intervals:

### HTTP Trigger for 1-Second AVG

```bash
# 1. Create HTTP Trigger
curl -X POST "http://localhost:8181/api/v3/configure/processing_engine_trigger" \
  --header "Authorization: Bearer $ADMIN_TOKEN" \
  --header "Content-Type: application/json" \
  -d '{
    "db": "power",
    "disabled": false,
    "plugin_filename": "gh:downsampler/downsampler.py",
    "trigger_name": "power_avg_http_trigger",
    "trigger_specification": "request:power-avg",
    "trigger_settings": {
      "run_async": false,
      "error_behavior": "log"
    }
  }'

# 2. Execute HTTP Request
curl -X POST http://localhost:8181/api/v3/engine/power-avg \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "source_measurement": "power_datacenter",
    "target_measurement": "power_datacenter_avg",
    "interval": "1sec",
    "batch_size": "1h",
    "calculations": [["metric_value", "avg"]],
    "partition_by_tags": "none",
    "backfill_start": "2026-02-20T23:00:00+00:00",
    "backfill_end": "2026-02-20T23:30:00+00:00"
  }'
```

### Generated Query

The plugin generates:

```sql
SELECT
    DATE_BIN(INTERVAL '1 seconds', time, '1970-01-01T00:00:00Z') AS _time,
    count(*) AS record_count,
    MIN(time) AS time_from,
    MAX(time) AS time_to,
    avg("metric_value") as "metric_value_avg",
    "primary_lineup"
FROM 'power_datacenter'
WHERE time >= '2026-02-20T23:00:00Z' AND time < '2026-02-20T23:30:00Z'
GROUP BY _time, primary_lineup
```

### Supported Time Units for Interval

| Unit | Example | Description |
|------|---------|-------------|
| `s` | `"1s"` | Seconds |
| `sec` | `"1sec"` | Seconds (alternative) |
| `min` | `"5min"` | Minutes |
| `h` | `"1h"` | Hours |
| `d` | `"1d"` | Days |
| `w` | `"1w"` | Weeks |
| `m` | `"1m"` | Months (converted to ~30 days) |
| `q` | `"1q"` | Quarters (converted to ~91 days) |
| `y` | `"1y"` | Years (converted to 365 days) |

### Excluding Unwanted Tags

If your measurement has additional tags you don't want in the GROUP BY clause, use `excluded_fields`:

```bash
curl -X POST http://localhost:8181/api/v3/engine/power-avg \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "source_measurement": "power_datacenter",
    "target_measurement": "power_datacenter_avg",
    "interval": "1sec",
    "batch_size": "1h",
    "calculations": [["metric_value", "avg"]],
    "partition_by_tags": "none",
    "excluded_fields": ["unwanted_tag1", "unwanted_tag2"],
    "backfill_start": "2026-02-20T23:00:00+00:00",
    "backfill_end": "2026-02-20T23:30:00+00:00"
  }'
```

## Performance Optimization

For high-volume data processing, use these parameters to minimize query overhead:

### include_metadata Parameter

By default, the plugin includes metadata columns (`record_count`, `time_from`, `time_to`). Set `include_metadata: false` to exclude them for better performance:

```json
{
  "source_measurement": "power_datacenter",
  "target_measurement": "power_datacenter_sum",
  "interval": "1sec",
  "batch_size": "1h",
  "calculations": [["metric_value", "sum"]],
  "partition_by_tags": "none",
  "include_metadata": false,
  "backfill_start": "2026-02-20T23:00:00+00:00",
  "backfill_end": "2026-02-20T23:30:00+00:00"
}
```

**With `include_metadata: true` (default):**
```sql
SELECT
    DATE_BIN(...) AS _time,
    count(*) AS record_count,
    MIN(time) AS time_from,
    MAX(time) AS time_to,
    sum("metric_value") as "metric_value_sum"
FROM ...
```

**With `include_metadata: false`:**
```sql
SELECT
    DATE_BIN(...) AS _time,
    sum("metric_value") as "metric_value_sum"
FROM ...
```

### excluded_fields Wildcard Support

Use `"*"` to exclude ALL tags from the GROUP BY clause and output:

```json
{
  "source_measurement": "power_datacenter",
  "target_measurement": "power_datacenter_sum",
  "interval": "1sec",
  "batch_size": "1h",
  "calculations": [["metric_value", "sum"]],
  "partition_by_tags": "none",
  "excluded_fields": ["*"],
  "include_metadata": false,
  "backfill_start": "2026-02-20T23:00:00+00:00",
  "backfill_end": "2026-02-20T23:30:00+00:00"
}
```

**Wildcard options:**
- `"excluded_fields": ["*"]` - Exclude all tags
- `"excluded_fields": "*"` - Also works (string form)
- `"excluded_fields": ["*", "some_field"]` - Exclude all tags plus a specific field

**Generated query (most efficient):**
```sql
SELECT
    DATE_BIN(INTERVAL '1 seconds', time, '1970-01-01T00:00:00Z') AS _time,
    sum("metric_value") as "metric_value_sum"
FROM 'power_datacenter'
WHERE time >= '...' AND time < '...'
GROUP BY _time
```

### Performance Impact

| Configuration | Aggregations | Output Columns | Relative Speed |
|--------------|--------------|----------------|----------------|
| Default (all tags, metadata) | 4+ per group | 5+ | Baseline |
| `include_metadata: false` | 1+ per group | 2+ | ~15-30% faster |
| `excluded_fields: ["*"]` | 1+ per group | 2+ | 2-10x faster* |
| Both optimizations | 1 per group | 2 | Best |

*Speed improvement depends on number of tags and their cardinality.

### Recommended Settings for High-Volume Data

```json
{
  "interval": "1sec",
  "batch_size": "5min",
  "calculations": [["metric_value", "sum"]],
  "partition_by_tags": "none",
  "excluded_fields": ["*"],
  "include_metadata": false
}
```

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
| **partition_by_tags Format** | List: `["az", "region"]` or `"none"` | Dot-separated: `"az.region"` or `"none"` |
| **Time Control** | `backfill_start/end` parameters | `window` parameter (e.g., "1h") |
| **Execution** | On-demand via API calls | Automatic based on schedule |
| **Use Case** | Bulk historical analysis | Real-time continuous processing |



