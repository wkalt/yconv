# yconv

Convert MCAP robotics recordings into queryable columnar formats. Go from raw sensor data to SQL in one command.

Robotics systems produce MCAP files — timestamped streams of lidar scans, camera frames, IMU readings, CAN bus messages, and more. Analyzing this data typically means writing one-off scripts or reaching for format-specific tools. yconv bridges the gap: it converts MCAP files into Parquet, LanceDB, Vortex, or DuckDB, and lets you query them with SQL immediately.

Message schemas are handled dynamically — no code generation required. yconv reads ROS1, Protobuf, and CDR (ROS2/DDS) message definitions at runtime and builds Arrow-based transcoders on the fly.

## Install

```bash
cargo install --path .
```

## Quick Start

Convert an MCAP file and drop into a SQL shell:

```bash
yconv convert recording.mcap -o recording.parquet --shell
```

```sql
sql> SELECT * FROM "/imu/data" WHERE angular_velocity_x > 0.5 LIMIT 10;
```

## Usage

### Convert

Convert MCAP to any supported columnar format. The output format is inferred from the file extension:

```bash
yconv convert testdata/demo.mcap -o output.parquet
```

```
[1/1] Converting testdata/demo.mcap to output.parquet...

Conversion complete:
  Files processed: 1
  Total messages: 1606
  Topics created: 7

Messages per topic:
  /diagnostics: 52
  /image_color/compressed: 234
  /radar/points: 156
  /radar/range: 156
  /radar/tracks: 156
  /tf: 774
  /velodyne_points: 78
```

Multiple input files are supported:

```bash
yconv convert run1.mcap run2.mcap run3.mcap -o combined.parquet
```

Write directly to cloud storage with LanceDB:

```bash
yconv convert recording.mcap -o s3://my-bucket/data.lance
yconv convert recording.mcap -o gs://my-bucket/data.lance
yconv convert recording.mcap -o az://my-container/data.lance
```

### Shell

Open an interactive SQL shell on any supported format:

```bash
yconv shell recording.parquet
yconv shell recording.lance
yconv shell recording.duckdb
```

The shell uses DuckDB as its query engine. Each MCAP topic becomes a table, named by its topic path.

### Analyze

Compare how different columnar formats handle your data. This is useful for identifying schemas that compress poorly in a given format:

```bash
yconv analyze data.mcap --clean
```

```
Analyzing: data.mcap (1.5 GB)

Converting to Parquet, Vortex, and LanceDB in parallel...

Per-Topic Compression Analysis:
┌──────────────────────────────────────────────────┬────────────┬────────────┬────────────┬────────────┐
│ Topic                                            │   Messages │    Parquet │     Vortex │    LanceDB │
├──────────────────────────────────────────────────┼────────────┼────────────┼────────────┼────────────┤
│ /can_bus_dbw/can_rx                              │     223856 │     4.5 MB │     3.8 MB │    12.4 MB │
│ /center_camera/camera_info                       │       6311 │   130.7 KB │    73.9 KB │     1.9 MB │
│ /center_camera/image_color/compressed            │       6311 │   276.2 MB │   276.0 MB │   294.4 MB │
│ /diagnostics                                     │       1256 │    45.7 KB │    88.4 KB │   136.6 KB │
│ /ecef/                                           │     126238 │     2.2 MB │     1.3 MB │     4.4 MB │
│ /fix                                             │     126238 │     2.8 MB │     2.0 MB │    13.6 MB │
│ /imu/data                                        │     126238 │     9.0 MB │     8.2 MB │    35.7 MB │
│ /left_camera/camera_info                         │       6288 │   130.2 KB │    73.8 KB │     1.9 MB │
│ /left_camera/image_color/compressed              │       6288 │   264.5 MB │   264.4 MB │   282.1 MB │
│ /pressure                                        │      15780 │   311.7 KB │   150.0 KB │   495.4 KB │
│ /right_camera/camera_info                        │       6311 │   130.7 KB │    73.9 KB │     1.9 MB │
│ /right_camera/image_color/compressed             │       6311 │   296.6 MB │   296.4 MB │   316.2 MB │
│ /time_reference                                  │     382500 │     6.3 MB │     6.0 MB │    18.7 MB │
│ /vehicle/brake_info_report                       │      15779 │   350.8 KB │   206.6 KB │   879.0 KB │
│ /vehicle/brake_report                            │      15755 │   396.0 KB │   339.8 KB │     1.0 MB │
│ /vehicle/dbw_enabled                             │          1 │      755 B │     3.6 KB │     3.3 KB │
│ /vehicle/filtered_accel                          │      15754 │   271.2 KB │   189.0 KB │   447.3 KB │
│ /vehicle/fuel_level_report                       │       3156 │    57.8 KB │    33.9 KB │   121.5 KB │
│ /vehicle/gear_report                             │       6302 │   126.3 KB │     FAILED │   224.5 KB │
│ /vehicle/gps/fix                                 │        316 │    14.9 KB │    21.9 KB │    49.9 KB │
│ /vehicle/gps/time                                │        316 │    10.2 KB │    11.0 KB │    20.3 KB │
│ /vehicle/gps/vel                                 │        316 │    15.4 KB │    21.1 KB │    34.2 KB │
│ /vehicle/imu/data_raw                            │      31483 │   824.8 KB │   543.8 KB │     8.4 MB │
│ /vehicle/joint_states                            │      47313 │     2.8 MB │     3.0 MB │     6.0 MB │
│ /vehicle/misc_1_report                           │       6302 │   132.1 KB │     FAILED │   298.0 KB │
│ /vehicle/sonar_cloud                             │       1597 │    36.6 KB │    69.6 KB │   122.7 KB │
│ /vehicle/steering_report                         │      15753 │   384.8 KB │   274.9 KB │   857.1 KB │
│ /vehicle/surround_report                         │       1597 │    35.1 KB │    37.4 KB │   141.2 KB │
│ /vehicle/suspension_report                       │      15781 │   296.3 KB │   146.8 KB │   800.8 KB │
│ /vehicle/throttle_info_report                    │      31559 │   664.5 KB │   306.2 KB │     1.5 MB │
│ /vehicle/throttle_report                         │      15755 │   333.8 KB │   229.9 KB │   794.0 KB │
│ /vehicle/tire_pressure_report                    │        631 │    15.1 KB │    20.1 KB │    36.7 KB │
│ /vehicle/twist_controller/parameter_descriptions │          1 │    21.0 KB │    24.4 KB │    34.5 KB │
│ /vehicle/twist_controller/parameter_updates      │          1 │     5.8 KB │     8.6 KB │    11.2 KB │
│ /vehicle/wheel_speed_report                      │      31559 │   836.0 KB │   498.0 KB │     1.6 MB │
│ /velodyne_packets                                │       3153 │   527.7 MB │   658.6 MB │   658.8 MB │
├──────────────────────────────────────────────────┼────────────┼────────────┼────────────┼────────────┤
│ TOTAL                                            │    1300106 │     1.4 GB │     1.5 GB │     1.6 GB │
└──────────────────────────────────────────────────┴────────────┴────────────┴────────────┴────────────┘

Compression Ratios (vs MCAP 1.5 GB):
  Parquet: 0.92x (1.4 GB) in 29.30s
  Vortex:  1.01x (1.5 GB) in 6.65s [2 failed]
  LanceDB: 1.10x (1.6 GB) in 3.52s

Output written to: data_analysis
Output directory cleaned up.
```

Topics that fail conversion for a specific format show `FAILED` in that column. The `--clean` flag removes the output directory after printing results.

### Reverse Conversion (experimental)

yconv can also convert columnar tables back to an MCAP stream, to emulate playback:

```bash
yconv convert demo.lance --output-format mcap --stdout | mcap cat --json | head -n 1
```

This is still experimental — streams are merged on log time, but message ordering within scans is not guaranteed, and there is no filtering on time or topic yet.

## Supported Formats

| Format  | Input | Output | Cloud Storage      |
|---------|-------|--------|--------------------|
| MCAP    | Yes   | Yes    | -                  |
| Parquet | Yes   | Yes    | -                  |
| LanceDB | Yes   | Yes    | S3, GCS, Azure    |
| Vortex  | Yes   | Yes    | -                  |
| DuckDB  | Yes   | Yes    | -                  |

## Message Encodings

| Encoding          | Status |
|-------------------|--------|
| ROS1              | Stable |
| Protobuf          | Stable |
| CDR (ROS2/DDS)    | Has bugs |

All encodings are handled dynamically via schema reflection — no pre-compiled message definitions needed.

## Diagnosing Compression Issues

The `analyze` command is particularly useful for finding schemas that a format handles poorly. Here's a typical workflow:

First, run the analysis:

```
$ yconv analyze testdata/demo.mcap
Analyzing: testdata/demo.mcap (58.7 MB)

Converting to Parquet, Vortex, and LanceDB in parallel...

Per-Topic Compression Analysis:
┌─────────────────────────┬────────────┬────────────────────┬────────────────────┬────────────────────┐
│ Topic                   │   Messages │            Parquet │             Vortex │            LanceDB │
├─────────────────────────┼────────────┼────────────────────┼────────────────────┼────────────────────┤
│ /diagnostics            │         52 │             8.4 KB │ 17.5 KB (2.09x) │ 25.5 KB (3.05x) │
│ /image_color/compressed │        234 │            23.6 MB │ 23.6 MB (1.00x) │ 25.2 MB (1.06x) │
│ /radar/points           │        156 │            25.3 KB │ 52.7 KB (2.09x) │ 61.6 KB (2.44x) │
│ /radar/range            │        156 │             6.8 KB │ 14.3 KB (2.11x) │ 14.9 KB (2.19x) │
│ /radar/tracks           │        156 │            43.0 KB │ 47.3 KB (1.10x) │ 53.5 KB (1.25x) │
│ /tf                     │        774 │            17.2 KB │ 11.7 KB (0.68x) │ 33.5 KB (1.94x) │
│ /velodyne_points        │         78 │            34.4 MB │ 95.2 MB (2.76x) │ 101.5 MB (2.95x) │
├─────────────────────────┼────────────┼────────────────────┼────────────────────┼────────────────────┤
│ TOTAL                   │       1606 │            58.2 MB │ 118.9 MB (2.04x) │ 126.9 MB (2.18x) │
└─────────────────────────┴────────────┴────────────────────┴────────────────────┴────────────────────┘

Compression Ratios (vs MCAP 58.7 MB):
  Parquet: 0.99x (58.2 MB) in 1.57s
  Vortex:  2.03x (118.9 MB) in 297ms
  LanceDB: 2.16x (126.9 MB) in 773ms

Output written to: demo_analysis
```

Identify which topics are doing poorly. Here, `/velodyne_points` is nearly 3x larger in Vortex and LanceDB, while `/tf` shows a split — Vortex actually compresses it better (0.68x) but LanceDB makes it nearly 2x larger. Use the MCAP CLI to inspect the schema:

```
$ mcap info testdata/demo.mcap
library:   mcap go v0.4.0
profile:   ros1
messages:  1606
duration:  7.780758504s
start:     2017-03-21T19:26:20.103843113-07:00 (1490149580.103843113)
end:       2017-03-21T19:26:27.884601617-07:00 (1490149587.884601617)
compression:
        zstd: [314/314 chunks] [119.10 MiB/58.57 MiB (50.82%)] [7.53 MiB/sec]
channels:
        (0) /diagnostics              52 msgs (6.6..6.7Hz)     : diagnostic_msgs/DiagnosticArray [ros1msg]
        (1) /image_color/compressed  234 msgs (29.9..30.1Hz)   : sensor_msgs/CompressedImage [ros1msg]
        (2) /tf                      774 msgs (99.3..99.5Hz)   : tf2_msgs/TFMessage [ros1msg]
        (3) /radar/points            156 msgs (19.9..20.0Hz)   : sensor_msgs/PointCloud2 [ros1msg]
        (4) /radar/range             156 msgs (19.9..20.0Hz)   : sensor_msgs/Range [ros1msg]
        (5) /radar/tracks            156 msgs (19.9..20.0Hz)   : radar_driver/RadarTracks [ros1msg]
        (6) /velodyne_points          78 msgs (9.9..10.0Hz)    : sensor_msgs/PointCloud2 [ros1msg]
```

Then inspect the schema definition with `mcap list schemas` to understand why a particular message type isn't compressing well in a given format. This often reveals issues like large binary blobs in `PointCloud2` data fields, or nested repeated messages that columnar formats handle differently.

## Architecture

yconv is built around two traits:

- **`RowSource`** — reads rows from any supported format
- **`RowSink`** — writes rows to any supported format

All conversions flow through Apache Arrow record batches. Adding a new format means implementing one or both traits. The conversion pipeline is:

```
MCAP / Parquet / Lance / Vortex / DuckDB
    → RowSource → Arrow RecordBatch → RowSink →
                  MCAP / Parquet / Lance / Vortex / DuckDB
```
