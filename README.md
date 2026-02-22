# Hoval CAN-Bus Prometheus Exporter

Reads Hoval heat pump data via CAN-Bus (TopTronic E protocol)
and exposes Prometheus metrics for scraping by VictoriaMetrics, Prometheus, or similar.

Tested with **Hoval Belaria Pro 13** (TTE-WEZ). Should work with other Hoval heat pumps
using the TopTronic E controller family — datapoint IDs may vary.

## Features

- Active polling of 37 datapoints (temperatures, setpoints, status, power)
- Passive decoding of multi-frame U32 responses (operating hours, thermal energy)
- Own CAN address (`msg_id=6`) to avoid collisions with the Hoval Gateway
- Sentinel value filtering (0x8000 / 0xFFFF = no sensor)
- Prometheus metrics on configurable HTTP port
- YAML configuration with CLI overrides
- Systemd service with hardening
- Dry-run mode (listen only, no poll requests)

## Architecture

```
┌──────────────────┐     CAN-Bus      ┌──────────────────────┐
│ Hoval heat pump  │◄────50 kbit/s────►│ Linux host           │
│ (TTE-WEZ)        │                   │                      │
│                  │                   │ ┌──────────────────┐ │
│  CAN H / L / GND├───────────────────►│ │ CAN adapter      │ │
└──────────────────┘                   │ │ (SocketCAN)      │ │
                                       │ └────────┬─────────┘ │
                                       │          │            │
                                       │ ┌────────▼─────────┐ │
                                       │ │ hoval-exporter.py│ │
                                       │ │  :9101/metrics   │ │
                                       │ └────────┬─────────┘ │
                                       │          │ scrape     │
                                       │ ┌────────▼─────────┐ │
                                       │ │ Prometheus /     │ │
                                       │ │ VictoriaMetrics  │ │
                                       │ └──────────────────┘ │
                                       └──────────────────────┘
```

## Quick start

### Prerequisites

- Linux host with SocketCAN interface (Raspberry Pi + CAN HAT, USB-CAN adapter, etc.)
- CAN interface configured at 50 kbit/s
- Python 3.9+

```bash
# Bring up CAN interface
sudo ip link set can0 up type can bitrate 50000

# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install python-can prometheus-client
pip install pyyaml  # optional, for config.yml support
```

### Run

```bash
# With defaults (can0, port 9101, poll every 30s)
python3 hoval-exporter.py

# Custom config
python3 hoval-exporter.py --config config.yml

# Listen-only mode (no poll requests sent)
python3 hoval-exporter.py --dry-run

# Debug logging
python3 hoval-exporter.py --log-level DEBUG

# Custom metrics port
python3 hoval-exporter.py --port 9200
```

### Verify

```bash
curl -s http://localhost:9101/metrics | grep "^hoval_" | sort
```

### Systemd service

```bash
# Edit hoval-exporter.service to match your paths, then:
sudo cp hoval-exporter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hoval-exporter.service
```

## Configuration

See `config.yml` for all options. CLI args override config file values.

```yaml
can_interface: can0
can_bitrate: 50000
poll_interval: 30       # seconds between poll cycles
poll_delay: 0.1         # seconds between individual requests
metrics_port: 9101

# WEZ addressing — must use gateway device type
wez_device_type: 8      # 0x08 = Gateway device type
wez_device_id: 1        # Device ID (usually 1)
poll_priority: 228      # 0xE4 = Gateway priority
poll_message_id: 6      # msg_id = 6 (Gateway uses 5)
```

## CAN Protocol — Hoval TopTronic E (TTE)

### Arbitration ID (29-bit extended CAN)

```
Bit:  28..24   23..16    15..8     7..0
      ├────────┼────────┼────────┼────────┤
      │msg_id  │priority│dev_type│dev_id  │
      │ (5bit) │ (8bit) │ (8bit) │ (8bit) │
      └────────┴────────┴────────┴────────┘
```

Broadcast address: `dev_type=0x0F, dev_id=0xFF` → `0x0FFF`

### Payload format

```
Byte:  0          1           2              3               4..5            6..7
       ├──────────┼───────────┼──────────────┼───────────────┼───────────────┼──────────┤
       │msg_len   │operation  │function_group│function_number│datapoint_id   │data      │
       │          │(8bit)     │(8bit)        │(8bit)         │(16bit BE)     │(0-2 byte)│
       └──────────┴───────────┴──────────────┴───────────────┴───────────────┴──────────┘
```

| Operation | Value  | Direction        |
|-----------|--------|------------------|
| GET       | `0x40` | Request → WEZ    |
| RESPONSE  | `0x42` | WEZ → Broadcast  |
| SET       | `0x46` | Request → WEZ    |

### Data types

| Type | Size   | Signed | Decoding                                      |
|------|--------|--------|-----------------------------------------------|
| U8   | 1 byte | No     | `raw[0]`                                      |
| U16  | 2 byte | No     | `int.from_bytes(raw[:2], 'big')`              |
| S16  | 2 byte | Yes    | `int.from_bytes(raw[:2], 'big', signed=True)` |
| U32  | 4 byte | No     | Multi-frame transport (see below)             |
| LIST | 1 byte | No     | Enum index, see status mappings               |

The `decimal` field specifies implicit scaling: `actual_value = raw_value × 10^(-decimal)`.

Sentinel values: S16 `0x8000` and U16 `0xFFFF` indicate "no sensor" and are filtered.

### Multi-frame transport (U32 datapoints)

U32 datapoints (operating hours, thermal energy) are transmitted as multi-frame
sequences instead of single-frame responses:

```
Start frame  (0x1F400FFF, 8 bytes):
  [flags:1][seq:1][op:1][fg:1][fn:1][dp_hi:1][dp_lo:1][data_0:1]

Continuation (0x1E800FFF, 6 bytes):
  [seq:1][data_1:1][data_2:1][data_3:1][crc_hi:1][crc_lo:1]

U32 value = [data_0, data_1, data_2, data_3] big-endian
```

- `seq` must match between start and continuation frames
- Last 2 bytes of continuation are CRC (ignored by exporter)
- These cannot be polled with single-frame GET; decoded passively

### WEZ addressing

> **Critical:** The TTE-WEZ only responds to GET requests from `dev_type=8`
> (gateway device type). The Hoval 2-TTE R2 Gateway uses `msg_id=5`
> (arb_id `0x05E40801`). This exporter uses `msg_id=6` (arb_id `0x06E40801`)
> to avoid protocol collisions. Requests with `msg_id=0x1F` or `dev_type=1`
> (as used by chrishrb/hoval-gateway for ventilation units) are silently
> ignored by the WEZ.

### Known devices on the bus

| Arb ID pattern | Device                 | Role                       |
|----------------|------------------------|----------------------------|
| `0x05E4_0801`  | Gateway (dev_type=8)   | Polls TTE-WEZ for data     |
| `0x06E4_0801`  | Exporter (dev_type=8)  | Our poll requests           |
| `0x1FC0_0FFF`  | Broadcast              | WEZ single-frame responses  |
| `0x1F40_0FFF`  | Broadcast              | Multi-frame start           |
| `0x1E80_0FFF`  | Broadcast              | Multi-frame continuation    |
| `0x1FE0_0801`  | WEZ internal           | WEZ self-polling            |

## Datapoints

### Temperatures

| Metric name                | fg  | fn  | dp_id | Type | Dec | Unit | Description                      |
|----------------------------|-----|-----|-------|------|-----|------|----------------------------------|
| `hoval_outdoor_temp_af1`   | 0   | 0   | 0     | S16  | 1   | °C   | Outdoor sensor 1 (AF1)           |
| `hoval_outdoor_temp_af2`   | 0   | 0   | 21100 | S16  | 1   | °C   | Outdoor sensor 2 (AF2)           |
| `hoval_flow_temp_hc1`      | 1   | 0   | 2     | S16  | 1   | °C   | Flow temperature HC1             |
| `hoval_flow_temp_hc2`      | 1   | 1   | 2     | S16  | 1   | °C   | Flow temperature HC2             |
| `hoval_flow_temp_hc3`      | 1   | 2   | 2     | S16  | 1   | °C   | Flow temperature HC3             |
| `hoval_dhw_temp`            | 2   | 0   | 4     | S16  | 1   | °C   | Domestic hot water temperature   |
| `hoval_return_temp`         | 60  | 254 | 29    | S16  | 1   | °C   | Return temperature               |
| `hoval_return_temp_hp`      | 10  | 1   | 8     | S16  | 1   | °C   | Return temperature heat producer |
| `hoval_hp_temp`             | 60  | 254 | 17    | S16  | 1   | °C   | Heat producer temperature        |

### Setpoints

| Metric name                          | fg  | fn  | dp_id | Type | Dec | Unit | Description                       |
|--------------------------------------|-----|-----|-------|------|-----|------|-----------------------------------|
| `hoval_room_setpoint_hc1`            | 1   | 0   | 1001  | S16  | 1   | °C   | Room setpoint HC1                 |
| `hoval_room_temp_hc1`                | 1   | 0   | 1     | S16  | 1   | °C   | Room temperature HC1              |
| `hoval_dhw_setpoint`                 | 2   | 0   | 1004  | S16  | 1   | °C   | Hot water setpoint                |
| `hoval_heating_setpoint`             | 60  | 254 | 0     | S16  | 1   | °C   | Heating circuit flow setpoint     |
| `hoval_storage_setpoint`             | 60  | 254 | 1     | S16  | 1   | °C   | Storage tank setpoint             |
| `hoval_flow_setpoint_hc1`            | 1   | 0   | 1002  | S16  | 1   | °C   | Flow setpoint HC1                 |
| `hoval_comfort_room_setpoint_hc1`    | 1   | 0   | 3051  | S16  | 1   | °C   | Comfort room setpoint HC1         |
| `hoval_eco_room_setpoint_hc1`        | 1   | 0   | 3053  | S16  | 1   | °C   | Eco room setpoint HC1             |
| `hoval_cooling_room_setpoint_hc1`    | 1   | 0   | 3054  | S16  | 1   | °C   | Cooling room setpoint HC1         |
| `hoval_flow_setpoint_const_hc1`      | 1   | 0   | 7036  | S16  | 1   | °C   | Flow setpoint constant mode HC1   |
| `hoval_comfort_dhw_setpoint`         | 2   | 0   | 5051  | S16  | 1   | °C   | Comfort hot water setpoint        |
| `hoval_eco_dhw_setpoint`             | 2   | 0   | 5086  | U8   | 0   | °C   | Eco hot water setpoint            |
| `hoval_flow_setpoint_hp`             | 10  | 1   | 1007  | S16  | 1   | °C   | Flow setpoint heat producer       |
| `hoval_fa_flow_setpoint`             | 60  | 254 | 16    | S16  | 1   | °C   | Function automation flow setpoint |

### Power / Modulation

| Metric name              | fg  | fn  | dp_id | Type | Dec | Unit | Description           |
|--------------------------|-----|-----|-------|------|-----|------|-----------------------|
| `hoval_modulation`       | 10  | 1   | 20052 | U8   | 0   | %    | Compressor modulation |
| `hoval_hp_power_pct`     | 60  | 254 | 30    | S16  | 0   | %    | Heat producer power   |
| `hoval_hp_power_abs_pct` | 60  | 254 | 31    | S16  | 0   | %    | Absolute power        |
| `hoval_power_limit`      | 60  | 254 | 8     | S16  | 1   | %    | Power limit           |

### Status

| Metric name                  | fg  | fn  | dp_id | Type | Dec | Description                |
|------------------------------|-----|-----|-------|------|-----|----------------------------|
| `hoval_status_hc1`           | 1   | 0   | 2051  | U8   | 0   | Heating circuit 1 status   |
| `hoval_status_hc2`           | 1   | 1   | 2051  | U8   | 0   | Heating circuit 2 status   |
| `hoval_status_dhw`           | 2   | 0   | 2052  | U8   | 0   | Hot water status           |
| `hoval_operating_message`    | 10  | 1   | 20053 | U8   | 0   | Operating message          |
| `hoval_operating_mode_hc1`   | 1   | 0   | 3050  | U8   | 0   | Operating mode HC1         |
| `hoval_operating_mode_dhw`   | 2   | 0   | 5050  | U8   | 0   | Operating mode hot water   |
| `hoval_operating_status_hp`  | 10  | 1   | 20051 | U8   | 0   | Heat producer status       |
| `hoval_fa_status`            | 60  | 254 | 34    | U8   | 0   | Function automation status |
| `hoval_fa_defrost_demand`    | 60  | 254 | 22    | S16  | 1   | Defrost demand / evaporator |

#### Heating circuit status codes

| Code | Status             | Code | Status             |
|------|--------------------|------|--------------------|
| 0    | Off                | 9    | Normal cooling     |
| 1    | Normal heating     | 12   | Fault              |
| 2    | Comfort heating    | 13   | Manual             |
| 3    | Eco heating        | 22   | Cooling external   |
| 4    | Frost protection   | 23   | Heating external   |
| 5    | Forced consumption | 26   | SmartGrid priority |
| 6    | Forced reduction   |      |                    |
| 7    | Holiday            |      |                    |
| 8    | Party              |      |                    |

#### Hot water status codes

| Code | Status             | Code | Status             |
|------|--------------------|------|--------------------|
| 0    | Off                | 6    | Draw-off           |
| 1    | Normal charging    | 8    | Reduced charging   |
| 2    | Comfort charging   | 12   | SmartGrid priority |
| 5    | Fault              | 13   | SmartGrid forced   |

### Counters

| Metric name                | fg  | fn  | dp_id | Type | Dec | Unit | Description               |
|----------------------------|-----|-----|-------|------|-----|------|---------------------------|
| `hoval_operating_hours`    | 10  | 1   | 2081  | U32  | 0   | h    | Operating hours            |
| `hoval_switching_cycles`   | 10  | 1   | 2080  | U32  | 0   | -    | Switching cycles           |
| `hoval_thermal_power`      | 10  | 1   | 29051 | U32  | 1   | kW   | Current thermal power      |
| `hoval_thermal_energy`     | 10  | 1   | 29050 | U32  | 3   | MWh  | Total thermal energy       |
| `hoval_compressor_starts`  | 10  | 1   | 2053  | U8   | 0   | -    | Compressor start counter   |

> **Note:** U32 datapoints are decoded via multi-frame transport (passive only,
> `poll=False`). They cannot be requested with single-frame GET.

### Exporter internals

| Metric name                                      | Type    | Description                        |
|--------------------------------------------------|---------|------------------------------------|
| `hoval_exporter_up`                              | Gauge   | 1 if exporter is running           |
| `hoval_exporter_last_poll_timestamp_seconds`     | Gauge   | Unix timestamp of last poll cycle  |
| `hoval_exporter_last_receive_timestamp_seconds`  | Gauge   | Last decoded CAN response          |
| `hoval_exporter_poll_errors_total`               | Counter | CAN send errors during polling     |
| `hoval_exporter_frames_received_total`           | Counter | All CAN frames received            |
| `hoval_exporter_responses_decoded_total`         | Counter | Successfully decoded TTE responses |
| `hoval_exporter_unknown_datapoints_total`        | Counter | Responses for unknown datapoints   |
| `hoval_exporter_info`                            | Gauge   | Version, interface, interval labels |

## Adding datapoints

Edit the `DEFAULT_DATAPOINTS` list in `hoval-exporter.py`. Each entry needs:

```python
DatapointDef(
    name="my_metric",           # Prometheus metric name (prefixed with hoval_)
    function_group=10,          # fg from datapoints.csv
    function_number=1,          # fn from datapoints.csv
    datapoint_id=20052,         # dp_id from datapoints.csv
    dtype="U8",                 # S16, U8, U16, U32, LIST
    decimal=0,                  # Implicit decimal places
    unit="percent",             # For Prometheus help text
    description="My metric",   # For Prometheus help text
    poll=True,                  # False for U32 (multi-frame only)
)
```

Datapoint IDs can be found in the
[hoval-gateway datapoints.csv](https://github.com/chrishrb/hoval-gateway/blob/main/docs/datapoints.csv).
Filter for your device type (WEZ, Lüftung, etc.).

## Troubleshooting

### No data in metrics

1. Check CAN interface: `ip link show can0` → should be UP
2. Check frames: `candump can0` → should show traffic
3. Check exporter logs: `journalctl -u hoval-exporter -n 50`
4. Check metrics: `curl localhost:9101/metrics | grep hoval_exporter_frames`
   - `frames_received_total` increasing? → CAN works, check datapoint matching
   - `frames_received_total` = 0? → CAN wiring issue
   - `unknown_datapoints_total` high? → WEZ sending DPs not in registry

### Plausibility check

| Metric                    | Expected range | Notes                  |
|---------------------------|----------------|------------------------|
| `hoval_outdoor_temp_af1`  | -20 to +40 °C  | Depends on season      |
| `hoval_flow_temp_hc1`     | 25 to 55 °C    | Higher in cold weather |
| `hoval_dhw_temp`           | 35 to 60 °C    | ~42°C typical          |
| `hoval_return_temp*`       | 20 to 45 °C    | Always < flow temp     |
| `hoval_modulation`         | 0 to 100 %     | 0 when idle            |

### CAN bus errors

If `poll_errors_total` increases:
- CAN bus overloaded → increase `poll_delay`
- Wiring issue → check H/L not swapped, GND connected
- Termination mismatch → check adapter termination resistor setting

## References

- [chrishrb/hoval-gateway](https://github.com/chrishrb/hoval-gateway) — Hoval TTE protocol reference and datapoints CSV
- [Hoval TopTronic E documentation](https://www.hoval.com) — Controller manuals (installer access required)

## License

MIT
