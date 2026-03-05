#!/usr/bin/env python3
"""
Hoval CAN-Bus Prometheus Exporter

Reads Hoval Belaria Pro 13 (TopTronic E / TTE-WEZ) data via CAN-Bus
and exposes metrics for Prometheus/VictoriaMetrics scraping.

Architecture:
  - Passive listener: Decodes all broadcast RESPONSE (0x42) frames on the bus
  - Active poller: Periodically sends GET_REQUEST (0x40) for configured datapoints
  - Prometheus HTTP endpoint exposes all collected metrics

CAN Protocol (Hoval TopTronic E):
  Arbitration ID (29-bit extended):
    [message_id:8][priority:8][device_type:8][device_id:8]
  Payload:
    [msg_len:5bit+flags:3bit][operation:8][func_group:8][func_number:8][dp_id:16][data:0-16]
  Operations: 0x40=GET, 0x42=RESPONSE, 0x46=SET

Usage:
  python3 hoval-exporter.py                    # default config
  python3 hoval-exporter.py --config config.yml # custom config
  python3 hoval-exporter.py --dry-run          # decode without polling
"""

import argparse
import struct
import time
import threading
import logging
import signal
import sys
import os
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Optional

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

import can
from prometheus_client import Gauge, Counter, start_http_server


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class Operation(IntEnum):
    GET_REQUEST  = 0x40
    RESPONSE     = 0x42
    SET_REQUEST  = 0x46


BROADCAST_ADDR = 0x0FFF  # device_type=0x0F, device_id=0xFF

# Multi-frame transport CAN IDs (broadcast)
MULTIFRAME_START = 0x1F400FFF  # Start frame: [flags][seq][op][fg][fn][dp_hi][dp_lo][data_0]
MULTIFRAME_CONT  = 0x1E800FFF  # Continuation:  [seq][data_1][data_2][data_3][crc_hi][crc_lo]

VERSION = "1.7.0"


# ---------------------------------------------------------------------------
# Datapoint model
# ---------------------------------------------------------------------------

@dataclass
class DatapointDef:
    """Definition of a single TTE datapoint."""
    name: str
    function_group: int
    function_number: int
    datapoint_id: int
    dtype: str          # S16, U8, U16, U32, S32, LIST
    decimal: int = 0
    unit: str = ""
    description: str = ""
    poll: bool = True   # whether to actively poll this datapoint

    @property
    def key(self) -> tuple:
        return (self.function_group, self.function_number, self.datapoint_id)

    def dp_id_bytes(self) -> bytes:
        return self.datapoint_id.to_bytes(2, byteorder='big')


# ---------------------------------------------------------------------------
# Default datapoint registry — Hoval Belaria Pro 13 (WEZ, Unit ID 1)
# Source: hoval-gateway/docs/datapoints.csv, WEZ rows
# ---------------------------------------------------------------------------

DEFAULT_DATAPOINTS = [
    # Temperatures
    DatapointDef("outdoor_temp_af1",        0,  0,     0, "S16", 1, "celsius", "Outdoor temperature sensor 1 (AF1)"),
    # AF2, HC2, HC3 removed — not physically connected on single-circuit Belaria Pro 13
    DatapointDef("flow_temp_hc1",           1,  0,     2, "S16", 1, "celsius", "Flow temperature heating circuit 1"),
    DatapointDef("dhw_temp",                2,  0,     4, "S16", 1, "celsius", "Domestic hot water temperature"),
    DatapointDef("return_temp",            60,254,    29, "S16", 1, "celsius", "Return temperature"),
    DatapointDef("return_temp_hp",         10,  1,     8, "S16", 1, "celsius", "Return temperature heat producer"),
    DatapointDef("hp_temp",                60,254,    17, "S16", 1, "celsius", "Heat producer temperature"),

    # Setpoints
    DatapointDef("room_setpoint_hc1",       1,  0,  1001, "S16", 1, "celsius", "Room setpoint heating circuit 1"),
    DatapointDef("room_temp_hc1",           1,  0,     1, "S16", 1, "celsius", "Room temperature heating circuit 1"),
    DatapointDef("dhw_setpoint",            2,  0,  1004, "S16", 1, "celsius", "Domestic hot water setpoint"),
    DatapointDef("heating_setpoint",       60,254,     0, "S16", 1, "celsius", "Heating circuit flow setpoint"),
    DatapointDef("storage_setpoint",       60,254,     1, "S16", 1, "celsius", "Storage tank setpoint"),

    # Power / Modulation
    DatapointDef("modulation",             10,  1, 20052, "U8",  0, "percent", "Compressor modulation"),
    DatapointDef("hp_power_pct",           60,254,    30, "S16", 0, "percent", "Heat producer power"),
    DatapointDef("hp_power_abs_pct",       60,254,    31, "S16", 0, "percent", "Heat producer absolute power"),
    DatapointDef("power_limit",            60,254,     8, "S16", 1, "percent", "Power limit"),

    # Status
    DatapointDef("status_hc1",              1,  0,  2051, "U8",  0, "status", "Heating circuit 1 status"),
    DatapointDef("status_dhw",              2,  0,  2052, "U8",  0, "status", "Domestic hot water status"),
    DatapointDef("operating_message",      10,  1, 20053, "U8",  0, "status", "Operating message"),

    # Counters — U32, decoded via multi-frame transport (passive only).
    DatapointDef("operating_hours",        10,  1,  2081, "U32", 0, "hours",  "Heat producer operating hours", poll=False),
    DatapointDef("switching_cycles",       10,  1,  2080, "U32", 0, "count",  "Heat producer switching cycles", poll=False),
    DatapointDef("thermal_power",          10,  1, 29051, "U32", 1, "kw",     "Current thermal heating power", poll=False),
    DatapointDef("thermal_energy",         10,  1, 29050, "U32", 3, "mwh",    "Total thermal heating energy", poll=False),

    # Additional datapoints discovered on bus (from Gateway polling)
    DatapointDef("mixed_flow_temp_hc1",  1,  0,     0, "S16", 1, "celsius", "Mixed flow temperature heating circuit 1"),
    DatapointDef("flow_setpoint_hc1",    1,  0,  1002, "S16", 1, "celsius", "Flow setpoint heating circuit 1"),
    DatapointDef("operating_mode_hc1",   1,  0,  3050, "U8",  0, "status",  "Operating mode heating circuit 1"),
    DatapointDef("eco_room_setpoint_hc1",   1,  0,  3053, "S16", 1, "celsius", "Eco room setpoint heating circuit 1"),
    DatapointDef("comfort_room_setpoint_hc1", 1, 0, 3051, "S16", 1, "celsius", "Comfort room setpoint heating circuit 1"),
    DatapointDef("cooling_room_setpoint_hc1", 1, 0, 3054, "S16", 1, "celsius", "Cooling room setpoint heating circuit 1"),
    DatapointDef("flow_setpoint_const_hc1", 1,  0,  7036, "S16", 1, "celsius", "Flow setpoint constant mode heating circuit 1"),
    DatapointDef("error_hc1",            1,  0,   500, "U8",  0, "status",  "Error register heating circuit 1 (0xFF=ok)"),
    DatapointDef("dhw_storage_bottom",   2,  0,     6, "S16", 1, "celsius", "Domestic hot water storage bottom sensor"),
    DatapointDef("error_dhw",            2,  0,   500, "U8",  0, "status",  "Error register domestic hot water (0xFF=ok)"),
    DatapointDef("operating_mode_dhw",   2,  0,  5050, "U8",  0, "status",  "Operating mode domestic hot water"),
    DatapointDef("comfort_dhw_setpoint", 2,  0,  5051, "S16", 1, "celsius", "Comfort hot water setpoint"),
    DatapointDef("eco_dhw_setpoint",     2,  0,  5086, "U8",  0, "celsius", "Eco hot water setpoint"),
    DatapointDef("flow_temp_hp",        10,  1,     7, "S16", 1, "celsius", "Flow temperature heat producer"),
    DatapointDef("flow_setpoint_hp",    10,  1,  1007, "S16", 1, "celsius", "Flow setpoint heat producer"),
    DatapointDef("operating_status_hp", 10,  1, 20051, "U8",  0, "status",  "Heat producer operating status"),
    DatapointDef("compressor_starts",   10,  1,  2053, "U8",  0, "count",   "Compressor starts"),
    DatapointDef("condenser_temp",      10,  1, 21028, "S16", 1, "celsius", "Condenser temperature"),
    DatapointDef("evaporator_temp",     10,  1, 21029, "S16", 1, "celsius", "Evaporator temperature"),
    DatapointDef("suction_gas_temp",    10,  1, 21030, "S16", 1, "celsius", "Suction gas temperature"),
    DatapointDef("electrical_power",       10,  1, 23002, "S16", 2, "kw",     "Electrical power input heat producer"),
    DatapointDef("thermal_power_realtime", 10,  1, 23003, "S16", 0, "kw",     "Thermal power output heat producer"),
    DatapointDef("fa_flow_setpoint",    60,254,    16, "S16", 1, "celsius", "Function automation flow setpoint"),
    DatapointDef("fa_defrost_demand",   60,254,    22, "S16", 1, "celsius", "Function automation defrost demand / evaporator"),
    DatapointDef("fa_status",           60,254,    34, "U8",  0, "status",  "Function automation status"),

    # Performance metrics
    # dp 45 is "Coefficient of Performance" at fg=60/fn=254 per official Modbus xlsx
    DatapointDef("cop_internal",           60,254,    45, "U8",  1, "ratio",   "Coefficient of performance (COP)"),
    # dp 23008 = "Total energy efficiency H-Gen" (= SPF / Jahresarbeitszahl)
    DatapointDef("spf",                    10,  1, 23008, "U8",  1, "ratio",   "Seasonal performance factor (SPF)"),

    # Additional temperatures — dp 84/85 at fg=60/fn=254 per Modbus xlsx
    DatapointDef("evaporator_inlet_temp",  60,254,    84, "S16", 1, "celsius", "Evaporator inlet / source flow temperature"),
    DatapointDef("evaporator_surface_temp",60,254,    85, "S16", 1, "celsius", "Evaporator surface / source return temperature"),

    # Pump speeds and flow
    DatapointDef("hp_pump_speed",          10,  1,  1022, "U8",  0, "percent", "Heat pump circulation pump speed"),
    DatapointDef("main_pump_speed",        10,  1,    22, "U8",  0, "percent", "Main circulation pump speed"),
    DatapointDef("flow_rate",              10,  1, 21105, "U16", 2, "lpm",     "Flow rate"),
    # dp 1009 is at fn=0 (additional heater context) per Modbus xlsx, not fn=1
    DatapointDef("hp_power_setpoint",      10,  0,  1009, "U8",  0, "percent", "Heat generator power setpoint"),

    # SmartGrid
    DatapointDef("smartgrid_status",        0,  0, 21090, "U8",  0, "status",  "Smart Grid status (0=normal,1=preferred,2=blocked,3=forced,255=inactive)"),

    # Electrical energy counter (passive, U32 multi-frame)
    DatapointDef("electrical_energy_total",10,  1, 23009, "U32", 3, "mwh",     "Total electrical energy consumed", poll=False),
]


# Status code mappings
HEIZKREIS_STATUS = {
    0: "off", 1: "normal_heating", 2: "comfort_heating",
    3: "eco_heating", 4: "frost_protection", 5: "forced_consumption",
    6: "forced_reduction", 7: "holiday", 8: "party",
    9: "normal_cooling", 10: "comfort_cooling", 11: "eco_cooling",
    12: "fault", 13: "manual", 14: "protected_cooling",
    15: "party_cooling", 16: "dryout_heating",
    17: "dryout_stationary", 18: "dryout_cooldown",
    19: "dryout_final", 22: "cooling_extern",
    23: "heating_extern", 26: "smartgrid_priority",
}

WARMWASSER_STATUS = {
    0: "off", 1: "normal_charging", 2: "comfort_charging",
    3: "forced_reduction", 4: "forced_charging", 5: "fault",
    6: "draw_off", 7: "warning", 8: "reduced_charging",
    12: "smartgrid_priority", 13: "smartgrid_forced",
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    """Exporter configuration with sensible defaults."""
    can_interface: str = "can0"
    can_bitrate: int = 50000
    poll_interval: int = 30         # seconds between poll cycles
    poll_delay: float = 0.1         # seconds between individual requests
    metrics_port: int = 9101
    metrics_bind: str = "127.0.0.1"     # bind address for metrics HTTP server
    log_level: str = "INFO"
    dry_run: bool = False           # listen only, don't send requests

    # WEZ addressing — the TTE-WEZ only responds to requests from
    # "gateway" type devices (dev_type=8). We use msg_id=6 to avoid
    # collisions with the 2-TTE R2 Gateway which uses msg_id=5.
    # Arb ID: 0x06E40801
    wez_device_type: int = 8        # 0x08 = Gateway device type
    wez_device_id: int = 1          # Device ID 1
    poll_priority: int = 0xE4       # 228 = Gateway priority
    poll_message_id: int = 6        # msg_id = 6 (Gateway uses 5)

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """Load config from YAML file. Unknown keys are ignored."""
        if not HAS_YAML:
            logging.warning("PyYAML not installed, using defaults")
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


# ---------------------------------------------------------------------------
# Value decoding
# ---------------------------------------------------------------------------

def decode_value(raw_bytes: bytes, dtype: str, decimal: int) -> Optional[float]:
    """Decode raw CAN payload bytes to a numeric value.

    The TTE protocol stores values as integers with an implicit decimal point.
    E.g. S16 with decimal=1: raw value 234 → 23.4

    Sentinel values:
      - S16: 0x8000 (-32768) = no sensor / invalid
      - U16: 0xFFFF (65535) = no sensor / invalid
      - U8:  0xFF (255) = no sensor / invalid (context-dependent)

    Args:
        raw_bytes: Raw payload bytes after the 6-byte header.
        dtype: Data type string from datapoints.csv (S16, U8, U16, U32, S32, LIST).
        decimal: Number of decimal places (value is divided by 10^decimal).

    Returns:
        Decoded float value, or None if decoding fails or value is a sentinel.
    """
    if not raw_bytes:
        return None
    try:
        if dtype == "S16":
            val = int.from_bytes(raw_bytes[:2], byteorder='big', signed=True)
            if val == -32768:  # 0x8000 = no sensor / invalid
                return None
        elif dtype == "U8" or dtype == "LIST":
            val = raw_bytes[0]
        elif dtype == "U16":
            val = int.from_bytes(raw_bytes[:2], byteorder='big', signed=False)
            if val == 0xFFFF:  # sentinel
                return None
        elif dtype == "U32":
            val = int.from_bytes(raw_bytes[:4], byteorder='big', signed=False)
        elif dtype == "S32":
            val = int.from_bytes(raw_bytes[:4], byteorder='big', signed=True)
        else:
            return None
        return round(val * 10 ** (-decimal), decimal) if decimal > 0 else float(val)
    except (IndexError, struct.error, OverflowError) as e:
        logging.debug("Decode error for dtype=%s: %s", dtype, e)
        return None


# ---------------------------------------------------------------------------
# CAN Protocol helpers
# ---------------------------------------------------------------------------

def build_arbitration_id(message_id: int, priority: int,
                         device_type: int, device_id: int) -> int:
    """Build 29-bit CAN extended arbitration ID from TTE components."""
    return (message_id << 24) | (priority << 16) | (device_type << 8) | device_id


def parse_arbitration_id(arb_id: int) -> dict:
    """Parse 29-bit CAN arbitration ID into TTE components."""
    return {
        "message_id":  arb_id >> 24,
        "priority":    (arb_id >> 16) & 0xFF,
        "device_type": (arb_id >> 8) & 0xFF,
        "device_id":   arb_id & 0xFF,
    }


def build_get_request(cfg: Config, dp: DatapointDef) -> can.Message:
    """Build a CAN GET_REQUEST frame for a WEZ datapoint.

    Frame format: [0x01][0x40][fg][fn][dp_hi][dp_lo][0x00]
    """
    arb_id = build_arbitration_id(
        cfg.poll_message_id, cfg.poll_priority,
        cfg.wez_device_type, cfg.wez_device_id
    )
    data = bytearray([
        0x01,                            # msg_len = 1 (single frame)
        Operation.GET_REQUEST,           # 0x40
        dp.function_group,
        dp.function_number,
        *dp.dp_id_bytes(),               # 2 bytes big-endian
        0x00,                            # payload placeholder
    ])
    return can.Message(arbitration_id=arb_id, data=data, is_extended_id=True)


# ---------------------------------------------------------------------------
# Prometheus Metrics Registry
# ---------------------------------------------------------------------------

class MetricsRegistry:
    """Manages Prometheus metrics for all datapoints."""

    def __init__(self, datapoints: list[DatapointDef]):
        self._gauges: dict[tuple, Gauge] = {}
        self._dp_map: dict[tuple, DatapointDef] = {}

        for dp in datapoints:
            metric_name = f"hoval_{dp.name}"
            help_text = f"{dp.description} [{dp.unit}]" if dp.unit else dp.description
            self._gauges[dp.key] = Gauge(metric_name, help_text)
            self._dp_map[dp.key] = dp

        # Exporter health metrics
        self.last_poll_ts    = Gauge("hoval_exporter_last_poll_timestamp_seconds",
                                     "Unix timestamp of last completed poll cycle")
        self.last_receive_ts = Gauge("hoval_exporter_last_receive_timestamp_seconds",
                                     "Unix timestamp of last decoded CAN response")
        self.poll_errors     = Counter("hoval_exporter_poll_errors_total",
                                       "Total number of CAN poll send errors")
        self.frames_total    = Counter("hoval_exporter_frames_received_total",
                                       "Total CAN frames received (all, including non-TTE)")
        self.responses_total = Counter("hoval_exporter_responses_decoded_total",
                                       "Total TTE RESPONSE frames successfully decoded")
        self.unknown_dp      = Counter("hoval_exporter_unknown_datapoints_total",
                                       "Responses for datapoints not in registry")
        self.info            = Gauge("hoval_exporter_info", "Exporter metadata",
                                     ["version", "can_interface", "poll_interval"])
        self.up              = Gauge("hoval_exporter_up", "1 if exporter is running")

    def set_value(self, key: tuple, value: float):
        if key in self._gauges:
            self._gauges[key].set(value)

    def has_key(self, key: tuple) -> bool:
        return key in self._gauges

    def get_dp(self, key: tuple) -> Optional[DatapointDef]:
        return self._dp_map.get(key)


# ---------------------------------------------------------------------------
# CAN Listener
# ---------------------------------------------------------------------------

class HovalListener(can.Listener):
    """Listens for CAN broadcast RESPONSE frames and updates Prometheus metrics.

    Handles two frame types:
      1. Single-frame responses on 0x1FC00FFF (standard 8-byte payload)
      2. Multi-frame responses on 0x1F400FFF + 0x1E800FFF (for U32 datapoints)

    Multi-frame transport format:
      Start  (0x1F400FFF): [flags:1][seq:1][op:1][fg:1][fn:1][dp_hi:1][dp_lo:1][data_0:1]
      Cont   (0x1E800FFF): [seq:1][data_1:1][data_2:1][data_3:1][crc_hi:1][crc_lo:1]
      → U32 value = data_0 << 24 | data_1 << 16 | data_2 << 8 | data_3
    """

    SINGLE_FRAME_ID = 0x1FC00FFF

    def __init__(self, registry: MetricsRegistry):
        self.registry = registry
        self.last_values: dict[tuple, tuple[float, float]] = {}
        # Pending multi-frame start frames, keyed by sequence counter
        self._pending_multiframe: dict[int, dict] = {}

    def on_message_received(self, msg: can.Message):
        self.registry.frames_total.inc()
        arb = msg.arbitration_id

        if arb == self.SINGLE_FRAME_ID:
            self._handle_single_frame(msg)
        elif arb == MULTIFRAME_START:
            self._handle_multiframe_start(msg)
        elif arb == MULTIFRAME_CONT:
            self._handle_multiframe_cont(msg)

    def _handle_single_frame(self, msg: can.Message):
        """Decode standard single-frame RESPONSE (0x1FC00FFF)."""
        data = msg.data
        if len(data) < 6:
            return
        if data[1] != Operation.RESPONSE:
            return

        fg    = data[2]
        fn    = data[3]
        dp_id = (data[4] << 8) | data[5]
        key   = (fg, fn, dp_id)

        if not self.registry.has_key(key):
            self.registry.unknown_dp.inc()
            return

        dp = self.registry.get_dp(key)
        raw_value = bytes(data[6:])
        self._update_metric(dp, key, raw_value)

    def _handle_multiframe_start(self, msg: can.Message):
        """Handle multi-frame start frame (0x1F400FFF).

        Store the header and first data byte, wait for continuation.
        """
        data = msg.data
        if len(data) < 8:
            return

        flags = data[0]
        seq   = data[1]
        op    = data[2]

        # Only process RESPONSE frames (0x42)
        if op != Operation.RESPONSE:
            return

        fg    = data[3]
        fn    = data[4]
        dp_id = (data[5] << 8) | data[6]
        data_0 = data[7]

        key = (fg, fn, dp_id)
        if not self.registry.has_key(key):
            return

        # Store pending, keyed by sequence counter
        self._pending_multiframe[seq] = {
            "key": key,
            "data_0": data_0,
            "timestamp": time.time(),
        }

        # Cleanup stale entries (older than 5 seconds)
        stale = [s for s, v in self._pending_multiframe.items()
                 if time.time() - v["timestamp"] > 5.0]
        for s in stale:
            del self._pending_multiframe[s]

    def _handle_multiframe_cont(self, msg: can.Message):
        """Handle multi-frame continuation frame (0x1E800FFF).

        Combine with pending start frame to extract U32 value.
        Format: [seq][data_1][data_2][data_3][crc_hi][crc_lo]
        """
        data = msg.data
        if len(data) < 4:
            return

        seq = data[0]
        if seq not in self._pending_multiframe:
            return

        pending = self._pending_multiframe.pop(seq)
        key = pending["key"]
        dp = self.registry.get_dp(key)
        if dp is None:
            return

        # Assemble U32: data_0 from start frame + data_1..data_3 from continuation
        raw_value = bytes([pending["data_0"], data[1], data[2], data[3]])
        self._update_metric(dp, key, raw_value)

    def _update_metric(self, dp: DatapointDef, key: tuple, raw_value: bytes):
        """Decode raw bytes and update Prometheus gauge."""
        value = decode_value(raw_value, dp.dtype, dp.decimal)
        if value is not None:
            self.last_values[key] = (value, time.time())
            self.registry.set_value(key, value)
            self.registry.last_receive_ts.set(time.time())
            self.registry.responses_total.inc()
            logging.debug("%-30s = %8.1f %s  (raw: %s)",
                          dp.name, value, dp.unit, raw_value.hex())


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------

class HovalPoller:
    """Periodically sends GET requests for WEZ datapoints.

    Sends requests sequentially with a configurable delay between each
    to avoid flooding the CAN bus. The WEZ responds with broadcast
    RESPONSE frames that are picked up by the listener.
    """

    def __init__(self, bus: can.BusABC, cfg: Config, datapoints: list[DatapointDef],
                 registry: MetricsRegistry):
        self.bus = bus
        self.cfg = cfg
        self.datapoints = [dp for dp in datapoints if dp.poll]
        self.registry = registry
        self._stop = threading.Event()

    def poll_once(self):
        """Send one GET request for each pollable datapoint."""
        errors = 0
        for dp in self.datapoints:
            try:
                msg = build_get_request(self.cfg, dp)
                self.bus.send(msg)
                logging.debug("Polled: fg=%d fn=%d dp=%d (%s)",
                              dp.function_group, dp.function_number,
                              dp.datapoint_id, dp.name)
            except can.CanError as e:
                errors += 1
                self.registry.poll_errors.inc()
                logging.warning("Poll error %s (fg=%d fn=%d dp=%d): %s",
                                dp.name, dp.function_group, dp.function_number,
                                dp.datapoint_id, e)
            time.sleep(self.cfg.poll_delay)

        self.registry.last_poll_ts.set(time.time())
        logging.info("Poll cycle: %d/%d OK (%d errors)",
                     len(self.datapoints) - errors, len(self.datapoints), errors)

    def run(self):
        """Main polling loop. Runs until stop() is called."""
        logging.info("Poller started: %d datapoints, interval=%ds, delay=%.1fs",
                     len(self.datapoints), self.cfg.poll_interval, self.cfg.poll_delay)

        # Initial poll immediately
        self.poll_once()

        while not self._stop.is_set():
            self._stop.wait(self.cfg.poll_interval)
            if not self._stop.is_set():
                self.poll_once()

    def stop(self):
        self._stop.set()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Hoval CAN-Bus Prometheus Exporter v" + VERSION)
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Path to YAML config file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Listen only, don't send poll requests")
    parser.add_argument("--log-level", type=str, default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Override log level")
    parser.add_argument("--port", type=int, default=None,
                        help="Override metrics port")
    parser.add_argument("--bind", type=str, default=None,
                        help="Override metrics bind address (default: 127.0.0.1)")
    return parser.parse_args()


def main():
    args = parse_args()

    # Load configuration
    cfg = Config()
    if args.config and Path(args.config).exists():
        try:
            cfg = Config.from_yaml(args.config)
        except Exception as e:
            logging.warning("Failed to load config %s: %s — using defaults", args.config, e)

    # CLI overrides
    if args.dry_run:
        cfg.dry_run = True
    if args.log_level:
        cfg.log_level = args.log_level
    if args.port:
        cfg.metrics_port = args.port
    if args.bind:
        cfg.metrics_bind = args.bind

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    # Force unbuffered output for systemd journal
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    logging.info("Hoval CAN-Bus Prometheus Exporter v%s", VERSION)
    logging.info("CAN: %s @ %d bit/s", cfg.can_interface, cfg.can_bitrate)
    logging.info("Metrics: http://%s:%d/metrics", cfg.metrics_bind, cfg.metrics_port)
    logging.info("Poll interval: %ds, dry_run: %s", cfg.poll_interval, cfg.dry_run)

    # Build datapoint registry
    datapoints = DEFAULT_DATAPOINTS
    registry = MetricsRegistry(datapoints)
    registry.up.set(1)
    registry.info.labels(
        version=VERSION,
        can_interface=cfg.can_interface,
        poll_interval=str(cfg.poll_interval)
    ).set(1)

    # Start Prometheus HTTP server
    start_http_server(cfg.metrics_port, addr=cfg.metrics_bind)

    # Open CAN bus
    try:
        bus = can.interface.Bus(
            channel=cfg.can_interface,
            interface="socketcan",
            receive_own_messages=False,
        )
        logging.info("CAN bus opened: %s", cfg.can_interface)
    except Exception as e:
        logging.error("Failed to open CAN interface %s: %s", cfg.can_interface, e)
        registry.up.set(0)
        sys.exit(1)

    # Listener (always active — reads all broadcast responses)
    listener = HovalListener(registry)
    notifier = can.Notifier(bus, [listener])

    # Poller (sends GET requests unless dry-run)
    poller = None
    poller_thread = None
    if not cfg.dry_run:
        poller = HovalPoller(bus, cfg, datapoints, registry)
        poller_thread = threading.Thread(target=poller.run, name="hoval-poller", daemon=True)
        poller_thread.start()
    else:
        logging.info("Dry-run mode: passive listening only, no poll requests")

    # Graceful shutdown
    shutdown_event = threading.Event()

    def shutdown(sig, frame):
        if shutdown_event.is_set():
            return  # prevent double-shutdown
        shutdown_event.set()
        logging.info("Shutting down (signal %s)...", sig)
        registry.up.set(0)
        if poller:
            poller.stop()
        notifier.stop()
        bus.shutdown()
        logging.info("Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logging.info("Exporter running. Datapoints: %d, pollable: %d",
                 len(datapoints), sum(1 for d in datapoints if d.poll))

    # Main thread: periodic health log
    try:
        while not shutdown_event.is_set():
            time.sleep(60)
            if listener.last_values:
                names = [registry.get_dp(k).name for k in
                         sorted(listener.last_values.keys())[:5]]
                logging.info("Health: %d unique datapoints received, latest: %s",
                             len(listener.last_values), ", ".join(names))
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
