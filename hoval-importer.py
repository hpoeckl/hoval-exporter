#!/usr/bin/env python3
"""
Hoval CAN-Bus Write API (Importer)

Exposes an HTTP API for writing datapoints to a Hoval heat pump via CAN-Bus
(TopTronic E protocol). Accepts JSON payloads and translates them into
TTE SET_REQUEST (0x46) frames.

Designed to run alongside hoval-exporter.py as a separate service.

Usage:
  python3 hoval-importer.py                       # default config
  python3 hoval-importer.py --config config.yml   # custom config
  python3 hoval-importer.py --enable-advanced      # unlock Tier 3 datapoints
  python3 hoval-importer.py --dry-run              # validate only, don't send CAN

Examples:
  curl -X POST http://localhost:9102/api/write \\
    -H "Content-Type: application/json" \\
    -d '{"name": "room_temp_hc1", "value": 21.5}'

  curl http://localhost:9102/api/datapoints
  curl http://localhost:9102/health
"""

import argparse
import json
import logging
import signal
import struct
import sys
import threading
import time
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import can
except ImportError:
    print("python-can not installed. Install with: pip install python-can")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "2.0.0"
SET_REQUEST = 0x46


# ---------------------------------------------------------------------------
# Writable datapoint definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WritableDef:
    """Definition of a writable TTE datapoint."""
    name: str
    function_group: int
    function_number: int
    datapoint_id: int
    dtype: str              # U8, S16, U16
    decimal: int
    min_val: float
    max_val: float
    unit: str
    description: str
    tier: int = 1           # 1=confirmed, 2=high confidence, 3=advanced


# Tier 1: Confirmed working via CAN SET
# Tier 2: Setpoints — high confidence (DHW SET observed in can_dump.log)
# Tier 3: Advanced — heating curve, power limits (use with caution)
WRITABLE_DATAPOINTS = {
    # Tier 1 — Confirmed
    "room_temp_hc1": WritableDef(
        "room_temp_hc1", 1, 0, 1, "S16", 1, 5.0, 35.0, "°C",
        "Room temperature HC1 (sensor value override)", tier=1),
    "control_strategy_hc1": WritableDef(
        "control_strategy_hc1", 1, 0, 3032, "U8", 0, 0, 5, "enum",
        "Control strategy (0=weather,1=weather+room,2=room,3=const,4=weather+const-cool,5=weather+room+const-cool)", tier=1),

    # Tier 2 — Setpoints
    "comfort_room_setpoint_hc1": WritableDef(
        "comfort_room_setpoint_hc1", 1, 0, 3051, "S16", 1, 10.0, 30.0, "°C",
        "Comfort room temperature setpoint HC1", tier=2),
    "eco_room_setpoint_hc1": WritableDef(
        "eco_room_setpoint_hc1", 1, 0, 3053, "S16", 1, 5.0, 20.0, "°C",
        "Eco room temperature setpoint HC1", tier=2),
    "cooling_room_setpoint_hc1": WritableDef(
        "cooling_room_setpoint_hc1", 1, 0, 3054, "S16", 1, 10.0, 30.0, "°C",
        "Cooling room temperature setpoint HC1", tier=2),
    "comfort_dhw_setpoint": WritableDef(
        "comfort_dhw_setpoint", 2, 0, 5051, "S16", 1, 10.0, 70.0, "°C",
        "Comfort domestic hot water setpoint", tier=2),
    "eco_dhw_setpoint": WritableDef(
        "eco_dhw_setpoint", 2, 0, 5086, "U8", 0, 10, 70, "°C",
        "Eco domestic hot water setpoint", tier=2),
    "operating_mode_hc1": WritableDef(
        "operating_mode_hc1", 1, 0, 3050, "U8", 0, 0, 8, "enum",
        "Operating mode HC1 (0=standby,1=week1,2=week2,4=const,5=eco,7=manual-heat,8=manual-cool)", tier=2),
    "operating_mode_dhw": WritableDef(
        "operating_mode_dhw", 2, 0, 5050, "U8", 0, 0, 6, "enum",
        "Operating mode DHW (0=standby,1=week1,2=week2,4=const,6=eco)", tier=2),

    # Tier 3 — Advanced (requires --enable-advanced)
    "heating_curve_base_temp": WritableDef(
        "heating_curve_base_temp", 1, 0, 3001, "S16", 1, 0, 90.0, "°C",
        "Heating curve base point supply temperature", tier=3),
    "heating_curve_design_temp": WritableDef(
        "heating_curve_design_temp", 1, 0, 3013, "S16", 1, 10.0, 90.0, "°C",
        "Heating curve design point supply temperature", tier=3),
    "heating_limit_outdoor": WritableDef(
        "heating_limit_outdoor", 1, 0, 3021, "S16", 1, -10.0, 50.0, "°C",
        "Heating limit outdoor temperature", tier=3),
    "max_power": WritableDef(
        "max_power", 0, 0, 1208, "U16", 0, 0, 10000, "kW",
        "Maximum heat output", tier=3),
    "power_limit_factor": WritableDef(
        "power_limit_factor", 0, 0, 1209, "U8", 0, 0, 100, "%",
        "Power limitation factor", tier=3),
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ImporterConfig:
    """Importer configuration."""
    can_interface: str = "can0"
    can_bitrate: int = 50000
    http_port: int = 9102
    http_bind: str = "127.0.0.1"
    log_level: str = "INFO"
    dry_run: bool = False
    enable_advanced: bool = False
    rate_limit: float = 1.0     # min seconds between writes to same datapoint

    # CAN addressing — msg_id=7 to avoid collisions with gateway (5) and exporter (6)
    msg_id: int = 7
    priority: int = 0xE4
    device_type: int = 8       # Gateway device type
    device_id: int = 1

    @classmethod
    def from_yaml(cls, path: str) -> "ImporterConfig":
        if not HAS_YAML:
            logging.warning("PyYAML not installed, using defaults")
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        # Look for importer-specific section, fall back to top-level
        importer_data = data.get("importer", data)
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in importer_data.items() if k in known_fields}
        return cls(**filtered)


# ---------------------------------------------------------------------------
# CAN Writer
# ---------------------------------------------------------------------------

class CANWriter:
    """Handles CAN bus connection and SET frame transmission."""

    def __init__(self, cfg: ImporterConfig):
        self.cfg = cfg
        self.arb_id = (cfg.msg_id << 24) | (cfg.priority << 16) | (cfg.device_type << 8) | cfg.device_id
        self._bus = None
        self._lock = threading.Lock()
        self._last_write: dict[str, float] = {}
        # Metrics
        self.writes_total = 0
        self.writes_failed = 0
        self.last_write_ts = 0.0

    def open(self):
        self._bus = can.interface.Bus(
            channel=self.cfg.can_interface,
            interface="socketcan",
            receive_own_messages=False,
        )
        logging.info("CAN bus opened: %s (arb_id=0x%08X)", self.cfg.can_interface, self.arb_id)

    def close(self):
        if self._bus:
            self._bus.shutdown()
            self._bus = None

    def write(self, dp: WritableDef, value: float) -> dict:
        """Validate, encode, and send a SET_REQUEST frame.

        Returns dict with result (ok/error, raw value, etc.)
        """
        # Tier check
        if dp.tier >= 3 and not self.cfg.enable_advanced:
            return {"ok": False, "error": f"Datapoint '{dp.name}' is Tier {dp.tier} (advanced). "
                    "Start with --enable-advanced to unlock."}

        # Range validation
        if value < dp.min_val or value > dp.max_val:
            return {"ok": False, "error": f"Value {value} outside range [{dp.min_val}, {dp.max_val}] for {dp.name}"}

        # Rate limiting
        now = time.time()
        with self._lock:
            last = self._last_write.get(dp.name, 0)
            if now - last < self.cfg.rate_limit:
                wait = self.cfg.rate_limit - (now - last)
                return {"ok": False, "error": f"Rate limited. Retry in {wait:.1f}s"}

        # Encode value
        dp_hi = (dp.datapoint_id >> 8) & 0xFF
        dp_lo = dp.datapoint_id & 0xFF

        if dp.dtype == "U8":
            raw = int(round(value))
            data_bytes = bytes([raw])
        elif dp.dtype == "S16":
            raw = int(round(value * (10 ** dp.decimal)))
            data_bytes = struct.pack(">h", raw)
        elif dp.dtype == "U16":
            raw = int(round(value * (10 ** dp.decimal)))
            data_bytes = struct.pack(">H", raw)
        else:
            return {"ok": False, "error": f"Unsupported dtype: {dp.dtype}"}

        payload = bytearray([
            0x01,           # single-frame marker
            SET_REQUEST,    # 0x46
            dp.function_group,
            dp.function_number,
            dp_hi,
            dp_lo,
        ]) + bytearray(data_bytes)

        msg = can.Message(arbitration_id=self.arb_id, data=payload, is_extended_id=True)

        # Dry-run mode
        if self.cfg.dry_run:
            logging.info("DRY-RUN: %s = %s (raw=%d, payload=%s)",
                         dp.name, value, raw, payload.hex())
            return {"ok": True, "name": dp.name, "value": value, "raw": raw, "dry_run": True}

        # Send
        try:
            with self._lock:
                self._bus.send(msg)
                self._last_write[dp.name] = time.time()
                self.writes_total += 1
                self.last_write_ts = time.time()
        except can.CanError as e:
            self.writes_failed += 1
            logging.error("CAN send error for %s: %s", dp.name, e)
            return {"ok": False, "error": f"CAN send error: {e}"}

        logging.info("WRITE: %s = %s (raw=%d)", dp.name, value, raw)
        return {"ok": True, "name": dp.name, "value": value, "raw": raw}


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------

class ImporterHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the write API."""

    server_version = f"HovalImporter/{VERSION}"
    writer: CANWriter = None
    cfg: ImporterConfig = None

    def log_message(self, format, *args):
        logging.debug("HTTP %s", format % args)

    def _send_json(self, data: dict, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/api/datapoints":
            self._handle_datapoints()
        elif self.path == "/metrics":
            self._handle_metrics()
        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self):
        if self.path == "/api/write":
            self._handle_write()
        else:
            self._send_json({"error": "Not found"}, 404)

    def _handle_health(self):
        self._send_json({
            "status": "ok",
            "version": VERSION,
            "can_interface": self.cfg.can_interface,
            "dry_run": self.cfg.dry_run,
            "advanced_enabled": self.cfg.enable_advanced,
            "writes_total": self.writer.writes_total,
            "writes_failed": self.writer.writes_failed,
            "last_write_ts": self.writer.last_write_ts,
        })

    def _handle_datapoints(self):
        dps = {}
        for name, dp in WRITABLE_DATAPOINTS.items():
            if dp.tier >= 3 and not self.cfg.enable_advanced:
                continue
            dps[name] = {
                "description": dp.description,
                "type": dp.dtype,
                "decimal": dp.decimal,
                "min": dp.min_val,
                "max": dp.max_val,
                "unit": dp.unit,
                "tier": dp.tier,
                "fg": dp.function_group,
                "fn": dp.function_number,
                "dp": dp.datapoint_id,
            }
        self._send_json(dps)

    def _handle_metrics(self):
        """Prometheus-style metrics endpoint."""
        lines = [
            "# HELP hoval_importer_writes_total Total successful CAN writes",
            "# TYPE hoval_importer_writes_total counter",
            f"hoval_importer_writes_total {self.writer.writes_total}",
            "# HELP hoval_importer_writes_failed_total Total failed CAN writes",
            "# TYPE hoval_importer_writes_failed_total counter",
            f"hoval_importer_writes_failed_total {self.writer.writes_failed}",
            "# HELP hoval_importer_last_write_timestamp_seconds Unix timestamp of last write",
            "# TYPE hoval_importer_last_write_timestamp_seconds gauge",
            f"hoval_importer_last_write_timestamp_seconds {self.writer.last_write_ts}",
            "# HELP hoval_importer_up 1 if importer is running",
            "# TYPE hoval_importer_up gauge",
            "hoval_importer_up 1",
            f'# HELP hoval_importer_info Importer metadata',
            f'# TYPE hoval_importer_info gauge',
            f'hoval_importer_info{{version="{VERSION}",can_interface="{self.cfg.can_interface}"}} 1',
        ]
        body = "\n".join(lines).encode("utf-8") + b"\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_write(self):
        # Read request body
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._send_json({"ok": False, "error": "Empty request body"}, 400)
            return

        try:
            body = json.loads(self.rfile.read(content_length))
        except json.JSONDecodeError as e:
            self._send_json({"ok": False, "error": f"Invalid JSON: {e}"}, 400)
            return

        name = body.get("name")
        value = body.get("value")

        if not name:
            self._send_json({"ok": False, "error": "Missing 'name' field"}, 400)
            return
        if value is None:
            self._send_json({"ok": False, "error": "Missing 'value' field"}, 400)
            return

        try:
            value = float(value)
        except (TypeError, ValueError):
            self._send_json({"ok": False, "error": f"Invalid value: {value}"}, 400)
            return

        dp = WRITABLE_DATAPOINTS.get(name)
        if dp is None:
            self._send_json({"ok": False, "error": f"Unknown datapoint: {name}"}, 404)
            return

        result = self.writer.write(dp, value)
        status = 200 if result.get("ok") else 400
        self._send_json(result, status)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=f"Hoval CAN-Bus Write API v{VERSION}")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Path to YAML config file")
    parser.add_argument("--port", type=int, default=None,
                        help="Override HTTP port (default: 9102)")
    parser.add_argument("--bind", type=str, default=None,
                        help="Override bind address (default: 127.0.0.1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate and log writes without sending CAN frames")
    parser.add_argument("--enable-advanced", action="store_true",
                        help="Unlock Tier 3 (advanced) datapoints")
    parser.add_argument("--log-level", type=str, default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Override log level")
    return parser.parse_args()


def main():
    args = parse_args()

    # Load configuration
    cfg = ImporterConfig()
    if args.config and Path(args.config).exists():
        try:
            cfg = ImporterConfig.from_yaml(args.config)
        except Exception as e:
            logging.warning("Failed to load config %s: %s — using defaults", args.config, e)

    # CLI overrides
    if args.port:
        cfg.http_port = args.port
    if args.bind:
        cfg.http_bind = args.bind
    if args.dry_run:
        cfg.dry_run = True
    if args.enable_advanced:
        cfg.enable_advanced = True
    if args.log_level:
        cfg.log_level = args.log_level

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    logging.info("Hoval CAN-Bus Write API v%s", VERSION)
    logging.info("CAN: %s @ %d bit/s (arb_id msg_id=%d)", cfg.can_interface, cfg.can_bitrate, cfg.msg_id)
    logging.info("HTTP: http://%s:%d", cfg.http_bind, cfg.http_port)
    logging.info("Dry-run: %s, Advanced: %s", cfg.dry_run, cfg.enable_advanced)

    available = sum(1 for dp in WRITABLE_DATAPOINTS.values()
                    if dp.tier < 3 or cfg.enable_advanced)
    logging.info("Writable datapoints: %d/%d available", available, len(WRITABLE_DATAPOINTS))

    # Open CAN bus
    writer = CANWriter(cfg)
    if not cfg.dry_run:
        try:
            writer.open()
        except Exception as e:
            logging.error("Failed to open CAN interface %s: %s", cfg.can_interface, e)
            sys.exit(1)

    # Setup HTTP handler with references to writer and config
    ImporterHandler.writer = writer
    ImporterHandler.cfg = cfg

    # Start HTTP server
    server = HTTPServer((cfg.http_bind, cfg.http_port), ImporterHandler)
    server_thread = threading.Thread(target=server.serve_forever, name="http-server", daemon=True)
    server_thread.start()

    logging.info("Importer running. Listening for write requests.")

    # Graceful shutdown
    shutdown_event = threading.Event()

    def shutdown(sig, frame):
        if shutdown_event.is_set():
            return
        shutdown_event.set()
        logging.info("Shutting down (signal %s)...", sig)
        server.shutdown()
        writer.close()
        logging.info("Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Main thread: wait for shutdown
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
