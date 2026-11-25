from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib

import numpy as np

from telemetry_generator.config import (
    TELEMETRY_INTERVAL_MINUTES,
)


class TelemetryGenerator:
    """
    Generates stateful, workload-aware telemetry
    for simulated data center servers.
    """

    TIMEZONE = ZoneInfo("Asia/Kolkata")

    # Different workload profiles by server type.
    WORKLOAD_PROFILES = {
        "Application": {
            "cpu_base": 42,
            "memory_base": 55,
            "network_base": 140,
            "disk_growth": 0.003,
        },
        "Database": {
            "cpu_base": 55,
            "memory_base": 72,
            "network_base": 180,
            "disk_growth": 0.008,
        },
        "Storage": {
            "cpu_base": 25,
            "memory_base": 48,
            "network_base": 110,
            "disk_growth": 0.005,
        },
        "Web": {
            "cpu_base": 38,
            "memory_base": 50,
            "network_base": 220,
            "disk_growth": 0.002,
        },
        "Compute": {
            "cpu_base": 65,
            "memory_base": 68,
            "network_base": 130,
            "disk_growth": 0.003,
        },
    }

    def __init__(self, servers, state_manager):
        self.servers = servers
        self.state_manager = state_manager

    @staticmethod
    def clamp(value, minimum, maximum):
        """Keep a metric within its valid range."""
        return max(minimum, min(value, maximum))

    def get_rng(self, server_id, timestamp):
        """
        Create a reproducible random generator for
        each server and telemetry interval.
        """

        seed_string = (
            f"{server_id}|{timestamp.isoformat()}"
        )

        seed = int.from_bytes(
            hashlib.sha256(
                seed_string.encode()
            ).digest()[:8],
            byteorder="big",
        )

        return np.random.default_rng(seed)

    def get_daily_workload_factor(self, timestamp):
        """
        Calculate daily workload variation.

        Activity generally rises during the day
        and decreases overnight.
        """

        hour = (
            timestamp.hour
            + timestamp.minute / 60
        )

        daily_cycle = np.sin(
            2 * np.pi * (hour - 8) / 24
        )

        return 0.65 + (0.25 * daily_cycle)

    def generate_server_telemetry(
        self,
        server,
        timestamp,
    ):
        """Generate one telemetry record for a server."""

        server_id = server["server_id"]

        previous = self.state_manager.get_state(
            server_id
        )

        if previous is None:
            raise ValueError(
                f"Missing state for {server_id}"
            )

        rng = self.get_rng(server_id, timestamp)

        profile = self.WORKLOAD_PROFILES[
            server["server_type"]
        ]

        workload_factor = (
            self.get_daily_workload_factor(timestamp)
        )

        # ======================================
        # CPU UTILIZATION
        # ======================================

        cpu_target = (
            profile["cpu_base"] * workload_factor
        )

        cpu = (
            previous["cpu_utilization"] * 0.65
            + cpu_target * 0.35
            + rng.normal(0, 3)
        )

        cpu = self.clamp(cpu, 5, 98)

        # ======================================
        # MEMORY UTILIZATION
        # ======================================

        memory_target = (
            profile["memory_base"]
            + (cpu - 50) * 0.15
        )

        memory = (
            previous["memory_utilization"] * 0.80
            + memory_target * 0.20
            + rng.normal(0, 1.5)
        )

        memory = self.clamp(memory, 10, 98)

        # ======================================
        # DISK UTILIZATION
        # ======================================

        disk = (
            previous["disk_utilization"]
            + profile["disk_growth"]
            + rng.normal(0, 0.02)
        )

        disk = self.clamp(disk, 5, 99)

        # ======================================
        # NETWORK TRAFFIC
        # ======================================

        network_in = (
            profile["network_base"]
            * workload_factor
            * (cpu / 50)
            + rng.normal(0, 8)
        )

        network_in = self.clamp(
            network_in, 1, 1000
        )

        network_out = (
            network_in * rng.uniform(0.65, 1.15)
            + rng.normal(0, 5)
        )

        network_out = self.clamp(
            network_out, 1, 1000
        )

        # ======================================
        # TEMPERATURE
        # ======================================

        temperature_target = (
            22 + (cpu * 0.18)
        )

        temperature = (
            previous["temperature"] * 0.75
            + temperature_target * 0.25
            + rng.normal(0, 0.5)
        )

        temperature = self.clamp(
            temperature, 18, 65
        )

        # ======================================
        # POWER CONSUMPTION
        # ======================================

        power_base = (
            100 + server["cpu_cores"] * 4
        )

        power = (
            power_base * (0.45 + cpu / 100)
            + rng.normal(0, 8)
        )

        power = self.clamp(
            power, 80, 1200
        )

        # ======================================
        # FAN SPEED
        # ======================================

        fan_speed = (
            1400 + temperature * 65
            + rng.normal(0, 100)
        )

        fan_speed = int(
            self.clamp(fan_speed, 1000, 8000)
        )

        # ======================================
        # SERVER STATUS
        # ======================================

        if (
            cpu >= 90
            or memory >= 95
            or temperature >= 45
            or disk >= 95
        ):
            status = "Critical"

        elif (
            cpu >= 80
            or memory >= 85
            or temperature >= 38
            or disk >= 85
        ):
            status = "Warning"

        else:
            status = "Healthy"

        # ======================================
        # UPDATE PERSISTENT STATE
        # ======================================

        new_state = {
            "cpu_utilization": round(cpu, 2),
            "memory_utilization": round(memory, 2),
            "disk_utilization": round(disk, 2),
            "network_in_mbps": round(network_in, 2),
            "network_out_mbps": round(network_out, 2),
            "temperature": round(temperature, 2),
            "power_consumption": round(power, 2),
            "fan_speed_rpm": fan_speed,
            "server_status": status,
        }

        self.state_manager.update_state(
            server_id,
            new_state,
        )

        # ======================================
        # CREATE TELEMETRY RECORD
        # ======================================

        return {
            **server,
            "timestamp": timestamp.isoformat(),
            **new_state,
        }

    def generate_batch(self, timestamp=None):
        """
        Generate one complete telemetry batch
        for all registered servers.
        """

        if timestamp is None:
            timestamp = datetime.now(
                self.TIMEZONE
            )

        if timestamp.tzinfo is None:
            raise ValueError(
                "Timestamp must be timezone-aware."
            )

        timestamp = timestamp.astimezone(
            self.TIMEZONE
        )

        if (
            timestamp.minute
            % TELEMETRY_INTERVAL_MINUTES != 0
            or timestamp.second != 0
            or timestamp.microsecond != 0
        ):
            raise ValueError(
                "Timestamp must align with a "
                "15-minute telemetry interval."
            )

        records = []

        for server in self.servers:
            record = self.generate_server_telemetry(
                server,
                timestamp,
            )

            records.append(record)

        return records