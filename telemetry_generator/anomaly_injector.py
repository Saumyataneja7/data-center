import hashlib

import numpy as np

from telemetry_generator.config import (
    ANOMALY_RATE_MIN,
    ANOMALY_RATE_MAX,
)


class AnomalyInjector:
    """
    Injects controlled operational anomalies
    into simulated server telemetry.
    """

    ANOMALY_TYPES = [
        "CPU_SPIKE",
        "MEMORY_LEAK",
        "OVERHEATING",
        "DISK_PRESSURE",
        "NETWORK_SURGE",
        "POWER_ANOMALY",
    ]

    def __init__(self, state_manager):
        self.state_manager = state_manager

    @staticmethod
    def clamp(value, minimum, maximum):
        return max(minimum, min(value, maximum))

    @staticmethod
    def create_rng(timestamp):
        """Create reproducible randomness per batch."""

        seed = int.from_bytes(
            hashlib.sha256(
                timestamp.isoformat().encode()
            ).digest()[:8],
            byteorder="big",
        )

        return np.random.default_rng(seed)

    def apply_anomaly(self, record, anomaly_type, rng):
        """Modify telemetry according to anomaly type."""

        server_id = record["server_id"]

        if anomaly_type == "CPU_SPIKE":

            record["cpu_utilization"] = round(
                self.clamp(
                    record["cpu_utilization"]
                    + rng.uniform(25, 45),
                    0,
                    99.5,
                ),
                2,
            )

        elif anomaly_type == "MEMORY_LEAK":

            record["memory_utilization"] = round(
                self.clamp(
                    record["memory_utilization"]
                    + rng.uniform(20, 35),
                    0,
                    99.5,
                ),
                2,
            )

        elif anomaly_type == "OVERHEATING":

            record["temperature"] = round(
                self.clamp(
                    record["temperature"]
                    + rng.uniform(12, 22),
                    18,
                    70,
                ),
                2,
            )

            record["fan_speed_rpm"] = int(
                self.clamp(
                    record["fan_speed_rpm"] + 1500,
                    1000,
                    8000,
                )
            )

        elif anomaly_type == "DISK_PRESSURE":

            record["disk_utilization"] = round(
                self.clamp(
                    record["disk_utilization"]
                    + rng.uniform(20, 35),
                    0,
                    99.9,
                ),
                2,
            )

        elif anomaly_type == "NETWORK_SURGE":

            record["network_in_mbps"] = round(
                self.clamp(
                    record["network_in_mbps"]
                    * rng.uniform(2.5, 4.0),
                    1,
                    1000,
                ),
                2,
            )

            record["network_out_mbps"] = round(
                self.clamp(
                    record["network_out_mbps"]
                    * rng.uniform(2.5, 4.0),
                    1,
                    1000,
                ),
                2,
            )

        elif anomaly_type == "POWER_ANOMALY":

            record["power_consumption"] = round(
                self.clamp(
                    record["power_consumption"]
                    * rng.uniform(1.5, 2.0),
                    80,
                    1500,
                ),
                2,
            )

        # Recalculate operational status.
        if (
            record["cpu_utilization"] >= 90
            or record["memory_utilization"] >= 95
            or record["temperature"] >= 45
            or record["disk_utilization"] >= 95
        ):
            record["server_status"] = "Critical"

        elif (
            record["cpu_utilization"] >= 80
            or record["memory_utilization"] >= 85
            or record["temperature"] >= 38
            or record["disk_utilization"] >= 85
        ):
            record["server_status"] = "Warning"

        else:
            record["server_status"] = "Healthy"

        # Assign anomaly severity.
        severity = (
            "Critical"
            if record["server_status"] == "Critical"
            else "Warning"
        )

        record["is_anomaly"] = True
        record["anomaly_type"] = anomaly_type
        record["anomaly_severity"] = severity

        # Preserve anomalous values as the latest
        # server state for the next interval.
        state_fields = [
            "cpu_utilization",
            "memory_utilization",
            "disk_utilization",
            "network_in_mbps",
            "network_out_mbps",
            "temperature",
            "power_consumption",
            "fan_speed_rpm",
            "server_status",
        ]

        self.state_manager.update_state(
            server_id,
            {
                field: record[field]
                for field in state_fields
            },
        )

    def inject_anomalies(self, records, timestamp):
        """
        Inject anomalies into 3–5% of records.

        Each record can receive at most one anomaly.
        """

        if not records:
            return records

        rng = self.create_rng(timestamp)

        for record in records:
            record["is_anomaly"] = False
            record["anomaly_type"] = None
            record["anomaly_severity"] = None

        anomaly_rate = rng.uniform(
            ANOMALY_RATE_MIN,
            ANOMALY_RATE_MAX,
        )

        anomaly_count = int(
            np.ceil(len(records) * anomaly_rate)
        )

        anomaly_indices = rng.choice(
            len(records),
            size=anomaly_count,
            replace=False,
        )

        for index in anomaly_indices:

            anomaly_type = str(
                rng.choice(self.ANOMALY_TYPES)
            )

            self.apply_anomaly(
                records[index],
                anomaly_type,
                rng,
            )

        return records