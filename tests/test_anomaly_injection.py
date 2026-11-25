from datetime import datetime
from zoneinfo import ZoneInfo
from math import ceil

from telemetry_generator.device_simulator import (
    create_device_registry,
)

from telemetry_generator.device_state import (
    DeviceStateManager,
)

from telemetry_generator.telemetry_generator import (
    TelemetryGenerator,
)

from telemetry_generator.anomaly_injector import (
    AnomalyInjector,
)

from telemetry_generator.config import (
    ANOMALY_RATE_MIN,
    ANOMALY_RATE_MAX,
)


def test_anomaly_injection():

    servers = create_device_registry()

    state_manager = DeviceStateManager()
    state_manager.initialize_all(servers)

    generator = TelemetryGenerator(
        servers,
        state_manager,
    )

    injector = AnomalyInjector(state_manager)

    timestamp = datetime(
        2026, 11, 25, 15, 0,
        tzinfo=ZoneInfo("Asia/Kolkata"),
    )

    # Generate normal telemetry.
    records = generator.generate_batch(timestamp)

    # Inject anomalies.
    records = injector.inject_anomalies(
        records,
        timestamp,
    )

    assert len(records) == 500

    anomalous_records = [
        r for r in records if r["is_anomaly"]
    ]

    anomaly_count = len(anomalous_records)

    minimum = ceil(500 * ANOMALY_RATE_MIN)
    maximum = ceil(500 * ANOMALY_RATE_MAX)

    assert minimum <= anomaly_count <= maximum

    # Validate anomaly labels.
    for record in anomalous_records:

        assert record["anomaly_type"] in (
            injector.ANOMALY_TYPES
        )

        assert record["anomaly_severity"] in (
            "Warning",
            "Critical",
        )

    # Validate normal records.
    normal_records = [
        r for r in records if not r["is_anomaly"]
    ]

    for record in normal_records:

        assert record["anomaly_type"] is None
        assert record["anomaly_severity"] is None

    # Check anomaly distribution.
    anomaly_distribution = {}

    for record in anomalous_records:

        anomaly_type = record["anomaly_type"]

        anomaly_distribution[anomaly_type] = (
            anomaly_distribution.get(anomaly_type, 0) + 1
        )

    print("Total records:", len(records))
    print("Anomalous records:", anomaly_count)
    print("Normal records:", len(normal_records))
    print("Anomaly distribution:", anomaly_distribution)

    print("Anomaly injection tests passed!")


if __name__ == "__main__":
    test_anomaly_injection()