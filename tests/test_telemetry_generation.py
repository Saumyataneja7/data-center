from datetime import datetime
from zoneinfo import ZoneInfo

from telemetry_generator.device_simulator import (
    create_device_registry,
)

from telemetry_generator.device_state import (
    DeviceStateManager,
)

from telemetry_generator.telemetry_generator import (
    TelemetryGenerator,
)


def test_telemetry_generation():

    # Create 500 simulated servers
    servers = create_device_registry()

    # Initialize server states
    state_manager = DeviceStateManager()
    state_manager.initialize_all(servers)

    # Create telemetry generator
    generator = TelemetryGenerator(
        servers,
        state_manager,
    )

    timezone = ZoneInfo("Asia/Kolkata")

    # Generate first batch at 10:00 AM
    timestamp_1 = datetime(
        2026, 11, 25, 10, 0,
        tzinfo=timezone,
    )

    batch_1 = generator.generate_batch(timestamp_1)

    assert len(batch_1) == 500

    # Generate second batch at 10:15 AM
    timestamp_2 = datetime(
        2026, 11, 25, 10, 15,
        tzinfo=timezone,
    )

    batch_2 = generator.generate_batch(timestamp_2)

    assert len(batch_2) == 500

    # Validate required metrics
    required_fields = [
        "server_id",
        "timestamp",
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

    for field in required_fields:
        assert field in batch_1[0]

    # Validate utilization ranges
    for record in batch_2:
        assert 0 <= record["cpu_utilization"] <= 100
        assert 0 <= record["memory_utilization"] <= 100
        assert 0 <= record["disk_utilization"] <= 100
        assert record["temperature"] >= 18
        assert record["power_consumption"] > 0

    # Confirm state changed across intervals
    server_id = servers[0]["server_id"]

    first_cpu = next(
        r["cpu_utilization"]
        for r in batch_1
        if r["server_id"] == server_id
    )

    second_cpu = next(
        r["cpu_utilization"]
        for r in batch_2
        if r["server_id"] == server_id
    )

    print("First interval CPU:", first_cpu)
    print("Second interval CPU:", second_cpu)

    # Persist the latest server states
    state_manager.save_states()

    print("First batch records:", len(batch_1))
    print("Second batch records:", len(batch_2))
    print("Telemetry generation tests passed!")


if __name__ == "__main__":
    test_telemetry_generation()