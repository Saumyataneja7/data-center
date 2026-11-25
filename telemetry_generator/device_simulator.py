import numpy as np

from telemetry_generator.config import (
    DATA_CENTERS,
    RANDOM_SEED,
    TOTAL_SERVERS,
)


def create_device_registry():
    """
    Create static metadata for all simulated servers.

    Returns:
        list[dict]: Server registry.
    """

    rng = np.random.default_rng(RANDOM_SEED)

    server_types = [
        "Application",
        "Database",
        "Storage",
        "Web",
        "Compute",
    ]

    server_probabilities = [
        0.30,
        0.20,
        0.15,
        0.20,
        0.15,
    ]

    registry = []

    for dc in DATA_CENTERS:

        for server_number in range(
            1, dc["server_count"] + 1
        ):

            server_id = (
                f"{dc['datacenter_id']}"
                f"-SRV-{server_number:03d}"
            )

            server_type = rng.choice(
                server_types,
                p=server_probabilities,
            )

            rack_number = ((server_number - 1) // 5) + 1

            rack_id = (
                f"{dc['datacenter_id']}"
                f"-RACK-{rack_number:02d}"
            )

            registry.append({
                "server_id": server_id,
                "datacenter_id": dc["datacenter_id"],
                "datacenter_name": dc["datacenter_name"],
                "city": dc["city"],
                "country": dc["country"],
                "rack_id": rack_id,
                "server_type": str(server_type),
                "operating_system": str(
                    rng.choice(["Linux", "Windows"])
                ),
                "cpu_cores": int(
                    rng.choice([8, 16, 32, 64])
                ),
                "memory_gb": int(
                    rng.choice([32, 64, 128, 256])
                ),
            })

    assert len(registry) == TOTAL_SERVERS

    return registry