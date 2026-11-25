import json

import numpy as np

from telemetry_generator.config import STATE_DIR, RANDOM_SEED


class DeviceStateManager:
    """
    Manages persistent operational states
    for simulated data center servers.
    """

    def __init__(self):
        self.state_file = STATE_DIR / "server_states.json"
        self.rng = np.random.default_rng(RANDOM_SEED)
        self.states = {}

    def initialize_state(self, server):
        server_id = server["server_id"]

        self.states[server_id] = {
            "cpu_utilization": round(float(self.rng.uniform(15, 55)), 2),
            "memory_utilization": round(float(self.rng.uniform(25, 65)), 2),
            "disk_utilization": round(float(self.rng.uniform(20, 70)), 2),
            "temperature": round(float(self.rng.uniform(22, 32)), 2),
            "power_consumption": round(float(self.rng.uniform(150, 500)), 2),
            "network_in_mbps": round(float(self.rng.uniform(10, 200)), 2),
            "network_out_mbps": round(float(self.rng.uniform(10, 200)), 2),
            "fan_speed_rpm": int(self.rng.integers(1500, 3500)),
            "server_status": "Healthy",
        }

    def initialize_all(self, servers):
        for server in servers:
            self.initialize_state(server)

    def save_states(self):
        """
        Atomically persist the current device states.
        """

        STATE_DIR.mkdir(parents=True, exist_ok=True)

        temporary_file = self.state_file.with_suffix(".tmp")

        with open(temporary_file, "w") as file:
            json.dump(self.states, file, indent=4)

            file.flush()
            import os
            os.fsync(file.fileno())

        temporary_file.replace(self.state_file)

        print(f"Saved {len(self.states)} server states.")

    def load_states(self):
        if not self.state_file.exists():
            print("No previous states found.")
            return False

        with open(self.state_file, "r") as file:
            self.states = json.load(file)

        print(f"Loaded {len(self.states)} server states.")

        return True

    def restore_states(self, states):
        """
        Restore a complete state snapshot from a recovery transaction.
        """

        if not isinstance(states, dict):
            raise ValueError("Invalid state snapshot.")

        self.states = states

    def get_state(self, server_id):
        return self.states.get(server_id)

    def update_state(self, server_id, new_state):
        if server_id not in self.states:
            raise KeyError(f"Unknown server: {server_id}")

        self.states[server_id].update(new_state)