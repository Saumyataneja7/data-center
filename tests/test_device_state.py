from telemetry_generator.device_simulator import (
    create_device_registry
)

from telemetry_generator.device_state import (
    DeviceStateManager
)


def test_device_state_manager():

    servers = create_device_registry()

    manager = DeviceStateManager()

    # Initialize all servers
    manager.initialize_all(servers)

    assert len(manager.states) == 500

    # Retrieve first server
    server_id = servers[0]["server_id"]

    state = manager.get_state(server_id)

    assert state is not None
    assert 0 <= state["cpu_utilization"] <= 100
    assert 0 <= state["memory_utilization"] <= 100

    # Test persistence
    manager.save_states()

    manager2 = DeviceStateManager()

    assert manager2.load_states()

    assert len(manager2.states) == 500

    assert manager2.get_state(server_id) == state

    print("Device state tests passed!")


if __name__ == "__main__":
    test_device_state_manager()