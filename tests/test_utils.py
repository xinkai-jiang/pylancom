import importlib.metadata

import pylancom
from pylancom.utils.utils import (
    create_hash_identifier,
    create_heartbeat_message,
    get_current_time,
)


def test_package_version():
    install_version = importlib.metadata.version("pylancom")
    package_version = pylancom.__version__
    assert (
        package_version == install_version
    ), f"Version mismatch: {package_version} != {install_version}"


def test_create_hash_identifier():
    identifier = create_hash_identifier()
    assert isinstance(identifier, str)
    assert len(identifier) == 36  # UUID length


def test_get_current_time():
    current_time = get_current_time()
    assert isinstance(current_time, str)
    print(f"Current time: {current_time}")
    assert len(current_time) == 12  # YYMMDDHHMMSS format


def test_create_heartbeat_message():
    node_id = create_hash_identifier()
    updated_time = get_current_time()
    heartbeat_message = create_heartbeat_message(node_id, updated_time)
    assert isinstance(heartbeat_message, bytes)
    assert len(heartbeat_message) == 57  # 6 + 3 + 36 + 12 = 57
    print(f"Heartbeat message: {heartbeat_message}")


if __name__ == "__main__":
    test_package_version()
    test_create_hash_identifier()
    test_get_current_time()
    test_create_heartbeat_message()
    print("All tests passed.")
