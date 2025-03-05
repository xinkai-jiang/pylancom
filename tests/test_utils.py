import importlib.metadata

import pylancom
from pylancom.utils.utils import (
    create_hash_identifier,
    create_heartbeat_message,
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


def test_create_heartbeat_message():
    node_id = create_hash_identifier()
    heartbeat_message = create_heartbeat_message(node_id, 0, 0)
    assert isinstance(heartbeat_message, bytes)
    assert len(heartbeat_message) == 51  # 6 + 3 + 36 + 12 = 51
    print(f"Heartbeat message: {heartbeat_message}")


if __name__ == "__main__":
    test_package_version()
    test_create_hash_identifier()
    test_create_heartbeat_message()
    print("All tests passed.")
