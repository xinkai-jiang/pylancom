from utils import random_name

import pylancom
from pylancom.component import Publisher, Service, Subscriber


def service_callback(msg: str) -> str:
    print(msg)
    return msg


def subscriber_callback(msg: str) -> None:
    print(msg)


def test_initialize_node():
    node = pylancom.init_node(random_name("Node"), "127.0.0.1")
    for i in range(5):
        Publisher(f"topic{i}")
        Subscriber(f"topic{i}", str, subscriber_callback)
    for _ in range(5):
        service = Service(random_name("Service"), str, str, service_callback)
    node.spin()


if __name__ == "__main__":
    test_initialize_node()
