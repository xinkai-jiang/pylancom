import multiprocessing as mp
import time
from typing import Callable, List

from utils import random_name

import pylancom
from pylancom import start_master_node
from pylancom.component import Publisher, Subscriber


def test_master_node_broadcast():
    master_node = start_master_node("127.0.0.1")
    master_node.spin()


def create_service_callback(service_name: str) -> Callable[[str], str]:
    def service_callback(msg: str) -> str:
        print(f"Service {service_name} received message: {msg}")
        return msg

    return service_callback


def create_subscriber_callback(
    topic_name: str,
) -> Callable[[str], None]:
    def subscriber_callback(msg: str) -> None:
        print(f"Subscriber {topic_name} received message: {msg}")

    return subscriber_callback


def start_node(publisher_list: List[str], subscriber_list: List[str]):
    node_name = random_name("Node")
    node = pylancom.init_node(node_name, "127.0.0.1")
    for name in publisher_list:
        Publisher(name)
    for name in subscriber_list:
        Subscriber(name, str, create_subscriber_callback(name))
    # for _ in range(5):
    # service = Service(random_name("Service"), str, str, create_service_callback())
    node.spin()


if __name__ == "__main__":
    p0 = mp.Process(target=test_master_node_broadcast)
    p0.start()
    time.sleep(1)
    p1 = mp.Process(target=start_node, args=(["A", "B"], ["C", "D"]))
    p2 = mp.Process(target=start_node, args=(["C", "D"], ["A", "B"]))
    p1.start()
    time.sleep(1)
    print("Starting second node")
    p2.start()
    p1.join()
    # p2.join()
