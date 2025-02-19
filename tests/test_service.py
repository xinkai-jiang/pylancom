import multiprocessing as mp
import time
from typing import Callable, Dict, List

from utils import random_name

import pylancom
from pylancom import start_master_node
from pylancom.component import Service, ServiceProxy


def test_master_node_broadcast():
    master_node = start_master_node("127.0.0.1")
    master_node.spin()


def create_service_callback(service_name: str) -> Callable[[str], str]:
    def service_callback(msg: str) -> str:
        try:
            print(f"Service {service_name} received message: {msg}")
            return msg
        except Exception as e:
            print(f"Error in service {service_name}: {e}")
            return "Error"

    return service_callback


def start_service_node(service_names: List[str]):
    print("Starting service node")
    node_name = random_name("Node")
    node = pylancom.init_node(node_name, "127.0.0.1")
    service_dict: Dict[str, Service] = {}
    for service_name in service_names:
        service_dict[service_name] = Service(
            service_name, str, str, create_service_callback(service_name)  # type: ignore
        )
    node.spin(block=True)


def send_request(service_name: str, msg: str):
    node_name = random_name("Node")
    node = pylancom.init_node(node_name, "127.0.0.1")
    node.spin(block=False)
    response = ServiceProxy.request(service_name, str, str, msg)
    print(f"Response from {service_name}: {response}")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    p0 = mp.Process(target=test_master_node_broadcast)
    p0.start()
    time.sleep(1)
    p1 = mp.Process(target=start_service_node, args=(["A", "B"],))
    p1.start()
    time.sleep(1)
    p2 = mp.Process(target=send_request, args=("A", "A"))
    p2.start()
    p1.join()
    # p2.join()
