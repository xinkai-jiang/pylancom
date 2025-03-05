import multiprocessing as mp
import random
import time
from typing import List

import pylancom
import pylancom.nodes
from pylancom.nodes.silent_node import SilentNode


class TestDiscoverNodes(SilentNode):
    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__(node_name, node_ip)
        self.node_name = node_name

    def initialize_event_loop(self):
        return super().initialize_event_loop()

    def process_heartbeat(self, data, ip):
        assert len(data) == 51, f"Data length: {len(data)}, expected 51"
        return super().process_heartbeat(data, ip)


def start_test_node():
    try:
        node_name = "TestDiscoverNode"
        print(f"Starting node {node_name}")
        node = TestDiscoverNodes(node_name, "127.0.0.1")
        node.spin()
        print(f"Node {node_name} stopped")
    except KeyboardInterrupt:
        node.stop_node()
        print(f"Node {node_name} stopped")


def start_node_task(num: int):
    try:
        node_name = f"Node_{num}"
        print(f"Starting node {node_name}")
        node = pylancom.init_node(node_name, "127.0.0.1")
        node.spin()
        print(f"Node {node_name} stopped")
    except KeyboardInterrupt:
        node.stop_node()
        print(f"Node {node_name} stopped")


if __name__ == "__main__":
    node_tasks: List[mp.Process] = []
    # test_node_task = mp.Process(target=start_test_node)
    # node_tasks.append(test_node_task)
    # test_node_task.start()
    for i in range(3):
        task = mp.Process(target=start_node_task, args=(i,))
        task.start()
        node_tasks.append(task)
        time.sleep(random.random())
    print("All nodes have been started")
    for task in node_tasks:
        task.join()
