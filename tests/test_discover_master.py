import multiprocessing as mp
import time
from typing import List

import pylancom
from pylancom import start_master_node
from pylancom.utils import search_for_master_node


def test_search_for_master_node():
    master_node = search_for_master_node(timeout=5)
    if master_node is None:
        print("Master node not found")
    else:
        print(f"Master node found at {master_node}")


def test_master_broadcast():
    try:
        master_node = start_master_node("127.0.0.1")
        master_node.spin()
    except KeyboardInterrupt:
        master_node.stop_node()
        print("Master node stopped")


def start_node_task(num: int):
    try:
        node_name = f"Node_{num}"
        print(f"Starting node {node_name}")
        node = pylancom.init_node(node_name, "127.0.0.1")
        node.spin(block=True)
    except KeyboardInterrupt:
        node.stop_node()
        print(f"Node {node_name} stopped")


if __name__ == "__main__":
    mp.Process(target=test_master_broadcast).start()
    time.sleep(1)
    node_tasks: List[mp.Process] = []
    for i in range(14):
        task = mp.Process(target=start_node_task, args=(i,))
        # task = mp.Process(target=test_search_for_master_node)
        task.start()
        node_tasks.append(task)
        # time.sleep(random.random())
    for task in node_tasks:
        task.join()
    print("All nodes have been started")
