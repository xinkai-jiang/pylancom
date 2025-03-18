import logging
import multiprocessing as mp
import time
from typing import List

from pylancom.lancom_type import IPAddress
from pylancom.nodes.abstract_node import AbstractNode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestNode(AbstractNode):
    def initialize_event_loop(self):
        self.submit_loop_task(self.listen_loop, False)


def start_node_task(node_name: str, node_ip: IPAddress):
    try:
        node = TestNode(node_name=node_name, node_ip=node_ip)
        node.spin()
    except KeyboardInterrupt:
        node.stop_node()
        logger.info(f"Node {node_name} stopped")


def main():
    node_tasks: List[mp.Process] = []
    node_ips = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]

    for i, node_ip in enumerate(node_ips):
        print(i)
        node_name = f"Node_{i}"
        task = mp.Process(target=start_node_task, args=(node_name, node_ip))
        task.start()
        node_tasks.append(task)
        time.sleep(1)  # Allow some time for the node to start

    time.sleep(10)  # Allow some time for nodes to communicate

    for task in node_tasks:
        task.terminate()
        task.join()

    logger.info("All nodes have been started and terminated")


if __name__ == "__main__":
    main()
