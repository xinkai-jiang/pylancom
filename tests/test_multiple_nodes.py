import asyncio
import logging
import multiprocessing as mp
from typing import List

import pytest

from pylancom.lancom_type import IPAddress, NodeInfo
from pylancom.nodes.abstract_node import AbstractNode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestNode(AbstractNode):
    def initialize_event_loop(self):
        self.submit_loop_task(self.multicast_loop, False)
        self.submit_loop_task(self.listen_loop, False)

    async def process_received_message(self, data: bytes) -> None:
        """Processes received multicast messages and prints them."""
        try:
            parts = data.split(b"|", 2)
            if len(parts) < 3:
                return
            _, node_id, node_info_bytes = parts[0], parts[1], parts[2]
            if node_id.decode() == self.id:
                return  # Ignore messages from itself
            node_info = NodeInfo()
            node_info.ParseFromString(node_info_bytes)
            logger.info(
                f"Received message from node {node_id.decode()}: {node_info}"
            )

        except Exception as e:
            logger.error(f"Error processing received message: {e}")


def start_node_task(node_name: str, node_ip: IPAddress):
    try:
        node = TestNode(node_name=node_name, node_ip=node_ip)
        node.spin()
    except KeyboardInterrupt:
        node.stop_node()
        logger.info(f"Node {node_name} stopped")


@pytest.mark.asyncio
async def test_multiple_nodes():
    node_tasks: List[mp.Process] = []
    node_ips = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]

    for i, node_ip in enumerate(node_ips):
        node_name = f"Node_{i}"
        task = mp.Process(target=start_node_task, args=(node_name, node_ip))
        task.start()
        node_tasks.append(task)
        await asyncio.sleep(1)  # Allow some time for the node to start

    await asyncio.sleep(10)  # Allow some time for nodes to communicate

    for task in node_tasks:
        task.terminate()
        task.join()

    logger.info("All nodes have been started and terminated")


if __name__ == "__main__":
    asyncio.run(test_multiple_nodes())
