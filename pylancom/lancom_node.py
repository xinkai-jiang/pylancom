from __future__ import annotations
import zmq
from asyncio import sleep as async_sleep
from json import dumps
from typing import Dict, List, Optional, Tuple
import multiprocessing as mp

from .lancom_master import LanComMasterNode
from .abstract_node import AbstractNode
from .log import logger
from .utils import MASTER_TOPIC_PORT
from .utils import search_for_master_node


class LanComNode(AbstractNode):

    instance: Optional[LanComNode] = None

    def __init__(self, node_name: str, node_ip: str) -> None:
        master_ip = search_for_master_node()
        if master_ip is None:
            raise Exception("Master node not found")
        self.master_ip = master_ip
        super().__init__(node_name, "LanComNode", node_ip)

    def initialize_event_loop(self):
        self.submit_loop_task(self.update_connection_state_loop)

    async def update_connection_state_loop(self):
        update_socket = self.create_socket(socket_type=zmq.SUB)
        update_socket.connect(f"tcp://{self.master_ip}:{MASTER_TOPIC_PORT}")
        update_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        try:
            while self.running:
                print("Waiting for message")
                message = await update_socket.recv_string()
                print(f"Received message: {message}")
                await async_sleep(0.01)
        except Exception as e:
            logger.error(f"Error occurred in update_connection_state_loop: {e}")


def start_master_node(node_ip: str) -> None:
    master_node = LanComMasterNode(node_ip)
    master_node.spin()


def init_node(node_name: str, node_ip: str) -> LanComNode:
    # if node_name == "Master":
    #     return MasterNode(node_name, node_ip)
    master_ip = search_for_master_node()
    if master_ip is None:
        mp.Process(target=start_master_node, args=(node_ip,)).start()
    return LanComNode(node_name, node_ip)
