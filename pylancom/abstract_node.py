from __future__ import annotations
import abc
import multiprocessing as mp
import asyncio
import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext
from pylancom.utils import create_address, get_zmq_socket_port
from typing import Dict, Callable, Awaitable
from concurrent.futures import ThreadPoolExecutor
import time


from .log import logger
from .utils import DISCOVERY_PORT
from .utils import NodeInfo
from .utils import generate_hash


class AbstractNode(abc.ABC):

    manager: "AbstractNode" = None

    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__()
        self.zmq_context: AsyncContext = zmq.asyncio.Context()  # type: ignore
        # publisher
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:0")
        # subscribers
        self.sub_sockets: Dict[str, zmq.asyncio.Socket] = {}
        # service
        self.service_socket = self.zmq_context.socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        self.service_cbs: Dict[bytes, Callable[[bytes], Awaitable]] = {}
        # message for broadcasting
        self.local_info: NodeInfo = {
            "name": node_name,
            "nodeID": generate_hash(),
            "ip": node_ip,
            "port": 0,
            "type": "Master",
            "servicePort": 0,
            "topicPort": 0,
            "serviceList": [],
            "topicList": [],
        }
        logger.info(f"Node {node_name} starts at {node_ip}:{DISCOVERY_PORT}")
        # start the server in a thread pool
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.server_future = self.executor.submit(self.thread_task)
        # wait for the loop starts
        while not hasattr(self, "loop"):
            time.sleep(0.01)
        logger.info(f"Node {self.local_info['name']} is initialized")

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    @abc.abstractmethod
    def thread_task(self) -> None:
        raise NotImplementedError

    def spin(self) -> None:
        pass
