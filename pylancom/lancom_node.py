from __future__ import annotations
import zmq
from zmq.asyncio import Context as AsyncContext
import socket
import asyncio
from asyncio import sleep as async_sleep
from json import loads, dumps
from typing import Dict, List, Optional, Callable
import multiprocessing as mp
import traceback

import zmq.asyncio

from .lancom_master import LanComMaster
from .abstract_node import AbstractNode
from .log import logger
from .utils import MASTER_TOPIC_PORT, MASTER_SERVICE_PORT
from .utils import search_for_master_node
from .utils import ConnectionState, MasterRequestType
from .utils import TopicName
from .utils import DISCOVERY_PORT
from .utils import NodeInfo, ServiceStatus
from .utils import HashIdentifier
from .utils import bmsgsplit, send_tcp_request
from . import utils


class LanComNode(AbstractNode):

    instance: Optional[LanComNode] = None

    def __init__(
        self, node_name: str, node_ip: str, node_type: str = "PyLanComNode"
    ) -> None:
        master_ip = search_for_master_node()
        super().__init__(node_ip)
        if master_ip is None:
            raise Exception("Master node not found")
        self.master_ip = master_ip
        self.master_id = None
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.node_socket.setblocking(False)
        self.node_socket.bind((node_ip, 0))
        # local_port = self.node_socket.getsockname()[1]
        self.zmq_context: AsyncContext = zmq.asyncio.Context()  # type: ignore
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:0")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        self.local_info: NodeInfo = {
            "name": node_name,
            "nodeID": utils.create_hash_identifier(),
            "ip": node_ip,
            "port": self.node_socket.getsockname()[1],
            "type": node_type,
            "topicPort": utils.get_zmq_socket_port(self.pub_socket),
            "topicList": [],
            "servicePort": utils.get_zmq_socket_port(self.service_socket),
            "serviceList": [],
            "subscriberList": [],
        }
        self.service_cbs: Dict[str, Callable[[bytes], bytes]] = {}
        self.log_node_state()

    def log_node_state(self):
        for key, value in self.local_info.items():
            print(f"    {key}: {value}")

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    async def update_master_state_loop(self):
        update_socket = self.create_socket(socket_type=zmq.SUB)
        update_socket.connect(f"tcp://{self.master_ip}:{MASTER_TOPIC_PORT}")
        update_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        try:
            while self.running:
                message = await update_socket.recv_string()
                await self.update_master_state(message)
                await async_sleep(0.01)
        except Exception as e:
            logger.error(f"Error occurred in update_connection_state_loop: {e}")

    # async def service_loop(self):
    #     logger.info("The service loop is running...")
    #     service_socket = self.service_socket
    #     while self.running:
    #         try:
    #             bytes_msg = await service_socket.recv_multipart()
    #             service_name, request = bmsgsplit(b"".join(bytes_msg))
    #             service_name = service_name.decode()
    #             # the zmq service socket is blocked and only run one at a time
    #             if service_name in self.service_cbs.keys():
    #                 response = self.service_cbs[service_name](request)
    #                 await service_socket.send(response)
    #         except asyncio.TimeoutError:
    #             logger.error("Timeout: callback function took too long")
    #             await service_socket.send(ServiceStatus.TIMEOUT)
    #         except Exception as e:
    #             logger.error(
    #                 f"One error occurred when processing the Service "
    #                 f'"{service_name}": {e}'
    #             )
    #             traceback.print_exc()
    #             await service_socket.send(ServiceStatus.ERROR)
    #         await async_sleep(0.01)
    #     logger.info("Service loop has been stopped")

    def initialize_event_loop(self):
        self.submit_loop_task(self.service_loop, self.service_socket, self.service_cbs)
        self.submit_loop_task(self.update_master_state_loop)

    def spin_task(self) -> None:
        logger.info(f"Node {self.local_info['name']} is running...")
        return super().spin_task()

    def stop_node(self):
        super().stop_node()
        self.node_socket.close()
        logger.info(f"Node {self.local_info['name']} is stopped")

    async def update_master_state(self, message: str) -> None:
        if message != self.master_id:
            self.master_id = message
            await self.connect_to_master()
        # for topic_name in self.sub_sockets.keys():
        #     if topic_name not in state["topic"].keys():
        #         for socket in self.sub_sockets[topic_name]:
        #             socket.close()
        #         self.sub_sockets.pop(topic_name)
        # self.connection_state = state

    async def connect_to_master(self) -> None:
        logger.debug(f"Connecting to master node at {self.master_ip}")
        loop = asyncio.get_running_loop()
        addr = (self.master_ip, MASTER_SERVICE_PORT)
        await send_tcp_request(self.node_socket, addr, "Hello")

    # def disconnect_from_master(self) -> None:
    #     pass
    # self.disconnect_from_node(self.master_ip, MASTER_TOPIC_PORT)


def start_master_node(node_ip: str) -> LanComMaster:
    # master_ip = search_for_master_node()
    # if master_ip is not None:
    #     raise Exception("Master node already exists")
    master_node = LanComMaster(node_ip)
    return master_node


def master_node_task(node_ip: str) -> None:
    master_node = start_master_node(node_ip)
    master_node.spin()


def init_node(node_name: str, node_ip: str) -> LanComNode:
    # if node_name == "Master":
    #     return MasterNode(node_name, node_ip)
    master_ip = search_for_master_node()
    if master_ip is None:
        logger.info("Master node not found, starting a new master node...")
        mp.Process(target=master_node_task, args=(node_ip,)).start()
    return LanComNode(node_name, node_ip)
