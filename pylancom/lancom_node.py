from __future__ import annotations

import multiprocessing as mp
import socket
import traceback
from asyncio import sleep as async_sleep
from json import dumps
from typing import Awaitable, Callable, Dict, List, Optional

import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from . import utils
from .abstract_node import AbstractNode
from .config import MASTER_SERVICE_PORT, MASTER_TOPIC_PORT
from .lancom_master import LanComMaster
from .log import logger
from .type import (
    ComponentInfo,
    MasterSocketReqType,
    NodeInfo,
    NodeSocketReqType,
    ResponseType,
)
from .utils import create_request, search_for_master_node


class LanComNode(AbstractNode):
    instance: Optional[LanComNode] = None

    def __init__(
        self, node_name: str, node_ip: str, node_type: str = "PyLanComNode"
    ) -> None:
        master_ip = search_for_master_node()
        if LanComNode.instance is not None:
            raise Exception("LanComNode already exists")
        LanComNode.instance = self
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
        self.sub_sockets: Dict[str, List[zmq.asyncio.Socket]] = {}
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
        self.service_cbs: Dict[str, Callable[[bytes], Awaitable]] = {}
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
            logger.error(
                f"Error occurred in update_connection_state_loop: {e}"
            )
            traceback.print_exc()

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
        self.submit_loop_task(
            self.service_loop, False, self.service_socket, self.service_cbs
        )
        self.submit_loop_task(self.update_master_state_loop, False)
        self.socket_service_cb: Dict[str, Callable[[str], str]] = {
            # NodeSocketReqType.PING.value: self.ping,
            NodeSocketReqType.SUBSCRIBE_TOPIC.value: self.subscribe_topic,
            # LanComSocketReqType.NODE_OFFLINE.value: self.node_offline,
            # LanComSocketReqType.GET_NODES_INFO.value: self.get_nodes_info,
        }

    def spin_task(self) -> None:
        logger.info(f"Node {self.local_info['name']} is running...")
        return super().spin_task()

    def stop_node(self):
        logger.info(f"Stopping node {self.local_info['name']}...")
        try:
            # NOTE: the loop will be stopped when pressing Ctrl+C
            # so we need to create a new socket to send offline request
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.master_ip, MASTER_SERVICE_PORT))
                request = create_request(
                    MasterSocketReqType.NODE_OFFLINE.value,
                    self.local_info["nodeID"],
                )
                sock.sendall(request.encode("utf-8"))
                response = sock.recv(4096)
                return response.decode("utf-8")
        except Exception as e:
            logger.error(f"Error sending offline request to master: {e}")
        super().stop_node()
        self.node_socket.close()
        logger.info(f"Node {self.local_info['name']} is stopped")

    async def update_master_state(self, message: str) -> None:
        if message != self.master_id:
            self.master_id = message
            logger.debug(f"Connecting to master node at {self.master_ip}")
            await self.send_socket_request_to_master(
                MasterSocketReqType.REGISTER_NODE.value, dumps(self.local_info)
            )
        # for topic_name in self.sub_sockets.keys():
        #     if topic_name not in state["topic"].keys():
        #         for socket in self.sub_sockets[topic_name]:
        #             socket.close()
        #         self.sub_sockets.pop(topic_name)
        # self.connection_state = state

    def subscribe_topic(self, msg: str) -> str:
        info: ComponentInfo = utils.loads(msg)
        topic_name = info["name"]
        if topic_name not in self.sub_sockets.keys():
            logger.warning(
                f"Master sending a wrong subscription request for {topic_name}"
            )
            return ResponseType.ERROR.value
        for _socket in self.sub_sockets[topic_name]:
            _socket.connect(f"tcp://{info['ip']}:{info['port']}")
        return ResponseType.SUCCESS.value

    async def send_socket_request_to_master(
        self, request_type: str, message: str
    ) -> None:
        addr = (self.master_ip, MASTER_SERVICE_PORT)
        request = create_request(request_type, message)
        await send_tcp_request_async(self.node_socket, addr, request)
        print("Request sent to master")

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
