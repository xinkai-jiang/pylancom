from __future__ import annotations

import multiprocessing as mp
import traceback
from json import dumps, loads
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import zmq
import zmq.asyncio
from zmq.asyncio import Socket as AsyncSocket

from . import utils
from .abstract_node import AbstractNode
from .config import MASTER_SERVICE_PORT, MASTER_TOPIC_PORT
from .lancom_master import LanComMaster
from .log import logger
from .type import (
    ComponentInfo,
    MasterReqType,
    NodeInfo,
    NodeReqType,
    ResponseType,
    TopicName,
)
from .utils import search_for_master_node


class LanComNode(AbstractNode):
    instance: Optional[LanComNode] = None

    def __init__(
        self, node_name: str, node_ip: str, node_type: str = "LanComNode"
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
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:0")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        self.sub_sockets: Dict[str, List[Tuple[AsyncSocket, Callable]]] = {}
        self.local_info: NodeInfo = {
            "name": node_name,
            "nodeID": utils.create_hash_identifier(),
            "ip": node_ip,
            "port": utils.get_zmq_socket_port(self.node_socket),
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

    async def update_master_state_loop(self):
        update_socket = self.create_socket(socket_type=zmq.SUB)
        update_socket.connect(f"tcp://{self.master_ip}:{MASTER_TOPIC_PORT}")
        update_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        try:
            while self.running:
                message = await update_socket.recv_string()
                await self.update_master_state(message)
                # await async_sleep(0.01)
        except Exception as e:
            logger.error(
                f"Error occurred in update_connection_state_loop: {e}"
            )
            traceback.print_exc()

    def initialize_event_loop(self):
        self.submit_loop_task(
            self.service_loop, False, self.service_socket, self.service_cbs
        )
        node_service_cb: Dict[str, Callable[[bytes], bytes]] = {
            NodeReqType.UPDATE_SUBSCRIPTION.value: self.update_subscription,
        }
        self.submit_loop_task(
            self.service_loop, False, self.node_socket, node_service_cb
        )
        self.submit_loop_task(self.update_master_state_loop, False)

    def spin_task(self) -> None:
        logger.info(f"Node {self.local_info['name']} is running...")
        return super().spin_task()

    def stop_node(self):
        logger.info(f"Stopping node {self.local_info['name']}...")
        try:
            # NOTE: the loop will be stopped when pressing Ctrl+C
            # so we need to create a new socket to send offline request
            request_socket = zmq.Context().socket(zmq.REQ)
            request_socket.connect(
                f"tcp://{self.master_ip}:{MASTER_SERVICE_PORT}"
            )
            node_id = self.local_info["nodeID"]
            request_socket.send_string(
                f"{MasterReqType.NODE_OFFLINE.value}|{node_id}"
            )
        except Exception as e:
            logger.error(f"Error sending offline request to master: {e}")
            traceback.print_exc()
        super().stop_node()
        self.node_socket.close()
        logger.info(f"Node {self.local_info['name']} is stopped")

    async def update_master_state(self, message: str) -> None:
        if message == self.master_id:
            return
        self.master_id = message
        logger.debug(f"Connecting to master node at {self.master_ip}")
        msg = await self.send_node_request_to_master(
            MasterReqType.REGISTER_NODE.value, dumps(self.local_info)
        )
        publisher_info: Dict[TopicName, List[ComponentInfo]] = loads(msg)
        for topic_name, publisher_list in publisher_info.items():
            if topic_name not in self.sub_sockets.keys():
                continue
            for topic_info in publisher_list:
                self.subscribe_topic(topic_name, topic_info)

    def update_subscription(self, msg: bytes) -> bytes:
        publisher_info: ComponentInfo = utils.loads(msg.decode())
        self.subscribe_topic(publisher_info["name"], publisher_info)
        return ResponseType.SUCCESS.value.encode()

    def register_subscription(
        self,
        topic_name: TopicName,
        zmq_socket: AsyncSocket,
        handle_func: Callable[[], Awaitable],
    ) -> None:
        if topic_name not in self.sub_sockets:
            self.sub_sockets[topic_name] = []
        self.sub_sockets[topic_name].append((zmq_socket, handle_func))

    def subscribe_topic(
        self, topic_name: TopicName, publisher_info: ComponentInfo
    ) -> None:
        if topic_name not in self.sub_sockets.keys():
            logger.warning(f"Wrong subscription request for {topic_name}")
            return
        for _socket, handle_func in self.sub_sockets[topic_name]:
            _socket.connect(
                f"tcp://{publisher_info['ip']}:{publisher_info['port']}"
            )
            self.submit_loop_task(handle_func, False)
        logger.info(
            f"Subscribers from Node {self.local_info['name']} "
            f"have been subscribed to topic {topic_name}"
        )

    async def send_node_request_to_master(
        self, request_type: str, message: str
    ) -> str:
        result = await self.send_request(
            request_type, self.master_ip, MASTER_SERVICE_PORT, message
        )
        return result


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
