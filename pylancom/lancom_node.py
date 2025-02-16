from __future__ import annotations

import multiprocessing as mp
import traceback
from asyncio import sleep as async_sleep
from json import dumps, loads
from typing import Awaitable, Callable, Dict, List, Optional

import zmq
import zmq.asyncio

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
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:0")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        self.sub_sockets: Dict[str, List[zmq.asyncio.Socket]] = {}
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
                await async_sleep(0.01)
        except Exception as e:
            logger.error(
                f"Error occurred in update_connection_state_loop: {e}"
            )
            traceback.print_exc()

    def initialize_event_loop(self):
        self.submit_loop_task(
            self.service_loop, False, self.service_socket, self.service_cbs
        )
        self.submit_loop_task(self.update_master_state_loop, False)
        self.socket_service_cb: Dict[str, Callable[[str], str]] = {
            # NodeReqType.PING.value: self.ping,
            NodeReqType.UPDATE_SUBSCRIBER.value: self.update_subscriber,
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
        topics_info: Dict[TopicName, List[ComponentInfo]] = loads(msg)
        for topic_name, publisher_list in topics_info.items():
            if topic_name not in self.sub_sockets.keys():
                continue
            for topic_info in publisher_list:
                self.subscribe_topic(topic_name, topic_info)
        # for topic_name in self.sub_sockets.keys():
        #     if topic_name not in state["topic"].keys():
        #         for socket in self.sub_sockets[topic_name]:
        #             socket.close()
        #         self.sub_sockets.pop(topic_name)
        # self.connection_state = state

    def update_subscriber(self, msg: str) -> str:
        info: ComponentInfo = utils.loads(msg)
        self.subscribe_topic(info["name"], info)
        return ResponseType.SUCCESS.value

    def subscribe_topic(
        self, topic_name: TopicName, topic_info: ComponentInfo
    ) -> None:
        # print(f"Subscribing to topic {topic_name}")
        if topic_name not in self.sub_sockets.keys():
            logger.warning(
                f"Master sending a wrong subscription request for {topic_name}"
            )
            return
        for _socket in self.sub_sockets[topic_name]:
            _socket.connect(f"tcp://{topic_info['ip']}:{topic_info['port']}")

    async def send_node_request_to_master(
        self, request_type: str, message: str
    ) -> str:
        return await self.send_request(
            request_type, self.master_ip, MASTER_SERVICE_PORT, message
        )

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
