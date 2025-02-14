from __future__ import annotations
from typing import Dict, Optional, List, Callable
import zmq
import asyncio
from asyncio import sleep as async_sleep
import socket
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_BROADCAST
import struct
from json import dumps, loads
import traceback
import time

import zmq.asyncio

from .log import logger
from .utils import DISCOVERY_PORT, MASTER_SERVICE_PORT, MASTER_TOPIC_PORT
from .utils import IPAddress, HashIdentifier, ServiceName, TopicName
from .utils import NodeInfo, ConnectionState, ComponentInfo

# from .abstract_node import AbstractNode
from .utils import bmsgsplit, create_hash_identifier
from .utils import ServiceStatus
from .utils import ServiceStatus, MasterRequestType
from .abstract_node import AbstractNode

__version__ = "0.1.0"


class NodesInfoManager:

    def __init__(self, master_id: HashIdentifier) -> None:
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.topics_info: Dict[TopicName, List[ComponentInfo]] = {}
        self.subscribers_info: Dict[HashIdentifier, List[ComponentInfo]] = {}
        self.services_info: Dict[ServiceName, ComponentInfo] = {}

        # self.connection_state: ConnectionState = {
        #     "masterID": local_info["nodeID"],
        #     "timestamp": time.time(),
        #     "topic": {},
        #     "service": {}
        # }
        # # local info is the master node info
        # self.local_info = local_info
        # self.node_id = local_info["nodeID"]

    def get_nodes_info(self) -> Dict[HashIdentifier, NodeInfo]:
        return self.nodes_info

    def check_service(self, service_name: ServiceName) -> Optional[NodeInfo]:
        for info in self.nodes_info.values():
            if service_name in info["serviceList"]:
                return info
        return None

    def check_topic(self, topic_name: TopicName) -> Optional[NodeInfo]:
        for info in self.nodes_info.values():
            if topic_name in info["topicList"]:
                return info
        return None

    # def register_node(self, info: NodeInfo):
    #     node_id = info["nodeID"]
    #     if node_id not in self.nodes_info.keys():
    #         logger.info(f"Node {info['name']} has been launched")
    #         topic_state = self.connection_state["topic"]
    #         for topic in info["topicList"]:
    #             topic_state[topic["name"]].append(topic)
    #         service_state = self.connection_state["service"]
    #         for service in info["serviceList"]:
    #             service_state[service["name"]] = service
    #     self.nodes_info[node_id] = info

    def update_node(self, info: NodeInfo):
        node_id = info["nodeID"]
        if node_id in self.nodes_info.keys():
            self.nodes_info[node_id] = info

    def remove_node(self, node_id: HashIdentifier):
        try:
            if node_id in self.nodes_info.keys():
                removed_info = self.nodes_info.pop(node_id)
                logger.info(f"Node {removed_info['name']} is offline")
        except Exception as e:
            logger.error(f"Error occurred when removing node: {e}")

    def get_node_info(self, node_name: str) -> Optional[NodeInfo]:
        for info in self.nodes_info.values():
            if info["name"] == node_name:
                return info
        return None

    # def get_connection_state(self) -> ConnectionState:
    #     self.connection_state["timestamp"] = time.time()
    #     return self.connection_state

    # def get_connection_state_bytes(self) -> bytes:
    #     return dumps(self.get_connection_state()).encode()

    def register_node(self, node_info: NodeInfo):
        self.nodes_info[node_info["nodeID"]] = node_info
        for topic_info in node_info["topicList"]:
            self.register_topic(topic_info)
        for service_info in node_info["serviceList"]:
            self.register_service(service_info)
        for subscriber_info in node_info["subscriberList"]:
            self.register_subscriber(subscriber_info)

    def register_topic(self, topic_info: ComponentInfo):
        if topic_info["name"] not in self.topics_info.keys():
            self.topics_info[topic_info["name"]] = []
            logger.info(f"Topic {topic_info['name']} has been registered")
        self.topics_info[topic_info["name"]].append(topic_info)

    def register_service(self, service_info: ComponentInfo):
        if service_info["name"] not in self.services_info.keys():
            self.services_info[service_info["name"]] = service_info
            logger.info(f"Service {service_info['name']} has been registered")
        else:
            logger.warning(f"Service {service_info['name']} has been updated")
            self.services_info[service_info["name"]] = service_info

    def register_subscriber(self, subscriber_info: ComponentInfo):
        # self.nodes_info[node_id]["topicList"].append(subscriber)
        pass


class LanComMaster(AbstractNode):
    def __init__(self, node_ip: IPAddress) -> None:
        super().__init__(node_ip, MASTER_SERVICE_PORT)
        self.nodes_info_manager = NodesInfoManager(self.id)
        self.node_ip = node_ip

    async def broadcast_loop(self):
        logger.info(f"Master Node is broadcasting at {self.node_ip}")
        # set up udp socket
        with socket.socket(AF_INET, SOCK_DGRAM) as _socket:
            _socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            # calculate broadcast ip
            ip_bin = struct.unpack("!I", socket.inet_aton(self.node_ip))[0]
            netmask = socket.inet_aton("255.255.255.0")
            netmask_bin = struct.unpack("!I", netmask)[0]
            broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
            broadcast_ip = socket.inet_ntoa(struct.pack("!I", broadcast_bin))
            while self.running:
                msg = f"LancomMaster|{__version__}|{self.id}|{self.node_ip}"
                _socket.sendto(msg.encode(), (broadcast_ip, DISCOVERY_PORT))
                await async_sleep(0.1)
        logger.info("Broadcasting has been stopped")

    def initialize_event_loop(self):
        service_socket = zmq.asyncio.Context().socket(zmq.REP)  # type: ignore
        services: Dict[str, Callable[[bytes], bytes]] = {
            MasterRequestType.PING.value: self.ping,
            MasterRequestType.REGISTER_NODE.value: self.register_node,
            MasterRequestType.NODE_OFFLINE.value: self.node_offline,
            MasterRequestType.GET_NODES_INFO.value: self.get_nodes_info,
        }
        self.submit_loop_task(self.service_loop, service_socket, services)
        self.submit_loop_task(self.broadcast_loop)
        self.submit_loop_task(self.publish_master_state_loop)

    async def publish_master_state_loop(self):
        pub_socket = zmq.asyncio.Context().socket(zmq.PUB)  # type: ignore
        pub_socket.bind(f"tcp://{self.node_ip}:{MASTER_TOPIC_PORT}")
        while self.running:
            pub_socket.send_string(self.id)
            await async_sleep(0.1)

    def stop_node(self):
        logger.info("Master is stopping...")
        return super().stop_node()

    def ping(self, msg: bytes) -> bytes:
        return str(time.time()).encode()

    def register_node(self, msg: bytes) -> bytes:
        node_info = loads(msg)
        self.nodes_info_manager.register_node(node_info)
        return self.nodes_info_manager.get_connection_state_bytes()

    def node_offline(self, msg: bytes) -> bytes:
        node_info = loads(msg)
        self.nodes_info_manager.update_node(node_info)
        return self.nodes_info_manager.get_connection_state_bytes()

    def node_offline(self, msg: bytes) -> bytes:
        node_id = msg.decode()
        self.nodes_info_manager.remove_node(node_id)
        return ServiceStatus.SUCCESS.value

    def get_nodes_info(self, msg: bytes) -> bytes:
        nodes_info = self.nodes_info_manager.get_nodes_info()
        return dumps(nodes_info).encode()


def start_master_node_task(node_ip: IPAddress) -> None:
    node = LanComMaster(node_ip)
    node.spin()
