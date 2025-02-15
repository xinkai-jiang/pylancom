from __future__ import annotations
from typing import Dict, Optional, List, Callable
import zmq
from asyncio import sleep as async_sleep
import socket
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_BROADCAST
import struct
from json import dumps, loads
import traceback
import time
import zmq.asyncio

from .abstract_node import AbstractNode
from .log import logger
from .config import __VERSION__ as __version__
from .config import DISCOVERY_PORT, MASTER_SERVICE_PORT, MASTER_TOPIC_PORT
from .utils import IPAddress, HashIdentifier
from .utils import byte2str, str2byte
from .type import MasterSocketReqType, ResponseType, ServiceName, TopicName
from .type import NodeInfo, ComponentInfo


class NodesInfoManager:

    def __init__(self, master_id: HashIdentifier) -> None:
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.topics_info: Dict[TopicName, List[ComponentInfo]] = {}
        self.subscribers_info: Dict[HashIdentifier, List[ComponentInfo]] = {}
        self.services_info: Dict[ServiceName, ComponentInfo] = {}

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
            traceback.print_exc()

    def get_node_info(self, node_name: str) -> Optional[NodeInfo]:
        for info in self.nodes_info.values():
            if info["name"] == node_name:
                return info
        return None

    def get_topics(self) -> Dict[TopicName, List[ComponentInfo]]:
        return self.topics_info

    def get_services(self) -> Dict[ServiceName, ComponentInfo]:
        return self.services_info

    def get_subscribers(self) -> Dict[HashIdentifier, List[ComponentInfo]]:
        return self.subscribers_info

    # def get_connection_state(self) -> ConnectionState:
    #     self.connection_state["timestamp"] = time.time()
    #     return self.connection_state

    # def get_connection_state_bytes(self) -> bytes:
    #     return dumps(self.get_connection_state()).encode()

    def register_node(self, node_info: NodeInfo):
        if node_info["nodeID"] in self.nodes_info.keys():
            logger.warning(f"Node {node_info['name']} has been updated")
        logger.info(f"Node {node_info['name']} is launched")
        self.nodes_info[node_info["nodeID"]] = node_info
        for topic_info in node_info["topicList"]:
            self.register_topic(topic_info)
        for service_info in node_info["serviceList"]:
            self.register_service(service_info)
        for subscriber_info in node_info["subscriberList"]:
            self.register_subscriber(subscriber_info)

    def register_topic(self, topic_info: ComponentInfo):
        topic_name = topic_info["name"]
        if topic_name not in self.topics_info.keys():
            self.topics_info[topic_name] = []
            logger.info(f"Topic {topic_info['name']} has been registered")
        self.topics_info[topic_name].append(topic_info)
        if topic_name not in self.subscribers_info.keys():
            # TODO: send connection request to all the subscribers
            pass

    def register_service(self, service_info: ComponentInfo):
        if service_info["name"] not in self.services_info.keys():
            self.services_info[service_info["name"]] = service_info
            logger.info(f"Service {service_info['name']} has been registered")
        else:
            logger.warning(f"Service {service_info['name']} has been updated")
            self.services_info[service_info["name"]] = service_info

    def register_subscriber(self, subscriber_info: ComponentInfo):
        topic_name = subscriber_info["name"]
        if topic_name not in self.subscribers_info.keys():
            self.subscribers_info[topic_name] = []
        self.subscribers_info[topic_name].append(subscriber_info)


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
        self.socket_service_cb: Dict[str, Callable[[bytes], bytes]] = {
            MasterSocketReqType.PING.value: self.ping,
            MasterSocketReqType.REGISTER_NODE.value: self.register_node,
            MasterSocketReqType.NODE_OFFLINE.value: self.node_offline,
            # MasterSocketReqType.GET_NODES_INFO.value: self.get_nodes_info,
        }
        self.submit_loop_task(self.broadcast_loop, False)
        self.submit_loop_task(self.publish_master_state_loop, False)
        self.submit_loop_task(self.service_loop, False, self.socket, self.socket_service_cb)

    async def publish_master_state_loop(self):
        pub_socket = zmq.asyncio.Context().socket(zmq.PUB)  # type: ignore
        pub_socket.bind(f"tcp://{self.node_ip}:{MASTER_TOPIC_PORT}")
        while self.running:
            pub_socket.send_string(self.id)
            await async_sleep(0.1)

    def stop_node(self):
        super().stop_node()
        logger.info("Master is stopped")

    def ping(self, msg: bytes) -> bytes:
        return str(time.time()).encode()

    def register_node(self, msg: bytes) -> bytes:
        mode_info: NodeInfo = loads(byte2str(msg))
        self.nodes_info_manager.register_node(mode_info)
        return str2byte(ResponseType.SUCCESS.value)

    def node_offline(self, msg: bytes) -> bytes:
        self.nodes_info_manager.remove_node(byte2str(msg))
        return str2byte(ResponseType.SUCCESS.value)

    def get_nodes_info(self, msg: bytes) -> bytes:
        nodes_info = self.nodes_info_manager.get_nodes_info()
        return dumps(nodes_info).encode()


def start_master_node_task(node_ip: IPAddress) -> None:
    node = LanComMaster(node_ip)
    node.spin()
