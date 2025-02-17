from __future__ import annotations

import socket
import struct
import time
import traceback
from asyncio import sleep as async_sleep
from json import dumps, loads
from socket import AF_INET, SO_BROADCAST, SOCK_DGRAM, SOL_SOCKET
from typing import Callable, Dict, List, Optional

import zmq
import zmq.asyncio

from .abstract_node import AbstractNode
from .config import (
    __VERSION__,
    DISCOVERY_PORT,
    MASTER_SERVICE_PORT,
    MASTER_TOPIC_PORT,
)
from .log import logger
from .type import (
    ComponentInfo,
    MasterReqType,
    NodeInfo,
    NodeReqType,
    ResponseType,
    ServiceName,
    TopicName,
)
from .utils import HashIdentifier, IPAddress, bytes2str, str2bytes


class NodesInfoManager:
    def __init__(self, master_id: HashIdentifier) -> None:
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.publishers_info: Dict[TopicName, List[ComponentInfo]] = {}
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
            if node_id not in self.nodes_info.keys():
                logger.warning(f"Node {node_id} is not found")
            removed_info = self.nodes_info.pop(node_id)
            logger.info(f"Node {removed_info['name']} is offline")
            for topic_info in removed_info["topicList"]:
                self.publishers_info[topic_info["name"]].remove(topic_info)
            for service_info in removed_info["serviceList"]:
                self.services_info.pop(service_info["name"])
            for subscriber_info in removed_info["subscriberList"]:
                topic_name = subscriber_info["name"]
                self.subscribers_info[topic_name].remove(subscriber_info)
        except Exception as e:
            logger.error(f"Error occurred when removing node: {e}")
            traceback.print_exc()

    def get_node_info(self, node_name: HashIdentifier) -> Optional[NodeInfo]:
        if node_name in self.nodes_info:
            return self.nodes_info[node_name]
        return None

    def get_publishers(self) -> Dict[TopicName, List[ComponentInfo]]:
        return self.publishers_info

    def get_services(self) -> Dict[ServiceName, ComponentInfo]:
        return self.services_info

    def get_subscribers(self) -> Dict[HashIdentifier, List[ComponentInfo]]:
        return self.subscribers_info

    def register_node(self, node_info: NodeInfo):
        if node_info["nodeID"] in self.nodes_info.keys():
            logger.warning(f"Node {node_info['name']} has been updated")
        self.nodes_info[node_info["nodeID"]] = node_info

    def register_publisher(self, topic_info: ComponentInfo) -> None:
        topic_name = topic_info["name"]
        if topic_name not in self.publishers_info.keys():
            self.publishers_info[topic_name] = []
            logger.info(f"Topic {topic_info['name']} has been registered")
        self.publishers_info[topic_name].append(topic_info)

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
                msg = f"LancomMaster|{__VERSION__}|{self.id}|{self.node_ip}"
                _socket.sendto(msg.encode(), (broadcast_ip, DISCOVERY_PORT))
                await async_sleep(0.1)
        logger.info("Broadcasting has been stopped")

    def initialize_event_loop(self):
        node_service_cb: Dict[str, Callable[[bytes], bytes]] = {
            MasterReqType.PING.value: self.ping,
            MasterReqType.REGISTER_NODE.value: self.register_node,
            MasterReqType.NODE_OFFLINE.value: self.node_offline,
            MasterReqType.GET_NODES_INFO.value: self.get_nodes_info,
        }
        self.submit_loop_task(
            self.service_loop, False, self.node_socket, node_service_cb
        )
        self.submit_loop_task(self.broadcast_loop, False)
        self.submit_loop_task(self.publish_master_state_loop, False)

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
        node_info: NodeInfo = loads(bytes2str(msg))
        for topic_info in node_info["topicList"]:
            subscribers_info = self.nodes_info_manager.get_subscribers()
            self.nodes_info_manager.register_publisher(topic_info)
            if topic_info["name"] not in subscribers_info:
                continue
            for subscriber_info in subscribers_info[topic_info["name"]]:
                target_node_info = self.nodes_info_manager.get_node_info(
                    subscriber_info["nodeID"]
                )
                if target_node_info is None:
                    # TODO: better warning message
                    logger.warning(
                        f"Subscriber {subscriber_info['name']} is not found"
                    )
                    continue
                self.submit_loop_task(
                    self.send_request,
                    False,
                    NodeReqType.UPDATE_SUBSCRIBER.value,
                    target_node_info["ip"],
                    target_node_info["port"],
                    dumps(topic_info),
                )
        self.nodes_info_manager.register_node(node_info)
        for service_info in node_info["serviceList"]:
            self.nodes_info_manager.register_service(service_info)
        for subscriber_info in node_info["subscriberList"]:
            self.nodes_info_manager.register_subscriber(subscriber_info)
        logger.info(f"Node {node_info['name']} is registered")
        return str2bytes(dumps(self.nodes_info_manager.get_publishers()))

    def node_offline(self, msg: bytes) -> bytes:
        self.nodes_info_manager.remove_node(bytes2str(msg))
        return str2bytes(ResponseType.SUCCESS.value)

    def get_nodes_info(self, msg: bytes) -> bytes:
        nodes_info = self.nodes_info_manager.get_nodes_info()
        return dumps(nodes_info).encode()


def start_master_node_task(node_ip: IPAddress) -> None:
    node = LanComMaster(node_ip)
    node.spin()
