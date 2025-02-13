from __future__ import annotations
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Callable, Awaitable, Union
import asyncio
from asyncio import sleep as async_sleep
import socket
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_BROADCAST
import struct
import zmq.asyncio
from json import dumps, loads
import concurrent.futures
import traceback


from .log import logger
from .utils import DISCOVERY_PORT, MASTER_TOPIC_PORT, MASTER_SERVICE_PORT
from .utils import IPAddress, HashIdentifier, ServiceName, TopicName
from .utils import NodeInfo, ConnectionState

# from .abstract_node import AbstractNode
from .utils import bmsgsplit
from .utils import MSG
from . import utils
from .abstract_node import AbstractNode


class NodesInfoManager:

    def __init__(self, local_info: NodeInfo) -> None:
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.connection_state: ConnectionState = {"topic": {}, "service": {}}
        # local info is the master node info
        self.local_info = local_info
        self.node_id = local_info["nodeID"]

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

    def register_node(self, info: NodeInfo):
        node_id = info["nodeID"]
        if node_id not in self.nodes_info.keys():
            logger.info(f"Node {info['name']} from " f"{info['ip']} has been launched")
            topic_state = self.connection_state["topic"]
            for topic in info["topicList"]:
                topic_state[topic["name"]].append(topic)
            service_state = self.connection_state["service"]
            for service in info["serviceList"]:
                service_state[service["name"]] = service
        self.nodes_info[node_id] = info

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


class LanComMasterNode(AbstractNode):
    def __init__(self, node_ip: IPAddress) -> None:
        super().__init__(
            "Master",
            "Master",
            node_ip,
            MASTER_TOPIC_PORT,
            MASTER_SERVICE_PORT,
        )

    def initialize_event_loop(self):
        self.submit_loop_task(self.broadcast_loop)
        self.submit_loop_task(self.nodes_info_publish_loop)

    async def broadcast_loop(self):
        logger.info(
            f"The Master Node is broadcasting at "
            f"{self.local_info['ip']}:{DISCOVERY_PORT}"
        )
        # set up udp socket
        with socket.socket(AF_INET, SOCK_DGRAM) as _socket:
            _socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            # calculate broadcast ip
            local_info = self.local_info
            _ip = local_info["ip"]
            ip_bin = struct.unpack("!I", socket.inet_aton(_ip))[0]
            netmask = socket.inet_aton("255.255.255.0")
            netmask_bin = struct.unpack("!I", netmask)[0]
            broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
            broadcast_ip = socket.inet_ntoa(struct.pack("!I", broadcast_bin))
            while self.running:
                msg = f"LancomMaster|version=0.1|{dumps(local_info)}"
                _socket.sendto(msg.encode(), (broadcast_ip, DISCOVERY_PORT))
                await async_sleep(0.1)
        logger.info("Broadcasting has been stopped")

    async def nodes_info_publish_loop(self):
        while self.running:
            print(f"Published: {dumps(self.local_info)}")
            self.pub_socket.send_string(f"{dumps(self.local_info)}")
            await async_sleep(0.1)

    async def service_loop(self):
        logger.info("The service loop is running...")
        service_socket = self.service_socket
        while self.running:
            bytes_msg = await service_socket.recv_multipart()
            service_name, request = bmsgsplit(b"".join(bytes_msg))
            service_name = service_name.decode()
            # the zmq service socket is blocked and only run one at a time
            if service_name in self.service_cbs.keys():
                try:
                    await self.service_cbs[service_name](request)
                except asyncio.TimeoutError:
                    logger.error("Timeout: callback function took too long")
                    await service_socket.send(MSG.SERVICE_TIMEOUT.value)
                except Exception as e:
                    logger.error(
                        f"One error occurred when processing the Service "
                        f'"{service_name}": {e}'
                    )
                    traceback.print_exc()
                    await service_socket.send(MSG.SERVICE_ERROR.value)
            await async_sleep(0.01)
        logger.info("Service loop has been stopped")

    def ping_callback(self, msg: bytes) -> bytes:
        pass

    def register_node_callback(self, node_info: NodeInfo) -> None:
        pass

    def update_node_callback(self, node_info: NodeInfo) -> None:
        pass

    def node_offline_callback(self, node_id: HashIdentifier) -> None:
        pass


def start_master_node_task(node_ip: IPAddress) -> None:
    node = LanComMasterNode(node_ip)
    node.spin()
