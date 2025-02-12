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
from .utils import DISCOVERY_PORT
from .utils import IPAddress, HashIdentifier, ServiceName, TopicName
from .utils import NodeInfo, ConnectionState
from .abstract_node import AbstractNode
from .utils import bmsgsplit
from .utils import MSG
from . import utils

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
            logger.info(
                f"Node {info['name']} from "
                f"{info['ip']} has been launched"
            )
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




class LanComMaster:
    def __init__(self, node_ip: IPAddress) -> None:
        self.context = zmq.asyncio.Context()  # type: ignore
        self.ip = node_ip
        self.pub_socket = self.create_socket(zmq.PUB)
        self.service_socket = self.create_socket(zmq.REP)
        self.local_info: NodeInfo = {
            "name": "LancomMaster",
            "nodeID": utils.create_hash_identifier(),
            "ip": node_ip,
            "type": "master",
            "topicPort": utils.get_zmq_socket_port(self.pub_socket),
            "topicList": [],
            "servicePort": utils.get_zmq_socket_port(self.service_socket),
            "serviceList": [],
        }
        self.service_cbs: Dict[str, Callable] = {}

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.context.socket(socket_type)


    def spin_task(self) -> None:
        logger.info("The node is running...")
        try:
            self.loop = asyncio.get_event_loop()  # Get the existing event loop
            self.running = True
            self.submit_loop_task(self.service_loop)
            self.initialize_event_loop()
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.stop_node()
        except Exception as e:
            logger.error(f"Unexpected error in thread_task: {e}")
        finally:
            logger.info("The node has been stopped")


    def stop_node(self):
        logger.info("Start to stop the node")
        self.running = False
        try:
            if self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError as e:
            logger.error(f"One error occurred when stop server: {e}")
        # self.executor.shutdown(wait=False)

    def submit_loop_task(
        self,
        task: Callable,
        *args,
    ) -> Optional[concurrent.futures.Future]:
        if not self.loop:
            raise RuntimeError("The event loop is not running")
        return asyncio.run_coroutine_threadsafe(task(*args), self.loop)


    async def broadcast_loop(self):
        logger.info("The server is broadcasting...")
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
        self.pub_socket.send_string(
            f"{dumps(self.local_info)}"
        )


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


def start_master_node_task(node_name: str, node_ip: str) -> None:
    node = MasterNode(node_name, node_ip)
    node.
