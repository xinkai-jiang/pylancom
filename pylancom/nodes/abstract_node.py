from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import socket
import struct
import time
import traceback
from asyncio import AbstractEventLoop, get_running_loop
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Dict, List, Optional, Union, cast
import platform

import msgpack
import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from ..config import __COMPATIBILITY__
from ..utils.log import logger
from ..lancom_type import (
    IPAddress,
    LanComMsg,
    NodeInfo,
    NodeReqType,
    Port,
    SocketInfo,
    TopicName,
)
from ..utils.msg import send_bytes_request


class NodesMap:
    def __init__(self):
        self.nodes_info: Dict[str, NodeInfo] = {}
        self.nodes_info_id: Dict[str, int] = {}
        self.nodes_heartbeat: Dict[str, str] = {}
        self.publishers_dict: Dict[str, SocketInfo] = {}
        self.services_dict: Dict[str, SocketInfo] = {}

    def check_node(self, node_id: str) -> bool:
        return node_id in self.nodes_info

    def check_info(self, node_id: str, info_id: int) -> bool:
        return self.nodes_info_id.get(node_id, "") == info_id

    def check_heartbeat(self, node_id: str, info_id: int) -> bool:
        return self.check_node(node_id) and self.check_info(node_id, info_id)

    def update_node(self, node_id: str, node_info: NodeInfo):
        if node_id not in self.nodes_info:
            logger.debug(f"Node {node_info['name']} has been registered")
        for pub_info in node_info["publishers"]:
            if pub_info["socketID"] not in self.publishers_dict:
                self.publishers_dict[pub_info["socketID"]] = pub_info
        for service_info in node_info["services"]:
            if service_info["socketID"] not in self.services_dict:
                self.services_dict[service_info["socketID"]] = service_info
        self.nodes_info[node_id] = node_info
        self.nodes_info_id[node_id] = node_info["infoID"]

    def remove_node(self, node_id: str) -> None:
        self.nodes_info.pop(node_id, None)
        self.nodes_info_id.pop(node_id, None)

    def get_publisher_info(self, topic_name: TopicName) -> List[SocketInfo]:
        publishers: List[SocketInfo] = []
        for pub_info in self.publishers_dict.values():
            if pub_info["name"] == topic_name:
                publishers.append(pub_info)
        return publishers

    def get_service_info(self, service_name: str) -> Optional[SocketInfo]:
        for service in self.services_dict.values():
            if service["name"] == service_name:
                return service
        return None


class AbstractNode(abc.ABC):
    def __init__(
        self,
        node_name: str,
        node_ip: IPAddress,
        multicast_addr: IPAddress = "224.0.0.1",
        multicast_port: int = 7720
    ) -> None:
        super().__init__()
        self.node_name = node_name
        self.node_ip = node_ip
        self.multicast_addr = multicast_addr
        self.multicast_port = multicast_port
        # for running on Windows localhost, use a different multicast address
        if self.node_ip == "127.0.0.1" and platform.system() == "Windows":
            self.multicast_addr = "239.255.255.250"
        self.zmq_context: AsyncContext = zmq.asyncio.Context()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.running = False
        self.nodes_map = NodesMap()
        self.loop: Optional[AbstractEventLoop] = None
        # start spin task
        self.executor.submit(self.spin_task)
        while not self.running:
            time.sleep(0.05)

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    def submit_loop_task(
        self,
        task: Coroutine,
        block: bool = False,
    ) -> Union[concurrent.futures.Future, Any]:
        if not self.loop:
            raise RuntimeError("The event loop is not running")
        future = asyncio.run_coroutine_threadsafe(task, self.loop)
        if block:
            return future.result()
        return future

    def spin(self) -> None:
        while self.running:
            time.sleep(0.05)

    def spin_task(self) -> None:
        logger.info("Starting spin task")
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.running = True
            self.initialize_event_loop()
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.stop_node()
        except Exception as e:
            logger.error(f"Unexpected error in thread_task: {e}")
            traceback.print_exc()
            self.stop_node()

    def stop_node(self):
        self.running = False
        try:
            if self.loop is not None:
                self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError as e:
            logger.error(f"One error occurred when stop server: {e}")
        self.executor.shutdown(wait=False)

    async def listen_loop(self):
        """Asynchronously listens for multicast messages from other nodes."""
        try:
            logger.debug("Starting multicast listening")
            _socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            group = socket.inet_aton(self.multicast_addr)
            _socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                struct.pack("4sL", group, socket.INADDR_ANY),
            )
            _socket.bind(("", self.multicast_port))
            while self.running:
                try:
                    data, addr = await get_running_loop().run_in_executor(
                        self.executor, _socket.recvfrom, 1024
                    )
                    await self.process_heartbeat(data, addr[0])
                except Exception as e:
                    logger.error(f"Error receiving multicast message: {e}")
                    traceback.print_exc()
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Listening loop error: {e}")
            traceback.print_exc()
        finally:
            _socket.close()
            logger.info("Multicast receiving has been stopped")

    async def process_heartbeat(self, data: bytes, ip: IPAddress) -> None:
        """Processes received multicast messages and prints them."""
        try:
            if data[:6] != b"LANCOM":
                return
            if data[6:8] != __COMPATIBILITY__:
                logger.warning(f"Incompatible version {data[6:9]}")
                return
            node_id = data[9:45].decode()
            node_port = int.from_bytes(data[-6:-4], "big")
            node_info_id = int.from_bytes(data[-4:], "big")
            # add a new node to the map
            if self.nodes_map.check_heartbeat(node_id, node_info_id):
                return
            node_info_bytes = await self.send_request(
                NodeReqType.NODE_INFO.value,
                ip,
                node_port,
                LanComMsg.EMPTY.value,
            )
            if node_info_bytes == LanComMsg.TIMEOUT.value.encode():
                return
            node_info = cast(NodeInfo, msgpack.loads(node_info_bytes))
            self.nodes_map.update_node(node_id, node_info)
            return
        except Exception as e:
            logger.error(f"Error processing received message: {e}")
            traceback.print_exc()

    async def send_request(
        self,
        service_name: str,
        ip: IPAddress,
        port: Port,
        msg: str,
    ) -> bytes:
        """Sends a request to another node."""
        addr = f"tcp://{ip}:{port}"
        result = await send_bytes_request(
            addr,
            service_name,
            msg.encode(),
        )
        return result

    @abc.abstractmethod
    def initialize_event_loop(self):
        self.submit_loop_task(self.listen_loop(), False)
