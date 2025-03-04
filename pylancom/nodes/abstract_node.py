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
from typing import Any, Dict, List, Optional, Union

import msgpack
import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from ..config import __COMPATIBILITY__, MULTICAST_ADDR, MULTICAST_PORT
from ..log import logger
from ..type import (
    IPAddress,
    LanComMsg,
    NodeInfo,
    NodeReqType,
    Port,
    SocketInfo,
    UpdateConnection,
)
from ..utils.utils import send_bytes_request


class NodesMap:
    def __init__(self):
        self.nodes_info: Dict[str, NodeInfo] = {}
        self.nodes_info_id: Dict[str, str] = {}
        self.nodes_heartbeat: Dict[str, str] = {}

    def check_node(self, node_id: str) -> bool:
        return node_id in self.nodes_info

    def check_info(self, node_id: str, info_id: int) -> bool:
        return self.nodes_info_id.get(node_id, "") == info_id

    def check_heartbeat(self, node_id: str, info_id: int) -> bool:
        return self.check_node(node_id) and self.check_info(node_id, info_id)

    def register_node(
        self, node_id: str, node_info: NodeInfo
    ) -> Dict[str, List[SocketInfo]]:
        self.nodes_info[node_id] = node_info
        self.nodes_info_id[node_id] = node_info["infoID"]
        return {
            "publishers": node_info["publisher"],
            "services": node_info["service"],
        }

    def update_node(
        self, node_id: str, node_info: NodeInfo
    ) -> Dict[str, List[SocketInfo]]:
        updated_info: UpdateConnection = {
            "publishers": [],
            "services": [],
        }
        for publisher in node_info["publisher"]:
            if publisher not in self.nodes_info[node_id]["publisher"]:
                updated_info["publishers"].append(publisher)
        for service in node_info["service"]:
            if service not in self.nodes_info[node_id]["service"]:
                updated_info["services"].append(service)
        self.nodes_info[node_id] = node_info
        self.nodes_info_id[node_id] = node_info["infoID"]
        node_name = node_info["name"]
        logger.debug(f"Node {node_name} has been updated")
        return updated_info

    def remove_node(self, node_id: str) -> None:
        self.nodes_info.pop(node_id, None)
        self.nodes_info_id.pop(node_id, None)


class AbstractNode(abc.ABC):
    def __init__(self, node_name: str, node_ip: IPAddress) -> None:
        super().__init__()
        self.node_name = node_name
        self.node_ip = node_ip
        self.zmq_context: AsyncContext = zmq.asyncio.Context()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.running = False
        self.nodes_map = NodesMap()
        self.loop: Optional[AbstractEventLoop] = None
        self.start_spin_task()

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    def submit_loop_task(
        self,
        task: asyncio.Coroutine,
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

    def start_spin_task(self) -> None:
        self.executor.submit(self.spin_task)
        while not self.running:
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
            if self.loop.is_running():
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
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            group = socket.inet_aton(MULTICAST_ADDR)
            _socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                struct.pack("4sL", group, socket.INADDR_ANY),
            )
            _socket.bind(("", MULTICAST_PORT))
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
            node_info: NodeInfo = msgpack.loads(node_info_bytes)
            updated_info = self.nodes_map.update_node(node_id, node_info)
            self.check_connection(updated_info)
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
    def check_connection(
        self, updated_info: Dict[str, List[SocketInfo]]
    ) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def initialize_event_loop(self):
        self.submit_loop_task(self.listen_loop(), False)
