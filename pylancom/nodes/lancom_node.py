from __future__ import annotations

import asyncio
import socket
import traceback
from typing import Callable, Dict, Optional, cast

import msgpack
import zmq.asyncio

from ..utils.log import logger
from ..lancom_type import IPAddress, LanComMsg, NodeInfo, NodeReqType
from ..utils.msg import (
    create_hash_identifier,
    create_heartbeat_message,
    get_socket_port,
)
from .abstract_node import AbstractNode


class LanComNode(AbstractNode):
    instance: Optional[LanComNode] = None

    def __init__(self, node_name: str, node_ip: IPAddress) -> None:
        if LanComNode.instance is not None:
            raise Exception("LanComNode has been initialized")
        LanComNode.instance = self
        self.node_id = create_hash_identifier()
        # Initialize the NodeInfo message
        self.local_info = NodeInfo(
            name=node_name,
            nodeID=self.node_id,
            ip=node_ip,
            type="LanComNode",
            infoID=0,
            port=0,
            publishers=[],
            services=[],
        )
        self.service_cbs: Dict[str, Callable[[bytes], bytes]] = {}
        super().__init__(node_name, node_ip)

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    async def multicast_loop(self):
        """Asynchronously sends multicast messages to announce the node."""
        try:
            _socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            _socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            _socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self.node_ip),
            )
            logger.debug(f"Multicast has been started at {self.node_ip}")
            while self.running:
                msg = create_heartbeat_message(
                    self.node_id,
                    self.local_info["port"],
                    self.local_info["infoID"],
                )
                _socket.sendto(msg, (self.multicast_addr, self.multicast_port))
                await asyncio.sleep(1)  # Prevent excessive CPU usage
        except Exception as e:
            logger.error(f"Multicast error: {e}")
            traceback.print_exc()
        finally:
            _socket.close()
            logger.info("Multicast has been stopped")

    async def service_loop(
        self,
        service_socket: zmq.asyncio.Socket,
        services: Dict[str, Callable[[bytes], bytes]],
    ) -> None:
        if self.loop is None:
            raise Exception("Event loop has not been initialized")
        while self.running:
            try:
                name_bytes, request = await service_socket.recv_multipart()
            except Exception as e:
                logger.error(f"Error occurred when receiving request: {e}")
                traceback.print_exc()
            service_name = name_bytes.decode()
            if service_name not in services.keys():
                logger.error(f"Service {service_name} is not available")
                continue
            try:
                result = await asyncio.wait_for(
                    self.loop.run_in_executor(
                        self.executor, services[service_name], request
                    ),
                    timeout=2.0,
                )
                # result = services[service_name](request)
                # logger.debug(service_name, result)
                await service_socket.send(result)
            except asyncio.TimeoutError:
                logger.error("Timeout: callback function took too long")
                await service_socket.send(LanComMsg.TIMEOUT.value)
            except Exception as e:
                logger.error(
                    f"One error occurred when processing the Service "
                    f'"{service_name}": {e}'
                )
                traceback.print_exc()
                await service_socket.send(LanComMsg.ERROR.value)
        logger.info("Service loop has been stopped")

    def initialize_event_loop(self):
        node_socket = self.create_socket(zmq.REP)
        node_socket.bind(f"tcp://{self.node_ip}:0")
        self.local_info["port"] = get_socket_port(node_socket)
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{self.node_ip}:0")
        self.nodes_map.update_node(self.node_id, self.local_info)
        node_service_cbs = {
            NodeReqType.PING.value: lambda x: LanComMsg.SUCCESS.value,
            NodeReqType.NODE_INFO.value: self.node_info_cbs,
        }
        self.submit_loop_task(self.service_loop(node_socket, node_service_cbs))
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{self.node_ip}:0")
        self.submit_loop_task(
            self.service_loop(self.service_socket, self.service_cbs)
        )
        self.submit_loop_task(self.multicast_loop())
        super().initialize_event_loop()

    def node_info_cbs(self, request: bytes) -> bytes:
        return cast(bytes, msgpack.dumps(self.local_info))
