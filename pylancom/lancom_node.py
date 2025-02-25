from __future__ import annotations

import abc
import asyncio
import socket
import struct
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict

import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from .config import __VERSION__, DISCOVERY_PORT, MULTICAST_ADDR
from .log import logger
from .protos.node_info_pb2 import (
    NodeInfo,  # Import the generated NodeInfo class
)
from .type import IPAddress, Port, ResponseType
from .utils import (
    create_hash_identifier,
    get_zmq_socket_port,
    send_bytes_request,
)


class LanComNode(abc.ABC):
    def __init__(self, node_name: str, node_ip: IPAddress) -> None:
        super().__init__()
        self.node_ip = node_ip
        self.node_name = node_name
        self.id = create_hash_identifier()
        self.zmq_context: AsyncContext = zmq.asyncio.Context()
        self.node_socket = self.create_socket(zmq.REP)
        self.node_socket.bind(f"tcp://{self.node_ip}:0")
        self.running = False
        self.executor = ThreadPoolExecutor()

        # Initialize the NodeInfo message
        self.local_info = NodeInfo()
        self.local_info.name = self.node_name
        self.local_info.nodeID = self.id
        self.local_info.ip = self.node_ip
        self.local_info.port = get_zmq_socket_port(self.node_socket)
        self.local_info.type = "LanComNode"

        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self.spin_task())  # Run event loop in background

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    async def spin_task(self) -> None:
        """Asynchronous task to keep the node running."""
        self.running = True
        try:
            await asyncio.gather(self.multicast_loop(), self.listen_loop())
        except Exception as e:
            logger.error(f"Unexpected error in spin_task: {e}")
            traceback.print_exc()

    async def multicast_loop(self):
        """Asynchronously sends multicast messages to announce the node."""
        try:
            _socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            _socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            _socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self.node_ip),
            )

            while self.running:
                current_time = time.strftime("%y-%m-%d-%H-%M-%S")
                node_info_bytes = self.local_info.SerializeToString()
                msg = (
                    f"LancomMaster|{__VERSION__}|{self.id}|".encode()
                    + node_info_bytes
                )
                _socket.sendto(msg, (MULTICAST_ADDR, DISCOVERY_PORT))
                await asyncio.sleep(1)  # Prevent excessive CPU usage

        except Exception as e:
            logger.error(f"Multicast error: {e}")
        finally:
            _socket.close()
            logger.info("Multicasting has been stopped")

    async def listen_loop(self):
        """Asynchronously listens for multicast messages from other nodes."""
        try:
            _socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            _socket.bind(("", DISCOVERY_PORT))
            group = socket.inet_aton(MULTICAST_ADDR)
            mreq = struct.pack("4sL", group, socket.INADDR_ANY)
            _socket.setsockopt(
                socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq
            )

            while self.running:
                try:
                    (
                        data,
                        addr,
                    ) = await asyncio.get_running_loop().run_in_executor(
                        self.executor, _socket.recvfrom, 1024
                    )
                    msg = data.decode()
                    if msg.startswith("LancomMaster"):
                        await self.update_master_state(msg)
                except Exception as e:
                    logger.error(f"Error receiving multicast message: {e}")
                    traceback.print_exc()
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Listening loop error: {e}")
        finally:
            _socket.close()
            logger.info("Multicast receiving has been stopped")

    async def update_master_state(self, message: str) -> None:
        """Processes master node announcements and updates local state."""
        try:
            parts = message.split("|")
            if len(parts) < 4:
                return
            _, version, master_id, node_info_bytes = (
                parts[0],
                parts[1],
                parts[2],
                "|".join(parts[3:]),
            )
            if master_id == getattr(self, "master_id", None):
                return

            self.master_id, self.master_ip = master_id, self.node_ip
            node_info = NodeInfo()
            node_info.ParseFromString(node_info_bytes.encode())
            logger.debug(
                f"Node {self.local_info.name} connected to master at {self.master_ip} with info: {node_info}"
            )

        except Exception as e:
            logger.error(f"Error updating master state: {e}")

    async def send_request(
        self, request_type: str, ip: IPAddress, port: Port, message: str
    ) -> str:
        """Sends a request to another node."""
        addr = f"tcp://{ip}:{port}"
        result = await send_bytes_request(
            addr, [request_type.encode(), message.encode()]
        )
        return result.decode()

    async def service_loop(
        self,
        service_socket: zmq.asyncio.Socket,
        services: Dict[str, Callable[[bytes], bytes]],
    ) -> None:
        """Handles service requests in an async loop."""
        while self.running:
            try:
                name_bytes, request = await service_socket.recv_multipart()
                service_name = name_bytes.decode()
                if service_name not in services:
                    logger.error(f"Service {service_name} is not available")
                    await service_socket.send(ResponseType.ERROR.value)
                    continue

                result = await asyncio.to_thread(
                    services[service_name], request
                )
                await service_socket.send(result)

            except asyncio.TimeoutError:
                logger.error("Timeout: Service took too long")
                await service_socket.send(ResponseType.TIMEOUT.value)
            except Exception as e:
                logger.error(f"Error in service {service_name}: {e}")
                traceback.print_exc()
                await service_socket.send(ResponseType.ERROR.value)

        logger.info("Service loop has been stopped")

    async def send_request(
        self, request_type: str, ip: IPAddress, port: Port, message: str
    ) -> str:
        addr = f"tcp://{ip}:{port}"
        result = await send_bytes_request(
            addr, [request_type.encode(), message.encode()]
        )
        return result.decode()

    async def service_loop(
        self,
        service_socket: zmq.asyncio.Socket,
        services: Dict[str, Callable[[bytes], bytes]],
    ) -> None:
        while self.running:
            try:
                name_bytes, request = await service_socket.recv_multipart()
                # print(f"Received message: {result}")
                service_name = name_bytes.decode()
            except Exception as e:
                logger.error(f"Error occurred when receiving request: {e}")
                traceback.print_exc()
            # the zmq service socket is blocked and only run one at a time
            print(
                f"Service {service_name} received message: {request.decode()}"
            )
            if service_name not in services.keys():
                logger.error(f"Service {service_name} is not available")
            try:
                result = await asyncio.wait_for(
                    self.loop.run_in_executor(
                        self.executor, services[service_name], request
                    ),
                    timeout=2.0,
                )
                # result = services[service_name](request)
                # logger.debug(service_name, result.decode())
                await service_socket.send(result)
            except asyncio.TimeoutError:
                logger.error("Timeout: callback function took too long")
                await service_socket.send(ResponseType.TIMEOUT.value)
            except Exception as e:
                logger.error(
                    f"One error occurred when processing the Service "
                    f'"{service_name}": {e}'
                )
                traceback.print_exc()
                await service_socket.send(ResponseType.ERROR.value)
        logger.info("Service loop has been stopped")
