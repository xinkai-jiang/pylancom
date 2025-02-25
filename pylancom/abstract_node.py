from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import socket
import struct
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Union

import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from .log import logger
from .protos.node_info_pb2 import NodeInfo
from .type import IPAddress
from .utils import create_hash_identifier, get_zmq_socket_port


class AbstractNode(abc.ABC):
    def __init__(self, node_name: str, node_ip: IPAddress) -> None:
        super().__init__()
        logger.info(f"Node {node_name} started")
        self.node_ip = node_ip
        self.zmq_context: AsyncContext = zmq.asyncio.Context()
        self.node_socket = self.create_socket(zmq.REP)
        self.node_socket.bind(f"tcp://{self.node_ip}:0")
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.id = create_hash_identifier()
        self.running = False
        self.node_info = NodeInfo()
        self.node_info.name = node_name
        self.node_info.nodeID = self.id
        self.node_info.ip = self.node_ip
        self.node_info.type = "PyLanComNode"
        self.node_info.port = get_zmq_socket_port(self.node_socket)
        self.loop = None
        self.start_spin_task()

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    def submit_loop_task(
        self,
        task: Callable,
        block: bool,
        *args,
    ) -> Union[concurrent.futures.Future, Any]:
        if not self.loop:
            raise RuntimeError("The event loop is not running")
        future = asyncio.run_coroutine_threadsafe(task(*args), self.loop)
        if block:
            return future.result()
        return future

    def spin(self) -> None:
        while self.running:
            time.sleep(0.05)

    def start_spin_task(self) -> None:
        self.executor.submit(self.spin_task)
        while hasattr(self, "loop") is False:
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

    @abc.abstractmethod
    def initialize_event_loop(self):
        raise NotImplementedError

    def stop_node(self):
        self.running = False
        try:
            if self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError as e:
            logger.error(f"One error occurred when stop server: {e}")
        # self.executor.shutdown(wait=False)

    async def multicast_loop(self):
        """Asynchronously sends multicast messages to announce the node."""
        logger.info("Starting multicasting")
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
                print("Multicasting")
                current_time = time.strftime("%y-%m-%d-%H-%M-%S")
                node_info_bytes = self.node_info.SerializeToString()
                msg = f"LancomNode|{self.id}|".encode() + node_info_bytes
                _socket.sendto(msg, ("224.0.0.1", 5007))
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
            _socket.bind(("", 5007))
            group = socket.inet_aton("224.0.0.1")
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
                    await self.process_received_message(data)
                except Exception as e:
                    logger.error(f"Error receiving multicast message: {e}")
                    traceback.print_exc()
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Listening loop error: {e}")
        finally:
            _socket.close()
            logger.info("Multicast receiving has been stopped")

    async def process_received_message(self, data: bytes) -> None:
        """Processes received multicast messages and prints them."""
        try:
            parts = data.split(b"|", 2)
            if len(parts) < 3:
                return
            _, node_id, node_info_bytes = parts[0], parts[1], parts[2]
            if node_id.decode() == self.id:
                return  # Ignore messages from itself
            node_info = NodeInfo()
            node_info.ParseFromString(node_info_bytes)
            logger.info(
                f"Received message from node {node_id.decode()}: {node_info}"
            )

        except Exception as e:
            logger.error(f"Error processing received message: {e}")

    def initialize_event_loop(self):
        self.submit_loop_task(self.multicast_loop, False)
        self.submit_loop_task(self.listen_loop, False)
