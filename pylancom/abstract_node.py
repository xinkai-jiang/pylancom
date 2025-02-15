from __future__ import annotations
import abc
import multiprocessing as mp
import socket
import asyncio
import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext
from pylancom.utils import get_zmq_socket_port
from typing import List, Dict, Callable, Union, Any
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time
from asyncio import sleep as async_sleep
import traceback

from .log import logger
from .type import NodeInfo, ResponseType
from .type import IPAddress, Port
from .utils import bmsgsplit, create_hash_identifier, bmsgsplit2str


class AbstractNode(abc.ABC):

    def __init__(self, node_ip: IPAddress, socket_port: Port = 0) -> None:
        super().__init__()
        self.id = create_hash_identifier()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((node_ip, socket_port))
        self.socket.setblocking(False)
        self.socket_service_cb: Dict[str, Callable[[str], str]] = {}

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

    def spin(self, block: bool = True) -> None:
        if block:
            self.spin_task()

    def spin_task(self) -> None:
        try:
            self.loop = asyncio.get_event_loop()  # Get the existing event loop
            self.running = True
            self.submit_loop_task(self.tcp_server, False)
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
            self.socket.close()
        except RuntimeError as e:
            logger.error(f"One error occurred when stop server: {e}")
        # self.executor.shutdown(wait=False)

    async def service_loop(
        self,
        service_socket: zmq.asyncio.Socket,
        services: Dict[str, Callable[[bytes], bytes]],
    ) -> None:
        logger.info("The service loop is running...")
        while self.running:
            bytes_msg = await service_socket.recv_multipart()
            service_name, request = bmsgsplit(b"".join(bytes_msg))
            service_name = service_name.decode()
            # the zmq service socket is blocked and only run one at a time
            if service_name not in services.keys():
                logger.error(f"Service {service_name} is not available")
            try:
                result = services[service_name](request)
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


    # async def tcp_server(self):
    #     try:
    #         server = await asyncio.start_server(self.handle_request, sock=self.socket)
    #         addr = server.sockets[0].getsockname()
    #         logger.info(f"TCP server started on {addr}")
    #         async with server:
    #             await server.serve_forever()
    #     except KeyboardInterrupt:
    #         logger.debug("TCP server is stopped")
    #     except Exception as e:
    #         logger.error(f"Error when starting TCP server: {e}")
    #         traceback.print_exc()
    #     finally:
    #         server.close()
    #         await server.wait_closed()

    # async def handle_request(
    #     self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    # ):
    #     """
    #     Handle incoming TCP client connections and echo received messages.
    #     """
    #     addr = writer.get_extra_info("peername")
    #     # logger.info(f"New connection from {addr}")
    #     # while True:
    #     try:
    #         data = await reader.read(4096)
    #         logger.info(f"Received data from {addr}: {data.decode()}")
    #         service_name, request = bmsgsplit2str(data)
    #         response = ResponseType.ERROR.value
    #         if service_name in self.socket_service_cb.keys():
    #             response = self.socket_service_cb[service_name](request)
    #         writer.write(response.encode())
    #         await writer.drain()
    #     except Exception as e:
    #         logger.error(f"Error with client {addr}: {e}")
    #         traceback.print_exc()
    #     finally:
    #         writer.close()
    #         await writer.wait_closed()
            # logger.info(f"Connection with {addr} closed")
