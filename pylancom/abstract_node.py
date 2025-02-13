from __future__ import annotations
import abc
import multiprocessing as mp
import asyncio
import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext
from pylancom.utils import get_zmq_socket_port
from typing import List, Dict, Callable, Awaitable, Optional
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time
from asyncio import sleep as async_sleep
import traceback

from .log import logger
from .utils import DISCOVERY_PORT
from .utils import NodeInfo, ComponentType, ConnectionState, ComponentInfo
from .utils import HashIdentifier
from .utils import MSG

from . import utils


class AbstractNode(abc.ABC):

    def __init__(
        self,
        node_name: str,
        node_type: str,
        node_ip: str,
        pub_port: int = 0,
        service_port: int = 0
    ) -> None:
        super().__init__()
        self.zmq_context: AsyncContext = zmq.asyncio.Context()  # type: ignore
        self.context = zmq.asyncio.Context()  # type: ignore
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:{pub_port}")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:{service_port}")
        self.local_info: NodeInfo = {
            "name": node_name,
            "nodeID": utils.create_hash_identifier(),
            "ip": node_ip,
            "type": node_type,
            "topicPort": utils.get_zmq_socket_port(self.pub_socket),
            "topicList": [],
            "servicePort": utils.get_zmq_socket_port(self.service_socket),
            "serviceList": [],
        }
        self.service_cbs: Dict[str, Callable] = {}
        self.log_node_state()

    def log_node_state(self):
        for key, value in self.local_info.items():
            print(f"    {key}: {value}")

    def create_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        return self.zmq_context.socket(socket_type)

    def submit_loop_task(
        self,
        task: Callable,
        *args,
    ) -> Optional[concurrent.futures.Future]:
        if not self.loop:
            raise RuntimeError("The event loop is not running")
        return asyncio.run_coroutine_threadsafe(task(*args), self.loop)

    def spin(self, block: bool = True) -> None:
        if block:
            self.spin_task()

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
            logger.info(f"Node {self.local_info['name']} has been stopped")

    @abc.abstractmethod
    def initialize_event_loop(self):
        raise NotImplementedError

    def stop_node(self):
        logger.info("Start to stop the node")
        self.running = False
        try:
            if self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError as e:
            logger.error(f"One error occurred when stop server: {e}")
        # self.executor.shutdown(wait=False)

    async def service_loop(self):
        logger.info("The service loop is running...")
        service_socket = self.service_socket
        while self.running:
            bytes_msg = await service_socket.recv_multipart()
            service_name, request = split_byte(b"".join(bytes_msg))
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


# class AbstractComponent(abc.ABC):
#     def __init__(self):
#         # TODO: start a new node if there is no manager
#         if AbstractNode.manager is None:
#             raise ValueError("NodeManager is not initialized")
#         self.manager: AbstractNode = AbstractNode.manager
#         self.running: bool = False
#         self.host_ip: str = self.manager.local_info["ip"]
#         self.local_name: str = self.manager.local_info["name"]

#     def shutdown(self) -> None:
#         self.running = False
#         self.on_shutdown()

#     @abc.abstractmethod
#     def on_shutdown(self):
#         raise NotImplementedError
