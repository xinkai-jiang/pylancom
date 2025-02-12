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
from .utils import MSG, split_byte

from . import utils

class AbstractNode(abc.ABC):

    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__()
        self.zmq_context: AsyncContext = zmq.asyncio.Context()  # type: ignore
        self.id: HashIdentifier = utils.create_hash_identifier()
        # message for broadcasting
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:{DISCOVERY_PORT}")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        # self.connection_state: ConnectionState = {"topic": {}, "service": {}}
        # logger.info(f"Node {node_name} starts at {node_ip}:{DISCOVERY_PORT}")
        # start the server in a thread pool
        # self.executor = ThreadPoolExecutor(max_workers=10)
        # self.server_future = self.executor.submit(self.thread_task)
        # # wait for the loop starts
        # while not hasattr(self, "loop"):
        #     time.sleep(0.01)
        # logger.info(f"Node {self.local_info['name']} is initialized")

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
        self.executor.shutdown(wait=False)

    def spin(self, block: bool = False) -> None:
        pass

    def check_topic(self, topic_name: str) -> Optional[List[ComponentInfo]]:
        topic_info = self.connection_state["topic"]
        if topic_name not in topic_info["topic"]:
            return None
        return topic_info[topic_name]["Publisher"]

    def check_service(self, service_name: str) -> Optional[ComponentInfo]:
        if service_name not in self.connection_state["service"]:
            return None
        return self.connection_state["service"][service_name]



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


class AbstractComponent(abc.ABC):
    def __init__(self):
        # TODO: start a new node if there is no manager
        if AbstractNode.manager is None:
            raise ValueError("NodeManager is not initialized")
        self.manager: AbstractNode = AbstractNode.manager
        self.running: bool = False
        self.host_ip: str = self.manager.local_info["ip"]
        self.local_name: str = self.manager.local_info["name"]

    def shutdown(self) -> None:
        self.running = False
        self.on_shutdown()

    @abc.abstractmethod
    def on_shutdown(self):
        raise NotImplementedError
