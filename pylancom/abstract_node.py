from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import threading
import time
import traceback
from typing import Any, Callable, Dict, Union

import zmq
import zmq.asyncio
from zmq.asyncio import Context as AsyncContext

from .log import logger
from .type import IPAddress, Port, ResponseType
from .utils import create_hash_identifier, send_bytes_request


class AbstractNode(abc.ABC):
    # instance: Optional[AbstractNode] = None

    def __init__(self, node_ip: IPAddress, socket_port: Port = 0) -> None:
        super().__init__()
        self.zmq_context: AsyncContext = zmq.asyncio.Context()  # type: ignore
        self.node_socket = self.create_socket(zmq.REP)  # type: ignore
        self.node_socket.bind(f"tcp://{node_ip}:{socket_port}")
        self.request_socket = self.create_socket(zmq.REQ)
        self.id = create_hash_identifier()

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

    def spin(self, block: bool = True) -> None:
        if block:
            self.spin_task()
        else:
            thread = threading.Thread(target=self.spin_task, daemon=True)
            thread.start()
            while hasattr(self, "loop") is False:
                time.sleep(0.05)

    def spin_task(self) -> None:
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
            if service_name not in services.keys():
                logger.error(f"Service {service_name} is not available")
            try:
                result = services[service_name](request)
                # logger.debug(service_name, result.decode())
                await service_socket.send(result)
            # TODO: fix the timeout issue
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
