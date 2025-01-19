from __future__ import annotations
import abc
from typing import Dict, Optional, Callable, Awaitable, Union
import asyncio
from asyncio import sleep as async_sleep
import zmq
import zmq.asyncio
import time
from json import dumps
import traceback

from abstract_node import AbstractNode, AbstractComponent
from .log import logger
from .utils import IPAddress, TopicName, ServiceName, HashIdentifier
from .utils import NodeInfo, DISCOVERY_PORT
from .utils import MSG, ComponentInfo, ComponentType
from .utils import split_byte, get_zmq_socket_port, create_address, generate_hash
from .utils import AsyncSocket


class Publisher(AbstractComponent):
    def __init__(self, topic_name: str, with_local_namespace: bool = False):
        super().__init__()
        self.topic_name = topic_name
        if with_local_namespace:
            self.topic_name = f"{self.local_name}/{topic_name}"
        # in case the topic is not updated to the master node
        if topic_name in self.manager.local_info["topicList"]:
            raise RuntimeError("Topic has been registered in the local node")
        if self.manager.check_topic(topic_name) is not None:
            logger.warning(f"Topic {topic_name} is already registered")
            return
        self.info: ComponentInfo = {
            "name": topic_name,
            "componentID": generate_hash(),
            "type": ComponentType.PUBLISHER.value,
            "ip": self.manager.local_info["ip"],
            "port": get_zmq_socket_port(self.socket),
        }
        self.manager.local_info["topicList"].append(self.info)
        self.socket = self.manager.pub_socket
        logger.info(msg=f'Topic "{topic_name}" is ready to publish')

    def publish_bytes(self, data: bytes) -> None:
        msg = b"".join([f"{self.topic_name}:".encode(), b"|", data])
        self.manager.submit_loop_task(self.send_bytes_async, msg)

    def publish_dict(self, data: Dict) -> None:
        self.publish_string(dumps(data))

    def publish_string(self, string: str) -> None:
        msg = f"{self.topic_name}:{string}"
        self.manager.submit_loop_task(self.send_bytes_async, msg.encode())

    def on_shutdown(self) -> None:
        self.manager.local_info["topicList"].remove(self.info)

    async def send_bytes_async(self, msg: bytes) -> None:
        await self.socket.send(msg)


class Streamer(Publisher):
    def __init__(
        self,
        topic_name: str,
        update_func: Callable[[], Optional[Union[str, bytes, Dict]]],
        fps: int,
        start_streaming: bool = False,
    ):
        super().__init__(topic_name)
        self.running = False
        self.dt: float = 1 / fps
        self.update_func = update_func
        self.topic_byte = self.topic_name.encode("utf-8")
        if start_streaming:
            self.start_streaming()

    def start_streaming(self):
        self.manager.submit_loop_task(self.update_loop)

    def generate_byte_msg(self) -> bytes:
        update_msg = self.update_func()
        if isinstance(update_msg, str):
            return update_msg.encode("utf-8")
        elif isinstance(update_msg, bytes):
            return update_msg
        elif isinstance(update_msg, dict):
            # return dumps(update_msg).encode("utf-8")
            return dumps(
                {
                    "updateData": self.update_func(),
                    "time": time.monotonic(),
                }
            ).encode("utf-8")
        raise ValueError("Update function should return str, bytes or dict")

    async def update_loop(self):
        self.running = True
        last = 0.0
        logger.info(f"Topic {self.topic_name} starts streaming")
        while self.running:
            try:
                diff = time.monotonic() - last
                if diff < self.dt:
                    await async_sleep(self.dt - diff)
                last = time.monotonic()
                await self.socket.send(
                    b"".join([self.topic_byte, b"|", self.generate_byte_msg()])
                )
            except Exception as e:
                logger.error(f"Error when streaming {self.topic_name}: {e}")
                traceback.print_exc()
        logger.info(f"Streamer for topic {self.topic_name} is stopped")


class ByteStreamer(Streamer):
    def __init__(
        self,
        topic: str,
        update_func: Callable[[], bytes],
        fps: int,
    ):
        super().__init__(topic, update_func, fps)
        self.update_func: Callable[[], bytes]

    def generate_byte_msg(self) -> bytes:
        return self.update_func()


class Subscriber(AbstractComponent):
    # TODO: test this class
    def __init__(self, topic_name: str, callback: Callable[[str], None]):
        super().__init__()
        self.sub_socket: AsyncSocket = self.manager.create_socket(zmq.SUB)
        self.subscribed_components: Dict[HashIdentifier, ComponentInfo] = {}
        self.topic_name = topic_name
        self.connected = False
        self.callback = callback
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_name)

    def connect(self, info: ComponentInfo) -> None:
        if info["type"] != ComponentType.PUBLISHER.value:
            raise ValueError("The component is not a publisher")
        if info["componentID"] in self.subscribed_components:
            return
        self.sub_socket.connect(f"tcp://{info['ip']}:{info['port']}")
        self.connected = True

    def remove_all_the_connections(self) -> None:
        for info in self.subscribed_components.values():
            self.sub_socket.disconnect(f"tcp://{info['ip']}:{info['port']}")
        self.connected = False

    # def change_connection(self, new_addr: NodeAddress) -> None:
    #     """Changes the connection to a new IP address."""
    #     if self.connected and self.remote_addr is not None:
    #         logger.info(f"Disconnecting from {self.remote_addr}")
    #         self.sub_socket.disconnect(f"tcp://{self.remote_addr}")
    #     self.sub_socket.connect(f"tcp://{new_addr}")
    #     self.remote_addr = new_addr
    #     self.connected = True

    async def wait_for_publisher(self) -> None:
        """Waits for a publisher to be available for the topic."""
        while self.running:
            publishers_info = self.manager.check_topic(self.topic_name)
            if publishers_info is not None:
                logger.info(
                    f"'{self.topic_name}' Subscriber starts connection"
                )
            await async_sleep(0.5)

    async def listen(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        while self.running:
            try:
                # Wait for a message
                msg = await self.sub_socket.recv_string()
                # Invoke the callback
                self.callback(msg)
            except Exception as e:
                logger.error(
                    f"Error in subscriber of topic '{self.topic_name}': {e}"
                )
                traceback.print_exc()

    def on_shutdown(self) -> None:
        self.running = False
        self.sub_socket.close()


class AbstractService(AbstractComponent):

    def __init__(
        self,
        service_name: str,
    ) -> None:
        super().__init__()
        self.service_name = service_name
        self.socket = self.manager.service_socket
        if service_name in self.manager.local_info["serviceList"]:
            raise RuntimeError("Service has been registered in the local node")
        if self.manager.check_service(service_name) is not None:
            logger.warning(f"Service {service_name} is already registered")
            return
        self.info: ComponentInfo = {
            "name": service_name,
            "componentID": generate_hash(),
            "type": ComponentType.SERVICE.value,
            "ip": self.manager.local_info["ip"],
            "port": get_zmq_socket_port(self.socket),
        }
        self.manager.local_info["serviceList"].append(self.info)
        self.manager.service_cbs[service_name.encode()] = self.callback
        logger.info(f'"{service_name}" Service is ready')

    async def callback(self, msg: bytes):
        result = await asyncio.wait_for(
            self.manager.loop.run_in_executor(
                self.manager.executor, self.process_bytes_request, msg
            ),
            timeout=5.0,
        )
        await self.socket.send(result)

    @abc.abstractmethod
    def process_bytes_request(self, msg: bytes) -> bytes:
        raise NotImplementedError

    def on_shutdown(self):
        self.manager.local_info["serviceList"].remove(self.info)
        logger.info(f'"{self.service_name}" Service is stopped')


class StrBytesService(AbstractService):

    def __init__(
        self,
        service_name: str,
        callback_func: Callable[[str], bytes],
    ) -> None:
        super().__init__(service_name)
        self.callback_func = callback_func

    def process_bytes_request(self, msg: bytes) -> bytes:
        return self.callback_func(msg.decode())


class StrService(AbstractService):

    def __init__(
        self,
        service_name: str,
        callback_func: Callable[[str], str],
    ) -> None:
        super().__init__(service_name)
        self.callback_func = callback_func

    def process_bytes_request(self, msg: bytes) -> bytes:
        return self.callback_func(msg.decode()).encode()
