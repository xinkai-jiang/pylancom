from __future__ import annotations

import abc
import time
import traceback
from asyncio import sleep as async_sleep
from json import dumps
from typing import Callable, Dict, Optional, TypeVar, cast

import zmq
import zmq.asyncio

from ..lancom_type import (
    AsyncSocket,
    ComponentType,
    HashIdentifier,
    SocketInfo,
    SocketTypeEnum,
)
from ..utils.log import logger
from ..utils.msg import (
    create_hash_identifier,
    get_socket_port,
    send_bytes_request,
)
from .lancom_node import LanComNode


class AbstractLanComSocket(abc.ABC):
    def __init__(
        self,
        name: str,
        component_type: ComponentType,
        with_local_namespace: bool,
    ) -> None:
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        self.node: LanComNode = LanComNode.instance
        if with_local_namespace:
            local_name = self.node.local_info["name"]
            self.name = f"{local_name}/{name}"
        else:
            self.name = name
        self.info: SocketInfo = {
            "name": self.name,
            "socketID": create_hash_identifier(),
            "nodeID": self.node.local_info["nodeID"],
            "type": component_type,
            "ip": self.node.local_info["ip"],
            "port": 0,
        }
        self.running: bool = False
        # self.host_ip: str = self.node.local_info["ip"]

    def shutdown(self) -> None:
        self.running = False
        self.on_shutdown()

    def set_up_socket(self, zmq_socket: AsyncSocket) -> None:
        self.socket = zmq_socket
        self.info["port"] = get_socket_port(zmq_socket)

    @abc.abstractmethod
    def on_shutdown(self):
        raise NotImplementedError


class Publisher(AbstractLanComSocket):
    def __init__(self, topic_name: str, with_local_namespace: bool = False):
        super().__init__(
            topic_name,
            SocketTypeEnum.PUBLISHER.value,
            with_local_namespace,
        )
        self.set_up_socket(self.node.pub_socket)
        self.node.local_info["publishers"].append(self.info)
        self.node.local_info["infoID"] += 1
        self.socket = self.node.pub_socket

    def publish_bytes(self, bytes_msg: bytes) -> None:
        # in case the publish too much messages
        self.node.submit_loop_task(self.send_bytes_async(bytes_msg), True)

    def publish_string(self, msg: str) -> None:
        self.publish_bytes(msg.encode())

    def publish_dict(self, data: Dict) -> None:
        self.publish_string(dumps(data))

    def on_shutdown(self) -> None:
        self.socket.close()

    async def send_bytes_async(self, bytes_msg: bytes) -> None:
        # await self.socket.send(msg)
        await self.socket.send_multipart([self.name.encode(), bytes_msg])


MessageT = TypeVar("MessageT", bytes, str, dict)


class Streamer(Publisher):
    def __init__(
        self,
        topic_name: str,
        update_func: Callable[[], MessageT],
        fps: int,
        msg_encoder: Callable[[MessageT], bytes],
        start_streaming: bool = False,
    ):
        super().__init__(topic_name)
        self.running = False
        self.dt: float = 1 / fps
        self.update_func = update_func
        self.topic_byte = self.name.encode("utf-8")
        self.msg_encoder = msg_encoder
        if start_streaming:
            self.start_streaming()

    def start_streaming(self):
        self.node.submit_loop_task(self.update_loop(), False)

    def generate_byte_msg(self) -> bytes:
        return self.msg_encoder(self.update_func())
        # if isinstance(update_msg, str):
        #     return update_msg.encode("utf-8")
        # elif isinstance(update_msg, bytes):
        #     return update_msg
        # elif isinstance(update_msg, dict):
        #     # return dumps(update_msg).encode("utf-8")
        #     return dumps(
        #         {
        #             "updateData": self.update_func(),
        #             "time": time.monotonic(),
        #         }
        #     ).encode("utf-8")
        # raise ValueError("Update function should return str, bytes or dict")

    async def update_loop(self) -> None:
        self.running = True
        last = 0.0
        logger.info(f"Topic {self.name} starts streaming")
        while self.running:
            try:
                diff = time.monotonic() - last
                if diff < self.dt:
                    await async_sleep(self.dt - diff)
                last = time.monotonic()
                await self.send_bytes_async(self.generate_byte_msg())
            except Exception as e:
                logger.error(f"Error when streaming {self.name}: {e}")
                traceback.print_exc()
        logger.info(f"Streamer for topic {self.name} is stopped")


class Subscriber(AbstractLanComSocket):
    def __init__(
        self,
        topic_name: str,
        msg_decoder: Callable[[bytes], MessageT],
        callback: Callable[[MessageT], None],
    ):
        super().__init__(topic_name, SocketTypeEnum.SUBSCRIBER.value, False)
        self.socket = self.node.create_socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.name.encode())
        self.subscribed_components: Dict[HashIdentifier, SocketInfo] = {}
        self.msg_decoder = msg_decoder
        self.connected = False
        self.callback = callback
        self.running = True
        self.node.submit_loop_task(self.listen_loop(), False)
        self.node.submit_loop_task(self.receive_loop(), False)

    async def receive_loop(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        logger.info(f"Subscriber {self.name} is subscribing ...")
        while self.running:
            try:
                # Wait for a message
                _, msg = await self.socket.recv_multipart()
                # Invoke the callback
                self.callback(self.msg_decoder(msg))
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' subscriber: {e}")
                traceback.print_exc()

    async def listen_loop(self) -> None:
        """Listens for new publishers and connects to them."""
        logger.info(f"Subscriber {self.name} is listening ...")
        while self.running:
            try:
                publishers = self.node.nodes_map.get_publisher_info(self.name)
                # print(f"Publishers: {publishers}")
                for pub_info in publishers:
                    if pub_info["socketID"] not in self.subscribed_components:
                        self.connect(pub_info)
                await async_sleep(0.5)
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' listener: {e}")
                traceback.print_exc()

    def connect(self, pub_info: SocketInfo) -> None:
        self.socket.connect(f"tcp://{pub_info['ip']}:{pub_info['port']}")
        self.subscribed_components[pub_info["socketID"]] = pub_info
        self.connected = True
        logger.info(
            f"Subscriber {self.name} is connected to {pub_info['name']}"
            f" from {pub_info['ip']}:{pub_info['port']}"
        )

    def on_shutdown(self) -> None:
        self.running = False
        self.socket.close()


RequestT = TypeVar("RequestT", bytes, str, dict)
ResponseT = TypeVar("ResponseT", bytes, str, dict)


class Service(AbstractLanComSocket):
    def __init__(
        self,
        service_name: str,
        request_decoder: Callable[[bytes], RequestT],
        response_encoder: Callable[[ResponseT], bytes],
        callback: Callable[[RequestT], ResponseT],
    ) -> None:
        super().__init__(service_name, SocketTypeEnum.SERVICE.value, False)
        self.set_up_socket(self.node.service_socket)
        # check the service is already registered locally
        for service_info in self.node.local_info["services"]:
            if service_info["name"] != self.name:
                continue
            raise RuntimeError("Service has been registered locally")
        if self.node.nodes_map.get_service_info(service_name) is not None:
            raise RuntimeError("Service has been registered")
        self.node.local_info["services"].append(self.info)
        self.node.local_info["infoID"] += 1
        self.node.service_cbs[self.name] = self.callback
        self.handle_request = callback
        self.request_decoder = request_decoder
        self.response_encoder = response_encoder
        logger.info(f'"{self.name}" Service is started')

    def callback(self, msg: bytes) -> bytes:
        request = self.request_decoder(msg)
        result = self.handle_request(request)
        return self.response_encoder(result)

    def on_shutdown(self):
        self.node.local_info["services"].remove(self.info)
        logger.info(f'"{self.name}" Service is stopped')


class ServiceProxy:
    @staticmethod
    def request(
        service_name: str,
        request_encoder: Callable[[RequestT], bytes],
        response_decoder: Callable[[bytes], ResponseT],
        request: RequestT,
    ) -> Optional[ResponseT]:
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        node = LanComNode.instance
        service_component = node.nodes_map.get_service_info(service_name)
        if service_component is None:
            logger.warning(f"Service {service_name} is not exist")
            return None
        request_bytes = request_encoder(request)
        addr = f"tcp://{service_component['ip']}:{service_component['port']}"
        response = node.submit_loop_task(
            send_bytes_request(addr, service_name, request_bytes),
            True,
        )
        return response_decoder(cast(bytes, response))
