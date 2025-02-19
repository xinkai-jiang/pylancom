from __future__ import annotations

import abc
import time
import traceback
from asyncio import sleep as async_sleep
from json import dumps
from typing import Callable, Dict, Optional, Type, Union, cast

import zmq
import zmq.asyncio

from . import utils
from .lancom_node import LanComNode
from .log import logger
from .type import (
    AsyncSocket,
    ComponentInfo,
    ComponentType,
    ComponentTypeEnum,
    HashIdentifier,
)
from .utils import (
    create_hash_identifier,
    get_zmq_socket_port,
    send_bytes_request,
)


class AbstractComponent(abc.ABC):
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
        self.info: ComponentInfo = {
            "name": self.name,
            "componentID": create_hash_identifier(),
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
        self.info["port"] = get_zmq_socket_port(zmq_socket)

    @abc.abstractmethod
    def on_shutdown(self):
        raise NotImplementedError


class Publisher(AbstractComponent):
    def __init__(self, topic_name: str, with_local_namespace: bool = False):
        super().__init__(
            topic_name,
            ComponentTypeEnum.PUBLISHER.value,
            with_local_namespace,
        )
        # in case the topic is not updated to the master node
        if topic_name in self.node.local_info["topicList"]:
            raise RuntimeError("Topic has been registered in the local node")
        # TODO: check if the topic is already registered
        if self.node.check_topic(topic_name) is not None:
            logger.warning(f"Topic {topic_name} is already registered")
            return
        self.set_up_socket(self.node.pub_socket)
        self.node.local_info["topicList"].append(self.info)
        self.socket = self.node.pub_socket

    def publish_bytes(self, bytes_msg: bytes) -> None:
        self.node.submit_loop_task(self.send_bytes_async, False, bytes_msg)

    def publish_string(self, msg: str) -> None:
        self.node.submit_loop_task(self.send_bytes_async, False, msg.encode())

    def publish_dict(self, data: Dict) -> None:
        self.publish_string(dumps(data))

    def on_shutdown(self) -> None:
        self.node.local_info["topicList"].remove(self.info)

    async def send_bytes_async(self, bytes_msg: bytes) -> None:
        # await self.socket.send(msg)
        await self.socket.send_multipart([self.name.encode(), bytes_msg])


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
        self.topic_byte = self.name.encode("utf-8")
        if start_streaming:
            self.start_streaming()

    def start_streaming(self):
        self.node.submit_loop_task(self.update_loop, False)

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
        logger.info(f"Topic {self.name} starts streaming")
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
                logger.error(f"Error when streaming {self.name}: {e}")
                traceback.print_exc()
        logger.info(f"Streamer for topic {self.name} is stopped")


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


MessageT = Union[bytes, str, dict]


class Subscriber(AbstractComponent):
    def __init__(
        self,
        topic_name: str,
        msg_type: Type[MessageT],
        callback: Callable[[MessageT], None],
    ):
        super().__init__(topic_name, ComponentTypeEnum.SUBSCRIBER.value, False)
        self.socket = self.node.create_socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.name.encode())
        self.subscribed_components: Dict[HashIdentifier, ComponentInfo] = {}
        if msg_type is bytes:
            self.decoder = cast(Callable[[bytes], bytes], lambda x: x)
        elif msg_type is str:
            self.decoder = utils.bytes2str
        elif msg_type is dict:
            self.decoder = utils.bytes2dict
        else:
            raise ValueError("Request type is not supported")
        self.connected = False
        self.callback = callback
        self.node.local_info["subscriberList"].append(self.info)
        self.node.register_subscription(self.name, self.socket, self.listen)
        self.running = True

    async def listen(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        logger.info(f"Subscriber {self.name} is listening...")
        while self.running:
            try:
                # Wait for a message
                _, msg = await self.socket.recv_multipart()
                # Invoke the callback
                self.callback(self.decoder(msg))
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' subscriber: {e}")
                traceback.print_exc()

    def on_shutdown(self) -> None:
        self.running = False
        self.socket.close()


RequestT = Union[bytes, str, dict]
ResponseT = Union[bytes, str, dict]


class Service(AbstractComponent):
    def __init__(
        self,
        service_name: str,
        request_type: Type[RequestT],
        response_type: Type[ResponseT],
        callback: Callable[[RequestT], ResponseT],
    ) -> None:
        super().__init__(service_name, ComponentTypeEnum.SERVICE.value, False)
        self.set_up_socket(self.node.service_socket)
        if service_name in self.node.local_info["serviceList"]:
            raise RuntimeError("Service has been registered in the local node")
        # TODO: check if the service is already registered
        # if self.node.check_service(service_name) is not None:
        #     logger.warning(f"Service {service_name} is already registered")
        #     return
        # self.decoder: Callable[[bytes], RequestT]
        if request_type is bytes:
            self.decoder = cast(Callable[[bytes], bytes], lambda x: x)
        elif request_type is str:
            self.decoder = utils.bytes2str
        elif request_type is dict:
            self.decoder = utils.bytes2dict
        else:
            raise ValueError("Request type is not supported")
        # self.encoder: Callable[[ResponseT], bytes]
        if response_type is bytes:
            self.encoder = cast(Callable[[bytes], bytes], lambda x: x)
        elif response_type is str:
            self.encoder = utils.str2bytes
        elif response_type is dict:
            self.encoder = utils.dict2bytes
        else:
            raise ValueError("Response type is not supported")
        self.node.local_info["serviceList"].append(self.info)
        self.node.service_cbs[self.name] = self.callback
        self.handle_request = callback
        logger.info(f'"{self.name}" Service is started')

    def callback(self, msg: bytes) -> bytes:
        request = self.decoder(msg)
        result = self.handle_request(request)
        # TODO: not sure if we need to use the executor
        # result = await asyncio.wait_for(
        #     self.node.loop.run_in_executor(
        #         self.node.executor, self.process_bytes_request, msg
        #     ),
        #     timeout=5.0,
        # )
        # TODO: check if the result is valid
        return self.encoder(result)  # type: ignore
        # await self.socket.send(self.encoder(result))

    def on_shutdown(self):
        self.node.local_info["serviceList"].remove(self.info)
        logger.info(f'"{self.name}" Service is stopped')


class ServiceProxy:
    @staticmethod
    def send_service_request(
        service_component: ComponentInfo,
        request_type: Type[RequestT],
        response_type: Type[ResponseT],
        request: RequestT,
        timeout: float = 5.0,
    ) -> Optional[ResponseT]:
        if request_type is bytes:
            request = cast(bytes, request)
        elif request_type is str:
            request = utils.str2bytes(cast(str, request))
        elif request_type is dict:
            request = utils.dict2bytes(cast(dict, request))
        else:
            raise ValueError("Unsupported request type")
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        node = LanComNode.instance
        addr = f"tcp://{service_component['ip']}:{service_component['port']}"
        response = node.submit_loop_task(
            send_bytes_request,
            True,
            addr,
            [service_component["name"].encode(), request],
        )
        # response = future.result(timeout)
        print(f"Response: {response}")
        if not isinstance(response, bytes):
            logger.warning(f"Service {service_component['name']} is not exist")
            return None
        if response_type is bytes:
            return cast(ResponseT, response)
        elif response_type is str:
            return cast(ResponseT, utils.bytes2str(response))
        elif response_type is dict:
            return cast(ResponseT, utils.bytes2dict(response))
        else:
            raise ValueError("Unsupported response type")

    @staticmethod
    def request(
        service_name: str,
        request_type: Type[RequestT],
        response_type: Type[ResponseT],
        request: RequestT,
        timeout: float = 5.0,
    ) -> Optional[ResponseT]:
        """
        Convenience method to perform a service request in one call.

        It chooses the appropriate encoder and decoder based on the request
        and response types. Supported types are: bytes, str, and dict.

        :param service_name: Name of the target service.
        :param request_type: The type of the request (e.g., str, dict, bytes).
        :param response_type: The type of the response (e.g., str, dict, bytes).
        :param request: The request data.
        :param endpoint: The ZeroMQ endpoint of the service.
        :param timeout: Maximum time to wait for a response.
        :return: The decoded response.
        """
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        node = LanComNode.instance
        service_component = node.check_service(service_name)
        if service_component is None:
            logger.warning(f"Service {service_name} is not exist")
            return None
        result = ServiceProxy.send_service_request(
            service_component,
            request_type,
            response_type,
            request,
            timeout,
        )
        return result  # type: ignore
