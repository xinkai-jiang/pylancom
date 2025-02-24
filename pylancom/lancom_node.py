from __future__ import annotations

import multiprocessing as mp
import socket
import struct
import traceback
from asyncio import sleep as async_sleep
from json import dumps, loads
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import zmq
import zmq.asyncio
from zmq.asyncio import Socket as AsyncSocket

from . import utils
from .abstract_node import AbstractNode
from .config import DISCOVERY_PORT, MASTER_SERVICE_PORT, MULTICAST_ADDR
from .lancom_master import LanComMaster
from .log import logger
from .type import (
    ComponentInfo,
    MasterReqType,
    NodeInfo,
    NodeReqType,
    ResponseType,
    TopicName,
)
from .utils import search_for_master_node, send_node_request_to_master_async


class LanComNode(AbstractNode):
    instance: Optional[LanComNode] = None

    def __init__(
        self, node_name: str, node_ip: str, node_type: str = "LanComNode"
    ) -> None:
        master_ip = search_for_master_node()
        if master_ip is None:
            raise Exception("Master node not found")
        if LanComNode.instance is not None:
            raise Exception("LanComNode already exists")
        LanComNode.instance = self
        super().__init__(node_ip)
        self.master_ip = master_ip
        self.master_id = None
        self.pub_socket = self.create_socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{node_ip}:0")
        self.service_socket = self.create_socket(zmq.REP)
        self.service_socket.bind(f"tcp://{node_ip}:0")
        self.sub_sockets: Dict[str, List[Tuple[AsyncSocket, Callable]]] = {}
        self.local_info: NodeInfo = {
            "name": node_name,
            "nodeID": utils.create_hash_identifier(),
            "ip": node_ip,
            # "port": utils.get_zmq_socket_port(self.node_socket),
            "port": 0,
            "type": node_type,
            "topicPort": utils.get_zmq_socket_port(self.pub_socket),
            "servicePort": utils.get_zmq_socket_port(self.service_socket),
        }
        self.local_publisher: Dict[str, List[ComponentInfo]] = {}
        self.local_subscriber: Dict[str, List[ComponentInfo]] = {}
        self.local_service: Dict[str, ComponentInfo] = {}
        self.service_cbs: Dict[str, Callable[[bytes], bytes]] = {}

    def log_node_state(self):
        for key, value in self.local_info.items():
            print(f"    {key}: {value}")

    async def listen_master_loop(self):
        logger.info("Master Node is listening for multicast messages")
        with socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        ) as _socket:
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
                    data, addr = _socket.recvfrom(1024)
                    msg = data.decode()
                    # logger.debug(f"Received multicast message: {addr}")
                    if msg.startswith("LancomMaster"):
                        await self.update_master_state(data.decode())
                except Exception as e:
                    logger.error(f"Error receiving multicast message: {e}")
                    traceback.print_exc()
                await async_sleep(0.5)
        logger.info("Multicast receiving has been stopped")

    async def update_master_state(self, message: str) -> None:
        parts = message.split("|")
        if len(parts) != 5:
            return
        # TODO: check the version
        version, master_id, master_ip, _ = parts[1:]
        if master_id == self.master_id:
            return
        self.master_id, self.master_ip = master_id, master_ip
        # self.send_node_request(
        #     MasterReqType.REGISTER_NODE.value, dumps(self.local_info)
        # )
        # await send_node_request_to_master_async(
        #     self.master_ip, MasterReqType.REGISTER_NODE.value, dumps(self.local_info)
        # )
        # publisher_info: Dict[TopicName, List[ComponentInfo]] = loads(msg)
        # for topic_name, publisher_list in publisher_info.items():
        #     if topic_name not in self.sub_sockets.keys():
        #         continue
        #     for topic_info in publisher_list:
        #         self.subscribe_topic(topic_name, topic_info)
        logger.debug(
            f"{self.local_info['name']} Connecting to master node at {self.master_ip}"
        )

    def initialize_event_loop(self):
        self.submit_loop_task(
            self.service_loop, False, self.service_socket, self.service_cbs
        )
        node_service_cb: Dict[str, Callable[[bytes], bytes]] = {
            NodeReqType.UPDATE_SUBSCRIPTION.value: self.update_subscription,
        }
        self.submit_loop_task(
            self.service_loop, False, self.node_socket, node_service_cb
        )
        self.submit_loop_task(self.listen_master_loop, False)

    def spin_task(self) -> None:
        logger.info(f"Node {self.local_info['name']} is running...")
        return super().spin_task()

    def stop_node(self):
        logger.info(f"Stopping node {self.local_info['name']}...")
        try:
            # NOTE: the loop will be stopped when pressing Ctrl+C
            # so we need to create a new socket to send offline request
            request_socket = zmq.Context().socket(zmq.REQ)
            request_socket.connect(
                f"tcp://{self.master_ip}:{MASTER_SERVICE_PORT}"
            )
            node_id = self.local_info["nodeID"]
            request_socket.send_multipart(
                [MasterReqType.NODE_OFFLINE.value.encode(), node_id.encode()]
            )
            # request_socket.send_string(f"{MasterReqType.NODE_OFFLINE.value}|{node_id}")
        except Exception as e:
            logger.error(f"Error sending offline request to master: {e}")
            traceback.print_exc()
        super().stop_node()
        self.node_socket.close()
        logger.info(f"Node {self.local_info['name']} is stopped")

    def update_subscription(self, msg: bytes) -> bytes:
        publisher_info: ComponentInfo = utils.loads(msg.decode())
        self.subscribe_topic(publisher_info["name"], publisher_info)
        return ResponseType.SUCCESS.value.encode()

    def subscribe_topic(
        self, topic_name: TopicName, publisher_info: ComponentInfo
    ) -> None:
        if topic_name not in self.sub_sockets.keys():
            logger.warning(f"Wrong subscription request for {topic_name}")
            return
        for _socket, handle_func in self.sub_sockets[topic_name]:
            _socket.connect(
                f"tcp://{publisher_info['ip']}:{publisher_info['port']}"
            )
            self.submit_loop_task(handle_func, False)
        logger.info(
            f"Subscribers from Node {self.local_info['name']} "
            f"have been subscribed to topic {topic_name}"
        )

    def send_node_request(self, request_type: str, message: str) -> str:
        if self.master_ip is None:
            raise Exception("Master node is not launched")
        future = self.submit_loop_task(
            send_node_request_to_master_async,
            False,
            self.master_ip,
            request_type,
            message,
        )
        result = future.result()
        return result

    def check_topic(self, topic_name: str) -> Optional[List[ComponentInfo]]:
        result = self.send_node_request(
            MasterReqType.GET_TOPIC_INFO.value,
            topic_name,
        )
        if result == ResponseType.EMPTY.value:
            return None
        return loads(result)

    def check_service(self, service_name: str) -> Optional[ComponentInfo]:
        result = self.send_node_request(
            MasterReqType.GET_SERVICE_INFO.value,
            service_name,
        )
        if result == ResponseType.EMPTY.value:
            return None
        return loads(result)

    def register_publisher(self, publisher_info: ComponentInfo) -> None:
        topic_name = publisher_info["name"]
        self.send_node_request(
            MasterReqType.REGISTER_PUBLISHER.value,
            dumps(publisher_info),
        )
        if topic_name not in self.local_publisher.keys():
            self.local_publisher[topic_name] = []
        self.local_publisher[topic_name].append(publisher_info)

    def register_subscriber(
        self,
        subscriber_info: ComponentInfo,
        zmq_socket: AsyncSocket,
        handle_func: Callable[[], Awaitable],
    ) -> None:
        topic_name = subscriber_info["name"]
        msg = self.send_node_request(
            MasterReqType.REGISTER_SUBSCRIBER.value,
            dumps(subscriber_info),
        )
        if topic_name not in self.local_subscriber.keys():
            self.local_subscriber[topic_name] = []
        self.local_subscriber[topic_name].append(subscriber_info)
        if topic_name not in self.sub_sockets:
            self.sub_sockets[topic_name] = []
        self.sub_sockets[topic_name].append((zmq_socket, handle_func))
        publisher_info: Dict[TopicName, List[ComponentInfo]] = loads(msg)
        for topic_name, publisher_list in publisher_info.items():
            if topic_name not in self.sub_sockets.keys():
                continue
            for topic_info in publisher_list:
                self.subscribe_topic(topic_name, topic_info)


def start_master_node(node_ip: str) -> LanComMaster:
    master_ip = search_for_master_node()
    if master_ip is not None:
        raise Exception("Master node already exists")
    master_node = LanComMaster(node_ip)
    return master_node


def master_node_task(node_ip: str) -> None:
    master_node = start_master_node(node_ip)
    master_node.spin()


def init_node(node_name: str, node_ip: str) -> LanComNode:
    # if node_name == "Master":
    #     return MasterNode(node_name, node_ip)
    master_ip = search_for_master_node()
    if master_ip is None:
        logger.warning("Master node not found, starting a new master node...")
        mp.Process(target=master_node_task, args=(node_ip,)).start()
    return LanComNode(node_name, node_ip)
