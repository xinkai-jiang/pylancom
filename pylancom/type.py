from enum import Enum
from typing import List, TypedDict

import zmq
import zmq.asyncio

IPAddress = str
Port = int
TopicName = str
ServiceName = str
AsyncSocket = zmq.asyncio.Socket
HashIdentifier = str
ComponentType = str


class MasterReqType(Enum):
    PING = "PING"
    REGISTER_NODE = "REGISTER_NODE"
    NODE_OFFLINE = "NODE_OFFLINE"
    REGISTER_PUBLISHER = "REGISTER_PUBLISHER"
    REGISTER_SUBSCRIBER = "REGISTER_SUBSCRIBER"
    REGISTER_SERVICE = "REGISTER_SERVICE"
    GET_NODES_INFO = "GET_NODES_INFO"
    GET_TOPIC_INFO = "GET_TOPIC_INFO"
    GET_SERVICE_INFO = "GET_SERVICE_INFO"


class NodeReqType(Enum):
    PING = "PING"
    NODE_INFO = "NODE_INFO"
    # UPDATE_SUBSCRIPTION = "UPDATE_SUBSCRIPTION"


class MsgType(Enum):
    BYTES = b"0"
    STR = b"1"
    PROTOBUF = b"2"
    JSON = b"3"
    NSGPACK = b"4"


class LanComMsg(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    EMPTY = "EMPTY"


class ComponentTypeEnum(Enum):
    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    SERVICE = "service"


class SocketInfo(TypedDict):
    name: str
    componentID: HashIdentifier
    nodeID: HashIdentifier
    type: ComponentType
    ip: IPAddress
    port: Port


class NodeInfo(TypedDict):
    name: str
    nodeID: HashIdentifier
    infoID: int
    ip: IPAddress
    type: str
    port: int
    publishers: List[SocketInfo]
    services: List[SocketInfo]


class UpdateConnection(TypedDict):
    publishers: List[SocketInfo]
    services: List[SocketInfo]
