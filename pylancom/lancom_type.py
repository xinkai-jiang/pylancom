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


class NodeReqType(Enum):
    PING = "PING"
    NODE_INFO = "NODE_INFO"


class LanComMsg(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    EMPTY = "EMPTY"


class SocketTypeEnum(Enum):
    PUBLISHER = "publisher"
    SUBSCRIBER = "subscriber"
    SERVICE = "service"


class SocketInfo(TypedDict):
    name: str
    socketID: HashIdentifier
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
