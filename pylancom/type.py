from enum import Enum
from typing import Dict, List, TypedDict

import zmq
import zmq.asyncio

IPAddress = str
Port = int
TopicName = str
ServiceName = str
AsyncSocket = zmq.asyncio.Socket
HashIdentifier = str
ComponentType = str


class ComponentTypeEnum(Enum):
    PUBLISHER = "PUBLISHER"
    SUBSCRIBER = "SUBSCRIBER"
    SERVICE = "SERVICE"


class MasterReqType(Enum):
    PING = "PING"
    REGISTER_NODE = "REGISTER_NODE"
    NODE_OFFLINE = "NODE_OFFLINE"
    REGISTER_TOPIC = "REGISTER_TOPIC"
    REGISTER_SERVICE = "REGISTER_SERVICE"
    GET_NODES_INFO = "GET_NODES_INFO"
    SERVICE_INFO = "SERVICE_INFO"
    CHECK_TOPIC = "CHECK_TOPIC"
    CHECK_SERVICE = "CHECK_SERVICE"


class NodeReqType(Enum):
    PING = "PING"
    UPDATE_SUBSCRIPTION = "UPDATE_SUBSCRIPTION"


class ResponseType(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    EMPTY = "EMPTY"


class ComponentInfo(TypedDict):
    name: str
    componentID: HashIdentifier
    nodeID: HashIdentifier
    type: ComponentType
    ip: IPAddress
    port: Port


class NodeInfo(TypedDict):
    name: str
    nodeID: HashIdentifier  # hash code since bytes is not JSON serializable
    ip: IPAddress
    type: str
    port: int
    topicPort: int
    topicList: List[ComponentInfo]
    servicePort: int
    serviceList: List[ComponentInfo]
    subscriberList: List[ComponentInfo]


class ConnectionState(TypedDict):
    masterID: HashIdentifier
    timestamp: float
    topic: Dict[TopicName, List[ComponentInfo]]
    service: Dict[ServiceName, ComponentInfo]
