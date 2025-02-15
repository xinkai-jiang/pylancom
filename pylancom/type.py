import zmq
import zmq.asyncio
from typing import List, Dict, TypedDict
from enum import Enum


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


class MasterSocketReqType(Enum):
    PING = "PING"
    REGISTER_NODE = "REGISTER_NODE"
    NODE_OFFLINE = "NODE_OFFLINE"
    REGISTER_TOPIC = "REGISTER_TOPIC"
    REGISTER_SERVICE = "REGISTER_SERVICE"
    GET_NODES_INFO = "GET_NODES_INFO"
    SERVICE_INFO = "REGISTER_NODE"


class NodeSocketReqType(Enum):
    PING = "PING"
    SUBSCRIBE_TOPIC = "SUBSCRIBE_TOPIC"


class ResponseType(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


class ComponentInfo(TypedDict):
    name: str
    componentID: HashIdentifier
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
