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
    UPDATE_SUBSCRIPTION = "UPDATE_SUBSCRIPTION"


class ResponseType(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    EMPTY = "EMPTY"


class ComponentTypeEnum(Enum):
    PUBLISHER = "PUBLISHER"
    SUBSCRIBER = "SUBSCRIBER"
    SERVICE = "SERVICE"


class ComponentInfo(TypedDict):
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
    publishers: List[ComponentInfo]
    subscribers: List[ComponentInfo]
    services: List[ComponentInfo]
    # topicPort: int
    # topicList: List[ComponentInfo]
    # servicePort: int
    # serviceList: List[ComponentInfo]
    # subscriberList: List[ComponentInfo]


class ConnectionState(TypedDict):
    masterID: HashIdentifier
    timestamp: float
    topic: Dict[TopicName, List[ComponentInfo]]
    service: Dict[ServiceName, ComponentInfo]
