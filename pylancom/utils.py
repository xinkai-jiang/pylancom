import zmq
import zmq.asyncio
import struct
import socket
from typing import List, Dict, TypedDict, Optional
import enum
from enum import IntEnum
from traceback import print_exc
import uuid

from .log import logger

IPAddress = str
Port = int
TopicName = str
ServiceName = str
AsyncSocket = zmq.asyncio.Socket
HashIdentifier = str


BROADCAST_INTERVAL = 0.5
HEARTBEAT_INTERVAL = 0.2
DISCOVERY_PORT = int(7720)
MASTER_TOPIC_PORT = int(7721)
MASTER_SERVICE_PORT = int(7722)


class MSG(IntEnum):
    PING = 0
    SERVICE_ERROR = 1
    SERVICE_TIMEOUT = 2


class ComponentType(IntEnum):
    PUBLISHER = 0
    SUBSCRIBER = 1
    SERVICE = 2


class ComponentInfo(TypedDict):
    name: str
    componentID: HashIdentifier
    type: str
    ip: IPAddress
    port: Port


class NodeInfo(TypedDict):
    name: str
    nodeID: HashIdentifier  # hash code since bytes is not JSON serializable
    ip: IPAddress
    # port: Port
    type: str
    topicPort: int
    topicList: List[ComponentInfo]
    servicePort: int
    serviceList: List[ComponentInfo]


class ConnectionState(TypedDict):
    topic: Dict[TopicName, List[ComponentInfo]]
    service: Dict[ServiceName, ComponentInfo]


def create_hash_identifier() -> HashIdentifier:
    return str(uuid.uuid4())


async def send_request(msg: str, addr: str, context: zmq.asyncio.Context) -> str:
    req_socket = context.socket(zmq.REQ)
    req_socket.connect(addr)
    try:
        await req_socket.send_string(msg)
    except Exception as e:
        logger.error(
            f"Error when sending message from send_message function in "
            f"pylancom.core.utils: {e}"
        )
    result = await req_socket.recv_string()
    req_socket.close()
    return result


# def calculate_broadcast_addr(ip_addr: IPAddress) -> IPAddress:
#     ip_bin = struct.unpack("!I", socket.inet_aton(ip_addr))[0]
#     netmask_bin = struct.unpack("!I", socket.inet_aton("255.255.255.0"))[0]
#     broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
#     return socket.inet_ntoa(struct.pack("!I", broadcast_bin))


def create_udp_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def get_zmq_socket_port(socket: zmq.asyncio.Socket) -> int:
    endpoint: bytes = socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return int(endpoint.decode().split(":")[-1])


def bmsgsplit(bytes_msg: bytes) -> List[bytes]:
    return bytes_msg.split(b"|", 1)


# def split_byte_to_str(bytes_msg: bytes) -> List[str]:
#     return [item.decode() for item in split_byte(bytes_msg)]


# def split_str(str_msg: str) -> List[str]:
#     return str_msg.split("|", 1)


def search_for_master_node(
    ip: str = "0.0.0.0", port: int = DISCOVERY_PORT, timeout: float = 1.0
) -> Optional[str]:
    """
    Listens on the given UDP port for incoming messages.

    Args:
        port (int): The port number to listen on.
        timeout (int): Timeout in seconds before quitting.

    Returns:
        str or None: Sender's IP address if a message is received, None otherwise.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((ip, port))  # Listen on all interfaces
        server_socket.settimeout(timeout)  # Set a timeout

        try:
            print(f"Listening for UDP messages on port {port}...")
            data, addr = server_socket.recvfrom(1024)  # Receive data (max 1024 bytes)
            if data:
                print(f"Received message from {addr[0]}: {data.decode()}")
                return addr[0]  # Return sender's IP address
        except socket.timeout:
            print("No message received. Exiting.")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None


# def search_for_master_node(
#     local_ip: Optional[IPAddress] = None,
#     search_time: int = 5,
#     time_out: float = 0.1
# ) -> Optional[Tuple[IPAddress, str]]:
#     if local_ip is not None:
#         broadcast_ip = calculate_broadcast_addr(local_ip)
#     else:
#         broadcast_ip = "255.255.255.255"
#     with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as _socket:
#         # wait for response
#         _socket.bind(("0.0.0.0", 0))
#         _socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
#         for _ in range(search_time):
#             _socket.sendto(
#                 EchoHeader.PING.value, (broadcast_ip, DISCOVERY_PORT)
#             )
#             _socket.settimeout(time_out)
#             try:
#                 data, addr = _socket.recvfrom(1024)
#                 logger.info(f"Find a master node at {addr[0]}:{addr[1]}")
#                 return addr[0], data.decode()
#             except socket.timeout:
#                 continue
#             except KeyboardInterrupt:
#                 break
#             except Exception as e:
#                 logger.error(f"Error when searching for master node: {e}")
#                 print_exc()
#     logger.info("No master node found, start as master node")
#     return None
