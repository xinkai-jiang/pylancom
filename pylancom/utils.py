import zmq
import zmq.asyncio
import asyncio
import struct
import socket
from typing import List, Dict, Optional, Tuple
import traceback
import uuid
from json import dumps, loads

from .log import logger
from .type import IPAddress, Port, HashIdentifier, AsyncSocket
from .config import DISCOVERY_PORT


def create_hash_identifier() -> HashIdentifier:
    return str(uuid.uuid4())


def create_request(request_type: str, data: str) -> str:
    return f"{request_type}|{data}"


def msgs_join(msgs: List[str]) -> str:
    return "|".join(msgs)


def byte2str(byte_msg: bytes) -> str:
    return byte_msg.decode()


def str2byte(str_msg: str) -> bytes:
    return str_msg.encode()


def dict2byte(dict_msg: Dict) -> bytes:
    return dumps(dict_msg).encode()


def byte2dict(byte_msg: bytes) -> Dict:
    return loads(byte_msg.decode())


def create_udp_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def get_zmq_socket_port(socket: zmq.asyncio.Socket) -> int:
    endpoint: bytes = socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return int(endpoint.decode().split(":")[-1])


def bmsgsplit(bytes_msg: bytes, separator: bytes = b"|", num: int = 1) -> List[bytes]:
    return bytes_msg.split(separator, num)


def bmsgsplit2str(bytes_msg: bytes, separator: str = "|", num: int = 1) -> List[str]:
    return bytes_msg.decode().split(separator, num)


def strmsgsplit(str_msg: str, separator: str = "|", num: int = 1) -> List[str]:
    return str_msg.split(separator, num)


# def split_byte_to_str(bytes_msg: bytes) -> List[str]:
#     return [item.decode() for item in split_byte(bytes_msg)]


# def split_str(str_msg: str) -> List[str]:
#     return str_msg.split("|", 1)


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


def calculate_broadcast_addr(ip_addr: IPAddress) -> IPAddress:
    ip_bin = struct.unpack("!I", socket.inet_aton(ip_addr))[0]
    netmask_bin = struct.unpack("!I", socket.inet_aton("255.255.255.0"))[0]
    broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
    return socket.inet_ntoa(struct.pack("!I", broadcast_bin))


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
            data, addr = server_socket.recvfrom(1024)
            if data:
                return addr[0]  # Return sender's IP address
        except socket.timeout:
            return None
        except Exception:
            traceback.print_exc()
            return None

# TODO: use zmq instead of socket
async def send_request_async(
    addr: Tuple[IPAddress, Port], message: str
) -> str:
    loop = asyncio.get_running_loop()
    # Connect to the target server
    await loop.sock_connect(sock, addr)
    # Send the message (encoded to bytes)
    await loop.sock_sendall(sock, message.encode("utf-8"))
    # Wait and receive the response (up to 4096 bytes)
    response = await loop.sock_recv(sock, 4096)
    # Close the socket after the transaction
    # sock.close()
    return response.decode("utf-8")


def send_tcp_request(addr: Tuple[IPAddress, Port], message: str) -> str:
    """
    Synchronously sends a TCP request to the specified address using a blocking socket,
    utilizing a context manager to ensure proper resource cleanup.

    :param addr: A tuple (IP address, port) to connect to.
    :param message: The message to send.
    :return: The response received from the server as a string.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(addr)
        sock.sendall(message.encode("utf-8"))
        response = sock.recv(4096)
        return response.decode("utf-8")
