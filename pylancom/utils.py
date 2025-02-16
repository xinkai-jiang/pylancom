import asyncio
import socket
import struct
import traceback
import uuid
from json import dumps, loads
from typing import Dict, List, Optional

import zmq
import zmq.asyncio

from .config import DISCOVERY_PORT
from .type import HashIdentifier, IPAddress


def create_hash_identifier() -> HashIdentifier:
    return str(uuid.uuid4())


def create_request(request_type: str, data: str) -> str:
    return f"{request_type}|{data}"


def msgs_join(msgs: List[str]) -> str:
    return "|".join(msgs)


def bytes2str(byte_msg: bytes) -> str:
    return byte_msg.decode()


def str2bytes(str_msg: str) -> bytes:
    return str_msg.encode()


def dict2bytes(dict_msg: Dict) -> bytes:
    return dumps(dict_msg).encode()


def bytes2dict(byte_msg: bytes) -> Dict:
    return loads(byte_msg.decode())


def create_udp_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def get_zmq_socket_port(socket: zmq.asyncio.Socket) -> int:
    endpoint: bytes = socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return int(endpoint.decode().split(":")[-1])


def bmsgsplit(
    bytes_msg: bytes, separator: bytes = b"|", num: int = 1
) -> List[bytes]:
    return bytes_msg.split(separator, num)


def bmsgsplit2str(
    bytes_msg: bytes, separator: str = "|", num: int = 1
) -> List[str]:
    return bytes_msg.decode().split(separator, num)


def strmsgsplit(str_msg: str, separator: str = "|", num: int = 1) -> List[str]:
    return str_msg.split(separator, num)


# def split_byte_to_str(bytes_msg: bytes) -> List[str]:
#     return [item.decode() for item in split_byte(bytes_msg)]


# def split_str(str_msg: str) -> List[str]:
#     return str_msg.split("|", 1)


# async def send_request(
#     msg: str, addr: str, context: zmq.asyncio.Context
# ) -> str:
#     req_socket = context.socket(zmq.REQ)
#     req_socket.connect(addr)
#     try:
#         await req_socket.send_string(msg)
#     except Exception as e:
#         logger.error(
#             f"Error when sending message from send_message function in "
#             f"pylancom.core.utils: {e}"
#         )
#     result = await req_socket.recv_string()
#     req_socket.close()
#     return result


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
        str or None: Sender's IP address if a message is received,
        None otherwise.
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


# async def send_request_async(
#     sock: zmq.asyncio.Socket, addr: str, message: str
# ) -> str:
#     """
#     Asynchronously sends a request via a ZeroMQ socket using asyncio.

#     :param sock: A zmq.asyncio.Socket instance.
#     :param addr: The address to connect to (e.g., "tcp://127.0.0.1:5555").
#     :param message: The message to send.
#     :return: The response received from the server as a string.
#     """
#     sock.connect(addr)
#     await sock.send_string(message)
#     response = await sock.recv_string()
#     sock.disconnect(addr)
#     return response


async def send_request_async(
    sock: zmq.asyncio.Socket, addr: str, message: str, timeout: float = 5.0
) -> str:
    try:
        sock.connect(addr)
        # Send the message; you can also wrap this in wait_for if needed.
        await sock.send_string(message)

        # Wait for a response with a timeout.
        response = await asyncio.wait_for(sock.recv_string(), timeout=timeout)
        return response
    except asyncio.TimeoutError as e:
        raise asyncio.TimeoutError(
            f"Request timed out after {timeout} seconds."
        ) from e
    finally:
        sock.disconnect(addr)
