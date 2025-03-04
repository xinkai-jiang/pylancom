import asyncio
import hashlib
import socket
import struct
import time
import uuid

import zmq
import zmq.asyncio

from ..config import __VERSION_BYTES__, MASTER_SERVICE_PORT
from ..type import HashIdentifier, IPAddress, Port


def create_hash_identifier() -> HashIdentifier:
    """
    Generate a unique hash identifier.
    This function creates a new UUID (Universally Unique Identifier).
    Returns:
        HashIdentifier: A unique hash identifier in string format.
        The hash identifier is 36 characters long.
    """

    return str(uuid.uuid4())


def create_sha256(s: str):
    return hashlib.sha256(s.encode()).hexdigest()


def get_current_time() -> str:
    """
    Generates a 6-byte timestamp in 'YYMMDDHHMMSS' format.

    Returns:
        A string representing the last updated time (YYMMDDHHMMSS).
    """
    return time.strftime("%y%m%d%H%M%S")


def create_heartbeat_message(node_id: str, port: Port, info_id: int) -> bytes:
    """
    Constructs the full SDP heartbeat message efficiently using join().

    - `node_id`: The generated NodeID.
    - `version`: A 3-byte version string (default: "001").

    Returns:
        A complete SDP heartbeat message in bytes.
    """
    return b"".join(
        [
            b"LANCOM",  # 6-byte header
            __VERSION_BYTES__,  # 3-byte version
            node_id.encode(),  # 36-byte NodeID
            port.to_bytes(2, "big"),  # 2-byte port
            info_id.to_bytes(4, "big"),  # 4-byte timestamp
        ]
    )


def get_zmq_socket_port(socket: zmq.asyncio.Socket) -> int:
    endpoint: bytes = socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return int(endpoint.decode().split(":")[-1])


async def send_request(
    msg: str, ip: str, port: int, context: zmq.asyncio.Context
) -> str:
    req_socket = context.socket(zmq.REQ)
    req_socket.connect(f"tcp://{ip}:{port}")
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


async def send_node_request_to_master_async(
    master_ip: IPAddress, request_type: str, message: str
) -> str:
    addr = f"tcp://{master_ip}:{MASTER_SERVICE_PORT}"
    result = await send_bytes_request(
        addr, [request_type.encode(), message.encode()]
    )
    return result.decode()


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


async def send_bytes_request(
    addr: str, service_name: str, bytes_msgs: bytes, timeout: float = 1.0
) -> bytes:
    try:
        sock = zmq.asyncio.Context().socket(zmq.REQ)
        sock.connect(addr)
        # Send the message; you can also wrap this in wait_for if needed.
        await sock.send_multipart([service_name.encode(), bytes_msgs])

        # Wait for a response with a timeout.
        response = await asyncio.wait_for(sock.recv(), timeout=timeout)
        return response
    except asyncio.TimeoutError as e:
        raise asyncio.TimeoutError(
            f"Request timed out after {timeout} seconds."
        ) from e
    finally:
        sock.disconnect(addr)
        sock.close()
