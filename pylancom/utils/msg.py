import asyncio
import hashlib
import socket
import struct
import uuid

import zmq
import zmq.asyncio

from ..config import __VERSION_BYTES__
from ..lancom_type import HashIdentifier, IPAddress, LanComMsg, Port
from .log import logger


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


def get_socket_port(socket: zmq.asyncio.Socket) -> int:
    endpoint: bytes = socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return int(endpoint.decode().split(":")[-1])


def calculate_broadcast_addr(ip_addr: IPAddress) -> IPAddress:
    ip_bin = struct.unpack("!I", socket.inet_aton(ip_addr))[0]
    netmask_bin = struct.unpack("!I", socket.inet_aton("255.255.255.0"))[0]
    broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
    return socket.inet_ntoa(struct.pack("!I", broadcast_bin))


async def send_bytes_request(
    addr: str, service_name: str, bytes_msgs: bytes, timeout: float = 1.0
) -> bytes:
    response = LanComMsg.TIMEOUT.value.encode()
    try:
        sock = zmq.asyncio.Context().socket(zmq.REQ)
        sock.connect(addr)
        # Send the message; you can also wrap this in wait_for if needed.
        await sock.send_multipart([service_name.encode(), bytes_msgs])

        # Wait for a response with a timeout.
        response = await asyncio.wait_for(sock.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"Request {service_name} timed out for {timeout} s.")
    finally:
        sock.disconnect(addr)
        sock.close()
        return response
