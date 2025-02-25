import asyncio
import socket
import struct

import pytest

from pylancom.abstract_node import AbstractNode
from pylancom.protos.node_info_pb2 import NodeInfo


class TestNode(AbstractNode):
    def __init__(self, node_name, node_ip):
        super().__init__(node_name, node_ip)

    def initialize_event_loop(self):
        self.submit_loop_task(self.multicast_loop, False)
        self.submit_loop_task(self.listen_loop, False)


@pytest.fixture
def test_node():
    node = TestNode(node_name="TestNode", node_ip="127.0.0.1")
    yield node
    node.stop_node()


@pytest.mark.asyncio
async def test_multicast_loop(test_node):
    await asyncio.sleep(2)  # Allow some time for the multicast loop to run
    # Create a socket to listen for multicast messages
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", 5007))
    group = socket.inet_aton("224.0.0.1")
    mreq = struct.pack("4sL", group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    data, _ = sock.recvfrom(1024)
    parts = data.split(b"|", 2)
    assert parts[0] == b"LancomNode"
    sock.close()


@pytest.mark.asyncio
async def test_listen_loop(test_node):
    # Create a socket to send a multicast message
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_MULTICAST_IF,
        socket.inet_aton("127.0.0.1"),
    )

    node_info = NodeInfo(
        name="TestNode",
        nodeID="test_id",
        ip="127.0.0.1",
        port=0,
        type="TestNode",
    )
    node_info_bytes = node_info.SerializeToString()
    msg = b"LancomNode|test_id|" + node_info_bytes
    sock.sendto(msg, ("224.0.0.1", 5007))
    await asyncio.sleep(
        2
    )  # Allow some time for the listen loop to process the message
    sock.close()
