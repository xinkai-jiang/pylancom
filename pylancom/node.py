from .abstract_node import AbstractNode
import socket
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_BROADCAST
import struct
import asyncio
from asyncio import sleep as async_sleep
from json import dumps
from typing import Dict, List, Optional, Tuple

from .log import logger
from .utils import DISCOVERY_PORT


class LanComNode(AbstractNode):

    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__(node_name, node_ip)

    def initialize_event_loop(self):
        pass

    async def spin_loop(self):
        """
        Connect to the TCP server and send periodic messages.
        """
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            logger.info(f"Connected to server at {self.host}:{self.port}")

            while self.running:
                message = f"Message {i + 1}"
                logger.info(f"Sending: {message}")
                writer.write(message.encode())
                await writer.drain()

                response = await reader.read(1024)
                logger.info(f"Received from server: {response.decode()}")

                await asyncio.sleep(
                    1
                )  # Wait for 1 second before sending the next message

            logger.info("Closing connection to the server")
            writer.close()
            await writer.wait_closed()

        except Exception as e:
            logger.error(f"Error in client: {e}")


class MasterNode(AbstractNode):
    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__(node_name, node_ip)

    async def spin_loop(self):
        pass

    def initialize_event_loop(self):
        self.submit_loop_task(
            self.tcp_server, self.local_info["ip"], DISCOVERY_PORT
        )

    async def tcp_server(self, host: str, port: int):
        server = await asyncio.start_server(self.handle_client, host, port)
        addr = server.sockets[0].getsockname()
        logger.info(f"TCP server started on {addr}")
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """
        Handle incoming TCP client connections and echo received messages.
        """
        addr = writer.get_extra_info("peername")
        logger.info(f"New connection from {addr}")

        try:
            while True:
                data = await reader.read(1024)  # Read up to 1024 bytes
                if not data:
                    logger.info(f"Connection closed by {addr}")
                    break
                logger.info(f"Received data from {addr}: {data.decode()}")
                writer.write(data)  # Echo the received data
                await writer.drain()  # Ensure the data is sent
        except Exception as e:
            logger.error(f"Error with client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection with {addr} closed")


def init_node(
    node_name: str, node_ip: str, port: Optional[int] = None
) -> AbstractNode:
    if node_name == "Master":
        return MasterNode(node_name, node_ip)
    return LanComNode(node_name, node_ip)
