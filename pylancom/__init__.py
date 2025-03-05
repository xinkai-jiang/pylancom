# from pylancom.abstract_node import AbstractNode
from .config import __VERSION__ as __version__
from .log import logger as lancom_logger
from .nodes.lancom_node import LanComNode


def init_node(node_name: str, node_ip: str) -> LanComNode:
    if LanComNode.instance is not None:
        return LanComNode.instance
    return LanComNode(node_name, node_ip)
