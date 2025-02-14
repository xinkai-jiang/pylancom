import pylancom

from utils import random_name


def test_initialize_node():
    node = pylancom.init_node(random_name("Node"), "127.0.0.1")
    node.spin()


if __name__ == "__main__":
    test_initialize_node()
