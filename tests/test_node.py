import pylancom
import random


def test_master_node():
    node = pylancom.init_node(str(random.randint(1000, 9999)), "127.0.0.1")
    node.spin()

if __name__ == "__main__":
    test_master_node()
