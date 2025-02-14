from pylancom import start_master_node


def test_master_node_broadcast():
    master_node = start_master_node("127.0.0.1")
    master_node.spin()


if __name__ == "__main__":
    test_master_node_broadcast()
