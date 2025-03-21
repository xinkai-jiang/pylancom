import argparse
import multiprocessing as mp
import time
from typing import Callable, Dict, List

from utils import random_name

import pylancom
from pylancom.nodes.lancom_socket import Publisher, Subscriber
from pylancom.utils.serialization import StrDecoder


def create_subscriber_callback(
    topic_name: str,
) -> Callable[[str], None]:
    def subscriber_callback(msg: str) -> None:
        print(f"Subscriber {topic_name} received message: {msg}")
        assert msg == f"{topic_name} message"

    return subscriber_callback


def start_node(publisher_list: List[str], subscriber_list: List[str]):
    node_name = random_name("Node")
    node = pylancom.init_node(node_name, "127.0.0.1")
    publisher_dict: Dict[str, Publisher] = {}
    subscriber_dict: Dict[str, Subscriber] = {}
    for name in publisher_list:
        publisher_dict[name] = Publisher(name)
    for name in subscriber_list:
        subscriber_dict[name] = Subscriber(
            name, StrDecoder, create_subscriber_callback(name)
        )
    try:
        i = 0
        while True:
            for name, publisher in publisher_dict.items():
                # lancom_logger.debug(f"Publishing message from {name}")
                publisher.publish_string(f"{name} message")
                time.sleep(1)
            i += 1
    except KeyboardInterrupt:
        node.stop_node()
        print("Node stopped")
    except Exception as e:
        print(f"Unexpected error: {e}")
        node.stop_node()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    args = parser.parse_args()
    p1 = mp.Process(target=start_node, args=(["A", "B"], ["C", "D"]))
    p2 = mp.Process(target=start_node, args=(["C", "D"], ["A", "B"]))
    p1.start()
    time.sleep(2)
    p2.start()
    p1.join()
    # p2.join()
