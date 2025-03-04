from .abstract_node import AbstractNode


class SilentNode(AbstractNode):
    def __init__(self, node_name: str, node_ip: str) -> None:
        super().__init__(node_name, node_ip)

    def initialize_event_loop(self):
        return super().initialize_event_loop()

    def check_connection(self, updated_info):
        pass
