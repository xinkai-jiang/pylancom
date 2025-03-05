# PyLanCom

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![Python Versions](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue)

A lightweight LAN communication framework for Python, providing node discovery and pub/sub messaging on local networks.

## Overview

PyLanCom bridges the gap between heavyweight communication frameworks like ROS2 and low-level messaging libraries like ZeroMQ. It provides:

- **Automatic node discovery** on LAN networks
- **Publisher/Subscriber** messaging patterns
- **Service/Client** request-response patterns
- **Minimal dependencies** with a small footprint
- **Simple API** for quick adoption

PyLanCom solves the node discovery problem that ZeroMQ lacks while avoiding the complexity and overhead of larger frameworks.

## Features

- **ZeroMQ-based communication**: High-performance, asynchronous messaging
- **Automatic node discovery**: Using multicast for service advertisement and discovery
- **Pub/Sub messaging**: Topic-based publish/subscribe pattern
- **Service/Client pattern**: Request-response communication between nodes
- **Streaming capability**: For continuous data publishing
- **Flexible message formats**: Support for strings, bytes, JSON, and msgpack
- **Async-friendly**: Built on asyncio for modern Python applications
- **Colorful logging**: Formatted debug output with customized log levels

## Installation

```bash
pip install pylancom
```

Or install from source:

```bash
git clone https://github.com/yourusername/pylancom.git
cd pylancom
pip install -e .
```

## Requirements

- Python 3.8+
- ZeroMQ (pyzmq)
- colorama
- msgpack

## Quick Start

### Initialize a Node

```python
import pylancom

# Initialize a node with a name and IP
node = pylancom.init_node("my_node", "127.0.0.1")
```

### Publisher Example

```python
from pylancom.nodes.lancom_socket import Publisher
from time import sleep

# Create a publisher
publisher = Publisher("my_topic")

# Publish messages
while True:
    publisher.publish_string("Hello from my_topic!")
    sleep(1)
```

### Subscriber Example

```python
from pylancom.nodes.lancom_socket import Subscriber
from pylancom.utils.msg_utils import StrDecoder

# Define a callback for received messages
def message_callback(msg):
    print(f"Received: {msg}")

# Create a subscriber with the same topic name
subscriber = Subscriber("my_topic", StrDecoder, message_callback)

# Keep the node running
node.spin()
```

### Service Example

```python
from pylancom.nodes.lancom_socket import Service
from pylancom.utils.msg_utils import StrEncoder, StrDecoder

# Define a service callback
def service_handler(request):
    print(f"Service received: {request}")
    return f"Processed: {request}"

# Create a service
service = Service("my_service", StrDecoder, StrEncoder, service_handler)

# Keep the node running
node.spin()
```

### Service Client Example

```python
from pylancom.nodes.lancom_socket import ServiceProxy
from pylancom.utils.msg_utils import StrEncoder, StrDecoder

# Make a request to a service
response = ServiceProxy.request("my_service", StrEncoder, StrDecoder, "Hello Service!")
print(f"Response: {response}")
```

## Data Streaming

For continuous data publishing:

```python
from pylancom.nodes.lancom_socket import Streamer
from pylancom.utils.msg_utils import StrEncoder

# Define a data source function
def get_sensor_data():
    return f"Temperature: {20 + random.random() * 10:.2f}°C"

# Create a streamer publishing at 10 Hz
streamer = Streamer("sensor_data", get_sensor_data, 10, StrEncoder, start_streaming=True)

# Keep the node running
node.spin()
```

## Advanced Usage

### Multiple Nodes Communication

PyLanCom supports communication between multiple nodes across different processes or even machines:

```python
# See examples/multiple_nodes_example.py for a complete implementation
```

### Custom Message Types

You can create custom encoders and decoders for your own message formats:

```python
from pylancom.utils.msg_utils import MsgpackEncoder, MsgpackDecoder

# Example with msgpack
publisher.publish_dict({"sensor": "temperature", "value": 22.5})

# Decode with the appropriate decoder in the subscriber
subscriber = Subscriber("my_topic", MsgpackDecoder, my_callback)
```

## Architecture

PyLanCom uses a combination of:

1. **Multicast Heartbeats**: For node discovery
2. **ZeroMQ PUB/SUB**: For topic-based messaging
3. **ZeroMQ REQ/REP**: For service calls
4. **Asyncio**: For non-blocking operation

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/pylancom.git
cd pylancom

# Install development dependencies
pip install -e .

# Setup pre-commit hooks
pip install pre-commit
pre-commit install
```

### Run Tests

```bash
python -m tests.test_utils
python -m tests.test_nodes_discover
python -m tests.test_topic
python -m tests.test_service
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Roadmap

- [ ] Add full documentation and API reference
- [ ] Add retry mechanisms for connection failures
- [ ] Support for secure communications (TLS)
- [ ] Add more message serialization options
- [ ] Create bindings for other languages

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgements

- ZeroMQ for providing the underlying messaging technology
- Colorama for the colored logging output