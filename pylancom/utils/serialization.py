import json
from typing import Any, Dict

import msgpack


def BytesEncoder(msg: bytes) -> bytes:
    return msg


def StrEncoder(msg: str) -> bytes:
    return msg.encode()


def JsonEncoder(msg: Dict) -> bytes:
    return json.dumps(msg).encode()


def MsgpackEncoder(msg: Dict) -> bytes:
    result = msgpack.dumps(msg)
    if result is None:
        raise ValueError("Failed to encode message")
    return result


def BytesDecoder(msg: bytes) -> bytes:
    return msg


def StrDecoder(msg: bytes) -> str:
    return msg.decode()


def JsonDecoder(msg: bytes) -> Any:
    return json.loads(msg.decode())


def MsgpackDecoder(msg: bytes) -> Any:
    return msgpack.loads(msg)
