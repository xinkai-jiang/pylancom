import json
from typing import Any, Callable, Dict, Tuple

import msgpack

from ..type import MsgType


def bytes2str(byte_msg: bytes) -> str:
    return byte_msg.decode()


def bytes2dict(byte_msg: bytes) -> Dict:
    return json.loads(byte_msg.decode())


def bytes2msgpack(byte_msg: bytes) -> Dict:
    return msgpack.loads(byte_msg)


# def bytes2Proto(byte_msg: bytes, proto: Any) -> Any:
#     proto.ParseFromString(byte_msg)
#     return proto


def str2bytes(str_msg: str) -> bytes:
    return str_msg.encode()


def dict2bytes(dict_msg: Dict) -> bytes:
    return json.dumps(dict_msg).encode()


def msgpack2dict(msgpack_msg: Dict) -> Dict:
    return msgpack.dumps(msgpack_msg)


Bytes2ObjMap: Dict[str, Callable[[bytes], Any]] = {
    MsgType.BYTES.value.decode(): lambda x: x,
    MsgType.STR.value.decode(): bytes2str,
    MsgType.JSON.value.decode(): bytes2dict,
    MsgType.NSGPACK.value.decode(): msgpack2dict,
}

Obj2BytesMap: Dict[str, Callable[[Any], bytes]] = {
    MsgType.BYTES.value.decode(): lambda x: x,
    MsgType.STR.value.decode(): str2bytes,
    MsgType.JSON.value.decode(): dict2bytes,
    MsgType.NSGPACK.value.decode(): dict2bytes,
}


def parse_msg(msg: bytes) -> Tuple[str, bytes]:
    return msg[:32].decode(), msg[32:]
