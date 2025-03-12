__MAJOR__ = int(1)
__MINOR__ = int(0)
__PATCH__ = int(0)

__VERSION__ = f"{__MAJOR__}.{__MINOR__}.{__PATCH__}"
__VERSION_BYTES__ = bytes([__MAJOR__, __MINOR__, __PATCH__])
__COMPATIBILITY__ = bytes([__MAJOR__, __MINOR__])
BROADCAST_INTERVAL = 0.5
HEARTBEAT_INTERVAL = 0.2
# this addr is suitable for windows
MULTICAST_ADDR = "239.255.255.250"
MULTICAST_PORT = int(7720)
MASTER_SERVICE_PORT = int(7721)
