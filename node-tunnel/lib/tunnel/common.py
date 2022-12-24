
from dataclasses import dataclass


@dataclass(frozen=True)
class NetworkPeer:
  name: str
  addresses: tuple[int]
  public_key: bytes
  preshared_key: bytes
  endpoints: tuple[tuple[int|str, int]] # A tuple of (address or hostname, port) pairs