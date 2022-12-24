
import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any, Iterable
import enum
import base64

from ..common import NetworkPeer
from .. import _logger

async def _asyncio_gather_filter_exceptions(
  log_msg: str,
  *coros: asyncio.Future[Any],
) -> Iterable[Any]:
  results = await asyncio.gather(*coros, return_exceptions=True)
  return filter(
    lambda x: x is not None,
    map(
      lambda x: _logger.exception(log_msg) if isinstance(x, Exception) else x,
      results,
    ),
  )

class InterfaceKind(enum.Enum):
  wireguard = 'wireguard'

@dataclass(frozen=True)
class NetworkInterfaceView:
  ...

  @property
  def name(self) -> str:
    ...

async def create_network_interface(
  kind: InterfaceKind,
  name: str,
  opts: dict[str, str],
) -> NetworkInterfaceView:
  ...

async def add_address_to_network_interface(
  name: str,
  address: int,
):
  ...

async def bring_up_network_interface(
  name: str,
):
  ...

async def wireguard_generate_keypair() -> tuple[bytes, bytes]:
  ...

async def wireguard_add_peer(
  name: str,
  public_key: str,
  preshared_key: str,
  keepalive: int,
  allowed_ips: Iterable[tuple[int,int]], # A tuple of IP Subnets as tuples of integers (address, netmask)
):
  ...

@contextlib.asynccontextmanager
async def controller_authenticate() -> "ControllerSession":
  ...

@dataclass(frozen=True)
class NodeTunnel:
  name: str
  addresses: tuple[int] | None # A tuple of IP addresses as integers
  private_key: bytes | None
  public_key: bytes | None
  seeding_peers: tuple[NetworkPeer] | None
  heartbeat_period: int # In seconds

  async def create(
    self,
    *_args,
    **_kwargs,
  ) -> "NodeTunnel":
    # Type Hints
    seeding_peers: Iterable[NetworkPeer] # NetworkPeers to seed the WireGuard interface with
    addresses: tuple[int] # IP addresses to assign to the WireGuard interface
    private_key: bytes
    public_key: bytes

    # Generate a WireGuard keypair
    private_key, public_key = self.private_key, self.public_key
    if not (private_key and public_key):
      _logger.info(f'Generating WireGuard keypair for interface {self.name}')
      private_key, public_key = await wireguard_generate_keypair()

    # Authenticate w/ the controller
    async with controller_authenticate() as controller_session:
      await controller_session.register_node(
        name=self.name,
        public_key=base64.b64encode(self.public_key).decode('utf-8'),
      )
      seeding_peers = self.seeding_peers
      if not seeding_peers:
        seeding_pairs = await controller_session.get_seeding_pairs()
      _logger.info(f"Interface {self.name} will be seeded with peers: {seeding_pairs}")
      address_leases = self.addresses
      if not address_leases:
        address_leases = tuple(await _asyncio_gather_filter_exceptions(
          f'Failed to lease IP address for interface {self.name}',
          controller_session.ipam_allocate_address(),
        ))
      addresses = _asyncio_gather_filter_exceptions(
        f'Failed to lease IP address for interface {self.name}',
        (
          controller_session.ipam_lease_address(ip_addr)
          for ip_addr in address_leases
        )
      )
      _logger.info(f"Interface {self.name} has leased addresses: {address_leases}")

    wg_inf = await create_network_interface(
      kind=InterfaceKind.wireguard,
      name=self.name,
      opts=...,
    )

    # Gather the results, logging any exceptions
    await _asyncio_gather_filter_exceptions(
      f'Failed to add address to interface {wg_inf.name}',
      (
        add_address_to_network_interface(
          name=wg_inf.name,
          address=addr,
        )
        for addr in addresses
      )
    )

    # Preseed the WireGuard interface with seeding peers
    await _asyncio_gather_filter_exceptions(
      f'Failed to add peer to interface {wg_inf.name}',
      (
        wireguard_add_peer(
          name=wg_inf.name,
          endpoints=peer.endpoints,
          public_key=base64.b64encode(peer.public_key).decode('utf-8'),
          preshared_key=base64.b64encode(peer.public_key).decode('utf-8'),
          keepalive=self.heartbeat_period,
          allowed_ips=[(addr, 0xffffffff) for addr in peer.addresses], # On setup allow only traffic to/from the peer
        )
        for peer in seeding_pairs
      )
    )

    await bring_up_network_interface(wg_inf.name)

    return NodeTunnel(
      name=self.name,
      addresses=tuple(addresses),
      private_key=private_key,
      public_key=public_key,
      seeding_peers=tuple(seeding_pairs),
      heartbeat_period=self.heartbeat_period,
    )
  
  async def read(
    self,
    *_args,
    **_kwargs,
  ) -> "NodeTunnel":
    ...

  async def update(
    self,
    *_args,
    **_kwargs,
  ) -> "NodeTunnel":
    ...

  async def delete(
    self,
    *_args,
    **_kwargs,
  ) -> "NodeTunnel":
    