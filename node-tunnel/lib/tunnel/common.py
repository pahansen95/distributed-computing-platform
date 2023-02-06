
import base64
from dataclasses import dataclass
import contextlib
import uri
import aiohttp
import ssl
import time
import json
import ipaddress
from typing import AsyncGenerator, Iterable

from . import _logger, Error, _k8s_api_group, _k8s_api_version, _k8s_label_db, _k8s_label

class SessionError(Error):
  pass

class SessionTokenExpiredError(SessionError):
  def __repr__(self) -> str:
    return "Session token expired"

class NoSeedingPeersError(SessionError):
  def __repr__(self) -> str:
    return "No seeding peer found"

@dataclass(frozen=True)
class NetworkPeer:
  name: str
  addresses: tuple[int]
  public_key: bytes
  preshared_key: bytes
  endpoints: tuple[tuple[int|str, int]] # A tuple of (address or hostname, port) pairs

@dataclass(frozen=True)
class KubernetesClient:
  
  @contextlib.asynccontextmanager
  async def authenticate(self, session_duration: int) -> AsyncGenerator["KubernetesClientSession", None]:
    ...

@dataclass(frozen=True)
class KubernetesClientSession:
  session: aiohttp.ClientSession
  
  async def _raise_if_token_expired(self) -> None:
    ...

  async def get(
    self,
    kind: str,
    api_version: str,
    namespace: str,
    name: str | None,
  ) -> dict[str, dict | str]:
    ...
  
  async def watch(
    self,
    kind: str,
    api_version: str,
    namespace: str,
    name: str | None,
  ) -> AsyncGenerator[dict[str, dict | str], None]:
    ...
    
  async def create(
    self,
    kind: str,
    api_version: str,
    namespace: str,
    name: str,
    body: dict[str, dict | str],
  ) -> dict[str, dict | str]:
    ...
  
  async def patch(
    self,
    kind: str,
    api_version: str,
    namespace: str,
    name: str,
    body: dict[str, dict | str],
  ) -> dict[str, dict | str]:
    ...
  
  async def delete(
    self,
    kind: str,
    api_version: str,
    namespace: str,
    name: str,
  ) -> None:
    ...

@dataclass(frozen=True)
class ControllerClient:
  name: str # A Globally unique name for the client
  namespace: str # The namespace in which the client is running
  controller_uri: str # The URI of the controller
  private_key: str | None # Path to the private key of the client certificate
  certificate: str | None # Path to the client certificate
  certificate_authority: str | None # Path to the certificate authority

  @contextlib.asynccontextmanager
  async def authenticate(self, session_duration: int) -> AsyncGenerator["ControllerClientSession", None]:
    url = uri.URI(self.controller_uri)
    if not any(scheme.lower() in url.scheme.lower() for scheme in ["http", "unix"]):
      raise Error(f"Invalid URI scheme {url.scheme}")
    
    ssl_context = False
    if any(scheme.lower() in url.scheme.lower() for scheme in ["https", "unixs", "tls", "ssl"]):
      ssl_context = ssl.SSLContext(
        protocol=ssl.PROTOCOL_TLS_CLIENT, # Negotiate the highest version of TLS supported by both the client and the server
      )
      try:
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.post_handshake_auth = True
        ssl_context.load_cert_chain(self.certificate, self.private_key)
        ssl_context.load_verify_locations(self.certificate_authority)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        # Dump information on how the TLS context was configured
        _logger.debug(f"TLS context ciphers: {ssl_context.get_ciphers()}")
        _logger.debug(f"TLS context certificate authorities: {ssl_context.get_ca_certs()}")
        _logger.debug(f"TLS context maximum version: {ssl_context.maximum_version}")
        _logger.debug(f"TLS context minimum version: {ssl_context.maximum_version}")
        _logger.debug(f"TLS context options: {ssl_context.options}")
        _logger.debug(f"TLS context protocol: {ssl_context.protocol}")
        _logger.debug(f"TLS context security level: {ssl_context.security_level}")
        _logger.debug(f"TLS context verify flags: {ssl_context.verify_flags}")
        _logger.debug(f"TLS context verify mode: {ssl_context.verify_mode}")
        _logger.debug(f"TLS context post handshake authentication: {ssl_context.post_handshake_auth}")
        _logger.debug(f"TLS context check hostname: {ssl_context.check_hostname}")
      except Exception as e:
        _logger.exception("Failed to setup TLS context")
        raise Error("Failed to setup TLS context") from e
    
    connector = None
    if any(scheme.lower() in url.scheme.lower() for scheme in ["unix"]):
      if ssl_context:
        _logger.warning("TLS is not supported over Unix sockets")
      connector = aiohttp.UnixConnector(
        path=url.path
      )
    elif any(scheme.lower() in url.scheme.lower() for scheme in ["http"]):
      connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        enable_cleanup_closed=True,
      )

    async with aiohttp.ClientSession(
      connector=connector,
      raise_for_status=True,
      headers={
        "Accept": f"application/json",
        "Content-Type": "application/json",
      }
    ) as session:
      """The Controller Server is a Kubernetes API server"""
      token_resource = {
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "metadata": {
          "name": f"{self.name}-session-token",
        },
        "spec": {
          "audiences": [self.name],
          "expirationSeconds": session_duration,
        },
      }
      with session.post(
        url=f"/api/v1/namespaces/{self.namespace}/serviceaccounts/{self.name}/token",
        json=token_resource,
      ) as resp:
        token_status = resp.json()["status"]
      session.headers["Authorization"] = f"Bearer {token_status['token']}",
      try:
        yield ControllerClientSession(
          client=self,
          session=session,
          expires=token_status["expirationTimestamp"], # TODO convert into a unix timestamp
          token=token_status["token"],
        )
        session.headers.pop("Authorization")
      finally:
        session.close()
  
@dataclass(frozen=True)
class ControllerClientSession:
  client: ControllerClient
  session: aiohttp.ClientSession
  expires: int
  token: bytes

  def _raise_if_token_expired(self):
    if self.expires < time.time():
      raise SessionTokenExpiredError()

  async def register_node(
    self,
    public_key: bytes,
  ):
    self._raise_if_token_expired()
    node = None
    try:
      async with self.session.get(
        url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.namespace}/networkpeers/{self.name}",
      ) as resp:
        node = resp.json()
      _logger.debug(f"Found existing node {node}")
    except aiohttp.ClientResponseError as e:
      if e.status != 404:
        raise
    
    if node is None:
      node = {
        "apiVersion": f"{_k8s_api_group}/{_k8s_api_version}",
        "kind": "NetworkPeer",
        "metadata": {
          "name": self.client.name,
        },
        "spec": {
          "publicKey": base64.b64encode(public_key).decode("utf-8"),
          "addresses": [],
          "endpoints": [],
        },
      }
      async with self.session.post(
        url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/networkpeers",
        json=node,
      ) as resp:
        node = resp.json()
    else:
      if public_key != base64.b64decode(node["spec"]["publicKey"]):
        _logger.info(f"Updating node {self.client.name} public key")
        node["spec"]["publicKey"] = base64.b64encode(public_key).decode("utf-8")
        async with self.session.patch(
          url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/networkpeers/{self.client.name}",
          headers={
            "Content-Type": "application/json-patch+json",
          },
          json=[
            {
              "op": "replace",
              "path": "/spec/publicKey",
              "value": base64.b64encode(public_key).decode("utf-8"),
            }
          ],
        ) as resp:
          node = resp.json()
    
    _logger.debug(f"{node}")
    _logger.info(f"Registered node {self.client.name}")
  
  async def register_seeding_peer(self, public_key: bytes, endpoint: str):
    self._raise_if_token_expired()
    await self.register_node(public_key)
    ... # TODO
  
  async def get_seeding_peers(self) -> Iterable[NetworkPeer]:
    self._raise_if_token_expired()
    # Get all the current NetworkPeers that have a label indicating they are ready
    async with self.session.get(
      url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/networkpeers",
      params={
        "labelSelector": _k8s_label("seeding_peer", True),
      },
    ) as resp:
      nodes = resp.json()["items"]
    _logger.debug(f"Found {len(nodes)} seeding peers")
    if len(nodes) == 0:
      raise NoSeedingPeersError()
    return iter(
      NetworkPeer(
        name=node["metadata"]["name"],
        public_key=base64.b64decode(node["spec"]["publicKey"]),
        addresses=node["spec"]["addresses"],
        endpoints=node["spec"]["endpoints"],
      )
      for node in nodes
    )
  
  async def ipam_request_address_allocation(self) -> bytes:
    self._raise_if_token_expired()
    allocation = None
    async with self.session.post(
      url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/ipamallocations",
      json={
        "apiVersion": f"{_k8s_api_group}/{_k8s_api_version}",
        "kind": "IPAMAllocation",
        "metadata": {
          "generateName": f"{self.client.name}-",
        },
        "spec": {
          "owner": self.client.name,
        },
      },
    ) as resp:
      allocation = resp.json()
    _logger.debug(f"Requested address allocation: {allocation}")

    # Wait for the allocation to be fulfilled by watching the resource
    async with self.session.get(
      url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/ipamallocations/{allocation['metadata']['name']}",
      params={
        "watch": True,
      },
    ) as resp:
      async for data in resp.content:
        obj_event = json.loads(data)
        if obj_event["type"] != "MODIFIED":
          continue
        allocation = json.loads(data)["object"]
        if allocation["status"]["state"] == "fulfilled":
          return ipaddress.ip_address(allocation["status"]["address"]).packed
  
  async def ipam_deallocate_address(self, address: bytes):
    self._raise_if_token_expired()
    ...


  async def ipam_request_address_lease(self, address: bytes, lease_duration: int):
    self._raise_if_token_expired()
    if len(address) == 4:
      addr = ipaddress.IPv4Address(address)
      lease_name = f"{self.client.name}-{address.hex():0>8}"
    elif len(address) == 16:
      addr = ipaddress.IPv6Address(address)
      lease_name = f"{self.client.name}-{address.hex():0>32}"
    else:
      raise ValueError("Invalid address length")
    
    lease = None
    try:
      async with self.session.get(
        url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/ipamleases/{lease_name}",
      ) as resp:
        lease = resp.json()
      _logger.debug(f"Found existing lease {lease}")
    except aiohttp.ClientResponseError as e:
      if e.status != 404:
        raise
    
    if lease is None:
      # Create a new lease
      async with self.session.post(
        url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/ipamleases",
        json={
          "apiVersion": f"{_k8s_api_group}/{_k8s_api_version}",
          "kind": "IPAMLease",
          "metadata": {
            "name": lease_name,
          },
          "spec": {
            "owner": self.client.name,
            "address": str(addr),
            "duration": lease_duration,
          },
        },
      ) as resp:
        lease = resp.json()
      
      # Wait for the lease to be fulfilled by watching the resource
      async with self.session.get(
        url=f"/apis/{_k8s_api_group}/{_k8s_api_version}/namespaces/{self.client.namespace}/ipamleases/{lease_name}",
        params={
          "watch": True,
        },
      ) as resp:
        async for data in resp.content:
          obj_event = json.loads(data)
          if obj_event["type"] != "MODIFIED":
            continue
          lease = json.loads(data)["object"]
          if lease["status"]["state"] == "fulfilled":
            return
  
  async def ipam_release_address_lease(self, address: bytes):
    self._raise_if_token_expired()
    ...

  async def ipam_fulfill_address_lease(self):
    self._raise_if_token_expired()
    ...