
from dataclasses import KW_ONLY, dataclass
from ..common import KubernetesClientSession

from .. import _logger, _k8s_api_group, _k8s_api_version

@dataclass(frozen=True)
class TunnelController:
  name: str
  namespace: str

  async def ipam_allocation_control_loop(
    self,
    api_session: KubernetesClientSession
  ) -> None:
    # Watch all IPAMAllocations objects
    async for event in api_session.watch(
      kind="IPAMAllocations",
      api_version=f"{_k8s_api_group}/{_k8s_api_version}",
      namespace=self.namespace,
      name=None,
    ):
      if event["type"] == "ADDED":
        self.ipam_allocation_fulfill(
          ...
        )
      elif event["type"] == "MODIFIED":
        _logger.debug(f"IPAMAllocation modified; operation not supported. Will Ignore.")
        ...
      elif event["type"] == "DELETED":
        self.ipam_allocation_cleanup(
          ...
        )
      else:
        _logger.debug(f"Unhandled event type: {event['type']}")
        ...
  
  async def ipam_lease_control_loop(
    self,
    api_session: KubernetesClientSession
  ) -> None:
    # Watch all IPAMLeases objects
    async for event in api_session.watch(
      kind="IPAMLeases",
      api_version=f"{_k8s_api_group}/{_k8s_api_version}",
      namespace=self.namespace,
      name=None,
    ):
      if event["type"] == "ADDED":
        self.ipam_lease_fulfill(
          ...
        )
      elif event["type"] == "MODIFIED":
        _logger.debug(f"IPAMLease modified; operation not supported. Will Ignore.")
        ...
      elif event["type"] == "DELETED":
        self.ipam_lease_cleanup(
          ...
        )
      else:
        _logger.debug(f"Unhandled event type: {event['type']}")
        ...

  async def ipam_allocation_fulfill(
    self,
    ipam_allocation_request: dict[str, dict | str],
    api_session: KubernetesClientSession,
  ):
    ...
    all_addresses = set(...)
    allocated_address = set(...)
    available_addresses = all_addresses - allocated_address
    
    request_hash = ...

    # Select an address based on the request hash
    address = get_address_by_hash(request_hash, available_addresses)

    # update the IPAMAllocation object
    async with api_session.update(
      kind="IPAMAllocations",
      api_version=f"{_k8s_api_group}/{_k8s_api_version}",
      namespace=self.namespace,
      name=ipam_allocation_request["metadata"]["name"],
    ) as patch:
      patch["status"]["address"] = address
      patch["status"]["state"] = "fulfilled"
  
  async def ipam_allocation_cleanup(self):
    ...
  
  async def ipam_lease_fulfill(self):
    ...
  
  async def ipam_lease_cleanup(self):
    ...
