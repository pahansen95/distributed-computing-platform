import quicli

from typing import Any

_logger: quicli.LogStream

_k8s_api_group = "k8s.peterhansen.io"
_k8s_api_version = "v1alpha1"

_k8s_label_db = {
  "seeding_peer": {
    "label": f"networkpeer.{_k8s_api_group}/seeding-peer",
    "value_type": bool,
  }
}

def _k8s_label(
  db_key: str,
  value: Any,
) -> str:
  val_type = _k8s_label_db[db_key]["value_type"]
  if issubclass(val_type, str):
    _value = value
  elif issubclass(val_type, bool):
    _value = "true" if value else "false"
  elif issubclass(val_type, int):
    _value = str(value)
  else:
    raise TypeError(f"Unsupported value type {val_type}")
  
  return f"{_k8s_label_db[db_key]['label']}={_value}"

class Error(Exception):
  ...