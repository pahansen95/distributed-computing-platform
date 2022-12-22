from dataclasses import dataclass, KW_ONLY, field
from typing import TypeVar, Type, Generic, Callable, Any
from types import NoneType, EllipsisType

from . import _logger

T = TypeVar('T')
_regex_db = {
  "env_var_name": r"^[A-Za-z_][A-Za-z0-9_]*$",
  "kwarg_key": r"^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$"
}

def _cast_kind(kind: Type[T], value: str | None, **_kwargs) -> T:
  if isinstance(value, NoneType):
    return value
  
  if not isinstance(kind, type):
    raise TypeError(f'kind expects type, got {type(kind)}')
  if not isinstance(value, str):
    raise TypeError(f'value expects str, got {type(value)}')
  
  if issubclass(kind, bool):
    return _cast_boolean(value)
  return kind(value)

def _cast_boolean(value: str, **_kwargs) -> bool:
  if value.strip().lower() in ["true", "yes", "on", "1"]:
    return True
  if value.strip().lower() in ["false", "no", "off", "0"]:
    return False
  raise ValueError(f'Cannot cast {value} to boolean')

@dataclass(frozen=True)
class ArgumentDef(Generic[T]):
  """A Base Definition Class that describes any Argument passed to a command.
  Each Argument defines...
    - A unique name
    - A kind, which is the type of the value
    - A default value, which is a function that takes the kind and returns a default value. By default this returns a special value, _NoValue, which is used to determine if the user passed a value or not.
    - A cast function, which is a function that takes the kind and the value and returns the casted value. By default this uses the kind's constructor.
  """
  name: str
  kind: Type[T]
  _: KW_ONLY
  default: Callable[[Type[T]], T] = lambda **_: None # Sentinel Value to denote that the user did not pass a value
  cast: Callable[[Type[T], str], T] = _cast_kind


@dataclass(frozen=True)
class PositionalArgumentDef(ArgumentDef[T]):
  """A positional argument is an argument that is passed to a command in a specific order.
  It includes all the properties of an Argument, but also defines...
    - A count, which is the number of args that will be consumed. By default this is 1. Specifying Ellipsis will consume all remaining args.
  """
  _: KW_ONLY
  count: int | EllipsisType = 1

@dataclass(frozen=True)
class KeywordArgumentDef(ArgumentDef[T]):
  """A keyword argument is an argument that is passed to a command by name.
  It takes the form of 'key=value' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  It includes all the properties of an Argument, but also defines...
    - A list of keys, which are the names that can be used to pass the argument. By default this is the name of the argument.
    - A multiple flag, which indicates if the argument can be passed multiple times. By default this is False. If True, the value will be a list of values.
  """
  keys: list[str]
  _: KW_ONLY
  multiple: bool = False

class FlagArgumentDef(ArgumentDef[T]):
  """A flag argument is an argument that is passed to a command by name.
  It takes the form of 'key' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  It includes all the properties of an Argument, but also defines...
    - A list of keys, which are the names that can be used to pass the argument. By default this is the name of the argument.
  """
  _: KW_ONLY
  keys: list[str] | None = None

@dataclass(frozen=True)
class BooleanFlagDef(FlagArgumentDef[bool]):
  """A boolean flag is a Flag Argument that is either True or False.
  It takes the form of 'key' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  It includes all the properties of a Flag Argument.
  """
  _: KW_ONLY
  default: Callable[[Type[bool]], bool] = lambda _: False

@dataclass(frozen=True)
class CountFlagDef(FlagArgumentDef[int]):
  """A count flag is a Flag Argument that can be passed multiple times to increment a counter.
  It takes the form of 'key' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  It includes all the properties of a Flag Argument.
  """
  _: KW_ONLY
  default: Callable[[Type[int]], int] = lambda _: 0

@dataclass(frozen=True)
class EnvVarDef(ArgumentDef[T]):
  """An EnvVarDef is a way to represent an environment variable exported to a command.
  It includes all the properties of an Argument, but also defines...
    - A list of keys, which are the names that map to the argument. All Keys must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
    - An optional prefix, which is a string that is prepended to the exported variable name. By default this is None. The Prefix must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
    - An optional suffix, which is a string that is appended to the exported variable name. By default this is None. The Suffix must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
  """
  keys: list[str]
  _: KW_ONLY
  prefix: str | None = None
  suffix: str | None = None

@dataclass(frozen=True)
class ArgSet:
  """An ArgSet Represents a set of arguments that are expected to be passed to a command & those arguments which are mutuall exclusive to them."""
  _: KW_ONLY
  inclusive: tuple[ArgumentDef[Any], ...] = field(default_factory=tuple)
  exclusive: tuple[ArgumentDef[Any], ...] = field(default_factory=tuple)

@dataclass(frozen=True)
class ModeDef:
  """A ModeDef (Mode Definition) is a way to represent multiple sets of arguments that all map to the same command intent.
  Take for example the command 'kubectl get':
    kubectl get pod my-pod
    kubeclt get -f my-pod.yaml
  In either case, the intent is to get a pod while the arguments passed are mutually exclusive.
  
  Modes can also be used to define a concrete interface for a command.
  """
  name: str
  description: str
  sets: tuple[ArgSet, ...]

@dataclass(frozen=True)
class CommandDef:
  """A CommandDef (Command Definition) is a way to represent a command and its arguments."""
  name: str
  description: str
  arguments: tuple[ArgumentDef[Any], ...]
  modes: tuple[ModeDef, ...]

__all__ = [
  'ArgumentDef',
  'PositionalArgumentDef',
  'KeywordArgumentDef',
  'FlagArgumentDef',
  'BooleanFlagDef',
  'CountFlagDef',
]