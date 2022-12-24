from dataclasses import dataclass, KW_ONLY, field
from typing import TypeVar, Type, Generic, Callable, Any, Iterable, Generator
from types import NoneType, EllipsisType

from . import _logger, Error, RuntimeError
import aiostreams as streams
import json
import uri

from frozendict import frozendict

T = TypeVar('T')
_regex_db = {
  "env_var_name": r"^[A-Za-z_][A-Za-z0-9_]*$",
  "kwarg_key": r"^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$"
}

class DefinitionError(Error):
  pass

class ArgValueError(Error):
  pass

class ArgTypeError(Error):
  pass

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

def _default_stream_assembler(
  input: dict[str, Any],
  output: dict[str, Any],
  **_kwargs,
) -> tuple[streams.Stream[bytes, str], streams.Stream[bytes, str]]:
  input_uri = uri.URI(input['uri'])
  if input_uri.scheme == 'fd':
    input_stream = streams.FileDescriptorSourceStream(
      int(input_uri.path),
      **input['opts'],
    )
  else:
    raise ValueError(f'Unsupported input uri scheme {input_uri.scheme}')
  
  output_uri = uri.URI(output['uri'])
  if output_uri.scheme == 'fd':
    output_stream = streams.FileDescriptorSinkStream(
      int(output_uri.path),
      **output['opts'],
    )
  else:
    raise ValueError(f'Unsupported output uri scheme {output_uri.scheme}')
  
  return (input_stream, output_stream)

@dataclass(frozen=True)
class StreamDef(Generic[T]):
  name: str
  description: str
  input_uri: str | None
  output_uri: str | None
  _: KW_ONLY
  input_opts: frozendict[str, Any] = field(default_factory=frozendict)
  output_opts: frozendict[str, Any] = field(default_factory=frozendict)
  serializer: Callable[[T], bytes] = lambda x: x.encode('utf-8')
  deserializer: Callable[[bytes], T] = lambda x: x.decode('utf-8')
  assemble: Callable[[dict, dict], tuple[streams.Stream, streams.Stream]] = _default_stream_assembler

  def __post_init__(self):
    if self.input_uri is None and self.output_uri is None:
      raise DefinitionError('At least one of input_uri or output_uri must be defined')

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
  cast: Callable[[Type[T], str], T] = _cast_kind
  _value: str | None = field(default=None, init=False, repr=False)

  @property
  def value(self) -> T:
    raise DefinitionError(f'{self.__class__.__name__} did not implement value')

  def _cast(self, kind: Type[T], value: str | None, **kwargs) -> T:
    try:
      value = self.cast(kind, value, **kwargs)
      if not isinstance(value, kind):
        raise ArgTypeError(f'cast returned {type(value)}, expected {kind}')
    except ArgTypeError:
      raise
    except:
      _logger.exception(f'Failed to cast {value} to {kind}')
      raise ArgTypeError(f'Failed to cast {value} to {kind}')
    return value
  
  
@dataclass(frozen=True)
class PositionalArgumentDef(ArgumentDef[T]):
  """A positional argument is an argument that is passed to a command in a specific order.
  It includes all the properties of an Argument, but also defines...
    - A count, which is the number of args that will be consumed. By default this is 1. Specifying Ellipsis will consume all remaining args.
  """
  _: KW_ONLY
  cast: Callable[[Type[T], str], T] = _cast_kind
  count: int | EllipsisType = 1
  _value: tuple[str] | None = field(default=None, init=False, repr=False)

  def __post_init__(self):
    if not isinstance(self.count, (int, EllipsisType)):
      raise TypeError(f'count expects int or EllipsisType, got {type(self.count)}')
    if isinstance(self.count, int) and self.count < 1:
      raise ValueError(f'count expects a positive integer, got {self.count}')

  @property
  def value(self) -> tuple[T]:
    value = self._value
    if value is None:
      raise ArgValueError(f'Missing value for {self.name}')
    if self.count is not Ellipsis and len(self._value) != self.count:
      raise ArgValueError(f'Expected {self.count} values for {self.name}, got {len(self._value)}')
    return tuple(
      self._cast(self.kind, v)
      for v in value
    )

  def register(self, value: str):
    if not isinstance(value, str):
      raise TypeError(f'value expects str, got {type(value)}')
    if isinstance(self.count, int) and len(self._value) >= self.count:
      raise ArgValueError(f'Too many values for {self.name}')
    
    if self._value is None:
      self._value = []
    self._value.append(value)

@dataclass(frozen=True)
class KeywordArgumentDef(ArgumentDef[T]):
  """A keyword argument is an argument that is passed to a command by name.
  It takes the form of 'key=value' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  It includes all the properties of an Argument, but also defines...
    - A list of keys, which are the names that can be used to pass the argument. By default this is the name of the argument.
    - A multiple flag, which indicates if the argument can be passed multiple times. By default this is False. If True, the value will be a list of values.
  """
  keys: tuple[str]
  _: KW_ONLY
  default: Callable[[Type[T]], T] = lambda **_: None
  multiple: bool = False

  @property
  def value(self) -> tuple[T] | T:
    value = self._value
    if value is None:
      value = self.default(self.kind)
      if self.multiple and not isinstance(value, (NoneType, Iterable)):
        raise ArgTypeError(f'Default value for {self.name} must be iterable')
    else:
      if self.multiple:
        value = iter(
          self._cast(self.kind, v)
          for v in value
        )
      else:
        value = self._cast(self.kind, value)
    if value is None:
      raise ArgValueError(f'Missing value for {self.name}')
    
    return tuple(value) if self.multiple else value

  def register(self, value: str):
    if not isinstance(value, str):
      raise TypeError(f'value expects str, got {type(value)}')
    if self.multiple and len(self._value) >= 1:
      raise ArgValueError(f'Too many values for {self.name}')
    
    if self.multiple:
      if self._value is None:
        self._value = []
      self._value.append(value)
    else:
      self._value = value


@dataclass(frozen=True)
class BooleanFlagDef(KeywordArgumentDef[bool]):
  """A boolean flag is a Flag Argument that is either True or False.
  It takes the form of 'key[=value]' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  Values are optional and if not specified will default to True.
  It includes all the properties of a Flag Argument.
  """
  _: KW_ONLY
  kind: Type[bool] = bool
  default: Callable[[Type[bool]], bool] = lambda _: True

  def __post_init__(self):
    if self.multiple:
      raise DefinitionError(f'{self.__class__.__name__} does not support multiple values')
    if not isinstance(self.kind, bool):
      raise DefinitionError(f'{self.__class__.__name__} expects a boolean kind, got {self.kind.__name__}')
  
  @property
  def value(self) -> int:
    value = self._value
    if value is None:
      value = self.default(self.kind)
    if value is None:
      raise ArgValueError(f'Missing value for {self.name}')
    
  def register(self, value: str | None):
    if not isinstance(value, (str, NoneType)):
      raise TypeError(f'value expects str, got {type(value)}')
    
    if value is None:
      value = 'True'

    self._value = _cast_boolean(value)

@dataclass(frozen=True)
class CountFlagDef(KeywordArgumentDef[int]):
  """A count flag is a Flag Argument that can be passed multiple times to increment a counter.
  It takes the form of 'key[=value]' where 'key' must match the following regex: ^(?:-+)([A-Za-z_][A-Za-z0-9_-]*)$
  Values are optional and if not specified will default to 1.
  Count Flags are additive. If the flag is passed multiple times, the value will be the sum of all the values.
  It includes all the properties of a Flag Argument.
  """
  _: KW_ONLY
  kind: Type[int] = int
  default: Callable[[Type[int]], int] = lambda _: 1
  multiple: bool = True
  _value: int = field(default=None, init=False, repr=False)

  def __post_init__(self):
    if not self.multiple:
      raise DefinitionError(f'{self.__class__.__name__} must support multiple values')
    if not isinstance(self.kind, int):
      raise DefinitionError(f'{self.__class__.__name__} expectes an Integer kind, got {self.kind.__name__}')

  @property
  def value(self) -> int:
    value = self._value
    if value is None:
      value = self.default(self.kind)
    if value is None:
      raise ArgValueError(f'Missing value for {self.name}')
    
    return value

  def register(self, value: str | None):
    if not isinstance(value, (str, NoneType)):
      raise TypeError(f'value expects str, got {type(value)}')
    
    if self._value is None:
      self._value = 0

    if value is None:
      value = '1'

    self._value += int(value, base=10)

@dataclass(frozen=True)
class EnvVarDef(ArgumentDef[T]):
  """An EnvVarDef is a way to represent an environment variable exported to a command.
  It includes all the properties of an Argument, but also defines...
    - A list of keys, which are the names that map to the argument. All Keys must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
    - An optional prefix, which is a string that is prepended to the exported variable name. By default this is None. The Prefix must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
    - An optional suffix, which is a string that is appended to the exported variable name. By default this is None. The Suffix must match the following regex: ^[A-Za-z_][A-Za-z0-9_]*$
  """
  key_bodies: tuple[str]
  _: KW_ONLY
  default: Callable[[Type[T]], T] = lambda **_: None
  prefix: str = ""
  suffix: str = ""

  @property
  def keys(self) -> Generator[str, None, None]:
    for key in self.key_bodies:
      yield f"{self.prefix}{key}{self.suffix}"
  
  @property
  def value(self) -> T:
    value = self._value
    if value is None:
      value = self.default(self.kind)
    else:
      value = self._cast(self.kind, value)
    if value is None:
      raise ArgValueError(f'Missing value for {self.name}')
    
    return value

  def register(self, value: str):
    if not isinstance(value, str):
      raise TypeError(f'value expects str, got {type(value)}')    
    self._value = value

@dataclass(frozen=True)
class ArgSet:
  """An ArgSet Represents a set of arguments that are expected to be passed to a command & those arguments which are mutuall exclusive to them."""
  _: KW_ONLY
  inclusive: tuple[KeywordArgumentDef[Any] | EnvVarDef[Any], ...] = field(default_factory=tuple)
  exclusive: tuple[KeywordArgumentDef[Any] | EnvVarDef[Any], ...] = field(default_factory=tuple)

@dataclass(frozen=True)
class RuntimeModeDef:
  """A RuntimeModeDef (Runtime Mode Definition) is a way to represent multiple sets of arguments that all map to the same command intent.
  Take for example the command 'kubectl get':
    kubectl get pod my-pod
    kubeclt get -f my-pod.yaml
  In either case, the intent is to get a pod while the arguments passed are mutually exclusive.
  
  Modes can also be used to define a concrete interface for a command.
  """
  name: str
  description: str
  args: tuple[PositionalArgumentDef[Any], ...]
  sets: tuple[ArgSet, ...]

  def matches(self, *arg_defs: ArgumentDef) -> bool:
    ...

@dataclass(frozen=True)
class CommandDef:
  """A CommandDef (Command Definition) is a way to represent a command and its arguments."""
  name: str
  keys: tuple[str]
  description: str
  arg_defs: tuple[ArgumentDef[Any], ...]
  modes: tuple[RuntimeModeDef, ...]

  def validate(self):
    ...

__all__ = [
  'ArgumentDef',
  'PositionalArgumentDef',
  'KeywordArgumentDef',
  'FlagArgumentDef',
  'BooleanFlagDef',
  'CountFlagDef',
]