
from aiostreams import InputStream, OutputStream
from typing import Iterable, Mapping, NoReturn, Callable, Coroutine

import enum
import semver

from . import _logger
from .declarative import CommandDef

class EXIT_CODE(enum.IntEnum):
  SUCCESS = 0
  ERROR = 1
  RUNTIME = enum.auto()
  UNHANDLED = enum.auto()

class CommandLineInterface:
  def __init__(
    self,
    name: str,
    version: str,
    description: str,
    input_streams: Iterable[InputStream[bytes]],
    output_streams: Iterable[OutputStream[bytes]],
    argv: Iterable[str],
    env: Mapping[str, str],
  ):
    self.name = name
    self.version = semver.parse(version)
    self.description = description
    self.input_streams = tuple(input_streams)
    self.output_streams = tuple(output_streams)
    self.argv = tuple(argv)
    self.env = dict(env)
    self._command_registry: dict[CommandDef, Callable | Coroutine] = {}
  
  def register_command(
    self,
    command_def: CommandDef,
    calls: Callable | Coroutine,
  ):
    # TODO: Validate command_def
    # TODO: Validate calls
    self._command_registry[command_def] = calls

  def __call__(self, *_args, **_kwargs) -> NoReturn:
    ...
    
  