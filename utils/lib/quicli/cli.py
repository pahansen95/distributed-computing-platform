
import asyncio
from asyncio.log import logger
from typing import Iterable, Mapping, NoReturn, Callable, Coroutine, Any

import enum
import semver
import inspect
import logit
import aiostreams as streams
import itertools

from . import _logger, Error, RuntimeError as _RuntimeError
from .declarative import ArgumentDef, BooleanFlagDef, CommandDef, CountFlagDef, DefinitionError, EnvVarDef, KeywordArgumentDef, RuntimeModeDef, PositionalArgumentDef

class RegistrationError(Error):
  pass

class UsageError(RuntimeError):
  def __init__(self, argv: list[str], msg: str, *args: object) -> None:
    self.argv = argv
    self.msg = msg
    super().__init__(*args)

class EXIT_CODE(enum.IntEnum):
  SUCCESS = 0
  ERROR = 1
  RUNTIME = enum.auto()
  USAGE = enum.auto()
  UNHANDLED = enum.auto() 

class CommandLineInterface:
  def __init__(
    self,
    name: str,
    version: str,
    description: str,
    argv: Iterable[str],
    env: Mapping[str, str],
  ):
    self.name = name
    self.version = semver.parse(version)
    self.description = description
    self.argv = tuple(argv)
    self.env = dict(env)
    self._command_registry: dict[CommandDef, Callable | Coroutine] = {}
  
  def register_command(
    self,
    command_def: CommandDef,
    calls: Callable | Coroutine, 
  ):
    if command_def.name in self._command_registry:
      raise RegistrationError(f'Command {command_def.name} already registered')

    try:
      command_def.validate()
    except Error as e:
      _logger.exception(flags=logit.LOG_FLAG.DEBUG)
      raise RegistrationError(f'Invalid command definition: {e}')
      
    if not (
      inspect.iscoroutinefunction(calls) or
      inspect.iscoroutine(calls) or
      inspect.ismethod(calls) or
      inspect.isfunction(calls)
    ):
      raise TypeError(f'Unsupported command type: {type(calls)}')
    
    _logger.debug(f'Registering command {command_def.name} with {calls.__class__.__name__}')
    self._command_registry[command_def] = calls

  def invoke(self, *_args, **_kwargs) -> EXIT_CODE:
    matched_args: dict[str, ArgumentDef] = {}

    # Slice argv by a `--` separator. The first list becomes the new argv, the second list becomes remainders
    try:
      argv, remainder = self.argv[:self.argv.index('--')-1], self.argv[self.argv.index('--') + 1:]
    except ValueError:
      argv, remainder = self.argv, []

    # Seperate out the kwargs from the positional args
    argv_kwargs = {}
    argv_args = []
    for arg in argv:
      if arg.startswith('-'):
        if "=" in arg:
          try:
            key, value = arg.lstrip('-').split('=')
          except ValueError:
            raise UsageError([arg], f"Keyword argument has bad formatting. A '=' must be followed by some value.")
        else:
          key, value = arg.lstrip('-'), None
        argv_kwargs[key] = value
      else:
        argv_args.append(arg)
    
    # Determine the Subcommand.
    subcommand = next(filter(lambda x: x.name == '', self._command_registry), None)
    if len(argv_args) > 0:
      for command_def in self._command_registry:
        if any(argv_args[0] == key for key in command_def.keys):
          subcommand = command_def
          argv_args.pop(0)
          break
    
    if subcommand is None:
      raise RuntimeError(f'No subcommand could be determined & no default subcommand was registered')
    
    # Parse the declared KeywordArgumentDefs
    kwarg_def: KeywordArgumentDef
    for kwarg_def in filter(lambda x: isinstance(x, KeywordArgumentDef), subcommand.arg_defs):
      if set(kwarg_def.keys).isdisjoint(argv_kwargs.keys()):
        continue
      for key in set(kwarg_def.keys).intersection(argv_kwargs.keys()):
        if argv_kwargs[key] is None and not isinstance(kwarg_def, (BooleanFlagDef, CountFlagDef)):
          raise UsageError([key], f"Keyword argument {key} requires a value")
        kwarg_def.register(argv_kwargs.pop(key))
        if kwarg_def.name not in matched_args:
          matched_args[kwarg_def.name] = kwarg_def
    
    # Parse the declared EnvVarDefs
    envvar_def: EnvVarDef
    for envvar_def in filter(lambda x: isinstance(x, EnvVarDef), subcommand.arg_defs):
      if set(envvar_def.keys).isdisjoint(self.env.keys()):
        continue
      for key in set(envvar_def.keys).intersection(self.env.keys()):
        envvar_def.register(self.env.pop(key))
        if envvar_def.name not in matched_args:
          matched_args[envvar_def.name] = envvar_def
    
    # Determine the Runtime mode given the parsed set of KeywordArgumentDefs & EnvVarDefs
    runtime_mode: RuntimeModeDef = next(filter(lambda x: x.name == '', subcommand.arg_defs), None)
    for mode_def in subcommand.modes:
      if mode_def.matches(*matched_args.values()):
        runtime_mode = mode_def
        break
    if runtime_mode is None:
      raise UsageError(f'The passed arguments & the environment variables do not match any of the declared runtime modes')
    
    # Parse the declared PositionalArgumentDefs
    positional_def: PositionalArgumentDef
    for positional_def in runtime_mode.args:
      if positional_def.name not in matched_args:
        matched_args[positional_def.name] = positional_def
      if positional_def.count is not ...:          
        if len(argv_args) < positional_def.count:
          raise UsageError(argv_args, f"Positional argument {positional_def.name} requires {positional_def.count} values but only got {len(argv_args)}")
      while len(argv_args) > 0:
        positional_def.register(arg)
        try:
          argv_args.pop(0)
        except IndexError:
          break

    # Invoke the command
    fn = self._command_registry[subcommand]
    fn_kwargs = {
      "runtime_mode": runtime_mode.name,
      "parsed_args": {arg.name: arg.value for arg in matched_args.values()},
      "remainder": remainder,
      "input_streams": {stream.name: stream for stream in self.input_streams},
      "output_streams": {stream.name: stream for stream in self.output_streams}
    }
    try:
      # Open the input & output streams
      for stream_def in itertools.chain(self.input_streams, self.output_streams):
        assert streams.implements_interface(
          type(stream_def),
          streams.Stream, streams.FileLike
        ), f'All input & output streams must implement the Stream & FileLike interfaces'
        stream_def.open()
      if inspect.iscoroutinefunction(fn):
        result = asyncio.run(fn(**fn_kwargs))
      else:
        result = fn(**fn_kwargs)
    finally:
      for stream_def in itertools.chain(self.input_streams, self.output_streams):
        try:
          stream_def.flush()
        except:
          _logger.exception(f'An error occurred while flushing the {stream_def.name} stream')
        try:
          stream_def.close()
        except Exception as e:
          _logger.exception(f'An error occurred while closing the {stream_def.name} stream')
    
    if result is None:
      result = EXIT_CODE.SUCCESS
    if isinstance(result, int):
      result = EXIT_CODE(result)
    if not isinstance(result, EXIT_CODE):
      raise DefinitionError(f'Command must return an EXIT_CODE, an int or None. Got {type(result)} instead')
    return result
  
  def __call__(self, *_args: Any, **_kwargs: Any) -> NoReturn:
    exit_code: EXIT_CODE = EXIT_CODE.UNHANDLED
    try:
      exit_code = self.invoke(*_args, **_kwargs)
    except UsageError as e:
      # TODO: Print Usage
      exit_code = EXIT_CODE.USAGE
    except _RuntimeError as e:
      _logger.exception('A runtime error occurred')
      exit_code = EXIT_CODE.RUNTIME
    except Error as e:
      _logger.exception('An error occurred')
      exit_code = EXIT_CODE.ERROR
    except Exception:
      _logger.exception('An unhandled exception occurred')
      exit_code = EXIT_CODE.UNHANDLED
    finally:
      try:
        _logger.log(
          logit.LOG_FLAG.SUCCESS if exit_code == EXIT_CODE.SUCCESS else logit.LOG_FLAG.CRITICAL,
          f'Exiting with code {exit_code.name} ({exit_code.value})'
        )
      except:
        pass
      exit(exit_code.value)
