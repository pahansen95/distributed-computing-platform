
from typing import TypeVar, Generic, Any
import contextlib
import itertools

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
W = TypeVar('W')
# X = TypeVar('X')
# Y = TypeVar('Y')
# Z = TypeVar('Z')

class Error(Exception):
  pass

class RuntimeError(Error):
  pass

class AbstractError(Error):
  pass

def implements_interface(cls: type | Any, *interface: type) -> bool:
  """Check if a class implements an interface"""
  
  return all(
    hasattr(cls, attr)
    for attr in filter(
      lambda attr: not attr.startswith('_'),
      itertools.chain(dir(_interface) for _interface in interface)
    )
  )

# Abstract Classes
class OpenMixin:
  """Open a stream"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('OpenMixin is an abstract class')
  def open(self) -> None:
    """Open the stream"""
    ...
class CloseMixin:
  """Close a stream"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('CloseMixin is an abstract class')
  def close(self) -> None:
    """Close the stream"""
    ...
class FlushMixin:
  """Flush a stream"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('FlushMixin is an abstract class')
  def flush(self) -> None:
    """Flush the stream"""
    ...
class ReadMixin(Generic[T]):
  """Read T from an input"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('ReadMixin is an abstract class')
  async def read(self) -> T:
    """Read a T Message from the input"""
    ...
class WriteMixin(Generic[T]):
  """Write T to an output"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('WriteMixin is an abstract class')
  async def write(self, message: T) -> None:
    """Write a T Message to the output"""
    ...
  ...
class SerializeMixin(Generic[U, V]):
  """Serialize U to V"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('SerializeMixin is an abstract class')
  async def serialize(self, obj: U) -> V:
    """Serialize a U obj to a V Message"""
    ...
class DeserializeMixin(Generic[U, V]):
  """Deserialize V to U"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    raise AbstractError('DeserializeMixin is an abstract class')
  async def deserialize(self, message: V) -> U:
    """Deserialize a V Message to a U Object"""
    ...

class SourceStream(ReadMixin[U], DeserializeMixin[U, V], Generic[U, V]):
  """Read U from an input and deserialize to V"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    if type(self) is SourceStream:
      raise AbstractError('SourceStream is an abstract class')
  async def recv(self) -> U:
    """Recieve a U Message from the input"""
    ...

class SinkStream(WriteMixin[V], SerializeMixin[V, U], Generic[U, V]):
  """Serialize V to U and write to an output"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    if type(self) is SinkStream:
      raise AbstractError('SinkStream is an abstract class')
  async def send(self, data: V) -> None:
    """Send a V Object to the output"""
    ... 

class SymmetricPipeline(SourceStream[U, V], SinkStream[V, U], Generic[U, V]):
  """Read U from an input, deserialize to V, serialize to U, and write to an output"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    if type(self) is SymmetricPipeline:
      raise AbstractError('SymmetricPipeline is an abstract class')

class AsymmetricPipeline(SourceStream[U, V], SinkStream[V, W], Generic[U, V, W]):
  """Read U from an input, deserialize to V, serialize to W, and write to an output"""
  def __init__(self, *args: Any, **kwargs: Any) -> None:
    if type(self) is AsymmetricPipeline:
      raise AbstractError('AsymmetricPipeline is an abstract class')

# Interfaces
FileLike = OpenMixin | CloseMixin | FlushMixin
Stream = SourceStream[U, V] | SinkStream[U, V]
Pipeline = SymmetricPipeline[U, V] | AsymmetricPipeline[U, V, W]

# Implementations

class NullSourceStream(OpenMixin, CloseMixin, FlushMixin, SourceStream[None, None]):
  """A source that produces no data. Can be used as a placeholder"""
  async def recv(self) -> None:
    """Recieve a None Message from the input"""
    return None
  async def open(self) -> None:
    """Open the stream"""
    return None
  async def close(self) -> None:
    """Close the stream"""
    return None
  async def flush(self) -> None:
    """Flush the stream"""
    return None

class NullSinkStream(OpenMixin, CloseMixin, FlushMixin, SourceStream[None, None]):
  """A sink that consumes no data. Can be used as a placeholder"""
  async def send(self, data: Any) -> None:
    """Send a None Object to the output"""
    return None
  async def open(self) -> None:
    """Open the stream"""
    return None
  async def close(self) -> None:
    """Close the stream"""
    return None
  async def flush(self) -> None:
    """Flush the stream"""
    return None

class NullPipeline(NullSourceStream, NullSinkStream):
  """A stream that produces and consumes no data; can be used as a placeholder"""
  ...

class FileDescriptorSourceStream(OpenMixin, CloseMixin, FlushMixin, SourceStream[bytes, str]):
  """A source that reads from a file descriptor
  TODO: This is a niave implementation for testing the library's interface
  """
  def __init__(self, fd: int, delimiter: bytes = b'\n') -> None:
    assert is_a_stream(type(self))
    self._fd = fd
    self._delimiter = delimiter
    self._source = None
    self._buffer = bytearray()
  
  @property
  def fd(self) -> int:
    return self._fd
  
  @property
  def delimiter(self) -> bytes:
    return self._delimiter

  async def open(self) -> None:
    """Open the stream"""
    self._source = open(self._fd, "rb")
  
  async def close(self) -> None:
    """Close the stream"""
    self._source.close()
    self._source = None
  
  async def flush(self) -> None:
    """Flush the stream"""
    self._source.flush()
  
  @contextlib.asynccontextmanager
  def __call__(self) -> "FileDescriptorSourceStream":
    try:
      self.open()
      yield self
    finally:
      self.flush()
      self.close()

  async def deserialize(self, message: bytes) -> str:
    return message.removesuffix(self._delimiter).decode("utf-8")
    
  async def read(self) -> bytes:
    """Read bytes from the input until the delimiter is encountered"""
    if self._source is None:
      raise RuntimeError("FileDescriptorSourceStream is not open")
    async for chunk in self._source:
      self._buffer.extend(chunk)
      if self._delimiter in self._buffer:
        msg = self._buffer[:self._buffer.index(self._delimiter) + len(self._delimiter) - 1]
        self._buffer = self._buffer[self._buffer.index(self._delimiter) + len(self._delimiter):]
        break
    return bytes(msg)

  async def recv(self) -> str:
    """Recieve a string object from the input"""
    return self.deserialize(
      message=(await self.read())
    )

class FileDescriptorSinkStream(SinkStream[str, bytes]):
  """A sink that writes to a file descriptor
  TODO: This is a niave implementation for testing the library's interface
  """
  def __init__(self, fd: int, delimiter: bytes = b'\n') -> None:
    assert is_a_stream(type(self))
    self._fd = fd
    self._delimiter = delimiter
    self._sink = None
  
  @property
  def fd(self) -> int:
    return self._fd
  
  @property
  def delimiter(self) -> bytes:
    return self._delimiter
  
  async def open(self) -> None:
    """Open the stream"""
    self._source = open(self._fd, "rb")
  
  async def close(self) -> None:
    """Close the stream"""
    self._source.close()
    self._source = None
  
  async def flush(self) -> None:
    """Flush the stream"""
    self._source.flush()
  
  @contextlib.asynccontextmanager
  def __call__(self) -> "FileDescriptorSourceStream":
    try:
      self.open()
      yield self
    finally:
      self.flush()
      self.close()

  async def serialize(self, message: str) -> bytes:
    return message.encode("utf-8") + self._delimiter

  async def write(self, message: bytes) -> None:
    """Write bytes to the output"""
    if self._sink is None:
      raise RuntimeError("FileDescriptorSinkStream is not open")
    bytes_written = 0
    while bytes_written < len(message):
      bytes_written += await self._sink.write(message[bytes_written:])

  async def send(self, message: str) -> None:
    """Send a string Message the output"""
    await self.write(
      message=self.serialize(message=message)
    )
