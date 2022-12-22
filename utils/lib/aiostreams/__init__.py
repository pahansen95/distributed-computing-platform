
from typing import TypeVar, Generic

T = TypeVar('T')

class InputStream(Generic[T]):
  ...

class OutputStream(Generic[T]):
  ...