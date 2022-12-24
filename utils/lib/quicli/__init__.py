import logit

# TODO: Search for a module scoped LogStream
_logger: logit.LogStream = logit.BlackHole()

class Error(Exception):
  pass

class RuntimeError(Error):
  pass

from .declarative import *