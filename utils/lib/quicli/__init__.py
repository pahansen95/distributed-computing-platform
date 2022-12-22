import logit

# TODO: Search for a module scoped LogStream
_logger: logit.LogStream = logit.BlackHole()

class Error(Exception):
  pass


from .declarative import *