import logit

# TODO: Search for a module scoped LogStream
_logger: logit.LogStream

class Error(Exception):
  pass


from .declarative import *