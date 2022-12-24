from typing import BinaryIO, Any
import enum
import sys
import traceback
import time
import datetime
import json

__all__ = [
  "LogStream",
  "LOG_FLAG",
]

class Error(Exception):
  pass

class RuntimeError(Error):
  pass

class LogError(Error):
  def __init__(self, reason: str) -> None:
    self.reason = reason

class LogRuntimeError(RuntimeError):
  def __init__(self, reason: str) -> None:
    self.reason = reason

class MessageFormatError(LogError):
  pass

class WriteError(LogRuntimeError):
  pass

class LOG_FLAG(enum.Flag):
  DEBUG = enum.auto()
  INFO = enum.auto()
  WARNING = enum.auto()
  ERROR = enum.auto()
  CRITICAL = enum.auto()
  SUCCESS = enum.auto()
  SECRET = enum.auto()

class LOG_KIND(enum.Enum):
  TEXT = enum.auto()
  COMPACT_TEXT = enum.auto()
  JSON = enum.auto()

class LogStream:
  success = lambda self, *args, **kwargs: self.log(LOG_FLAG.SUCCESS, *args, **kwargs)
  critical = lambda self, *args, **kwargs: self.log(LOG_FLAG.CRITICAL, *args, **kwargs)
  error = lambda self, *args, **kwargs: self.log(LOG_FLAG.ERROR, *args, **kwargs)
  warning = lambda self, *args, **kwargs: self.log(LOG_FLAG.WARNING, *args, **kwargs)
  info = lambda self, *args, **kwargs: self.log(LOG_FLAG.INFO, *args, **kwargs)
  debug = lambda self, *args, **kwargs: self.log(LOG_FLAG.DEBUG, *args, **kwargs)
  secret_success = lambda self, *args, **kwargs: self.log(LOG_FLAG.SUCCESS | LOG_FLAG.SECRET, *args, **kwargs)
  secret_critical = lambda self, *args, **kwargs: self.log(LOG_FLAG.CRITICAL | LOG_FLAG.SECRET, *args, **kwargs)
  secret_error = lambda self, *args, **kwargs: self.log(LOG_FLAG.ERROR | LOG_FLAG.SECRET, *args, **kwargs)
  secret_warning = lambda self, *args, **kwargs: self.log(LOG_FLAG.WARNING | LOG_FLAG.SECRET, *args, **kwargs)
  secret_info = lambda self, *args, **kwargs: self.log(LOG_FLAG.INFO | LOG_FLAG.SECRET, *args, **kwargs)
  secret_debug = lambda self, *args, **kwargs: self.log(LOG_FLAG.DEBUG | LOG_FLAG.SECRET, *args, **kwargs)
  exception = lambda self, *args, **kwargs: self._exception(*args, **kwargs) 
  
  def __init__(
    self,
    stream: BinaryIO,
    output_kind: LOG_KIND = LOG_KIND.TEXT,
    text_format: str = "{ts}::{flags}::{msg}",
    json_indent: int | None = None,
    asynchronous: bool = False,
  ):
    self._time_datum = {
      "monotonic": time.monotonic(),
      "real": time.time(),
    }
    self._stream: BinaryIO = stream
    self._children: list[LogStream] = []
    self._output_kind = output_kind
    self._text_format = text_format
    self._json_indent = json_indent
    self._async = asynchronous
  
  def time(self) -> float:
    return (time.monotonic() - self._time_datum["monotonic"]) + self._time_datum["real"]

  def flush(self):
    self._stream.flush()

  def _write(self, buffer: bytes):
    bytes_written = 0
    while bytes_written < len(buffer):
      bytes_written += self._stream.write(buffer[bytes_written:])

  def log(
    self,
    flags: LOG_FLAG,
    *msg: str,
    **kwargs: Any,
  ):
    ts = self.time()
    try:
      if self._output_kind == LOG_KIND.TEXT:
        msg_text = self._text_format.format(**{
          "ts": datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat(),
          "flags": "".join([
            flag.name[0].upper()
            if flag & flags
            else "-"
            for flag in LOG_FLAG
          ]),
          "msg": "\n".join([
            f"{line}"
            for line in msg
          ]),
          **kwargs,
        })
      elif self._output_kind == LOG_KIND.COMPACT_TEXT:
        msg_text = self._text_format.format(**{
          "ts": ts,
          "flags": "".join([
            flag.name[0].upper()
            for flag in LOG_FLAG
            if flag & flags
          ]),
          "msg": "\n".join([
            f"{line}"
            for line in msg
          ]),
          **kwargs,
        }).replace("\n", "\\n")
      elif self._output_kind == LOG_KIND.JSON:
        msg_text = json.dumps(
          {
            "ts": {
              "time": datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat(),
              "spec": "iso8601",
            },
            "flags": [
              flag.name.upper()
              for flag in flags
            ],
            "msg": msg,
            **kwargs,
          },
          indent=self._json_indent,
        )
      else:
        raise ValueError(f"Unknown output kind: {self._output_kind}")
      msg_bytes = "{}\n".format(msg_text.rstrip()).encode("utf-8")
    except:
      raise MessageFormatError("Failed to format log message:\n{}".format('\n'.join(traceback.format_exception(*sys.exc_info()))))

    try:
      if self._async:
        raise NotImplementedError("Async logging not implemented")
      else:
        self._write(msg_bytes)
    except:
      raise WriteError("Failed to write to log stream:\n{}".format('\n'.join(traceback.format_exception(*sys.exc_info()))))
  
  def _exception(
    self,
    msg: str | None = None,
    exc_info: tuple[type, BaseException, Any] | None = None,
    flags: LOG_FLAG = LOG_FLAG.ERROR,
  ):
    if exc_info is None:
      exc_info = sys.exc_info()
    log_msg = "\n".join(
      traceback.format_exception(*exc_info)
    )
    if msg is not None:
      log_msg = f"{msg}\n{log_msg}"
    
    self.log(
      flags,
      log_msg,
    )    

class BlackHole(LogStream):
  log = lambda self, *args, **kwargs: None
  flush = lambda self, *args, **kwargs: None

  def __init__(self, *args, **kwargs):
    pass
