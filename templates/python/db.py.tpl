{{ define "db" -}}

from typing import Any, Protocol, Mapping, Optional, Sequence, Union

class Cursor(Protocol):
    @property
    def lastrowid(self) -> int: ...
    @property
    def rowcount(self) -> int: ...
    def close(self) -> object: ...
    def execute(self, stmt: str, params: Union[Sequence[Any], Mapping[str, Any]] = ...) -> object: ...
    def executemany(self, stmt: str, params_pack: Sequence[Sequence[Any]]) -> object: ...
    def fetchone(self) -> Union[Sequence[Any], None]: ...
    def fetchmany(self, n: int = ...) -> Sequence[Sequence[Any]]: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...

class XOError(RuntimeError):
    pass

class InsertFailedError(XOError):
    pass

class UpdateFailedError(XOError):
    pass

class UpsertFailedError(XOError):
    pass

class NotFoundError(XOError):
    def __init__(self):
        super().__init__("not found")

{{- end }}
