{{ define "db" -}}

from typing import Any, Protocol, Mapping, Optional, Sequence

class Connection(Protocol):
    def close(self) -> object: ...
    def commit(self) -> object: ...
    def cursor(self) -> 'Cursor': ...

class Cursor(Protocol):
    @property
    def lastrowid(self) -> int: ...
    @property
    def rowcount(self) -> int: ...
    def close(self) -> object: ...
    def execute(self, __operation: str, __parameters: Sequence[Any] | Mapping[str, Any] = ...) -> object: ...
    def executemany(self, __operation: str, __seq_of_parameters: Sequence[Sequence[Any]]) -> object: ...
    def fetchone(self) -> Sequence[Any] | None: ...
    def fetchmany(self, __size: int = ...) -> Sequence[Sequence[Any]]: ...
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
