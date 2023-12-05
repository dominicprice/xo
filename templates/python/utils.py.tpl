{{ define "utils" }}

from dataclasses import dataclass
from typing import TypeAlias

SQLName: TypeAlias = str

class UnsetType:
{{ I 1 }}pass

Unset = UnsetType()

@dataclass
class Column:
{{ I 1 }}name: str
{{ I 1 }}primary_key: bool

{{ end }}