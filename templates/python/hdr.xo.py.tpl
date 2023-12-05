{{ define "hdr" }}

from enum import Enum

from {{ pkg "utils" }} import Column, Unset, UnsetType, SQLName

from typing import Any, TYPE_CHECKING
if TYPE_CHECKING:
{{ I 1 }}from _typeshed.dbapi import DBAPICursor

{{ end }}