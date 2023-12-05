{{ define "tableschema" }}
{{ $t := .Data }}

class {{ $t.Name | pytablename }}:
{{ I 1 }}"""Represents a column from the {{ $t.Name }} table"""

{{ I 1 }}class Columns(Column, Enum):
{{ range $col := $t.Columns -}}
{{ I 2 }}{{ $col.Name | pyfieldname }} = ({{ $col.Name | pyval }}, {{ $col.IsPrimary | pyval }})
{{ end }}

{{ I 1 }}def __init__(self, 
{{ range $col := $t.Columns -}}
{{ I 2 }}{{ $col.Name | pyfieldname }}: {{ $col.Type | pytype }} | UnsetType = Unset,
{{ end -}}
):
{{ range $col := $t.Columns -}}
{{ I 2 }}self._{{ $col.Name | pyfieldname }}: {{ $col.Type | pytype }} | UnsetType = {{ $col.Name | pyfieldname }}
{{ end -}}

{{ range $col := $t.Columns }}
{{ I 1 }}@property
{{ I 1 }}def {{ $col.Name | pyfieldname }}(self) -> {{ $col.Type | pytype }}:
{{ I 2 }}if isinstance(self._{{ $col.Name | pyfieldname }}, UnsetType):
{{ I 3 }}raise ValueError("{{ $col.Name | pyfieldname }} is unset")
{{ I 2 }}return self._{{ $col.Name | pyfieldname }}

{{ I 1 }}@{{ $col.Name | pyfieldname }}.setter
{{ I 1 }}def {{ $col.Name | pyfieldname }}(self, v: {{ $col.Type | pytype }}):
{{ I 2 }}self._{{ $col.Name | pyfieldname }} = v
{{ end }}

{{ I 1 }}def insert(self, cursor: "DBAPICursor"):
{{ I 2 }}_, cols, values = self._get_fields(False)
{{ I 2 }}ret_names, ret_cols, _ = self._get_fields(True)

{{ I 2 }}if cols:
{{ I 3 }}stmt = (
{{ I 4 }}"""INSERT INTO {{ $t.Name }}("""
{{ I 4 }}+ ", ".join(cols)
{{ I 4 }}+ ") VALUES ("
{{ I 4 }}+ ", ".join("?" for _ in values)
{{ I 4 }}+ ") RETURNING "
{{ I 4 }}+ ", ".join(r for r in ret_cols)
{{ I 3 }})
{{ I 2 }}else:
{{ I 3 }}stmt = (
{{ I 4 }}"""INSERT INTO {{ $t.Name }}"""
{{ I 4 }}+ " DEFAULT VALUES RETURNING "
{{ I 4 }}+ ", ".join(r for r in ret_cols)
{{ I 3 }})

{{ I 2 }}cursor.execute(stmt, values)
{{ I 2 }}row = cursor.fetchone()
{{ I 2 }}if row is None:
{{ I 3 }}return
{{ I 2 }}for i, r in enumerate(ret_names):
{{ I 3 }}setattr(self, r, row[i])

{{ I 1 }}def update(self, cursor: "DBAPICursor"):
{{ I 2 }}_, cols, values = self._get_fields(False)
{{ I 2 }}ret_names, ret_cols, _ = self._get_fields(True)
{{ I 2 }}_, where_cols, where_values = self._get_primary_keys()

{{ I 2 }}if cols:
{{ I 3 }}stmt = (
{{ I 4 }}"""UPDATE {{ $t.Name }}"""
{{ I 4 }}+ " SET "
{{ I 4 }}+ ", ".join(c + " = ?" for c in cols)
{{ I 4 }}+ ") WHERE "
{{ I 4 }}+ " AND ".join(w + " = ?" for w in where_cols)
{{ I 4 }}+ " RETURNING "
{{ I 4 }}+ ", ".join(r for r in ret_cols)
{{ I 3 }})
{{ I 2 }}else:
{{ I 3 }}stmt = (
{{ I 4 }}"""SELECT"""
{{ I 4 }}+ ", ".join(r for r in ret_cols)
{{ I 4 }}+ """ FROM {{ $t.Name }} """
{{ I 4 }}+ " WHERE "
{{ I 4 }}+ " AND ".join(w + " = ?" for w in where_cols)
{{ I 3 }})

{{ I 2 }}cursor.execute(stmt, values + where_values)
{{ I 2 }}row = cursor.fetchone()
{{ I 2 }}if row is None:
{{ I 3 }}return
{{ I 2 }}for i, r in enumerate(ret_names):
{{ I 3 }}setattr(self, r, row[i])

{{ I 1 }}def delete(self, cursor: "DBAPICursor"):
{{ I 2 }}_, fields, values = self._get_fields(False)
{{ I 2 }}stmt = (
{{ I 3 }}"""DELETE FROM {{ $t.Name }}"""
{{ I 3 }}+ " WHERE "
{{ I 3 }}+ " AND ".join(f + " = ?" for f in fields)
{{ I 2 }})
{{ I 2 }}cursor.execute(stmt, values)

{{ I 1 }}def _get_fields(self, unset: bool | None = None) -> tuple[list[str], list[SQLName], list[Any]]:
{{ I 2 }}names: list[str] = []
{{ I 2 }}sqlnames: list[SQLName] = []
{{ I 2 }}values: list[Any] = []
{{ I 2 }}if unset is True:
{{ range $col := $t.Columns -}}
{{ I 3 }}if self._{{ $col.Name | pyfieldname }} is Unset:
{{ I 4 }}names += [{{ $col.Name | pyfieldname | pyval}}]
{{ I 4 }}sqlnames += [{{ $col.Name | pyval }}]
{{ I 4 }}values += [self._{{ $col.Name | pyfieldname }}]
{{ end -}}
{{ I 2 }}elif unset is False:
{{ range $col := $t.Columns -}}
{{ I 3 }}if self._{{ $col.Name | pyfieldname }} is not Unset:
{{ I 4 }}names += [{{ $col.Name | pyfieldname | pyval}}]
{{ I 4 }}sqlnames += [{{ $col.Name | pyval }}]
{{ I 4 }}values += [self._{{ $col.Name | pyfieldname }}]
{{ end -}}
{{ I 2 }}else:
{{ range $col := $t.Columns -}}
{{ I 3 }}names += [{{ $col.Name | pyfieldname | pyval}}]
{{ I 3 }}sqlnames += [{{ $col.Name | pyval }}]
{{ I 3 }}values += [self._{{ $col.Name | pyfieldname }}]
{{ end -}}
{{ I 2 }}return names, sqlnames, values

{{ I 1 }}def _get_primary_keys(self, throw_if_unset: bool = True) -> tuple[list[str], list[SQLName], list[Any]]:
{{ I 2 }}names: list[str] = []
{{ I 2 }}sqlnames: list[SQLName] = []
{{ I 2 }}values: list[Any] = []
{{ range $col := $t.Columns -}}
{{- if $col.IsPrimary }}
{{ I 2 }}names += [{{ $col.Name | pyfieldname | pyval }}]
{{ I 2 }}sqlnames += [{{ $col.Name | pyval }}]
{{ I 2 }}if throw_if_unset and self._{{ $col.Name | pyfieldname }} is Unset:
{{ I 3 }}raise ValueError("primary key {{ $col.Name }} is unset")
{{ I 2 }}values += [self._{{ $col.Name | pyfieldname }}]
{{ end -}}
{{ end -}}
{{ I 2 }}return names, sqlnames, values

{{ end }}
