{{ define "viewschema" }}
{{ $v := .Data }}

class {{ $v.Name | pytablename }}:
{{ I 1 }}"""Represents a column from the {{ $v.Name }} view"""

{{ I 1 }}def __init__(self, 
{{ range $col := $v.Columns -}}
{{ I 2 }}{{ $col.Name | pyfieldname }}: {{ $col.Type | pytype }},
{{ end -}}
):
{{ range $col := $v.Columns -}}
{{ I 2 }}self.{{ $col.Name | pyfieldname }}: {{ $col.Type | pytype }} = {{ $col.Name | pyfieldname }}
{{ end -}}

{{ end }}