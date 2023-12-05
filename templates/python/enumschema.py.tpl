{{ define "enumschema" }}
{{ $e := .Data }}

class {{ $e.Name | pytablename }}(Enum):
{{ range $v := $e.Values }}
   {{ I 1 }}{{ $v.Name | pytablename }} = {{ $v.ConstValue }}
{{ end }}

{{ end }}