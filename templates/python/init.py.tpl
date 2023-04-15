{{ define "init" -}}
# Package {{ pkg }} contains generated code for schema '{{ schema }}'.

{{ range .Data -}}
from {{ . }} import *
{{ end }}

{{- end }}
