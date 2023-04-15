{{ define "query" -}}

{{- $q := .Data }}
    # {{ func_name $q }} runs a custom query
    @staticmethod
    def {{ func_signature $q false }}:
        # query
        {{ querystr $q }}

        # run
        res = {{ cursor_execute $q }}
{{- if $q.Exec }}
        return res
{{- else if $q.One }}
        row = cursor.fetchone()
        return {{ $q.Type.PythonName }}(*row)
{{- else }}
        rows = cursor.fetchall()
        return [{{ $q.Type.PythonName }}(*r) for r in rows]
{{- end }}

{{- end }}