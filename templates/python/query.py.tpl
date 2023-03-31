{{ define "query" }}
{{- $q := .Data -}}
{{- if $q.Comment -}}
# {{ $q.Comment | eval (func_name $q) }}
{{- else -}}
# {{ func_name $q }} runs a custom query{{ if $q.Exec }} as a sql.Result{{ else if not $q.Flat }}, returning results as {{ $q.Type.PythonName }}{{ end }}.
{{- end }}
{{ func $q }} {
	# query
	{{ querystr $q }}
	# run
	logf({{ names "" "sqlstr" $q }})
{{ if $q.Exec -}}
	return {{ db "Exec" $q }}
{{- else if $q.Flat -}}
{{- range $q.Type.Fields -}}
	var {{ .PythonName }} {{ type .Type }}
{{ end -}}
	if err := {{ db "QueryRow" $q }}.Scan({{ names "&" $q.Type.Fields }}); err != nil {
		return {{ zero $q.Type.Fields "logerror(err)" }}
	}
	return {{ names "" $q.Type "nil" }}
{{- else if $q.One -}}
	var {{ short $q.Type }} {{ type $q.Type.PythonName }}
	if err := {{ db "QueryRow" $q }}.Scan({{ names (print "&" (short $q.Type) ".") $q.Type.Fields }}); err != nil {
		return nil, logerror(err)
	}
	return &{{ short $q.Type }}, nil
{{- else -}}
	rows, err := {{ db "Query" $q }}
	if err != nil {
		return nil, logerror(err)
	}
	defer rows.Close()
	# load results
	var res []*{{ type $q.Type.PythonName }}
	for rows.Next() {
		var {{ short $q.Type}} {{ type $q.Type.PythonName }}
		# scan
		if err := rows.Scan({{ names (print "&" (short $q.Type) ".") $q.Type.Fields }}); err != nil {
			return nil, logerror(err)
		}
		res = append(res, &{{ short $q.Type }})
	}
	if err := rows.Err(); err != nil {
		return nil, logerror(err)
	}
	return res, nil
{{- end }}
}

{{ if true -}}
{{- if $q.Comment -}}
# {{ $q.Comment | eval (func_name $q) }}
{{- else -}}
# {{ func_name $q }} runs a custom query{{ if $q.Exec }} as a sql.Result{{ else if not $q.Flat }}, returning results as {{ $q.Type.PythonName }}{{ end }}.
{{- end }}
{{ func $q }} {
	return {{ func_name $q }}({{ names_all "" "context.Background()" "db" $q }})
}
{{- end }}
{{ end }}

{{ define "typedef" }}
{{- $q := .Data -}}
{{- if $q.Comment -}}
# {{ $q.Comment | eval $q.PythonName }}
{{- else -}}
# {{ $q.PythonName }} represents a row from '{{ schema $q.SQLName }}'.
{{- end }}
type {{ $q.PythonName }} struct {
{{ range $q.Fields -}}
    {{ field . }}
{{ end -}}
}
{{ end }}
