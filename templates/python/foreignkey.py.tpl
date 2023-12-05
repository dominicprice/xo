{{ define "foreignkey" }}
{{ $f := .Data }}

{{ I 1 }}def get_{{ $f.RefTable | pyfieldname }}(self) -> {{ $f.RefTable | pytablename }}:
{{ I 2 }}stmt = """SELECT """ + 

{{ end }}