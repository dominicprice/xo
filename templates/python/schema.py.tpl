{{ define "enum" }}
{{- $e := .Data -}}

class {{ $e.PythonName }}(IntEnum):
    """
    The '{{ $e.SQLName }}' enum type from schema '{{ schema }}'.
    """
{{ range $e.Values -}}
    {{ $e.PythonName }} = {{ .ConstValue }}
{{ end -}}

{{ end }}

{{ define "foreignkey" }}
{{- $k := .Data -}}

    {{ recv (func_name $k) $k }}:
        """
        Returns the {{ $k.RefTable }} associated with the
        {{ $k.Table.PythonName }}'s ({{ names "" $k.Fields }})

        Generated from foreign key '{{ $k.SQLName }}'.
        """
        return {{ foreign_key $k }}

{{ end }}

{{ define "index" }}
{{- $i := .Data }}
    @staticmethod
    {{ func (func_name $i) $i }}:
        """
        Retrieves a row from the '{{schema $i.Table.SQLName }}' as a {{ $i.Table.PythonName }}.

        Generated from '{{ $i.SQLName }}'.
        """
        # query
        {{ sqlstr "index" "            " $i }}

        # run
        {{ db "execute" $i }}
{{- if $i.IsUnique }}
        row = cursor.fetchone()
        if row is None:
            raise NotFoundError
        return {{ $i.Table.PythonName }}(*row)
{{- else }}
        res: list[{{ $i.Table.PythonName }}] = []
        while row := cursor.fetchone():
            res += [{{ $i.Table.PythonName }}(*row)
        return res 
{{ end -}}

{{end}}

{{ define "procs" }}

# Procedures are not currently implemented

{{- end }}


{{ define "typedef" }}
{{- $t := .Data -}}
class {{ $t.PythonName }}:
    """
{{- if $t.Comment -}}
    {{ $t.Comment | eval $t.PythonName }}
{{- else }}
    {{ $t.PythonName }} represents a row from '{{ schema $t.SQLName }}'.
{{- end }}
    """
    def __init__(self,
{{- range $t.Fields }}
        {{ .PythonName }}: {{ .Type }} = {{ .Zero }},
{{- end }}
    ):
{{- range $t.Fields }}
        self.{{ .PythonName }} = {{ .PythonName }}
{{- end }}
{{- if $t.PrimaryKeys }}

        # xo fields
        self._exists = False
        self._deleted = False
{{ end }}

    def exists(self) -> bool:
        """
        Returns true when the {{ $t.PythonName }} exists in the database
        """
        return self._exists


    def deleted(self) -> bool:
        """
        Returns true when the {{ $t.PythonName }} has been marked for
        deletion from the database
        """
        return self._deleted


    def insert(self, cursor: Cursor):
        """
        Inserts the {{ $t.PythonName }} into the database.
        """
        if self._exists:
            raise InsertFailedError("already exists")
        elif self._deleted:
            raise InsertFailedError("marked for deletion")
{{ if $t.Manual }}
        # insert (manual)
        {{ sqlstr "insert_manual" "            " $t }}

        # run
        {{ db "execute" $t }}
{{ else }}
        # insert (primary key generated and returned by database)
        {{ sqlstr "insert" "            " $t }}

        # run
        {{ db "execute" $t }}
        self.{{ (index $t.PrimaryKeys 0).PythonName }} = cursor.lastrowid
{{- end }}
        self._exists = True

{{ if eq (len $t.Fields) (len $t.PrimaryKeys) }}
# ------ NOTE: Update statements omitted due to lack of fields other than primary key ------
{{ else }}
    def update(self, cursor: Cursor):
        """
        Updates a {{ $t.PythonName }} in the database.
        """
        if not self._exists:
            raise UpdateFailedError("does not exist")
        elif self._deleted:
            raise UpdateFailedError("marked for deletion")

        # update with {{ if driver "postgres" }}composite {{ end }}primary key
        {{ sqlstr "update" "            " $t }}

        # run
        {{ db_update "execute" $t }}
{{ end }}

    def save(self, cursor: Cursor):
        """
        Saves the {{ $t.PythonName }} to the database.
        """
        if self._exists:
            self.update(cursor)
        else:
            self.insert(cursor)


    def upsert(self, cursor: Cursor):
        """
        Performs an upsert for {{ $t.PythonName }}.
        """
        if self._deleted:
            raise UpsertFailedError("marked for deletion")

        # upsert
        {{ sqlstr "upsert" "            " $t }}

        # run
        {{ db "execute" $t }}
        # set exists
        self._exists = True


    def delete(self, cursor: Cursor):
        """
        Deletes the {{ $t.PythonName }} from the database.
        """
        if not self._exists or self._deleted:
            return # does not exist or already deleted
{{ if eq (len $t.PrimaryKeys) 1 }}
        # delete with single primary key
        {{ sqlstr "delete" "            " $t }}

        # run
        {{ db "execute" (index $t.PrimaryKeys 0).PythonName }}
{{ else }}
        # delete with composite primary key
        {{ sqlstr "delete" "            " $t }}

        # run
        {{ db "execute" $t.PrimaryKeys }}
{{- end }}
        # set deleted
        self._deleted = True

{{ end }}
