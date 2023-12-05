package python

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/kenshaw/inflector"
	"github.com/kenshaw/snaker"
	"github.com/xo/xo/types"
	xo "github.com/xo/xo/types"
)

// Init registers the template.
func Init(ctx context.Context, f func(xo.TemplateType)) error {
	f(xo.TemplateType{
		Modes: []string{"query", "schema"},
		Flags: []xo.Flag{
			{
				ContextKey: IndentKey,
				Type:       "string",
				Desc:       "indent spacing",
				Default:    "    ",
			},
			{
				ContextKey: PackageNameKey,
				Type:       "string",
				Desc:       "package name",
				Default:    "models",
			},
		},
		Funcs: func(ctx context.Context, _ string) (template.FuncMap, error) {
			funcMap := template.FuncMap{
				// I returns the given number of levels of indentation
				"I": func(n int) string {
					return strings.Repeat(Indent(ctx), n)
				},
				// class_name returns a python class name from an sql name
				"pytablename": func(s string) string {
					return snaker.ForceCamelIdentifier(inflector.Singularize(s))
				},
				// var_name returns a python variable name from an sql name
				"pyfieldname": func(s string) string {
					return snaker.CamelToSnake(s)
				},
				// pyval returns a python literal representation of a go value
				"pyval": func(i any) string {
					switch v := i.(type) {
					case string:
						return `"""` + v + `"""`
					case int:
						return fmt.Sprintf("%d", v)
					case bool:
						if v {
							return "True"
						}
						return "False"
					}
					panic(fmt.Sprintf("can't cast %T to python type", i))
				},
				// pytype returns a python type from an sql type
				"pytype": func(s types.Type) string {
					t := ""
					switch s.Type {
					case "bool", "boolean":
						t = "bool"
					case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
						t = "str"
					case "tinyint", "smallint", "year", "mediumint", "int", "integer", "bigint":
						t = "int"
					case "float", "double", "decimal":
						t = "float"
					case "binary", "blob", "longblob", "mediumblob", "tinyblob", "varbinary", "json":
						t = "bytes"
					case "timestamp", "datetime", "date":
						t = "datetime"
					case "time":
						t = "time"
					}
					if t == "" {
						panic("unknown type " + s.Type)
					}
					if s.Nullable {
						return t + " | None"
					}
					return t
				},
				"pkg": func(names ...string) string {
					// return strings.Join(append([]string{PackageName(ctx)}, names...), ".")
					return "." + strings.Join(names, ".")
				},
			}
			return funcMap, nil
		},
		Process: func(ctx context.Context, mode string, set *xo.Set, emit func(xo.Template)) error {
			toFilename := func(s string) string {
				return strings.ToLower(inflector.Singularize(s)) + ".py"
			}
			if mode == "schema" {
				// emit utils file
				emit(xo.Template{
					Partial: "utils",
					Dest:    "utils.py",
					Data:    nil,
				})
				for _, schema := range set.Schemas {
					for _, enum := range schema.Enums {
						filename := toFilename(enum.Name)
						emit(xo.Template{
							Partial: "hdr",
							Dest:    filename,
							Data:    nil,
						})
						emit(xo.Template{
							Partial: "enumschema",
							Dest:    filename,
							Data:    enum,
						})
					}

					for _, proc := range schema.Procs {
						// emit proc
						_ = proc
					}

					for _, table := range schema.Tables {
						filename := toFilename(table.Name)
						emit(xo.Template{
							Partial: "hdr",
							Dest:    filename,
							Data:    nil,
						})
						emit(xo.Template{
							Partial: "tableschema",
							Dest:    filename,
							Data:    table,
						})
						for _, fkey := range table.ForeignKeys {
							emit(xo.Template{
								Partial: "foreignkey",
								Dest:    filename,
								Data:    fkey,
							})
						}
						for _, index := range table.Indexes {
							emit(xo.Template{
								Partial: "index",
								Dest:    filename,
								Data:    index,
							})
						}
					}

					for _, view := range schema.Views {
						filename := toFilename(view.Name)
						emit(xo.Template{
							Partial: "hdr",
							Dest:    filename,
							Data:    nil,
						})
						emit(xo.Template{
							Partial: "tableschema",
							Dest:    filename,
							Data:    view,
						})
					}
				}
			} else if mode == "query" {
				for _, query := range set.Queries {
					// emit query
					_ = query
				}
			}

			return nil
		},
	})
	return nil
}

// Context keys.
var (
	IndentKey      xo.ContextKey = "indent"
	PackageNameKey xo.ContextKey = "package-name"
)

// Indent returns indent from the context.
func Indent(ctx context.Context) string {
	s, _ := ctx.Value(IndentKey).(string)
	return s
}

// Ugly returns ugly from the context.
func PackageName(ctx context.Context) string {
	b, _ := ctx.Value(PackageNameKey).(string)
	return b
}
