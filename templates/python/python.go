//go:build xotpl

package pytpl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/kenshaw/inflector"
	"github.com/kenshaw/snaker"
	"github.com/xo/xo/loader"
	xo "github.com/xo/xo/types"
)

const ext = ".py"

var ErrNoSingle = errors.New("in query exec mode, the --single or -S must be provided")

// Init registers the template.
func Init(ctx context.Context, f func(xo.TemplateType)) error {
	knownTypes := map[string]bool{
		"bool":         true,
		"str":          true,
		"bytes":        true,
		"int":          true,
		"float":        true,
		"list[bool]":   true,
		"list[bytes]":  true,
		"list[float]":  true,
		"list[int]":    true,
		"list[string]": true,
	}
	shorts := map[string]string{
		"bool":         "b",
		"str":          "s",
		"bytes":        "b",
		"int":          "i",
		"float":        "f",
		"list[bool]":   "l",
		"list[bytes]":  "l",
		"list[float]":  "l",
		"list[int]":    "l",
		"list[string]": "l",
	}
	f(xo.TemplateType{
		Modes: []string{"query", "schema"},
		Flags: []xo.Flag{
			{
				ContextKey: AppendKey,
				Type:       "bool",
				Desc:       "enable append mode",
				Short:      "a",
				Aliases:    []string{"append"},
			},
			{
				ContextKey: NotFirstKey,
				Type:       "bool",
				Desc:       "disable package file (ie. not first generated file)",
				Short:      "2",
				Default:    "false",
			},
			{
				ContextKey: PkgKey,
				Type:       "string",
				Desc:       "package name",
			},
			{
				ContextKey: TagKey,
				Type:       "[]string",
				Desc:       "build tags",
			},
			{
				ContextKey: ImportKey,
				Type:       "[]string",
				Desc:       "package imports",
			},
			{
				ContextKey: CustomKey,
				Type:       "string",
				Desc:       "package name for custom types",
			},
			{
				ContextKey: ConflictKey,
				Type:       "string",
				Desc:       "name conflict suffix",
				Default:    "Val",
			},
			{
				ContextKey: InitialismKey,
				Type:       "[]string",
				Desc:       "add initialism (e.g. ID, API, URI, ...)",
			},
			{
				ContextKey: EscKey,
				Type:       "[]string",
				Desc:       "escape fields",
				Default:    "none",
				Enums:      []string{"none", "schema", "table", "column", "all"},
			},
			{
				ContextKey: InjectKey,
				Type:       "string",
				Desc:       "insert code into generated file headers",
				Default:    "",
			},
			{
				ContextKey: InjectFileKey,
				Type:       "string",
				Desc:       "insert code into generated file headers from a file",
				Default:    "",
			},
		},
		Funcs: func(ctx context.Context, _ string) (template.FuncMap, error) {
			funcs, err := NewFuncs(ctx)
			if err != nil {
				return nil, err
			}
			return funcs, nil
		},
		NewContext: func(ctx context.Context, _ string) context.Context {
			ctx = context.WithValue(ctx, KnownTypesKey, knownTypes)
			ctx = context.WithValue(ctx, ShortsKey, shorts)
			return ctx
		},
		Order: func(ctx context.Context, mode string) []string {
			base := []string{"header", "db"}
			switch mode {
			case "query":
				return append(base, "typedef", "query")
			case "schema":
				return append(base, "enum", "proc", "typedef", "query", "index", "foreignkey")
			}
			return nil
		},
		Pre: func(ctx context.Context, mode string, set *xo.Set, out fs.FS, emit func(xo.Template)) error {
			if err := addInitialisms(ctx); err != nil {
				return err
			}
			files, err := fileNames(ctx, mode, set)
			if err != nil {
				return err
			}
			// If -2 is provided, skip package template outputs as requested.
			// If -a is provided, skip to avoid duplicating the template.
			if !NotFirst(ctx) && !Append(ctx) {
				emit(xo.Template{
					Partial: "db",
					Dest:    "db.py",
				})
				// Don't generate header for db.xo.go.
				files["db.py"] = true
			}
			if Append(ctx) {
				for filename := range files {
					f, err := out.Open(filename)
					switch {
					case errors.Is(err, os.ErrNotExist):
						continue
					case err != nil:
						return err
					}
					defer f.Close()
					data, err := io.ReadAll(f)
					if err != nil {
						return err
					}
					emit(xo.Template{
						Src:     "{{.Data}}",
						Partial: "header", // ordered first
						Data:    string(data),
						Dest:    filename,
					})
					delete(files, filename)
				}
			}
			for filename := range files {
				emit(xo.Template{
					Partial: "header",
					Dest:    filename,
				})
			}
			return nil
		},
		Process: func(ctx context.Context, mode string, set *xo.Set, emit func(xo.Template)) error {
			if mode == "query" {
				for _, query := range set.Queries {
					if err := emitQuery(ctx, query, emit); err != nil {
						return err
					}
				}
			} else {
				for _, schema := range set.Schemas {
					if err := emitSchema(ctx, schema, emit); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Post: func(ctx context.Context, mode string, files map[string][]byte, emit func(string, []byte)) error {
			for file, content := range files {
				emit(file, content)
			}
			return nil
		},
	})
	return nil
}

// fileNames returns a list of file names that will be generated by the
// template based on the parameters and schema.
func fileNames(ctx context.Context, mode string, set *xo.Set) (map[string]bool, error) {
	// In single mode, only the specified file be generated.
	singleFile := xo.Single(ctx)
	if singleFile != "" {
		return map[string]bool{
			singleFile: true,
		}, nil
	}
	// Otherwise, infer filenames from set.
	files := make(map[string]bool)
	addFile := func(filename string) {
		// Filenames are always lowercase.
		filename = strings.ToLower(filename)
		files[filename+ext] = true
	}
	switch mode {
	case "schema":
		for _, schema := range set.Schemas {
			for _, e := range schema.Enums {
				addFile(fileExport(e.Name))
			}
			for _, p := range schema.Procs {
				fileName := fileExport(p.Name)
				if p.Type == "function" {
					addFile("sf_" + fileName)
				} else {
					addFile("sp_" + fileName)
				}
			}
			for _, t := range schema.Tables {
				addFile(fileExport(singularize(t.Name)))
			}
			for _, v := range schema.Views {
				addFile(fileExport(singularize(v.Name)))
			}
		}
	case "query":
		for _, query := range set.Queries {
			addFile(query.Type)
			if query.Exec {
				// Single mode is handled at the start of the function but it
				// must be used for Exec queries.
				return nil, ErrNoSingle
			}
		}
	default:
		panic("unknown mode: " + mode)
	}
	return files, nil
}

// Funcs is a set of template funcs.
type Funcs struct {
	driver     string
	schema     string
	nth        func(int) string
	first      bool
	pkg        string
	tags       []string
	imports    []string
	conflict   string
	custom     string
	escSchema  bool
	escTable   bool
	escColumn  bool
	inject     string
	oracleType string
	// knownTypes is the collection of known Python types.
	knownTypes map[string]bool
	// shorts is the collection of Python style short names for types, mainly
	// used for use with declaring a func receiver on a type.
	shorts map[string]string
}

// NewFuncs creates custom template funcs for the context.
func NewFuncs(ctx context.Context) (template.FuncMap, error) {
	first := !NotFirst(ctx)
	// load inject
	inject := Inject(ctx)
	if s := InjectFile(ctx); s != "" {
		buf, err := ioutil.ReadFile(s)
		if err != nil {
			return nil, fmt.Errorf("unable to read file: %v", err)
		}
		inject = string(buf)
	}
	driver, _, schema := xo.DriverDbSchema(ctx)
	nth, err := loader.NthParam(ctx)
	if err != nil {
		return nil, err
	}
	funcs := &Funcs{
		first:      first,
		driver:     driver,
		schema:     schema,
		nth:        nth,
		pkg:        Pkg(ctx),
		tags:       Tags(ctx),
		imports:    Imports(ctx),
		conflict:   Conflict(ctx),
		custom:     Custom(ctx),
		escSchema:  Esc(ctx, "schema"),
		escTable:   Esc(ctx, "table"),
		escColumn:  Esc(ctx, "column"),
		inject:     inject,
		oracleType: OracleType(ctx),
		knownTypes: KnownTypes(ctx),
		shorts:     Shorts(ctx),
	}
	return funcs.FuncMap(), nil
}

// FuncMap returns the func map.
func (f *Funcs) FuncMap() template.FuncMap {
	return template.FuncMap{
		// general
		"first":   f.firstfn,
		"driver":  f.driverfn,
		"schema":  f.schemafn,
		"pkg":     f.pkgfn,
		"tags":    f.tagsfn,
		"imports": f.importsfn,
		"inject":  f.injectfn,
		// func and query
		"func_name":   f.func_name_none,
		"func":        f.funcdef,
		"recv":        f.methoddef,
		"foreign_key": f.foreign_key,
		"db":          f.db,
		"db_prefix":   f.db_prefix,
		"db_update":   f.db_update,
		"db_named":    f.db_named,
		"named":       f.named,
		"logf":        f.logf,
		"logf_pkeys":  f.logf_pkeys,
		"logf_update": f.logf_update,
		// type
		"names":        f.names,
		"names_all":    f.names_all,
		"names_ignore": f.names_ignore,
		"params":       f.params,
		"zero":         f.zero,
		"type":         f.typefn,
		"field":        f.field,
		"short":        f.short,
		// sqlstr funcs
		"querystr": f.querystr,
		"sqlstr":   f.sqlstr,
		// helpers
		"check_name": checkName,
		"eval":       eval,
	}
}

func (f *Funcs) firstfn() bool {
	if f.first {
		f.first = false
		return true
	}
	return false
}

// driverfn returns true if the driver is any of the passed drivers.
func (f *Funcs) driverfn(drivers ...string) bool {
	for _, driver := range drivers {
		if f.driver == driver {
			return true
		}
	}
	return false
}

// schemafn takes a series of names and joins them with the schema name.
func (f *Funcs) schemafn(names ...string) string {
	s := f.schema
	// escape table names
	if f.escTable {
		for i, name := range names {
			names[i] = escfn(name)
		}
	}
	n := strings.Join(names, ".")
	switch {
	case s == "" && n == "":
		return ""
	case f.driver == "sqlite3" && n == "":
		return f.schema
	case f.driver == "sqlite3":
		return n
	case s != "" && n != "":
		if f.escSchema {
			s = escfn(s)
		}
		s += "."
	}
	return s + n
}

// pkgfn returns the package name.
func (f *Funcs) pkgfn() string {
	return f.pkg
}

// tagsfn returns the tags.
func (f *Funcs) tagsfn() []string {
	return f.tags
}

// importsfn returns the imports.
func (f *Funcs) importsfn() []PackageImport {
	var imports []PackageImport
	for _, s := range f.imports {
		alias, pkg := "", s
		if i := strings.Index(pkg, " "); i != -1 {
			alias, pkg = pkg[:i], strings.TrimSpace(pkg[i:])
		}
		imports = append(imports, PackageImport{
			Alias: alias,
			Pkg:   strconv.Quote(pkg),
		})
	}
	return imports
}

// injectfn returns the injected content provided from args.
func (f *Funcs) injectfn() string {
	return f.inject
}

// func_name_none builds a func name.
func (f *Funcs) func_name_none(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case Query:
		return x.Name
	case Table:
		return x.PythonName
	case ForeignKey:
		return x.PythonName
	case Proc:
		n := x.PythonName
		if x.Overloaded {
			n = x.OverloadedName
		}
		return n
	case Index:
		return x.Func
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 1: %T ]]", v)
}

// func_name_context generates a name for the func.
func (f *Funcs) func_name_context(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case Query:
		return x.Name
	case Table:
		return x.PythonName
	case ForeignKey:
		return x.PythonName
	case Proc:
		n := x.PythonName
		if x.Overloaded {
			n = x.OverloadedName
		}
		return n
	case Index:
		return x.Func
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 2: %T ]]", v)
}

// funcdef builds a func definition.
func (f *Funcs) funcdef(name string, v interface{}) string {
	var p, r []string
	p = append(p, "cursor: Cursor")
	switch x := v.(type) {
	case Query:
		// params
		for _, z := range x.Params {
			p = append(p, fmt.Sprintf("%s: %s", z.Name, z.Type))
		}
		// returns
		switch {
		case x.Exec:
			r = append(r, "tuple")
		case x.Flat:
			for _, z := range x.Type.Fields {
				r = append(r, f.typefn(z.Type))
			}
		case x.One:
			r = append(r, x.Type.PythonName)
		default:
			r = append(r, "list["+x.Type.PythonName+"]")
		}
	case Proc:
		// params
		p = append(p, f.params(x.Params, true))
		// returns
		if !x.Void {
			for _, ret := range x.Returns {
				r = append(r, f.typefn(ret.Type))
			}
		}
	case Index:
		// params
		p = append(p, f.params(x.Fields, true))
		// returns
		rt := x.Table.PythonName
		if !x.IsUnique {
			rt = "[]" + rt
		}
		r = append(r, rt)
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 3: %T ]]", v)
	}
	params := strings.Join(p, ", ")
	returns := strings.Join(r, ", ")
	if len(r) > 1 {
		returns = "tuple[" + returns + "]"
	}
	return fmt.Sprintf("def %s(%s) -> %s", name, params, returns)
}

// func_none generates a func signature for v without context.
// func (f *Funcs) func_none(v interface{}) string {
// 	return f.funcfn(f.func_name_none(v), false, v)
// }

// methoddef builds a method definition.
func (f *Funcs) methoddef(name string, v interface{}) string {
	var p, r []string
	// determine params and return type
	p = append(p, "self", "cursor: Cursor")
	switch x := v.(type) {
	case ForeignKey:
		r = append(r, x.RefTable)
	}
	return fmt.Sprintf("def %s(%s) -> %s", name, strings.Join(p, ", "), strings.Join(r, ", "))
}

func (f *Funcs) foreign_key(v interface{}) string {
	var name string
	var p []string
	switch x := v.(type) {
	case ForeignKey:
		name = x.RefFunc
		p = append(p, "cursor", f.convertTypes(x))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 7: %T ]]", v)
	}
	return fmt.Sprintf("%s(%s)", name, strings.Join(p, ", "))
}

// db generates a db.<name>(sqlstr, ...)
func (f *Funcs) db(name string, v ...interface{}) string {
	// params
	return fmt.Sprintf("cursor.%s(sqlstr, [%s])", name, f.names("self.", v...))
}

// db_prefix generates a db.<name>(sqlstr, <prefix>.param, ...).
//
// Will skip the specific parameters based on the type provided.
func (f *Funcs) db_prefix(name string, skip bool, vs ...interface{}) string {
	var prefix string
	var params []interface{}
	for i, v := range vs {
		var ignore []string
		switch x := v.(type) {
		case string:
			params = append(params, x)
		case Table:
			prefix = f.short(x.PythonName) + "."
			// skip primary keys
			if skip {
				for _, field := range x.Fields {
					if field.IsSequence {
						ignore = append(ignore, field.PythonName)
					}
				}
			}
			p := f.names_ignore(prefix, v, ignore...)
			// p is "" when no columns are present except for primary key
			// params
			if p != "" {
				params = append(params, p)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 8 (%d): %T ]]", i, v)
		}
	}
	return f.db(name, params...)
}

// db_update generates a db.<name>(sqlstr, regularparams,
// primaryparams)
func (f *Funcs) db_update(name string, v interface{}) string {
	var ignore, p []string
	switch x := v.(type) {
	case Table:
		for _, pk := range x.PrimaryKeys {
			ignore = append(ignore, pk.PythonName)
		}
		p = append(p, f.names_ignore("self.", x, ignore...), f.names("self.", x.PrimaryKeys))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 9: %T ]]", v)
	}
	return f.db(name, strings.Join(p, ", "))
}

// db_named generates a db.<name>(Named(name, res)...)
func (f *Funcs) db_named(name string, v interface{}) string {
	var p []string
	switch x := v.(type) {
	case Proc:
		for _, z := range x.Params {
			p = append(p, f.named(z.SQLName, z.PythonName, false))
		}
		for _, z := range x.Returns {
			p = append(p, f.named(z.SQLName, z.PythonName, true))
		}
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 10: %T ]]", v)
	}
	return f.db(name, strings.Join(p, ", "))
}

func (f *Funcs) named(name, value string, out bool) string {
	switch {
	case out && f.driver == "oracle" && f.oracleType == "ora":
		return fmt.Sprintf("Out{Dest: %s}", value)
	case out:
		return fmt.Sprintf("Named(%q, Out{Dest: %s})", name, value)
	}
	return fmt.Sprintf("Named(%q, %s)", name, value)
}

func (f *Funcs) logf_pkeys(v interface{}) string {
	p := []string{"sqlstr"}
	switch x := v.(type) {
	case Table:
		p = append(p, f.names(f.short(x.PythonName)+".", x.PrimaryKeys))
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

func (f *Funcs) logf(v interface{}, ignore ...interface{}) string {
	var ignoreNames []string
	p := []string{"sqlstr"}
	// build ignore list
	for i, x := range ignore {
		switch z := x.(type) {
		case string:
			ignoreNames = append(ignoreNames, z)
		case Field:
			ignoreNames = append(ignoreNames, z.PythonName)
		case []Field:
			for _, f := range z {
				ignoreNames = append(ignoreNames, f.PythonName)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 11 (%d): %T ]]", i, x)
		}
	}
	// add fields
	switch x := v.(type) {
	case Table:
		p = append(p, f.names_ignore(f.short(x.PythonName)+".", x, ignoreNames...))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 12: %T ]]", v)
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

func (f *Funcs) logf_update(v interface{}) string {
	var ignore []string
	p := []string{"sqlstr"}
	switch x := v.(type) {
	case Table:
		prefix := f.short(x.PythonName) + "."
		for _, pk := range x.PrimaryKeys {
			ignore = append(ignore, pk.PythonName)
		}
		p = append(p, f.names_ignore(prefix, x, ignore...), f.names(prefix, x.PrimaryKeys))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 13: %T ]]", v)
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

// names generates a list of names.
func (f *Funcs) namesfn(all bool, prefix string, z ...interface{}) string {
	var names []string
	for i, v := range z {
		switch x := v.(type) {
		case string:
			names = append(names, prefix+x)
		case Query:
			for _, p := range x.Params {
				if !all && p.Interpolate {
					continue
				}
				names = append(names, prefix+p.Name)
			}
		case Table:
			for _, p := range x.Fields {
				names = append(names, prefix+checkName(p.PythonName))
			}
		case []Field:
			for _, p := range x {
				names = append(names, prefix+checkName(p.PythonName))
			}
		case Proc:
			if params := f.params(x.Params, false); params != "" {
				names = append(names, params)
			}
		case Index:
			names = append(names, f.params(x.Fields, false))
		default:
			names = append(names, fmt.Sprintf("/* UNSUPPORTED TYPE 14 (%d): %T */", i, v))
		}
	}
	return strings.Join(names, ", ")
}

// names generates a list of names (excluding certain ones such as interpolated
// names).
func (f *Funcs) names(prefix string, z ...interface{}) string {
	return f.namesfn(false, prefix, z...)
}

// names_all generates a list of all names.
func (f *Funcs) names_all(prefix string, z ...interface{}) string {
	return f.namesfn(true, prefix, z...)
}

// names_ignore generates a list of all names, ignoring fields that match the value in ignore.
func (f *Funcs) names_ignore(prefix string, v interface{}, ignore ...string) string {
	m := make(map[string]bool)
	for _, n := range ignore {
		m[n] = true
	}
	var vals []Field
	switch x := v.(type) {
	case Table:
		for _, p := range x.Fields {
			if m[p.PythonName] {
				continue
			}
			vals = append(vals, p)
		}
	case []Field:
		for _, p := range x {
			if m[p.PythonName] {
				continue
			}
			vals = append(vals, p)
		}
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 15: %T ]]", v)
	}
	return f.namesfn(true, prefix, vals)
}

// querystr generates a querystr for the specified query and any accompanying
// comments.
func (f *Funcs) querystr(v interface{}) string {
	var query, comments []string
	switch x := v.(type) {
	case Query:
		query, comments = x.Query, x.Comments
	default:
		return fmt.Sprintf("sqlstr = [[ UNSUPPORTED TYPE 16: %T ]]", v)
	}
	var lines []string
	for i := 0; i < len(query); i++ {
		line := "'''" + query[i] + "'''"
		if i != len(query)-1 {
			line += " + \\"
		}
		if s := strings.TrimSpace(comments[i]); s != "" {
			line += "# " + s
		}
		lines = append(lines, line)
	}
	sqlstr := stripRE.ReplaceAllString(strings.Join(lines, "\n"), " ")
	return fmt.Sprintf("sqlstr = %s", sqlstr)
}

var stripRE = regexp.MustCompile(`\s+\+\s+` + "''''''")

// Context keys.
var (
	AppendKey     xo.ContextKey = "append"
	KnownTypesKey xo.ContextKey = "known-types"
	ShortsKey     xo.ContextKey = "shorts"
	NotFirstKey   xo.ContextKey = "not-first"
	ArrayModeKey  xo.ContextKey = "array-mode"
	PkgKey        xo.ContextKey = "pkg"
	TagKey        xo.ContextKey = "tag"
	ImportKey     xo.ContextKey = "import"
	UUIDKey       xo.ContextKey = "uuid"
	CustomKey     xo.ContextKey = "custom"
	ConflictKey   xo.ContextKey = "conflict"
	InitialismKey xo.ContextKey = "initialism"
	EscKey        xo.ContextKey = "esc"
	FieldTagKey   xo.ContextKey = "field-tag"
	ContextKey    xo.ContextKey = "context"
	InjectKey     xo.ContextKey = "inject"
	InjectFileKey xo.ContextKey = "inject-file"
	OracleTypeKey xo.ContextKey = "oracle-type"
)

// Append returns append from the context.
func Append(ctx context.Context) bool {
	b, _ := ctx.Value(AppendKey).(bool)
	return b
}

// KnownTypes returns known-types from the context.
func KnownTypes(ctx context.Context) map[string]bool {
	m, _ := ctx.Value(KnownTypesKey).(map[string]bool)
	return m
}

// Shorts retruns shorts from the context.
func Shorts(ctx context.Context) map[string]string {
	m, _ := ctx.Value(ShortsKey).(map[string]string)
	return m
}

// NotFirst returns not-first from the context.
func NotFirst(ctx context.Context) bool {
	b, _ := ctx.Value(NotFirstKey).(bool)
	return b
}

// ArrayMode returns array-mode from the context.
func ArrayMode(ctx context.Context) string {
	s, _ := ctx.Value(ArrayMode).(string)
	return s
}

// Pkg returns pkg from the context.
func Pkg(ctx context.Context) string {
	s, _ := ctx.Value(PkgKey).(string)
	if s == "" {
		s = filepath.Base(xo.Out(ctx))
	}
	return s
}

// Tags returns tags from the context.
func Tags(ctx context.Context) []string {
	v, _ := ctx.Value(TagKey).([]string)
	// build tags
	var tags []string
	for _, tag := range v {
		if tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags
}

// Imports returns package imports from the context.
func Imports(ctx context.Context) []string {
	v, _ := ctx.Value(ImportKey).([]string)
	// build imports
	var imports []string
	for _, s := range v {
		if s != "" {
			imports = append(imports, s)
		}
	}
	// add uuid import
	if s, _ := ctx.Value(UUIDKey).(string); s != "" {
		imports = append(imports, s)
	}
	return imports
}

// Custom returns the custom package from the context.
func Custom(ctx context.Context) string {
	s, _ := ctx.Value(CustomKey).(string)
	return s
}

// Conflict returns conflict from the context.
func Conflict(ctx context.Context) string {
	s, _ := ctx.Value(ConflictKey).(string)
	return s
}

// Esc indicates if esc should be escaped based from the context.
func Esc(ctx context.Context, esc string) bool {
	v, _ := ctx.Value(EscKey).([]string)
	return !contains(v, "none") && (contains(v, "all") || contains(v, esc))
}

// FieldTag returns field-tag from the context.
func FieldTag(ctx context.Context) string {
	s, _ := ctx.Value(FieldTagKey).(string)
	return s
}

// Context returns context from the context.
func Context(ctx context.Context) string {
	s, _ := ctx.Value(ContextKey).(string)
	return s
}

// Inject returns inject from the context.
func Inject(ctx context.Context) string {
	s, _ := ctx.Value(InjectKey).(string)
	return s
}

// InjectFile returns inject-file from the context.
func InjectFile(ctx context.Context) string {
	s, _ := ctx.Value(InjectFileKey).(string)
	return s
}

// OracleType returns oracle-type from the context.
func OracleType(ctx context.Context) string {
	s, _ := ctx.Value(OracleTypeKey).(string)
	return s
}

// convertEnum converts a xo.Enum.
func convertEnum(e xo.Enum) Enum {
	var vals []EnumValue
	pythonName := snake(e.Name)
	for _, v := range e.Values {
		name := snake(strings.ToLower(v.Name))
		if strings.HasSuffix(name, pythonName) && pythonName != name {
			name = strings.TrimSuffix(name, pythonName)
		}
		vals = append(vals, EnumValue{
			PythonName: name,
			SQLName:    v.Name,
			ConstValue: *v.ConstValue,
		})
	}
	return Enum{
		PythonName: pythonName,
		SQLName:    e.Name,
		Values:     vals,
	}
}

// convertProc converts a xo.Proc.
func convertProc(ctx context.Context, overloadMap map[string][]Proc, order []string, p xo.Proc) ([]string, error) {
	_, _, schema := xo.DriverDbSchema(ctx)
	proc := Proc{
		Type:       p.Type,
		PythonName: snake(p.Name),
		SQLName:    p.Name,
		Signature:  fmt.Sprintf("%s.%s", schema, p.Name),
		Void:       p.Void,
	}
	// proc params
	var types []string
	for _, z := range p.Params {
		f, err := convertField(ctx, snake, z)
		if err != nil {
			return nil, err
		}
		proc.Params = append(proc.Params, f)
		types = append(types, z.Type.Type)
	}
	// add to signature, generate name
	proc.Signature += "Callable[[" + strings.Join(types, ", ") + "], "
	proc.OverloadedName = overloadedName(types, proc)
	types = nil
	// proc return
	for _, z := range p.Returns {
		f, err := convertField(ctx, snake, z)
		if err != nil {
			return nil, err
		}
		proc.Returns = append(proc.Returns, f)
		types = append(types, z.Type.Type)
	}
	// append signature
	if !p.Void {
		format := ", [%s]]"
		if len(p.Returns) == 1 {
			format = ", %s]"
		}
		proc.Signature += fmt.Sprintf(format, strings.Join(types, ", "))
	} else {
		proc.Signature += ", None]"
	}
	// add proc
	procs, ok := overloadMap[proc.PythonName]
	if !ok {
		order = append(order, proc.PythonName)
	}
	overloadMap[proc.PythonName] = append(procs, proc)
	return order, nil
}

// convertTable converts a xo.Table to a Table.
func convertTable(ctx context.Context, t xo.Table) (Table, error) {
	var cols, pkCols []Field
	for _, z := range t.Columns {
		f, err := convertField(ctx, pascal, z)
		if err != nil {
			return Table{}, err
		}
		cols = append(cols, f)
		if z.IsPrimary {
			pkCols = append(pkCols, f)
		}
	}
	return Table{
		PythonName:  pascal(singularize(t.Name)),
		SQLName:     t.Name,
		Fields:      cols,
		PrimaryKeys: pkCols,
		Manual:      t.Manual,
	}, nil
}

func convertIndex(ctx context.Context, t Table, i xo.Index) (Index, error) {
	var fields []Field
	for _, z := range i.Fields {
		f, err := convertField(ctx, pascal, z)
		if err != nil {
			return Index{}, err
		}
		fields = append(fields, f)
	}
	return Index{
		SQLName:   i.Name,
		Func:      pascal(i.Func),
		Table:     t,
		Fields:    fields,
		IsUnique:  i.IsUnique,
		IsPrimary: i.IsPrimary,
	}, nil
}

func convertFKey(ctx context.Context, t Table, fk xo.ForeignKey) (ForeignKey, error) {
	var fields, refFields []Field
	// convert fields
	for _, f := range fk.Fields {
		field, err := convertField(ctx, snake, f)
		if err != nil {
			return ForeignKey{}, err
		}
		fields = append(fields, field)
	}
	// convert ref fields
	for _, f := range fk.RefFields {
		refField, err := convertField(ctx, snake, f)
		if err != nil {
			return ForeignKey{}, err
		}
		refFields = append(refFields, refField)
	}
	return ForeignKey{
		PythonName: snake(fk.Func),
		SQLName:    fk.Name,
		Table:      t,
		Fields:     fields,
		RefTable:   pascal(singularize(fk.RefTable)),
		RefFields:  refFields,
		RefFunc:    snake(fk.RefFunc),
	}, nil
}

func convertField(ctx context.Context, tf transformFunc, f xo.Field) (Field, error) {
	typ, zero, err := pythonType(ctx, f.Type)
	if err != nil {
		return Field{}, err
	}
	return Field{
		Type:       typ,
		PythonName: tf(f.Name),
		SQLName:    f.Name,
		Zero:       zero,
		IsPrimary:  f.IsPrimary,
		IsSequence: f.IsSequence,
	}, nil
}

// emitQuery emits the query.
func emitQuery(ctx context.Context, query xo.Query, emit func(xo.Template)) error {
	var table Table
	// build type if needed
	if !query.Exec {
		var err error
		if table, err = buildQueryType(ctx, query); err != nil {
			return err
		}
	}
	// emit type definition
	if !query.Exec && !query.Flat && !Append(ctx) {
		emit(xo.Template{
			Partial:  "typedef",
			Dest:     strings.ToLower(table.PythonName) + ext,
			SortType: query.Type,
			SortName: query.Name,
			Data:     table,
		})
	}
	// build query params
	var params []QueryParam
	for _, param := range query.Params {
		params = append(params, QueryParam{
			Name:        param.Name,
			Type:        param.Type.Type,
			Interpolate: param.Interpolate,
			Join:        param.Join,
		})
	}
	// emit query
	emit(xo.Template{
		Partial:  "query",
		Dest:     strings.ToLower(table.PythonName) + ext,
		SortType: query.Type,
		SortName: query.Name,
		Data: Query{
			Name:        buildQueryName(query),
			Query:       query.Query,
			Comments:    query.Comments,
			Params:      params,
			One:         query.Exec || query.Flat || query.One,
			Flat:        query.Flat,
			Exec:        query.Exec,
			Interpolate: query.Interpolate,
			Type:        table,
			Comment:     query.Comment,
		},
	})
	return nil
}

func buildQueryType(ctx context.Context, query xo.Query) (Table, error) {
	tf := snake
	var fields []Field
	for _, z := range query.Fields {
		f, err := convertField(ctx, tf, z)
		if err != nil {
			return Table{}, err
		}
		// dont use convertField; the types are already provided by the user
		if query.ManualFields {
			f = Field{
				PythonName: z.Name,
				SQLName:    snake(z.Name),
				Type:       z.Type.Type,
			}
		}
		fields = append(fields, f)
	}
	sqlName := snake(query.Type)
	return Table{
		PythonName: query.Type,
		SQLName:    sqlName,
		Fields:     fields,
		Comment:    query.TypeComment,
	}, nil
}

// buildQueryName builds a name for the query.
func buildQueryName(query xo.Query) string {
	if query.Name != "" {
		return query.Name
	}
	// generate name if not specified
	name := query.Type
	if !query.One {
		name = inflector.Pluralize(name)
	}
	// add params
	if len(query.Params) == 0 {
		name = "get"
	} else {
		name += "by"
		for _, p := range query.Params {
			name += pascal(p.Name)
		}
	}
	return name
}

// emitSchema emits the xo schema for the template set.
func emitSchema(ctx context.Context, schema xo.Schema, emit func(xo.Template)) error {
	// emit enums
	for _, e := range schema.Enums {
		enum := convertEnum(e)
		emit(xo.Template{
			Partial:  "enum",
			Dest:     strings.ToLower(enum.PythonName) + ext,
			SortName: enum.PythonName,
			Data:     enum,
		})
	}
	// build procs
	overloadMap := make(map[string][]Proc)
	// procOrder ensures procs are always emitted in alphabetic order for
	// consistency in single mode
	var procOrder []string
	for _, p := range schema.Procs {
		var err error
		if procOrder, err = convertProc(ctx, overloadMap, procOrder, p); err != nil {
			return err
		}
	}
	// emit procs
	for _, name := range procOrder {
		procs := overloadMap[name]
		prefix := "sp_"
		if procs[0].Type == "function" {
			prefix = "sf_"
		}
		// Set flag to change name to their overloaded versions if needed.
		for i := range procs {
			procs[i].Overloaded = len(procs) > 1
		}
		emit(xo.Template{
			Dest:     prefix + strings.ToLower(name) + ext,
			Partial:  "procs",
			SortName: prefix + name,
			Data:     procs,
		})
	}
	// emit tables
	for _, t := range append(schema.Tables, schema.Views...) {
		table, err := convertTable(ctx, t)
		if err != nil {
			return err
		}
		emit(xo.Template{
			Dest:     strings.ToLower(table.PythonName) + ext,
			Partial:  "typedef",
			SortType: table.Type,
			SortName: table.PythonName,
			Data:     table,
		})
		// emit indexes
		for _, i := range t.Indexes {
			index, err := convertIndex(ctx, table, i)
			if err != nil {
				return err
			}
			emit(xo.Template{
				Dest:     strings.ToLower(table.PythonName) + ext,
				Partial:  "index",
				SortType: table.Type,
				SortName: index.SQLName,
				Data:     index,
			})
		}
		// emit fkeys
		for _, fk := range t.ForeignKeys {
			fkey, err := convertFKey(ctx, table, fk)
			if err != nil {
				return err
			}
			emit(xo.Template{
				Dest:     strings.ToLower(table.PythonName) + ext,
				Partial:  "foreignkey",
				SortType: table.Type,
				SortName: fkey.SQLName,
				Data:     fkey,
			})
		}
	}
	return nil
}

func (f *Funcs) sqlstr(typ string, indent string, v interface{}) string {
	var lines []string
	switch typ {
	case "insert_manual":
		lines = f.sqlstr_insert_manual(v)
	case "insert":
		lines = f.sqlstr_insert(v)
	case "update":
		lines = f.sqlstr_update(v)
	case "upsert":
		lines = f.sqlstr_upsert(v)
	case "delete":
		lines = f.sqlstr_delete(v)
	case "proc":
		lines = f.sqlstr_proc(v)
	case "index":
		lines = f.sqlstr_index(v)
	default:
		return fmt.Sprintf("sqlstr = `UNKNOWN QUERY TYPE: %s`", typ)
	}
	delim := "''' + \\\n" + indent + "'''"
	return fmt.Sprintf("sqlstr = \\\n%s'''%s'''", indent, strings.Join(lines, delim))
}

// sqlstr_insert_base builds an INSERT query
// If not all, sequence columns are skipped.
func (f *Funcs) sqlstr_insert_base(all bool, v interface{}) []string {
	switch x := v.(type) {
	case Table:
		// build names and values
		var n int
		var fields, vals []string
		for _, z := range x.Fields {
			if z.IsSequence && !all {
				continue
			}
			fields, vals = append(fields, f.colname(z)), append(vals, f.nth(n))
			n++
		}
		return []string{
			"INSERT INTO " + f.schemafn(x.SQLName) + " (",
			strings.Join(fields, ", "),
			") VALUES (",
			strings.Join(vals, ", "),
			")",
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 17: %T ]]", v)}
}

// sqlstr_insert_manual builds an INSERT query that inserts all fields.
func (f *Funcs) sqlstr_insert_manual(v interface{}) []string {
	return f.sqlstr_insert_base(true, v)
}

// sqlstr_insert builds an INSERT query, skipping the sequence field with
// applicable RETURNING clause for generated primary key fields.
func (f *Funcs) sqlstr_insert(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		var seq Field
		var count int
		for _, field := range x.Fields {
			if field.IsSequence {
				seq = field
			} else {
				count++
			}
		}
		lines := f.sqlstr_insert_base(false, v)
		// add return clause
		switch f.driver {
		case "oracle":
			switch f.oracleType {
			case "ora":
				lines[len(lines)-1] += ` RETURNING ` + f.colname(seq) + ` INTO ` + f.nth(count)
			case "godror":
				lines[len(lines)-1] += ` RETURNING ` + f.colname(seq) + ` /*LASTINSERTID*/ INTO :pk`
			default:
				return []string{fmt.Sprintf("[[ UNSUPPORTED ORACLE TYPE: %s]]", f.oracleType)}
			}
		case "postgres":
			lines[len(lines)-1] += ` RETURNING ` + f.colname(seq)
		case "sqlserver":
			lines[len(lines)-1] += "; SELECT ID = CONVERT(BIGINT, SCOPE_IDENTITY())"
		}
		return lines
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 18: %T ]]", v)}
}

// sqlstr_update_base builds an UPDATE query, using primary key fields as the WHERE
// clause, adding prefix.
//
// When prefix is empty, the WHERE clause will be in the form of name = $1.
// When prefix is non-empty, the WHERE clause will be in the form of name = <PREFIX>name.
//
// Similarly, when prefix is empty, the table's name is added after UPDATE,
// otherwise it is omitted.
func (f *Funcs) sqlstr_update_base(prefix string, v interface{}) (int, []string) {
	switch x := v.(type) {
	case Table:
		// build names and values
		var n int
		var list []string
		for _, z := range x.Fields {
			if z.IsPrimary {
				continue
			}
			name, param := f.colname(z), f.nth(n)
			if prefix != "" {
				param = prefix + name
			}
			list = append(list, fmt.Sprintf("%s = %s", name, param))
			n++
		}
		name := ""
		if prefix == "" {
			name = f.schemafn(x.SQLName) + " "
		}
		return n, []string{
			"UPDATE " + name + "SET ",
			strings.Join(list, ", ") + " ",
		}
	}
	return 0, []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 19: %T ]]", v)}
}

// sqlstr_update builds an UPDATE query, using primary key fields as the WHERE
// clause.
func (f *Funcs) sqlstr_update(v interface{}) []string {
	// build pkey vals
	switch x := v.(type) {
	case Table:
		var list []string
		n, lines := f.sqlstr_update_base("", v)
		for i, z := range x.PrimaryKeys {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(n+i)))
		}
		return append(lines, "WHERE "+strings.Join(list, " AND "))
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 20: %T ]]", v)}
}

func (f *Funcs) sqlstr_upsert(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		// build insert
		lines := f.sqlstr_insert_base(true, x)
		switch f.driver {
		case "postgres", "sqlite3":
			return append(lines, f.sqlstr_upsert_postgres_sqlite(x)...)
		case "mysql":
			return append(lines, f.sqlstr_upsert_mysql(x)...)
		case "sqlserver", "oracle":
			return f.sqlstr_upsert_sqlserver_oracle(x)
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 21 %s: %T ]]", f.driver, v)}
}

// sqlstr_upsert_postgres_sqlite builds an uspert query for postgres and sqlite
//
// INSERT (..) VALUES (..) ON CONFLICT DO UPDATE SET ...
func (f *Funcs) sqlstr_upsert_postgres_sqlite(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		// add conflict and update
		var conflicts []string
		for _, f := range x.PrimaryKeys {
			conflicts = append(conflicts, f.SQLName)
		}
		lines := []string{" ON CONFLICT (" + strings.Join(conflicts, ", ") + ") DO "}
		_, update := f.sqlstr_update_base("EXCLUDED.", v)
		return append(lines, update...)
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 22: %T ]]", v)}
}

// sqlstr_upsert_mysql builds an uspert query for mysql
//
// INSERT (..) VALUES (..) ON DUPLICATE KEY UPDATE SET ...
func (f *Funcs) sqlstr_upsert_mysql(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		lines := []string{" ON DUPLICATE KEY UPDATE "}
		var list []string
		i := len(x.Fields)
		for _, z := range x.Fields {
			if z.IsSequence {
				continue
			}
			name := f.colname(z)
			list = append(list, fmt.Sprintf("%s = VALUES(%s)", name, name))
			i++
		}
		return append(lines, strings.Join(list, ", "))
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 23: %T ]]", v)}
}

// sqlstr_upsert_sqlserver_oracle builds an upsert query for sqlserver
//
// MERGE [table] AS target USING (SELECT [pkeys]) AS source ...
func (f *Funcs) sqlstr_upsert_sqlserver_oracle(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		var lines []string
		// merge [table]...
		switch f.driver {
		case "sqlserver":
			lines = []string{"MERGE " + f.schemafn(x.SQLName) + " AS t "}
		case "oracle":
			lines = []string{"MERGE " + f.schemafn(x.SQLName) + "t "}
		}
		// using (select ..)
		var fields, predicate []string
		for i, field := range x.Fields {
			fields = append(fields, fmt.Sprintf("%s %s", f.nth(i), field.SQLName))
		}
		for _, field := range x.PrimaryKeys {
			predicate = append(predicate, fmt.Sprintf("s.%s = t.%s", field.SQLName, field.SQLName))
		}
		// closing part for select
		var closing string
		switch f.driver {
		case "sqlserver":
			closing = `) AS s `
		case "oracle":
			closing = `FROM DUAL ) s `
		}
		lines = append(lines, `USING (`,
			`SELECT `+strings.Join(fields, ", ")+" ",
			closing,
			`ON `+strings.Join(predicate, " AND ")+" ")
		// build param lists
		var updateParams, insertParams, insertVals []string
		for _, field := range x.Fields {
			// sequences are always managed by db
			if field.IsSequence {
				continue
			}
			// primary keys
			if !field.IsPrimary {
				updateParams = append(updateParams, fmt.Sprintf("t.%s = s.%s", field.SQLName, field.SQLName))
			}
			insertParams = append(insertParams, field.SQLName)
			insertVals = append(insertVals, "s."+field.SQLName)
		}
		// when matched then update...
		lines = append(lines,
			`WHEN MATCHED THEN `, `UPDATE SET `,
			strings.Join(updateParams, ", ")+" ",
			`WHEN NOT MATCHED THEN `,
			`INSERT (`,
			strings.Join(insertParams, ", "),
			`) VALUES (`,
			strings.Join(insertVals, ", "),
			`);`,
		)
		return lines
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 24: %T ]]", v)}
}

// sqlstr_delete builds a DELETE query for the primary keys.
func (f *Funcs) sqlstr_delete(v interface{}) []string {
	switch x := v.(type) {
	case Table:
		// names and values
		var list []string
		for i, z := range x.PrimaryKeys {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(i)))
		}
		return []string{
			"DELETE FROM " + f.schemafn(x.SQLName) + " ",
			"WHERE " + strings.Join(list, " AND "),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 25: %T ]]", v)}
}

// sqlstr_index builds a index fields.
func (f *Funcs) sqlstr_index(v interface{}) []string {
	switch x := v.(type) {
	case Index:
		// build table fieldnames
		var fields []string
		for _, z := range x.Table.Fields {
			fields = append(fields, f.colname(z))
		}
		// index fields
		var list []string
		for i, z := range x.Fields {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(i)))
		}
		return []string{
			"SELECT ",
			strings.Join(fields, ", ") + " ",
			"FROM " + f.schemafn(x.Table.SQLName) + " ",
			"WHERE " + strings.Join(list, " AND "),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 26: %T ]]", v)}
}

// sqlstr_proc builds a stored procedure call.
func (f *Funcs) sqlstr_proc(v interface{}) []string {
	switch x := v.(type) {
	case Proc:
		if x.Type == "function" {
			return f.sqlstr_func(v)
		}
		// sql string format
		var format string
		switch f.driver {
		case "postgres", "mysql":
			format = "CALL %s(%s)"
		case "sqlserver":
			format = "%[1]s"
		case "oracle":
			format = "BEGIN %s(%s); END;"
		}
		// build params list; add return fields for orcle
		l := x.Params
		if f.driver == "oracle" {
			l = append(l, x.Returns...)
		}
		var list []string
		for i, field := range l {
			s := f.nth(i)
			if f.driver == "oracle" {
				s = ":" + field.SQLName
			}
			list = append(list, s)
		}
		// dont prefix with schema for oracle
		name := f.schemafn(x.SQLName)
		if f.driver == "oracle" {
			name = x.SQLName
		}
		return []string{
			fmt.Sprintf(format, name, strings.Join(list, ", ")),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 27: %T ]]", v)}
}

func (f *Funcs) sqlstr_func(v interface{}) []string {
	switch x := v.(type) {
	case Proc:
		var format string
		switch f.driver {
		case "postgres":
			format = "SELECT * FROM %s(%s)"
		case "mysql":
			format = "SELECT %s(%s)"
		case "sqlserver":
			format = "SELECT %s(%s) AS OUT"
		case "oracle":
			format = "SELECT %s(%s) FROM dual"
		}
		var list []string
		l := x.Params
		for i := range l {
			list = append(list, f.nth(i))
		}
		return []string{
			fmt.Sprintf(format, f.schemafn(x.SQLName), strings.Join(list, ", ")),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 28: %T ]]", v)}
}

// convertTypes generates the conversions to convert the foreign key field
// types to their respective referenced field types.
func (f *Funcs) convertTypes(fkey ForeignKey) string {
	var p []string
	for i := range fkey.Fields {
		field := fkey.Fields[i]
		refField := fkey.RefFields[i]
		expr := f.short(fkey.Table) + "." + field.PythonName
		// types match, can match
		if field.Type == refField.Type {
			p = append(p, expr)
			continue
		}
		// convert types
		typ, refType := field.Type, refField.Type
		if strings.HasPrefix(typ, "Optional[") {
			typ = typ[9 : len(typ)-1]
			expr = expr + "." + typ
		}
		if strings.ToLower(refType) != typ {
			expr = refType + "(" + expr + ")"
		}
		p = append(p, expr)
	}
	return strings.Join(p, ", ")
}

// params converts a list of fields into their named Python parameters, skipping
// any Field with Name contained in ignore. addType will cause the Python Type to
// be added after each variable name. addPrefix will cause the returned string
// to be prefixed with ", " if the generated string is not empty.
//
// Any field name encountered will be checked against goReservedNames, and will
// have its name substituted by its corresponding looked up value.
//
// Used to present a comma separated list of Go variable names for use with as
// either a Go func parameter list, or in a call to another Go func.
// (ie, ", a, b, c, ..." or ", a T1, b T2, c T3, ...").
func (f *Funcs) params(fields []Field, addType bool) string {
	var vals []string
	for _, field := range fields {
		vals = append(vals, f.param(field, addType))
	}
	return strings.Join(vals, ", ")
}

func (f *Funcs) param(field Field, addType bool) string {
	n := strings.Split(snaker.CamelToSnake(field.PythonName), "_")
	s := strings.ToLower(n[0]) + field.PythonName[len(n[0]):]
	// check go reserved names
	if r, ok := pythonReservedNames[strings.ToLower(s)]; ok {
		s = r
	}
	// add the python type
	if addType {
		s += ": " + f.typefn(field.Type)
	}
	// add to vals
	return s
}

// zero generates a zero list.
func (f *Funcs) zero(z ...interface{}) string {
	var zeroes []string
	for i, v := range z {
		switch x := v.(type) {
		case string:
			zeroes = append(zeroes, x)
		case Table:
			for _, p := range x.Fields {
				zeroes = append(zeroes, f.zero(p))
			}
		case []Field:
			for _, p := range x {
				zeroes = append(zeroes, f.zero(p))
			}
		case Field:
			if _, ok := f.knownTypes[x.Type]; ok || x.Zero == "None" {
				zeroes = append(zeroes, x.Zero)
				break
			}
			zeroes = append(zeroes, "None")
		default:
			zeroes = append(zeroes, fmt.Sprintf("/* UNSUPPORTED TYPE 29 (%d): %T */", i, v))
		}
	}
	return strings.Join(zeroes, ", ")
}

// typefn generates the Python type, prefixing the custom package name if applicable.
func (f *Funcs) typefn(typ string) string {
	if strings.Contains(typ, ".") {
		return typ
	}
	var prefix, suffix string
	for strings.HasPrefix(typ, "list[") || strings.HasPrefix(typ, "Optional[") {
		parts := strings.SplitN(typ, "[", 2)
		typ = parts[1][:len(parts[1])-1]
		prefix += parts[0] + "["
		suffix += "]"
	}
	if _, ok := f.knownTypes[typ]; ok || f.custom == "" {
		return prefix + typ + suffix
	}
	return prefix + f.custom + "." + typ + suffix
}

// field generates a field definition for a struct.
func (f *Funcs) field(field Field) (string, error) {
	return fmt.Sprintf("%s: %s // %s", field.PythonName, f.typefn(field.Type), field.SQLName), nil
}

// short generates a safe Go identifier for typ. typ is first checked
// against shorts, and if not found, then the value is calculated and
// stored in the shorts for future use.
//
// A short is the concatenation of the lowercase of the first character in
// the words comprising the name. For example, "MyCustomName" will have have
// the short of "mcn".
//
// If a generated short conflicts with a Go reserved name or a name used in
// the templates, then the corresponding value in goReservedNames map will be
// used.
//
// Generated shorts that have conflicts with any scopeConflicts member will
// have nameConflictSuffix appended.
func (f *Funcs) short(v interface{}) string {
	var n string
	switch x := v.(type) {
	case string:
		n = x
	case Table:
		n = x.PythonName
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 30: %T ]]", v)
	}
	// check short name map
	name, ok := f.shorts[n]
	if !ok {
		// calc the short name
		var u []string
		for _, s := range strings.Split(strings.ToLower(snaker.CamelToSnake(n)), "_") {
			if len(s) > 0 && s != "id" {
				u = append(u, s[:1])
			}
		}
		// ensure no name conflict
		name = checkName(strings.Join(u, ""))
		// store back to short name map
		f.shorts[n] = name
	}
	// append suffix if conflict exists
	if _, ok := templateReservedNames[name]; ok {
		name += f.conflict
	}
	return name
}

// colname returns the ColumnName of a field escaped if needed.
func (f *Funcs) colname(z Field) string {
	if f.escColumn {
		return escfn(z.SQLName)
	}
	return z.SQLName
}

// EnumValue is a enum value template.
type EnumValue struct {
	PythonName string
	SQLName    string
	ConstValue int
}

// Enum is a enum type template.
type Enum struct {
	PythonName string
	SQLName    string
	Values     []EnumValue
	Comment    string
}

// Proc is a stored procedure template.
type Proc struct {
	Type           string
	PythonName     string
	OverloadedName string
	SQLName        string
	Signature      string
	Params         []Field
	Returns        []Field
	Void           bool
	Overloaded     bool
	Comment        string
}

// Table is a type (ie, table/view/custom query) template.
type Table struct {
	Type        string
	PythonName  string
	SQLName     string
	PrimaryKeys []Field
	Fields      []Field
	Manual      bool
	Comment     string
}

// ForeignKey is a foreign key template.
type ForeignKey struct {
	PythonName string
	SQLName    string
	Table      Table
	Fields     []Field
	RefTable   string
	RefFields  []Field
	RefFunc    string
	Comment    string
}

// Index is an index template.
type Index struct {
	SQLName   string
	Func      string
	Table     Table
	Fields    []Field
	IsUnique  bool
	IsPrimary bool
	Comment   string
}

// Field is a field template.
type Field struct {
	PythonName string
	SQLName    string
	Type       string
	Zero       string
	IsPrimary  bool
	IsSequence bool
	Comment    string
}

// QueryParam is a custom query parameter template.
type QueryParam struct {
	Name        string
	Type        string
	Interpolate bool
	Join        bool
}

// Query is a custom query template.
type Query struct {
	Name        string
	Query       []string
	Comments    []string
	Params      []QueryParam
	One         bool
	Flat        bool
	Exec        bool
	Interpolate bool
	Type        Table
	Comment     string
}

// PackageImport holds information about a Go package import.
type PackageImport struct {
	Alias string
	Pkg   string
}

// String satisfies the fmt.Stringer interface.
func (v PackageImport) String() string {
	if v.Alias != "" {
		return fmt.Sprintf("import %s as %s", v.Pkg, v.Alias)
	}
	return fmt.Sprintf("import %s", v.Pkg)
}

type transformFunc func(...string) string

func snake(names ...string) string {
	return snaker.CamelToSnake(strings.Join(names, "_"))
}

func pascal(names ...string) string {
	return snaker.ForceLowerCamelIdentifier(strings.Join(names, "_"))
}

func fileExport(names ...string) string {
	return snaker.ForceCamelIdentifier(strings.Join(names, "_"))
}

func overloadedName(sqlTypes []string, proc Proc) string {
	if len(proc.Params) == 0 {
		return proc.PythonName
	}
	var names []string
	// build parameters for proc.
	// if the proc's parameter has no name, use the types of the proc instead
	for i, f := range proc.Params {
		if f.SQLName == fmt.Sprintf("p%d", i) {
			names = append(names, snake(strings.Split(sqlTypes[i], " ")...))
			continue
		}
		names = append(names, snake(f.PythonName))
	}
	if len(names) == 1 {
		return fmt.Sprintf("%s_by_%s", proc.PythonName, names[0])
	}
	front, last := strings.Join(names[:len(names)-1], ""), names[len(names)-1]
	return fmt.Sprintf("%s_by_%s_and_%s", proc.PythonName, front, last)
}

func pythonType(ctx context.Context, typ xo.Type) (string, string, error) {
	driver, _, schema := xo.DriverDbSchema(ctx)
	var f func(xo.Type, string, string, string) (string, string, error)
	switch driver {
	case "mysql":
		return "", "", errors.New("mysql driver not supported for python")
	case "oracle":
		return "", "", errors.New("oracle driver not supported for python")
	case "postgres":
		return "", "", errors.New("postgres driver not supported for python")
	case "sqlite3":
		f = loader.Sqlite3PythonType
	case "sqlserver":
		return "", "", errors.New("sqlserver driver not supported for python")
	default:
		return "", "", fmt.Errorf("unknown driver %q", driver)
	}
	return f(typ, schema, "int", "int")
}

// addInitialisms adds snaker initialisms from the context.
func addInitialisms(ctx context.Context) error {
	z := ctx.Value(InitialismKey)
	y, _ := z.([]string)
	var v []string
	for _, s := range y {
		if s != "" {
			v = append(v, s)
		}
	}
	return snaker.DefaultInitialisms.Add(v...)
}

// contains determines if v contains s.
func contains(v []string, s string) bool {
	for _, z := range v {
		if z == s {
			return true
		}
	}
	return false
}

// singularize singularizes s.
func singularize(s string) string {
	if i := strings.LastIndex(s, "_"); i != -1 {
		return s[:i+1] + inflector.Singularize(s[i+1:])
	}
	return inflector.Singularize(s)
}

func checkName(name string) string {
	if n, ok := pythonReservedNames[name]; ok {
		return n
	}
	return name
}

// escfn escapes s.
func escfn(s string) string {
	return `"` + s + `"`
}

// eval evalutates a template s against v.
func eval(v interface{}, s string) (string, error) {
	tpl, err := template.New(fmt.Sprintf("[EVAL %q]", s)).Parse(s)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if err := tpl.Execute(buf, v); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// templateReservedNames are the template reserved names.
var templateReservedNames = map[string]bool{
	// variables
	"ctx":  true,
	"db":   true,
	"err":  true,
	"log":  true,
	"logf": true,
	"res":  true,
	"rows": true,

	// packages
	"context": true,
	"csv":     true,
	"driver":  true,
	"errors":  true,
	"fmt":     true,
	"hstore":  true,
	"regexp":  true,
	"sql":     true,
	"strings": true,
	"time":    true,
	"uuid":    true,
}

// pythonReservedNames is a map of of go reserved names to "safe" names.
var pythonReservedNames = map[string]string{
	"False":    "false",
	"None":     "none",
	"True":     "true",
	"and":      "and_",
	"as":       "as_",
	"assert":   "assrt",
	"break":    "brk",
	"class":    "cls",
	"continue": "cnt",
	"def":      "def_",
	"elif":     "elf",
	"else":     "els",
	"except":   "expt",
	"finally":  "fnl",
	"for":      "for_",
	"from":     "frm",
	"global":   "glb",
	"if":       "if_",
	"import":   "impt",
	"in":       "in_",
	"is":       "is_",
	"lambda":   "lbd",
	"nonlocal": "nlcl",
	"not":      "not_",
	"or":       "or_",
	"pass":     "pss",
	"raise":    "rse",
	"return":   "rtn",
	"try":      "try_",
	"while":    "whl",
	"with":     "wth",
	"yield":    "yld",
	// go types
	"int":        "i",
	"float":      "f",
	"complex":    "c",
	"list":       "l",
	"tuple":      "t",
	"range":      "r",
	"str":        "s",
	"bytes":      "b",
	"bytearray":  "ba",
	"memoryview": "m",
	"set":        "st",
	"frozenset":  "fs",
	"dict":       "d",
	"type":       "t",
	"bool":       "bl",
}
