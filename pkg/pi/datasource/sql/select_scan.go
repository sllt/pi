package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/datasource"
)

var (
	errSelectDataNotPointer = errors.New("data is not a pointer")
	errSelectUnsupported    = errors.New("unsupported select destination type")

	scannerType    = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType       = reflect.TypeOf(time.Time{})
	timePtrType    = reflect.TypeOf((*time.Time)(nil))
	timePtrPtrType = reflect.TypeOf((**time.Time)(nil))

	timestampLayouts = []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}

	structMetaCache sync.Map // map[reflect.Type]*structScanMeta

	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

type queryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

type structScanMeta struct {
	columns map[string][]int
}

type structFieldNode struct {
	typ  reflect.Type
	path []int
}

//nolint:exhaustive // We only support slice and struct destinations.
func selectData(ctx context.Context, logger datasource.Logger, queryContext queryFunc, data any, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Destination must be settable so callers can read scanned results.
	rvo := reflect.ValueOf(data)
	if !rvo.IsValid() || rvo.Kind() != reflect.Ptr || rvo.IsNil() {
		if logger != nil {
			logger.Error("we did not get a pointer. data is not settable.")
		}

		return errSelectDataNotPointer
	}

	rv := rvo.Elem()

	switch rv.Kind() {
	case reflect.Slice:
		return selectSlice(ctx, logger, queryContext, query, args, rvo, rv)
	case reflect.Struct:
		return selectStruct(ctx, logger, queryContext, query, args, rv)
	default:
		if logger != nil {
			logger.Debugf("a pointer to %v was not expected.", rv.Kind().String())
		}

		return fmt.Errorf("%w: %s", errSelectUnsupported, rv.Kind())
	}
}

func selectSlice(ctx context.Context, logger datasource.Logger, queryContext queryFunc, query string, args []any, rvo, rv reflect.Value) error {
	rows, err := queryContext(ctx, query, args...)
	if err != nil {
		if logger != nil {
			logger.Errorf("error running query: %v", err)
		}

		return err
	}

	defer rows.Close()

	elemType := rv.Type().Elem()
	isPtrElem := elemType.Kind() == reflect.Ptr
	isStructRow := false
	var structType reflect.Type

	if !isLeafScanType(elemType) {
		if elemType.Kind() == reflect.Struct {
			isStructRow = true
			structType = elemType
		} else if isPtrElem && elemType.Elem().Kind() == reflect.Struct {
			isStructRow = true
			structType = elemType.Elem()
		}
	}

	rv = reflect.MakeSlice(rv.Type(), 0, 0)

	var colPaths [][]int
	for rows.Next() {
		if isStructRow {
			if colPaths == nil {
				var err error
				colPaths, err = resolveColumnPaths(rows, structType)
				if err != nil {
					return err
				}
			}

			val := reflect.New(structType)
			if err := scanRowToStruct(rows, val.Elem(), colPaths); err != nil {
				return err
			}

			if isPtrElem {
				rv = reflect.Append(rv, val)
			} else {
				rv = reflect.Append(rv, val.Elem())
			}
			continue
		}

		val := reflect.New(elemType)
		if err := rows.Scan(wrapScanTarget(val)); err != nil {
			return err
		}
		rv = reflect.Append(rv, val.Elem())
	}

	if err := rows.Err(); err != nil {
		if logger != nil {
			logger.Errorf("error parsing rows : %v", err)
		}

		return err
	}

	if rvo.Elem().CanSet() {
		rvo.Elem().Set(rv)
	}

	return nil
}

func selectStruct(ctx context.Context, logger datasource.Logger, queryContext queryFunc, query string, args []any, rv reflect.Value) error {
	rows, err := queryContext(ctx, query, args...)
	if err != nil {
		if logger != nil {
			logger.Errorf("error running query: %v", err)
		}

		return err
	}

	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			if logger != nil {
				logger.Errorf("error parsing rows : %v", err)
			}

			return err
		}

		return sql.ErrNoRows
	}

	if isLeafScanType(rv.Type()) {
		if err := rows.Scan(wrapScanTarget(rv.Addr())); err != nil {
			return err
		}
	} else {
		colPaths, err := resolveColumnPaths(rows, rv.Type())
		if err != nil {
			return err
		}
		if err := scanRowToStruct(rows, rv, colPaths); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		if logger != nil {
			logger.Errorf("error parsing rows : %v", err)
		}

		return err
	}

	return nil
}

func resolveColumnPaths(rows *sql.Rows, structType reflect.Type) ([][]int, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	meta := getStructScanMeta(structType)
	colPaths := make([][]int, len(columns))
	for i, col := range columns {
		colPaths[i] = meta.columns[col]
	}

	return colPaths, nil
}

func scanRowToStruct(rows *sql.Rows, v reflect.Value, colPaths [][]int) error {
	dest := make([]any, len(colPaths))
	for i, path := range colPaths {
		if path != nil {
			dest[i] = fieldPtrByPath(v, path)
		} else {
			var discard any
			dest[i] = &discard
		}
	}

	return rows.Scan(dest...)
}

func isLeafScanType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	if t.Implements(scannerType) {
		return true
	}
	if t.Kind() != reflect.Ptr && reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t == timeType
}

func getStructScanMeta(t reflect.Type) *structScanMeta {
	if cached, ok := structMetaCache.Load(t); ok {
		return cached.(*structScanMeta)
	}

	columns := make(map[string][]int)
	visited := make(map[reflect.Type]bool)
	queue := []structFieldNode{{typ: t}}

	// BFS ensures outer fields shadow embedded struct fields with the same column name.
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if visited[node.typ] {
			continue
		}
		visited[node.typ] = true

		for i := 0; i < node.typ.NumField(); i++ {
			f := node.typ.Field(i)
			if !f.IsExported() {
				// Allow unexported anonymous embedded value structs (e.g. `type baseModel struct{ ID int64 }`),
				// since Go reflect clears flagEmbedRO when stepping into their exported fields.
				// Unexported embedded pointer structs must be skipped because nil allocation via Set panics.
				if !(f.Anonymous && f.Type.Kind() == reflect.Struct && !isLeafScanType(f.Type)) {
					continue
				}
			}

			colName, _, _ := strings.Cut(f.Tag.Get("db"), ",")
			colName = strings.TrimSpace(colName)
			if colName == "-" {
				continue
			}

			path := make([]int, len(node.path)+1)
			copy(path, node.path)
			path[len(node.path)] = i

			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if f.Anonymous && colName == "" && ft.Kind() == reflect.Struct && !isLeafScanType(f.Type) {
				queue = append(queue, structFieldNode{typ: ft, path: path})
				continue
			}
			if !f.IsExported() {
				continue
			}

			if colName == "" {
				colName = ToSnakeCase(f.Name)
			}
			if _, exists := columns[colName]; !exists {
				columns[colName] = path
			}
		}
	}

	meta := &structScanMeta{columns: columns}
	actual, _ := structMetaCache.LoadOrStore(t, meta)

	return actual.(*structScanMeta)
}

func fieldPtrByPath(v reflect.Value, path []int) any {
	curr := v
	for i, idx := range path {
		if curr.Kind() == reflect.Ptr {
			if curr.IsNil() {
				curr.Set(reflect.New(curr.Type().Elem()))
			}
			curr = curr.Elem()
		}
		curr = curr.Field(idx)
		if i < len(path)-1 && curr.Kind() == reflect.Ptr && curr.IsNil() {
			curr.Set(reflect.New(curr.Type().Elem()))
		}
	}

	return wrapScanTarget(curr.Addr())
}

type timeValueScanner struct {
	dst *time.Time
}

func (s timeValueScanner) Scan(src any) error {
	t, ok, err := scanTimeSource(src)
	if err != nil {
		return err
	}
	if !ok {
		*s.dst = time.Time{}
		return nil
	}
	*s.dst = t
	return nil
}

type timePtrScanner struct {
	dst **time.Time
}

func (s timePtrScanner) Scan(src any) error {
	t, ok, err := scanTimeSource(src)
	if err != nil {
		return err
	}
	if !ok {
		*s.dst = nil
		return nil
	}
	*s.dst = &t
	return nil
}

func wrapScanTarget(ptrVal reflect.Value) any {
	switch ptrVal.Type() {
	case timePtrType:
		return timeValueScanner{dst: ptrVal.Interface().(*time.Time)}
	case timePtrPtrType:
		return timePtrScanner{dst: ptrVal.Interface().(**time.Time)}
	default:
		return ptrVal.Interface()
	}
}

func scanTimeSource(src any) (time.Time, bool, error) {
	switch v := src.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return v, true, nil
	case string:
		return parseTimestampString(v)
	case []byte:
		return parseTimestampString(string(v))
	default:
		return time.Time{}, false, fmt.Errorf("sql: unsupported Scan, storing driver.Value type %T into time.Time", src)
	}
}

func parseTimestampString(raw string) (time.Time, bool, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, false, nil
	}
	if idx := strings.Index(s, " m="); idx >= 0 {
		s = strings.TrimSpace(s[:idx])
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("sql: cannot parse timestamp %q into time.Time", raw)
}

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}
