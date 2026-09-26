package qb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

var (
	errInvalidAggregateBuilder = errors.New(`[builder] aggregate builder must implement Symbol() or Symble()`)
	errInvalidJSONPathType     = errors.New(`[builder] json path must be string`)
	errInvalidJSONPathValue    = errors.New(`[builder] json path cannot be empty`)
	errUnsupportedJSONType     = errors.New(`[builder] json value contains unsupported type`)
	errUnsupportedJSONMapKey   = errors.New(`[builder] json object map key must be string`)
)

// AggregateQuery is a helper function to execute the aggregate query and return the result
func AggregateQuery(ctx context.Context, db *sql.DB, table string, where map[string]interface{}, aggregate interface{}) (ResultResolver, error) {
	return defaultBuilder.AggregateQuery(ctx, db, table, where, aggregate)
}

// AggregateQueryWithDialect executes aggregate query with the provided dialect.
func AggregateQueryWithDialect(ctx context.Context, db *sql.DB, dialect, table string, where map[string]interface{}, aggregate interface{}) (ResultResolver, error) {
	b, err := New(dialect)
	if err != nil {
		return resultResolve{0}, err
	}

	return b.AggregateQuery(ctx, db, table, where, aggregate)
}

// AggregateQuery is a helper function to execute the aggregate query and return the result.
func (b Builder) AggregateQuery(ctx context.Context, db *sql.DB, table string, where map[string]interface{}, aggregate interface{}) (ResultResolver, error) {
	symbol, err := resolveAggregateSymbol(aggregate)
	if err != nil {
		return resultResolve{0}, err
	}

	cond, vals, err := b.BuildSelect(table, where, []string{symbol})
	if nil != err {
		return resultResolve{0}, err
	}
	rows, err := db.QueryContext(ctx, cond, vals...)
	if nil != err {
		return resultResolve{0}, err
	}
	defer rows.Close()

	var result interface{}
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return resultResolve{0}, err
		}
	}
	if err = rows.Err(); err != nil {
		return resultResolve{0}, err
	}
	return resultResolve{result}, nil
}

// ResultResolver is a helper for retrieving data
// caller should know the type and call the responding method
type ResultResolver interface {
	Int64() int64
	Float64() float64
}

type resultResolve struct {
	data interface{}
}

func (r resultResolve) Int64() int64 {
	switch t := r.data.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	case []uint8:
		i64, err := strconv.ParseInt(string(t), 10, 64)
		if nil != err {
			return int64(r.Float64())
		}
		return i64
	default:
		return 0
	}
}

// from go-mysql-driver/mysql the value returned could be int64 float64 float32

func (r resultResolve) Float64() float64 {
	switch t := r.data.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case []uint8:
		f64, _ := strconv.ParseFloat(string(t), 64)
		return f64
	default:
		return float64(r.Int64())
	}
}

// AggregateSymbolBuilder needs to be implemented so executor can
// get what should be put into `select Symbol() from xxx where yyy`.
type AggregateSymbolBuilder interface {
	Symbol() string
}

// AggregateSymbleBuilder is deprecated, use AggregateSymbolBuilder instead.
type AggregateSymbleBuilder interface {
	Symble() string
}

type agBuilder string

func (a agBuilder) Symbol() string {
	return string(a)
}

// Symble is kept for backward compatibility.
func (a agBuilder) Symble() string {
	return a.Symbol()
}

// AggregateCount count(col)
func AggregateCount(col string) agBuilder {
	return agBuilder("count(" + col + ")")
}

// AggregateSum sum(col)
func AggregateSum(col string) agBuilder {
	return agBuilder("sum(" + col + ")")
}

// AggregateAvg avg(col)
func AggregateAvg(col string) agBuilder {
	return agBuilder("avg(" + col + ")")
}

// AggregateMax max(col)
func AggregateMax(col string) agBuilder {
	return agBuilder("max(" + col + ")")
}

// AggregateMin min(col)
func AggregateMin(col string) agBuilder {
	return agBuilder("min(" + col + ")")
}

func resolveAggregateSymbol(aggregate interface{}) (string, error) {
	switch v := aggregate.(type) {
	case AggregateSymbolBuilder:
		symbol := strings.TrimSpace(v.Symbol())
		if symbol == "" {
			return "", errInvalidAggregateBuilder
		}
		return symbol, nil
	case AggregateSymbleBuilder:
		symbol := strings.TrimSpace(v.Symble())
		if symbol == "" {
			return "", errInvalidAggregateBuilder
		}
		return symbol, nil
	default:
		return "", errInvalidAggregateBuilder
	}
}

// OmitEmpty returns a copied map with zero-value keys removed.
func OmitEmpty(where map[string]interface{}, omitKey []string) map[string]interface{} {
	if where == nil {
		return nil
	}
	copied := copyWhere(where)
	for _, key := range omitKey {
		v, ok := copied[key]
		if !ok {
			continue
		}

		if isZero(reflect.ValueOf(v)) {
			delete(copied, key)
		}
	}
	return copied
}

type IsZeroer interface {
	IsZero() bool
}

var IsZeroType = reflect.TypeOf((*IsZeroer)(nil)).Elem()

// isZero reports whether a value is a zero value
// Including support: Bool, Array, String, Float32, Float64, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr
// Map, Slice, Interface, Struct
func isZero(v reflect.Value) bool {
	if v.IsValid() && v.Type().Implements(IsZeroType) {
		return v.Interface().(IsZeroer).IsZero()
	}
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Array, reflect.String:
		return v.Len() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Map, reflect.Slice:
		return v.IsNil() || v.Len() == 0
	case reflect.Interface:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}

	if v.Kind() != reflect.Struct {
		return false
	}

	// Traverse the Struct and only return true
	// if all of its fields return IsZero == true
	n := v.NumField()
	for i := 0; i < n; i++ {
		vf := v.Field(i)
		if !isZero(vf) {
			return false
		}
	}
	return true
}

type rawSql struct {
	sqlCond string
	values  []interface{}
}

func (r rawSql) Build() ([]string, []interface{}) {
	return []string{r.sqlCond}, r.values
}

func Custom(query string, args ...interface{}) Comparable {
	return rawSql{sqlCond: query, values: args}
}

// JsonContains checks whether target JSON contains all items in jsonLike.
//
// MySQL only.
//
// notice: fullJsonPath should hard code, never from user input.
func JsonContains(fullJsonPath string, jsonLike interface{}) Comparable {
	// MEMBER OF cant not deal null in json array
	if jsonLike == nil {
		return rawSql{
			sqlCond: "JSON_CONTAINS(" + fullJsonPath + ",'null')",
			values:  nil,
		}
	}

	s, v, err := genJsonObj(jsonLike)
	if err != nil {
		return errorComparable{err: err}
	}
	// jsonLike is number, string, bool
	_, ok := jsonLike.(string) // this check avoid eg jsonLike "JSONa"
	if ok || !strings.HasPrefix(s, "JSON") {
		return rawSql{
			sqlCond: "(" + s + " MEMBER OF(" + fullJsonPath + "))",
			values:  v,
		}
	}
	// jsonLike is array or map
	return rawSql{
		sqlCond: "JSON_CONTAINS(" + fullJsonPath + "," + s + ")",
		values:  v,
	}
}

// JsonSet aims to set/update json field values.
//
// MySQL only.
//
// notice: jsonPath should hard code, never from user input;
//
// usage update := map[string]interface{}{"_custom_xxx": builder.JsonSet(field, "$.code", 1, "$.user_info", map[string]any{"name": "", "age": 18})}
func JsonSet(field string, pathAndValuePair ...interface{}) Comparable {
	return jsonUpdateCall("JSON_SET", field, pathAndValuePair...)
}

// JsonArrayAppend generates JSON object args and calls JSON_ARRAY_APPEND.
//
// MySQL only.
//
// usage update := map[string]interface{}{"_custom_xxx": builder.JsonArrayAppend(field, "$", 1, "$[last]", []string{"2","3"}}
func JsonArrayAppend(field string, pathAndValuePair ...interface{}) Comparable {
	return jsonUpdateCall("JSON_ARRAY_APPEND", field, pathAndValuePair...)
}

// JsonArrayInsert generates JSON object args and calls JSON_ARRAY_INSERT to insert at index.
//
// MySQL only.
//
// usage update := map[string]interface{}{"_custom_xxx": builder.JsonArrayInsert(field, "$[0]", 1, "$[0]", []string{"2","3"}}
func JsonArrayInsert(field string, pathAndValuePair ...interface{}) Comparable {
	return jsonUpdateCall("JSON_ARRAY_INSERT", field, pathAndValuePair...)
}

// JsonRemove call MySQL JSON_REMOVE function; remove element from Array or Map
// path removed in order, prev remove affect the later operation, maybe the array shrink
//
// MySQL only.
//
// remove last array element; update := map[string]interface{}{"_custom_xxx":builder.JsonRemove(field,'$.list[last]')}
// remove element; update := map[string]interface{}{"_custom_xxx":builder.JsonRemove(field,'$.key0')}
func JsonRemove(field string, path ...string) Comparable {
	if len(path) == 0 {
		// do nothing, update xxx set a=a;
		return rawSql{
			sqlCond: field + "=" + field,
			values:  nil,
		}
	}

	vals := make([]interface{}, 0, len(path))
	placeholders := make([]string, 0, len(path))
	for _, p := range path {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			return errorComparable{err: errInvalidJSONPathValue}
		}
		placeholders = append(placeholders, "?")
		vals = append(vals, trimmed)
	}

	return rawSql{
		sqlCond: field + "=JSON_REMOVE(" + field + "," + strings.Join(placeholders, ",") + ")",
		values:  vals,
	}
}

// jsonUpdateCall build args then call fn
func jsonUpdateCall(fn string, field string, pathAndValuePair ...interface{}) Comparable {
	if len(pathAndValuePair) == 0 || len(pathAndValuePair)%2 != 0 {
		return rawSql{sqlCond: field, values: nil}
	}
	val := make([]interface{}, 0, len(pathAndValuePair)/2)
	var buf strings.Builder
	buf.WriteString(field)
	buf.WriteByte('=')
	buf.WriteString(fn + "(")
	buf.WriteString(field)
	for i := 0; i < len(pathAndValuePair); i += 2 {
		path, ok := pathAndValuePair[i].(string)
		if !ok {
			return errorComparable{err: errInvalidJSONPathType}
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return errorComparable{err: errInvalidJSONPathValue}
		}
		buf.WriteString(",?,")
		val = append(val, path)

		jsonSql, jsonVals, err := genJsonObj(pathAndValuePair[i+1])
		if err != nil {
			return errorComparable{err: err}
		}
		buf.WriteString(jsonSql)
		val = append(val, jsonVals...)
	}
	buf.WriteByte(')')

	return rawSql{
		sqlCond: buf.String(),
		values:  val,
	}
}

type errorComparable struct {
	err error
}

func (e errorComparable) Build() ([]string, []interface{}) {
	return nil, nil
}

func (e errorComparable) buildError() error {
	return e.err
}

// genJsonObj build MySQL JSON object using JSON_ARRAY, JSON_OBJECT or ?; return sql string and args.
func genJsonObj(obj interface{}) (string, []interface{}, error) {
	if obj == nil {
		return "null", nil, nil
	}
	rValue := reflect.ValueOf(obj)
	if !rValue.IsValid() {
		return "null", nil, nil
	}
	for rValue.Kind() == reflect.Ptr {
		if rValue.IsNil() {
			return "null", nil, nil
		}
		rValue = rValue.Elem()
	}
	rType := rValue.Kind()
	var s []string
	var vals []interface{}
	switch rType {
	case reflect.Array, reflect.Slice:
		s = append(s, "JSON_ARRAY(")
		length := rValue.Len()
		for i := 0; i < length; i++ {
			subS, subVals, err := genJsonObj(rValue.Index(i).Interface())
			if err != nil {
				return "", nil, err
			}
			s = append(s, subS, ",")
			vals = append(vals, subVals...)
		}

		if s[len(s)-1] == "," {
			s[len(s)-1] = ")"
		} else { // empty slice
			s = append(s, ")")
		}
	case reflect.Map:
		if rValue.Type().Key().Kind() != reflect.String {
			return "", nil, errUnsupportedJSONMapKey
		}
		s = append(s, "JSON_OBJECT(")
		// sort keys in map to keep generate result same.
		keys := rValue.MapKeys()
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})
		length := rValue.Len()
		for i := 0; i < length; i++ {
			k := keys[i]
			v := rValue.MapIndex(k)
			subS, subVals, err := genJsonObj(v.Interface())
			if err != nil {
				return "", nil, err
			}
			s = append(s, "?,", subS, ",")
			vals = append(vals, k.String())
			vals = append(vals, subVals...)
		}

		if s[len(s)-1] == "," {
			s[len(s)-1] = ")"
		} else { // empty map
			s = append(s, ")")
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return "?", []interface{}{rValue.Interface()}, nil
	case reflect.Bool:
		if rValue.Bool() {
			return "true", nil, nil
		}
		return "false", nil, nil
	default:
		return "", nil, fmt.Errorf("%w: %s", errUnsupportedJSONType, rType.String())
	}
	return strings.Join(s, ""), vals, nil
}
