package qb

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	errInsertDataNotMatch = errors.New("insert data not match")
	errInsertNullData     = errors.New("insert null data")
	errOrderByParam       = errors.New("order param only should be ASC or DESC")
)

// the order of a map is unpredicatable so we need a sort algorithm to sort the fields
// and make it predicatable
var (
	defaultSortAlgorithm = sort.Strings
)

// Comparable requires type implements the Build method
type Comparable interface {
	Build() ([]string, []interface{})
}

type buildErrComparable interface {
	buildError() error
}

func comparableBuildErr(comp Comparable) error {
	withErr, ok := comp.(buildErrComparable)
	if !ok {
		return nil
	}
	return withErr.buildError()
}

// NullType is the NULL type in mysql
type NullType byte

func (nt NullType) String() string {
	if nt == IsNull {
		return "IS NULL"
	}
	return "IS NOT NULL"
}

const (
	_ NullType = iota
	// IsNull the same as `is null`
	IsNull
	// IsNotNull the same as `is not null`
	IsNotNull
)

type nullComparable map[string]interface{}

func (n nullComparable) Build() ([]string, []interface{}) {
	length := len(n)
	if nil == n || 0 == length {
		return nil, nil
	}
	sortedKey := make([]string, 0, length)
	cond := make([]string, 0, length)
	for k := range n {
		sortedKey = append(sortedKey, k)
	}
	defaultSortAlgorithm(sortedKey)
	for _, field := range sortedKey {
		v, ok := n[field]
		if !ok {
			continue
		}
		rv, ok := v.(NullType)
		if !ok {
			continue
		}
		cond = append(cond, field+" "+rv.String())
	}
	return cond, nil
}

type nilComparable byte

func (n nilComparable) Build() ([]string, []interface{}) {
	return nil, nil
}

// Like means like
type Like map[string]interface{}

// Build implements the Comparable interface
func (l Like) Build() ([]string, []interface{}) {
	if nil == l || 0 == len(l) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range l {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := l[cond[j]]
		cond[j] = cond[j] + " LIKE ?"
		vals = append(vals, val)
	}
	return cond, vals
}

type NotLike map[string]interface{}

// Build implements the Comparable interface
func (l NotLike) Build() ([]string, []interface{}) {
	if nil == l || 0 == len(l) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range l {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := l[cond[j]]
		cond[j] = cond[j] + " NOT LIKE ?"
		vals = append(vals, val)
	}
	return cond, vals
}

// Eq means equal(=)
type Eq map[string]interface{}

// Build implements the Comparable interface
func (e Eq) Build() ([]string, []interface{}) {
	return build(e, "=")
}

// Ne means Not Equal(!=)
type Ne map[string]interface{}

// Build implements the Comparable interface
func (n Ne) Build() ([]string, []interface{}) {
	return build(n, "!=")
}

// Lt means less than(<)
type Lt map[string]interface{}

// Build implements the Comparable interface
func (l Lt) Build() ([]string, []interface{}) {
	return build(l, "<")
}

// Lte means less than or equal(<=)
type Lte map[string]interface{}

// Build implements the Comparable interface
func (l Lte) Build() ([]string, []interface{}) {
	return build(l, "<=")
}

// Gt means greater than(>)
type Gt map[string]interface{}

// Build implements the Comparable interface
func (g Gt) Build() ([]string, []interface{}) {
	return build(g, ">")
}

// Gte means greater than or equal(>=)
type Gte map[string]interface{}

// Build implements the Comparable interface
func (g Gte) Build() ([]string, []interface{}) {
	return build(g, ">=")
}

// In means in
type In map[string][]interface{}

// Build implements the Comparable interface
func (i In) Build() ([]string, []interface{}) {
	if nil == i || 0 == len(i) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range i {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := i[cond[j]]
		cond[j] = buildIn(cond[j], val)
		vals = append(vals, val...)
	}
	return cond, vals
}

func buildIn(field string, vals []interface{}) (cond string) {
	cond = strings.TrimRight(strings.Repeat("?,", len(vals)), ",")
	cond = fmt.Sprintf("%s IN (%s)", quoteField(field), cond)
	return
}

// NotIn means not in
type NotIn map[string][]interface{}

// Build implements the Comparable interface
func (i NotIn) Build() ([]string, []interface{}) {
	if nil == i || 0 == len(i) {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range i {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := i[cond[j]]
		cond[j] = buildNotIn(cond[j], val)
		vals = append(vals, val...)
	}
	return cond, vals
}

func buildNotIn(field string, vals []interface{}) (cond string) {
	cond = strings.TrimRight(strings.Repeat("?,", len(vals)), ",")
	cond = fmt.Sprintf("%s NOT IN (%s)", quoteField(field), cond)
	return
}

type Between map[string][]interface{}

func (bt Between) Build() ([]string, []interface{}) {
	return betweenBuilder(bt, false)
}

func betweenBuilder(bt map[string][]interface{}, notBetween bool) ([]string, []interface{}) {
	if len(bt) == 0 {
		return nil, nil
	}
	var cond []string
	var vals []interface{}
	for k := range bt {
		cond = append(cond, k)
	}
	defaultSortAlgorithm(cond)
	for j := 0; j < len(cond); j++ {
		val := bt[cond[j]]
		cond_j, err := buildBetween(notBetween, cond[j], val)
		if nil != err {
			// Fail closed to avoid silently widening queries when BETWEEN args are invalid.
			cond[j] = "(1=0)"
			continue
		}
		cond[j] = cond_j
		vals = append(vals, val...)
	}
	return cond, vals
}

type NotBetween map[string][]interface{}

func (nbt NotBetween) Build() ([]string, []interface{}) {
	return betweenBuilder(nbt, true)
}

func buildBetween(notBetween bool, key string, vals []interface{}) (string, error) {
	if len(vals) != 2 {
		return "", errors.New("vals of between must be a slice with two elements")
	}
	var operator string
	if notBetween {
		operator = "NOT BETWEEN"
	} else {
		operator = "BETWEEN"
	}
	return fmt.Sprintf("(%s %s ? AND ?)", key, operator), nil
}

type NestWhere []Comparable

func (nw NestWhere) Build() ([]string, []interface{}) {
	var cond []string
	var vals []interface{}
	nestWhereString, nestWhereVals := whereConnector("AND", nw...)
	cond = append(cond, nestWhereString)
	vals = nestWhereVals
	return cond, vals
}

type OrWhere []Comparable

func (ow OrWhere) Build() ([]string, []interface{}) {
	var cond []string
	var vals []interface{}
	orWhereString, orWhereVals := whereConnector("OR", ow...)
	cond = append(cond, orWhereString)
	vals = orWhereVals
	return cond, vals
}

func build(m map[string]interface{}, op string) ([]string, []interface{}) {
	if nil == m || 0 == len(m) {
		return nil, nil
	}
	length := len(m)
	cond := make([]string, length)
	vals := make([]interface{}, 0, length)
	var i int
	for key := range m {
		cond[i] = key
		i++
	}
	defaultSortAlgorithm(cond)
	for i = 0; i < length; i++ {
		v := m[cond[i]]
		if raw, ok := v.(Raw); ok {
			cond[i] += op + string(raw)
			continue
		}
		vals = append(vals, v)
		cond[i] = assembleExpression(cond[i], op)
	}
	return cond, vals
}

func assembleExpression(field, op string) string {
	return quoteField(field) + op + "?"
}

func resolveFields(m map[string]interface{}) []string {
	var fields []string
	for k := range m {
		fields = append(fields, quoteField(k))
	}
	defaultSortAlgorithm(fields)
	return fields
}

func whereConnector(andOr string, conditions ...Comparable) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", nil
	}
	var where []string
	var values []interface{}
	for _, cond := range conditions {
		cons, vals := cond.Build()
		if nil == cons {
			continue
		}
		where = append(where, cons...)
		values = append(values, vals...)
	}
	if 0 == len(where) {
		return "", nil
	}
	whereString := "(" + strings.Join(where, " "+andOr+" ") + ")"
	return whereString, values
}

// deprecated
func quoteField(field string) string {
	return field
}

type insertType string

const (
	commonInsert  insertType = "INSERT INTO"
	ignoreInsert  insertType = "INSERT IGNORE INTO"
	replaceInsert insertType = "REPLACE INTO"
)

func (b Builder) buildInsert(table string, setMap []map[string]interface{}, insertType insertType) (string, []interface{}, error) {
	query, vals, err := b.buildInsertRaw(table, setMap, insertType)
	if err != nil {
		return "", nil, err
	}

	return b.finalizeQuery(query, vals)
}

func (b Builder) buildInsertRaw(table string, setMap []map[string]interface{}, insertType insertType) (string, []interface{}, error) {
	var fields []string
	var vals []interface{}
	if len(setMap) < 1 {
		return "", nil, errInsertNullData
	}

	command := "INSERT INTO"
	suffix := ""

	switch insertType {
	case commonInsert:
		// INSERT INTO
	case ignoreInsert:
		switch b.dialect {
		case DialectMySQL:
			command = "INSERT IGNORE INTO"
		case DialectPostgres, DialectSQLite:
			suffix = " ON CONFLICT DO NOTHING"
		default:
			return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
		}
	case replaceInsert:
		switch b.dialect {
		case DialectMySQL, DialectSQLite:
			command = "REPLACE INTO"
		case DialectPostgres:
			return "", nil, b.unsupportedFeature("BuildReplaceInsert")
		default:
			return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
		}
	default:
		return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
	}

	fields = resolveFields(setMap[0])
	placeholder := "(" + strings.TrimRight(strings.Repeat("?,", len(fields)), ",") + ")"
	var sets []string
	for _, mapItem := range setMap {
		sets = append(sets, placeholder)
		for _, field := range fields {
			val, ok := mapItem[field]
			if !ok {
				return "", nil, errInsertDataNotMatch
			}
			vals = append(vals, val)
		}
	}

	query := fmt.Sprintf("%s %s (%s) VALUES %s%s", command, quoteField(table), strings.Join(fields, ","), strings.Join(sets, ","), suffix)

	return query, vals, nil
}

func (b Builder) buildInsertOnDuplicate(table string, data []map[string]interface{}, update map[string]interface{}) (string, []interface{}, error) {
	if b.dialect != DialectMySQL {
		return "", nil, b.unsupportedFeature("BuildInsertOnDuplicate")
	}

	insertCond, insertVals, err := b.buildInsertRaw(table, data, commonInsert)
	if err != nil {
		return "", nil, err
	}
	sets, updateVals, err := resolveUpdate(update)
	if err != nil {
		return "", nil, err
	}
	format := "%s ON DUPLICATE KEY UPDATE %s"
	cond := fmt.Sprintf(format, insertCond, sets)
	vals := make([]interface{}, 0, len(insertVals)+len(updateVals))
	vals = append(vals, insertVals...)
	vals = append(vals, updateVals...)

	return b.finalizeQuery(cond, vals)
}

func resolveUpdate(update map[string]interface{}) (sets string, vals []interface{}, err error) {
	keys := make([]string, 0, len(update))
	for key := range update {
		keys = append(keys, key)
	}
	defaultSortAlgorithm(keys)
	var sb strings.Builder
	for _, k := range keys {
		v := update[k]
		if _, ok := v.(Raw); ok {
			sb.WriteString(fmt.Sprintf("%s=%s,", k, v))
			continue
		}
		if strings.HasPrefix(k, "_custom_") {
			if custom, ok := v.(Comparable); ok {
				if err = comparableBuildErr(custom); err != nil {
					return "", nil, err
				}
				sql, val := custom.Build()
				for _, s := range sql {
					sb.WriteString(s)
					sb.WriteByte(',')
				}
				vals = append(vals, val...)
			}
			continue
		}
		vals = append(vals, v)
		sb.WriteString(fmt.Sprintf("%s=?,", quoteField(k)))
	}
	sets = strings.TrimRight(sb.String(), ",")
	return sets, vals, nil
}

func (b Builder) buildUpdate(table string, update map[string]interface{}, limit uint, conditions ...Comparable) (string, []interface{}, error) {
	format := "UPDATE %s SET %s"
	sets, vals, err := resolveUpdate(update)
	if err != nil {
		return "", nil, err
	}
	cond := fmt.Sprintf(format, quoteField(table), sets)
	whereString, whereVals := whereConnector("AND", conditions...)

	if limit == 0 {
		if "" != whereString {
			cond = fmt.Sprintf("%s WHERE %s", cond, whereString)
			vals = append(vals, whereVals...)
		}

		return b.finalizeQuery(cond, vals)
	}

	switch b.dialect {
	case DialectMySQL:
		if "" != whereString {
			cond = fmt.Sprintf("%s WHERE %s", cond, whereString)
			vals = append(vals, whereVals...)
		}
		cond += " LIMIT ?"
		vals = append(vals, int(limit))

	case DialectPostgres, DialectSQLite:
		limitIdentifier := b.limitIdentifier()
		limitedRowsSelect := fmt.Sprintf("SELECT %s FROM %s", limitIdentifier, quoteField(table))
		if whereString != "" {
			limitedRowsSelect = fmt.Sprintf("%s WHERE %s", limitedRowsSelect, whereString)
		}

		limitedRowsSelect += " LIMIT ?"
		cond = fmt.Sprintf("%s WHERE %s IN (%s)", cond, limitIdentifier, limitedRowsSelect)
		vals = append(vals, whereVals...)
		vals = append(vals, int(limit))

	default:
		return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
	}

	return b.finalizeQuery(cond, vals)
}

func (b Builder) buildDelete(table string, limit uint, conditions ...Comparable) (string, []interface{}, error) {
	whereString, vals := whereConnector("AND", conditions...)
	format := "DELETE FROM %s"
	args := make([]interface{}, 0, 2)
	args = append(args, quoteField(table))

	if limit == 0 {
		if len(whereString) > 0 {
			format += " WHERE %s"
			args = append(args, whereString)
		}
		cond := fmt.Sprintf(format, args...)

		return b.finalizeQuery(cond, vals)
	}

	switch b.dialect {
	case DialectMySQL:
		if len(whereString) > 0 {
			format += " WHERE %s"
			args = append(args, whereString)
		}
		cond := fmt.Sprintf(format, args...)
		cond += " LIMIT ?"
		vals = append(vals, int(limit))

		return b.finalizeQuery(cond, vals)

	case DialectPostgres, DialectSQLite:
		limitIdentifier := b.limitIdentifier()
		limitedRowsSelect := fmt.Sprintf("SELECT %s FROM %s", limitIdentifier, quoteField(table))
		if whereString != "" {
			limitedRowsSelect = fmt.Sprintf("%s WHERE %s", limitedRowsSelect, whereString)
		}
		limitedRowsSelect += " LIMIT ?"
		cond := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", quoteField(table), limitIdentifier, limitedRowsSelect)
		vals = append(vals, int(limit))

		return b.finalizeQuery(cond, vals)

	default:
		return "", nil, fmt.Errorf("%w: %q", errUnsupportedDialect, b.dialect)
	}
}

// These package-level helpers are kept for internal test compatibility.
func buildInsert(table string, setMap []map[string]interface{}, insertType insertType) (string, []interface{}, error) {
	return defaultBuilder.buildInsert(table, setMap, insertType)
}

func buildInsertOnDuplicate(table string, data []map[string]interface{}, update map[string]interface{}) (string, []interface{}, error) {
	return defaultBuilder.buildInsertOnDuplicate(table, data, update)
}

func buildUpdate(table string, update map[string]interface{}, limit uint, conditions ...Comparable) (string, []interface{}, error) {
	return defaultBuilder.buildUpdate(table, update, limit, conditions...)
}

func buildDelete(table string, limit uint, conditions ...Comparable) (string, []interface{}, error) {
	return defaultBuilder.buildDelete(table, limit, conditions...)
}

func buildSelect(table string, ufields []string, groupBy, orderBy, lockMode string, limit *eleLimit, conditions ...Comparable) (string, []interface{}, error) {
	lockClause := ""
	if lockMode != "" {
		var err error
		lockClause, err = defaultBuilder.lockClause(lockMode)
		if err != nil {
			return "", nil, err
		}
	}

	return defaultBuilder.buildSelect(table, ufields, groupBy, orderBy, lockClause, limit, conditions...)
}

func splitCondition(conditions []Comparable) ([]Comparable, []Comparable) {
	var having []Comparable
	var i int
	for i = len(conditions) - 1; i >= 0; i-- {
		if _, ok := conditions[i].(nilComparable); ok {
			break
		}
	}
	if i >= 0 && i < len(conditions)-1 {
		having = conditions[i+1:]
		return conditions[:i], having
	}
	return conditions, nil
}

func (b Builder) buildSelect(table string, ufields []string, groupBy, orderBy, lockClause string, limit *eleLimit, conditions ...Comparable) (string, []interface{}, error) {
	fields := "*"
	if len(ufields) > 0 {
		for i := range ufields {
			ufields[i] = quoteField(ufields[i])
		}
		fields = strings.Join(ufields, ",")
	}
	bd := strings.Builder{}
	bd.WriteString("SELECT ")
	bd.WriteString(fields)
	bd.WriteString(" FROM ")
	bd.WriteString(table)
	where, having := splitCondition(conditions)
	whereString, vals := whereConnector("AND", where...)
	if "" != whereString {
		bd.WriteString(" WHERE ")
		bd.WriteString(whereString)
	}
	if "" != groupBy {
		bd.WriteString(" GROUP BY ")
		bd.WriteString(groupBy)
	}
	if nil != having {
		havingString, havingVals := whereConnector("AND", having...)
		bd.WriteString(" HAVING ")
		bd.WriteString(havingString)
		vals = append(vals, havingVals...)
	}
	if "" != orderBy {
		bd.WriteString(" ORDER BY ")
		bd.WriteString(orderBy)
	}
	if nil != limit {
		if b.dialect == DialectMySQL {
			bd.WriteString(" LIMIT ?,?")
			vals = append(vals, int(limit.begin), int(limit.step))
		} else {
			bd.WriteString(" LIMIT ? OFFSET ?")
			vals = append(vals, int(limit.step), int(limit.begin))
		}
	}
	if "" != lockClause {
		bd.WriteString(lockClause)
	}

	return b.finalizeQuery(bd.String(), vals)
}
