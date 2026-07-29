package bqsink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

var (
	typeTime       = reflect.TypeFor[time.Time]()
	typeDate       = reflect.TypeFor[civil.Date]()
	typeCivilTime  = reflect.TypeFor[civil.Time]()
	typeDateTime   = reflect.TypeFor[civil.DateTime]()
	typeRat        = reflect.TypeFor[big.Rat]()
	typeRawMessage = reflect.TypeFor[json.RawMessage]()
)

// marshalRawMessage passes JSON text through unchanged, since a json.RawMessage
// already holds the text a JSON column stores. An empty one becomes NULL, because
// it is not valid JSON.
func marshalRawMessage(fv reflect.Value) (bigquery.Value, error) {
	v := derefValue(fv)
	if !v.IsValid() {
		return nil, nil
	}
	raw, ok := v.Interface().(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("expected a json.RawMessage, got %s", v.Type())
	}
	if len(raw) == 0 {
		return nil, nil
	}
	return string(raw), nil
}

// marshalAsJSON encodes a value as the JSON text a JSON column stores.
func marshalAsJSON(fv reflect.Value) (bigquery.Value, error) {
	v := derefValue(fv)
	if !v.IsValid() {
		return nil, nil
	}
	switch v.Kind() {
	case reflect.Map, reflect.Interface, reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
	}
	return encodeJSON(v.Interface())
}

// encodeJSON encodes v without escaping HTML, so that URLs and query strings stay
// readable in BigQuery rather than arriving full of &.
func encodeJSON(v any) (bigquery.Value, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("cannot encode %T as JSON: %w", v, err)
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// wellKnownTypes are the struct types BigQuery treats as scalars rather than
// expanding into a RECORD.
var wellKnownTypes = map[reflect.Type]bigquery.FieldType{
	typeTime:      bigquery.TimestampFieldType,
	typeDate:      bigquery.DateFieldType,
	typeCivilTime: bigquery.TimeFieldType,
	typeDateTime:  bigquery.DateTimeFieldType,
	typeRat:       bigquery.NumericFieldType,
}

// marshalCivilOf builds the conversion a time column option needs. Each one drops
// a component of the instant and none can fail, which is why an option exists for
// these and not for a conversion whose rounding the caller would have to choose.
func marshalCivilOf(convert func(time.Time) bigquery.Value) func(reflect.Value) (bigquery.Value, error) {
	return func(fv reflect.Value) (bigquery.Value, error) {
		v := derefValue(fv)
		if !v.IsValid() {
			return nil, nil
		}
		t, ok := v.Interface().(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected a time.Time, got %s", v.Type())
		}
		return convert(t), nil
	}
}

// timeColumnTypes are the columns a time.Time may be written as besides TIMESTAMP,
// keyed by the tag option that asks for one.
//
// The conversions read the value's own location, so a caller chooses the calendar
// the column records by handing over a time.Time already in it: time.Now().In(jst)
// with the "date" option records the Tokyo date.
var timeColumnTypes = map[string]struct {
	fieldType bigquery.FieldType
	marshal   func(reflect.Value) (bigquery.Value, error)
}{
	"date": {
		bigquery.DateFieldType,
		marshalCivilOf(func(t time.Time) bigquery.Value { return civil.DateOf(t) }),
	},
	"datetime": {
		bigquery.DateTimeFieldType,
		marshalCivilOf(func(t time.Time) bigquery.Value { return civil.DateTimeOf(t) }),
	},
	"time": {
		bigquery.TimeFieldType,
		marshalCivilOf(func(t time.Time) bigquery.Value { return civil.TimeOf(t) }),
	},
}

// rowPlan describes how a struct type becomes a row: which columns it has, what
// their types are, and how each field's value is converted.
//
// Schema inference and row marshaling both read the same plan, so the columns a
// schema declares and the columns a row writes cannot drift apart.
type rowPlan struct {
	goType reflect.Type
	fields []fieldPlan

	// partitioning and clustering describe the table rather than any one column,
	// so they are collected from the fields that name them. Only the top level
	// struct may set them.
	partitioning  *bigquery.TimePartitioning
	clustering    *bigquery.Clustering
	requireFilter bool

	// description and labels describe the table as well, and come from an embedded
	// TableMeta rather than from a column.
	description string
	labels      map[string]string
}

// fieldPlan is the plan for one column.
type fieldPlan struct {
	name string

	// index locates the field, walking through embedded structs the way
	// reflect.Value.FieldByIndex does.
	index []int

	required    bool
	repeated    bool
	nullIfZero  bool
	description string
	fieldType   bigquery.FieldType

	// marshal converts a field value when a registered Marshaler or the type's own
	// FieldMarshaler applies to it, and is nil otherwise.
	marshal func(reflect.Value) (bigquery.Value, error)

	// nested is the plan for a RECORD column's fields, and is nil otherwise.
	nested *rowPlan
}

func buildRowPlan(t reflect.Type, marshalers *Marshalers) (*rowPlan, error) {
	if err := marshalers.err(); err != nil {
		return nil, err
	}
	return buildRowPlanFor(deref(t), marshalers)
}

func buildRowPlanFor(t reflect.Type, marshalers *Marshalers) (*rowPlan, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("bqsink: cannot map %s to a row: not a struct", t)
	}
	candidates, err := collectFields(t)
	if err != nil {
		return nil, err
	}
	meta, err := tableMetaOf(t)
	if err != nil {
		return nil, fmt.Errorf("bqsink: %s: %w", t, err)
	}
	plan := &rowPlan{goType: t, description: meta.description, labels: meta.labels}
	for _, c := range candidates {
		f, err := buildFieldPlan(c, marshalers)
		if err != nil {
			return nil, err
		}
		plan.fields = append(plan.fields, *f)
	}
	if len(plan.fields) == 0 {
		return nil, fmt.Errorf("bqsink: cannot map %s to a row: no usable exported fields", t)
	}
	if err := plan.resolveLayout(candidates); err != nil {
		return nil, fmt.Errorf("bqsink: %s: %w", t, err)
	}
	return plan, nil
}

// resolveLayout collects the partitioning and clustering the fields asked for, and
// checks them against the limits BigQuery enforces, so that a mistake surfaces from
// New rather than from the first attempt to create the table.
func (p *rowPlan) resolveLayout(candidates []candidateField) error {
	byPosition := map[int]string{}
	for i := range candidates {
		tag := candidates[i].tag
		f := &p.fields[i]
		if tag.partition != "" {
			if p.partitioning != nil {
				return fmt.Errorf("columns %s and %s both carry the %q tag, but a table has one partitioning column",
					p.partitioning.Field, f.name, PartitionTagKey)
			}
			if err := checkPartitionColumn(f, tag.partition); err != nil {
				return err
			}
			p.partitioning = &bigquery.TimePartitioning{Field: f.name, Type: tag.partition}
			p.requireFilter = tag.requireFilter
		}
		if tag.cluster == 0 {
			continue
		}
		if other, taken := byPosition[tag.cluster]; taken {
			return fmt.Errorf("columns %s and %s both claim %s position %d",
				other, f.name, ClusterTagKey, tag.cluster)
		}
		if err := checkClusterColumn(f); err != nil {
			return err
		}
		byPosition[tag.cluster] = f.name
	}
	if len(byPosition) == 0 {
		if p.requireFilter && p.partitioning == nil {
			return fmt.Errorf("the %q option needs a partitioning column", "require")
		}
		return nil
	}
	// The positions have to run 1..n, since a gap means a column was meant to be
	// there and its tag is missing or wrong.
	fields := make([]string, 0, len(byPosition))
	for position := 1; position <= len(byPosition); position++ {
		name, ok := byPosition[position]
		if !ok {
			return fmt.Errorf("%s position %d is missing; the positions must run from 1 to %d without a gap",
				ClusterTagKey, position, len(byPosition))
		}
		fields = append(fields, name)
	}
	p.clustering = &bigquery.Clustering{Fields: fields}
	return nil
}

// partitionableTypes are the column types BigQuery accepts for time partitioning:
// "The field specified for time partitioning can only be of type TIMESTAMP, DATE
// or DATETIME."
var partitionableTypes = map[bigquery.FieldType]bool{
	bigquery.TimestampFieldType: true,
	bigquery.DateFieldType:      true,
	bigquery.DateTimeFieldType:  true,
}

// unclusterableTypes are the column types BigQuery refuses to cluster on: "Field c
// has type FLOAT, which is not supported for clustering."
var unclusterableTypes = map[bigquery.FieldType]bool{
	bigquery.FloatFieldType:  true,
	bigquery.JSONFieldType:   true,
	bigquery.BytesFieldType:  true,
	bigquery.RecordFieldType: true,
}

func checkPartitionColumn(f *fieldPlan, granularity bigquery.TimePartitioningType) error {
	if !partitionableTypes[f.fieldType] {
		return fmt.Errorf("column %s is %s, but partitioning needs TIMESTAMP, DATE or DATETIME",
			f.name, f.fieldType)
	}
	if f.repeated {
		return fmt.Errorf("column %s is repeated, which cannot be a partitioning column", f.name)
	}
	// Hourly partitioning is the one granularity a DATE column cannot carry:
	// "hourly partitioning can only be of type TIMESTAMP or DATETIME".
	if granularity == bigquery.HourPartitioningType && f.fieldType == bigquery.DateFieldType {
		return fmt.Errorf("column %s is DATE, which cannot be partitioned by hour; use TIMESTAMP or DATETIME", f.name)
	}
	return nil
}

func checkClusterColumn(f *fieldPlan) error {
	if unclusterableTypes[f.fieldType] {
		return fmt.Errorf("column %s is %s, which BigQuery cannot cluster on", f.name, f.fieldType)
	}
	if f.repeated {
		// "Fields specified for clustering can only be NULLABLE or REQUIRED."
		return fmt.Errorf("column %s is repeated, which BigQuery cannot cluster on", f.name)
	}
	return nil
}

func buildFieldPlan(c candidateField, marshalers *Marshalers) (*fieldPlan, error) {
	f := &fieldPlan{
		name:        c.name,
		index:       c.index,
		required:    c.tag.required,
		nullIfZero:  c.tag.nullIfZero,
		description: c.tag.description,
	}
	t := deref(c.sf.Type)
	if isRepeated(t) {
		f.repeated = true
		f.required = false
		t = t.Elem()
	}
	if err := f.resolveType(t, c.tag, marshalers); err != nil {
		return nil, fmt.Errorf("bqsink: field %s: %w", c.sf.Name, err)
	}
	return f, nil
}

// resolveType settles the column's type and, where one applies, the conversion
// that produces its value.
//
// The order of precedence is: a "date", "datetime" or "time" tag naming the column
// a time.Time becomes, then a registered Marshaler, then the type's own
// FieldMarshaler, then the struct types BigQuery treats as scalars, then a "record"
// tag asking for a RECORD, then the mapping from the Go type. A type whose
// structure BigQuery has no column type for becomes JSON rather than an error.
//
// The time options come first only so that a Marshaler registered for time.Time
// can be reported as the contradiction it is rather than silently winning.
//
// wellKnownTypes coming before the "record" tag is what makes `bqsink:",record"` on
// a time.Time have no effect, since such a type is a scalar column and never a
// RECORD to descend into.
func (f *fieldPlan) resolveType(t reflect.Type, tag fieldTag, marshalers *Marshalers) error {
	if tag.timeType != "" {
		return f.resolveTimeType(t, tag, marshalers)
	}
	if m, ok := marshalers.lookup(t); ok {
		if m.fieldType == bigquery.RecordFieldType {
			return fmt.Errorf("the registered marshaler for %s declares RECORD, which needs a nested schema bqsink cannot derive", deref(t))
		}
		f.fieldType = m.fieldType
		f.marshal = func(fv reflect.Value) (bigquery.Value, error) {
			v := derefValue(fv)
			if !v.IsValid() {
				return nil, nil
			}
			return m.marshal(v.Interface())
		}
		return nil
	}
	if fm, ok := fieldMarshalerOf(t); ok {
		ft := fm.BigQueryFieldType()
		if ft == bigquery.RecordFieldType {
			return fmt.Errorf("%s implements FieldMarshaler but declares RECORD, which needs a nested schema bqsink cannot derive", deref(t))
		}
		f.fieldType = ft
		f.marshal = marshalViaFieldMarshaler
		return nil
	}
	dt := deref(t)
	if ft, ok := wellKnownTypes[dt]; ok {
		f.fieldType = ft
		return nil
	}
	if tag.record {
		if dt.Kind() != reflect.Struct {
			return fmt.Errorf("the %q option needs a struct, but the type is %s", "record", dt)
		}
		nested, err := buildRowPlanFor(dt, marshalers)
		if err != nil {
			return err
		}
		f.fieldType = bigquery.RecordFieldType
		f.nested = nested
		return nil
	}
	switch dt.Kind() {
	case reflect.String:
		f.fieldType = bigquery.StringFieldType
		return nil
	case reflect.Bool:
		f.fieldType = bigquery.BooleanFieldType
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		f.fieldType = bigquery.IntegerFieldType
		return nil
	case reflect.Uint, reflect.Uint64:
		// INT64 is signed, so the upper half of a uint64 does not fit. NUMERIC
		// holds it, at the cost of the column no longer being an integer type.
		f.fieldType = bigquery.NumericFieldType
		return nil
	case reflect.Float32, reflect.Float64:
		f.fieldType = bigquery.FloatFieldType
		return nil
	case reflect.Slice, reflect.Array:
		if isByteSequence(dt) && dt != typeRawMessage {
			f.fieldType = bigquery.BytesFieldType
			return nil
		}
	}
	return f.resolveJSON(dt)
}

// resolveTimeType makes the column the DATE, DATETIME or TIME its tag asks for.
//
// Only time.Time reaches here. A named type whose underlying type is time.Time is
// rejected too, since bqsink cannot see through it: wellKnownTypes is an exact
// lookup, so such a type is a JSON column and the option would not describe it.
func (f *fieldPlan) resolveTimeType(t reflect.Type, tag fieldTag, marshalers *Marshalers) error {
	column, ok := timeColumnTypes[tag.timeType]
	if !ok {
		return fmt.Errorf("unknown tag option %q", tag.timeType)
	}
	if dt := deref(t); dt != typeTime {
		return fmt.Errorf("the %q option needs a time.Time, but the type is %s", tag.timeType, dt)
	}
	if _, ok := marshalers.lookup(t); ok {
		return fmt.Errorf("the %q option and the marshaler registered for time.Time disagree about this column; drop one of them",
			tag.timeType)
	}
	f.fieldType = column.fieldType
	f.marshal = column.marshal
	return nil
}

// resolveJSON makes the column JSON, which is how bqsink represents a type whose
// structure has no BigQuery column type of its own.
func (f *fieldPlan) resolveJSON(dt reflect.Type) error {
	switch dt.Kind() {
	case reflect.Struct, reflect.Interface:
	case reflect.Map:
		if dt.Key().Kind() != reflect.String {
			return fmt.Errorf("a map needs string keys to become a JSON column, but the key is %s", dt.Key())
		}
	case reflect.Slice, reflect.Array:
		if dt != typeRawMessage {
			return fmt.Errorf("unsupported type %s", dt)
		}
	default:
		return fmt.Errorf("unsupported type %s", dt)
	}
	f.fieldType = bigquery.JSONFieldType
	if dt == typeRawMessage {
		f.marshal = marshalRawMessage
		return nil
	}
	f.marshal = marshalAsJSON
	return nil
}

// schema returns the columns the plan declares.
func (p *rowPlan) schema() bigquery.Schema {
	schema := make(bigquery.Schema, 0, len(p.fields))
	for i := range p.fields {
		f := &p.fields[i]
		field := &bigquery.FieldSchema{
			Name:        f.name,
			Type:        f.fieldType,
			Required:    f.required,
			Repeated:    f.repeated,
			Description: f.description,
		}
		if f.nested != nil {
			field.Schema = f.nested.schema()
		}
		schema = append(schema, field)
	}
	return schema
}

// columnNames returns the names of the columns the plan writes.
func (p *rowPlan) columnNames() []string {
	names := make([]string, len(p.fields))
	for i := range p.fields {
		names[i] = p.fields[i].name
	}
	return names
}

// marshalRow converts rv into the columns to write.
func (p *rowPlan) marshalRow(rv reflect.Value) (map[string]bigquery.Value, error) {
	rv = derefValue(rv)
	if !rv.IsValid() {
		return nil, fmt.Errorf("bqsink: cannot write a nil %s", p.goType)
	}
	row := make(map[string]bigquery.Value, len(p.fields))
	for i := range p.fields {
		f := &p.fields[i]
		fv, err := rv.FieldByIndexErr(f.index)
		if err != nil {
			// An embedded pointer on the way to the field is nil, so the column
			// has no value to write.
			row[f.name] = nil
			continue
		}
		value, err := f.marshalValue(fv)
		if err != nil {
			return nil, fmt.Errorf("bqsink: column %s: %w", f.name, err)
		}
		row[f.name] = value
	}
	return row, nil
}

func (f *fieldPlan) marshalValue(fv reflect.Value) (bigquery.Value, error) {
	if f.nullIfZero {
		// On a repeated column "nullifzero" means no elements, so an empty slice
		// counts as well as a nil one.
		if f.repeated && isEmptySequence(fv) {
			return nil, nil
		}
		if !f.repeated && isZeroValue(fv) {
			return nil, nil
		}
	}
	switch fv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if fv.IsNil() {
			return nil, nil
		}
	}
	if f.repeated {
		return f.marshalRepeated(fv)
	}
	return f.marshalScalar(fv)
}

// isEmptySequence reports whether fv holds no elements.
func isEmptySequence(fv reflect.Value) bool {
	v := derefValue(fv)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		return v.Len() == 0
	}
	return false
}

func (f *fieldPlan) marshalRepeated(fv reflect.Value) (bigquery.Value, error) {
	fv = derefValue(fv)
	if !fv.IsValid() {
		return nil, nil
	}
	values := make([]bigquery.Value, fv.Len())
	for i := range values {
		v, err := f.marshalScalar(fv.Index(i))
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		values[i] = v
	}
	return values, nil
}

func (f *fieldPlan) marshalScalar(fv reflect.Value) (bigquery.Value, error) {
	if f.marshal != nil {
		return f.marshal(fv)
	}
	if fv.Kind() == reflect.Pointer && fv.IsNil() {
		return nil, nil
	}
	if f.nested != nil {
		return f.nested.marshalRow(fv)
	}
	if _, ok := wellKnownTypes[deref(fv.Type())]; ok {
		return wellKnownValue(fv)
	}
	return scalarValue(derefValue(fv))
}

// wellKnownValue returns the value in the form the BigQuery SDK expects: NUMERIC
// as *big.Rat, and the date and time types as their value type.
func wellKnownValue(fv reflect.Value) (bigquery.Value, error) {
	v := derefValue(fv)
	if !v.IsValid() {
		return nil, nil
	}
	if v.Type() != typeRat {
		return v.Interface(), nil
	}
	if fv.Kind() == reflect.Pointer {
		return fv.Interface(), nil
	}
	rat, ok := v.Interface().(big.Rat)
	if !ok {
		return nil, fmt.Errorf("expected a big.Rat, got %s", v.Type())
	}
	return &rat, nil
}

func scalarValue(fv reflect.Value) (bigquery.Value, error) {
	if !fv.IsValid() {
		return nil, nil
	}
	switch fv.Kind() {
	case reflect.String:
		return fv.String(), nil
	case reflect.Bool:
		return fv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fv.Int(), nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(fv.Uint()), nil
	case reflect.Uint, reflect.Uint64:
		return new(big.Rat).SetUint64(fv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return fv.Float(), nil
	case reflect.Slice:
		if isByteSequence(fv.Type()) {
			return fv.Bytes(), nil
		}
	case reflect.Array:
		if isByteSequence(fv.Type()) {
			out := make([]byte, fv.Len())
			reflect.Copy(reflect.ValueOf(out), fv)
			return out, nil
		}
	}
	return nil, fmt.Errorf("cannot convert %s into a BigQuery value", fv.Type())
}

type isZeroer interface{ IsZero() bool }

// isZeroValue reports whether fv is zero, preferring an IsZero method where the
// type has one, as the "omitzero" option of encoding/json/v2 does. That is what
// makes a zero time.Time recognisable.
func isZeroValue(fv reflect.Value) bool {
	if !fv.IsValid() {
		return true
	}
	if fv.Kind() == reflect.Pointer && fv.IsNil() {
		return true
	}
	if z, ok := zeroChecker(fv); ok {
		return z.IsZero()
	}
	return fv.IsZero()
}

func zeroChecker(fv reflect.Value) (isZeroer, bool) {
	if z, ok := fv.Interface().(isZeroer); ok {
		return z, true
	}
	if fv.CanAddr() {
		if z, ok := fv.Addr().Interface().(isZeroer); ok {
			return z, true
		}
		return nil, false
	}
	ptr := reflect.New(fv.Type())
	ptr.Elem().Set(fv)
	z, ok := ptr.Interface().(isZeroer)
	return z, ok
}

func derefValue(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}
		}
		rv = rv.Elem()
	}
	return rv
}
