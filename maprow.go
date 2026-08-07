package bqsink

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// mapPlan converts a map[string]any row into the columns a declared schema
// says it should have.
//
// Unlike rowPlan, nothing here is derived from a Go type: the schema is the
// input rather than the output, so there are no fields to walk and no tags to
// read. What a column accepts is instead fixed by its FieldType, checked
// against the map's keys and values at marshalRow time.
type mapPlan struct {
	schema     bigquery.Schema
	byName     map[string]*bigquery.FieldSchema
	nested     map[string]*mapPlan
	marshalers *Marshalers
}

// buildMapPlan is buildMapPlanFor with the check buildMapPlanFor's own
// recursion into a RECORD column does not need to repeat.
func buildMapPlan(schema bigquery.Schema, marshalers *Marshalers) (*mapPlan, error) {
	if err := marshalers.err(); err != nil {
		return nil, err
	}
	return buildMapPlanFor(schema, marshalers)
}

// buildMapPlanFor indexes schema by column name and, for every RECORD column,
// builds the plan its nested schema needs once here rather than once per row.
func buildMapPlanFor(schema bigquery.Schema, marshalers *Marshalers) (*mapPlan, error) {
	byName := make(map[string]*bigquery.FieldSchema, len(schema))
	nested := make(map[string]*mapPlan)
	for _, f := range schema {
		byName[f.Name] = f
		if f.Type != bigquery.RecordFieldType {
			continue
		}
		n, err := buildMapPlanFor(f.Schema, marshalers)
		if err != nil {
			return nil, fmt.Errorf("bqsink: column %s: %w", f.Name, err)
		}
		nested[f.Name] = n
	}
	return &mapPlan{schema: schema, byName: byName, nested: nested, marshalers: marshalers}, nil
}

// marshalRow implements rowConverter. rv arrives as the map[string]any Sink
// was given, unwrapped by neither prepare nor here: prepare only calls
// fillable — which would wrap it behind a pointer — for a row type that
// implements RowFiller, and map[string]any never does. derefValue still runs
// in case a caller reaches this through a *map[string]any instead.
func (p *mapPlan) marshalRow(rv reflect.Value) (map[string]bigquery.Value, error) {
	rv = derefValue(rv)
	if !rv.IsValid() || rv.IsNil() {
		return nil, errors.New("bqsink: cannot write a nil map[string]any row")
	}
	row, ok := rv.Interface().(map[string]any)
	if !ok {
		return nil, fmt.Errorf("bqsink: cannot write a %s row: want map[string]any", rv.Type())
	}
	return p.marshalMap(row)
}

// marshalMap converts one row, or one RECORD column's value, into the columns
// the plan's schema declares.
func (p *mapPlan) marshalMap(row map[string]any) (map[string]bigquery.Value, error) {
	for key := range row {
		if _, ok := p.byName[key]; !ok {
			return nil, fmt.Errorf("bqsink: row has column %q, which the schema does not declare", key)
		}
	}
	out := make(map[string]bigquery.Value, len(p.schema))
	for _, field := range p.schema {
		value, err := p.marshalField(field, row)
		if err != nil {
			return nil, fmt.Errorf("bqsink: column %s: %w", field.Name, err)
		}
		out[field.Name] = value
	}
	return out, nil
}

// marshalField settles one column's value: missing from the map or holding a
// nil of any kind becomes NULL, which a REQUIRED column refuses, and
// otherwise the value is dispatched by whether the column is repeated.
func (p *mapPlan) marshalField(field *bigquery.FieldSchema, row map[string]any) (bigquery.Value, error) {
	value, present := row[field.Name]
	if !present || isNilAny(value) {
		if field.Required {
			return nil, errors.New("a REQUIRED column cannot be NULL")
		}
		return nil, nil
	}
	if field.Repeated {
		return p.marshalRepeated(field, value)
	}
	return p.marshalScalar(field, value)
}

// marshalRepeated converts a REPEATED column's value, which the accepted type
// table requires to be a []any, converting each element as marshalField's
// scalar case would.
//
// A nil element, typed or not, is an error rather than NULL: BigQuery refuses
// to write an array holding one ("Array cannot have a null element"), even
// though ARRAY_LENGTH counts it in a query's own intermediate value. The
// column itself being nil is a different question, settled by marshalField
// before this is ever called, and still becomes NULL.
func (p *mapPlan) marshalRepeated(field *bigquery.FieldSchema, value any) (bigquery.Value, error) {
	elements, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("a repeated column needs a []any, got %T", value)
	}
	out := make([]bigquery.Value, len(elements))
	for i, element := range elements {
		if isNilAny(element) {
			return nil, fmt.Errorf("element %d: a repeated column cannot hold a NULL element", i)
		}
		converted, err := p.marshalScalar(field, element)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		out[i] = converted
	}
	return out, nil
}

// marshalScalar dispatches one non-nil value in the order the declaration
// promises: a registered Marshaler found by the value's own dynamic type,
// then the value's own FieldMarshaler, then the accepted type table. A
// Marshaler or FieldMarshaler that disagrees with the column's declared
// FieldType is an error rather than a silent win, the same as the other two
// steps disagreeing with what the map actually holds.
func (p *mapPlan) marshalScalar(field *bigquery.FieldSchema, value any) (bigquery.Value, error) {
	rv := reflect.ValueOf(value)
	if m, ok := p.marshalers.lookup(rv.Type()); ok {
		if m.fieldType != field.Type {
			return nil, fmt.Errorf("the registered marshaler for %s declares %s, but the column is %s",
				rv.Type(), m.fieldType, field.Type)
		}
		v := derefValue(rv)
		if !v.IsValid() {
			return nil, nil
		}
		return m.marshal(v.Interface())
	}
	if fm, ok := fieldMarshalerOf(rv.Type()); ok {
		if ft := fm.BigQueryFieldType(); ft != field.Type {
			return nil, fmt.Errorf("%T implements FieldMarshaler declaring %s, but the column is %s", value, ft, field.Type)
		}
		return marshalViaFieldMarshaler(rv)
	}
	return p.marshalAccepted(field, value)
}

// marshalAccepted converts value under the accepted type table, the one path
// left once neither a registered Marshaler nor the value's own FieldMarshaler
// claimed the column.
func (p *mapPlan) marshalAccepted(field *bigquery.FieldSchema, value any) (bigquery.Value, error) {
	switch field.Type {
	case bigquery.StringFieldType, bigquery.GeographyFieldType:
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.String {
			return nil, fmt.Errorf("a %s column needs a string, got %T", field.Type, value)
		}
		return rv.String(), nil
	case bigquery.IntegerFieldType:
		return marshalInteger(value)
	case bigquery.FloatFieldType:
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64 {
			return nil, fmt.Errorf("a FLOAT column needs a float32 or float64, got %T", value)
		}
		return rv.Float(), nil
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		r, ok := value.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("a %s column needs a *big.Rat, got %T", field.Type, value)
		}
		return r, nil
	case bigquery.BooleanFieldType:
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Bool {
			return nil, fmt.Errorf("a BOOLEAN column needs a bool, got %T", value)
		}
		return rv.Bool(), nil
	case bigquery.TimestampFieldType:
		t, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("a TIMESTAMP column needs a time.Time, got %T", value)
		}
		return t, nil
	case bigquery.DateFieldType:
		d, ok := value.(civil.Date)
		if !ok {
			return nil, fmt.Errorf("a DATE column needs a civil.Date, got %T", value)
		}
		return d, nil
	case bigquery.TimeFieldType:
		t, ok := value.(civil.Time)
		if !ok {
			return nil, fmt.Errorf("a TIME column needs a civil.Time, got %T", value)
		}
		return t, nil
	case bigquery.DateTimeFieldType:
		dt, ok := value.(civil.DateTime)
		if !ok {
			return nil, fmt.Errorf("a DATETIME column needs a civil.DateTime, got %T", value)
		}
		return dt, nil
	case bigquery.BytesFieldType:
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Slice || !isByteSequence(rv.Type()) {
			return nil, fmt.Errorf("a BYTES column needs a []byte, got %T", value)
		}
		return rv.Bytes(), nil
	case bigquery.JSONFieldType:
		switch v := value.(type) {
		case json.RawMessage:
			return v, nil
		case []byte:
			return v, nil
		default:
			rv := reflect.ValueOf(value)
			if rv.Kind() == reflect.String {
				return rv.String(), nil
			}
			return nil, fmt.Errorf("a JSON column needs a json.RawMessage, string or []byte, got %T", value)
		}
	case bigquery.RecordFieldType:
		return p.marshalNestedRecord(field, value)
	default:
		return nil, fmt.Errorf("cannot write a %T into a %s column", value, field.Type)
	}
}

// marshalNestedRecord converts a RECORD column's value, delegating to the
// mapPlan buildMapPlanFor already built for field's nested schema.
func (p *mapPlan) marshalNestedRecord(field *bigquery.FieldSchema, value any) (bigquery.Value, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("a RECORD column needs a map[string]any, got %T", value)
	}
	nested, ok := p.nested[field.Name]
	if !ok {
		return nil, fmt.Errorf("bqsink: column %s is a RECORD column with no plan for its nested schema", field.Name)
	}
	return nested.marshalMap(m)
}

// marshalInteger converts value into the int64 an INTEGER column holds,
// accepting every Go integer kind and a float whose fractional part is zero.
func marshalInteger(value any) (bigquery.Value, error) {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64FromUint64(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return int64FromFloat(rv.Float())
	default:
		return nil, fmt.Errorf("an INTEGER column needs an integer or a whole float, got %T", value)
	}
}

func int64FromUint64(v uint64) (bigquery.Value, error) {
	if v > math.MaxInt64 {
		return nil, errors.New("a uint64 value overflows INT64")
	}
	return int64(v), nil
}

// maxInt64Boundary is 2^63, the smallest float64 no longer holding an int64.
// float64(math.MaxInt64) itself rounds up to exactly this value, so comparing
// against that constant instead would let 2^63 through.
const maxInt64Boundary = 9223372036854775808.0

func int64FromFloat(v float64) (bigquery.Value, error) {
	if math.IsNaN(v) {
		return nil, errors.New("NaN cannot be written to an INTEGER column")
	}
	if math.IsInf(v, 0) {
		return nil, errors.New("a float64 value does not fit in an INTEGER column")
	}
	if v >= maxInt64Boundary || v < -maxInt64Boundary {
		return nil, errors.New("a float64 value overflows INT64")
	}
	if v != math.Trunc(v) {
		return nil, errors.New("a float64 value has a fractional part, which an INTEGER column cannot hold")
	}
	return int64(v), nil
}

// isNilAny reports whether value is nil, either as the untyped nil interface
// itself or as a typed nil such as a nil *big.Rat, a nil map or a nil slice.
// A column's value is checked this way rather than by comparing it to nil
// directly, since a typed nil compares unequal to nil once it is boxed in the
// any the map holds it as.
//
// A pointer is unwrapped through every level, matching derefValue: a **widget
// whose outer pointer is non-nil but whose inner *widget is nil is nil all the
// same, and a REQUIRED column must refuse it rather than let marshalScalar's
// own IsValid guard turn it into a silent NULL.
func isNilAny(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return true
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}
