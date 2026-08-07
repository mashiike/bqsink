package bqsink

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// mapDeclaration builds the Declaration DeclarationFromMetadata settles for a
// table holding schema, failing the test if it could not be built. Tests reach
// the conversion through decl.plan.marshalRow rather than through a Sinker,
// the way plan_test.go reaches buildRowPlan's output through inferSchema.
func mapDeclaration(t *testing.T, schema bigquery.Schema, marshalers ...*Marshalers) Declaration {
	t.Helper()
	decl := DeclarationFromMetadata(&bigquery.TableMetadata{Schema: schema}, marshalers...)
	if decl.err != nil {
		t.Fatalf("DeclarationFromMetadata() error = %v", decl.err)
	}
	return decl
}

// mapTestSinker builds a Sinker over w (or a fresh fakeWriter, when w is nil) for
// the declaration DeclarationFromMetadata settles from schema, and installs a
// fakeTable already matching schema so Sink does not have to migrate anything.
// It is the map-row analogue of newTestSinker.
func mapTestSinker(t *testing.T, schema bigquery.Schema, w RowsWriter, opts ...Option) *Sinker {
	t.Helper()
	if w == nil {
		w = newFakeWriter(t)
	}
	s, err := NewSinker(w, DeclarationFromMetadata(&bigquery.TableMetadata{Schema: schema}), opts...)
	if err != nil {
		t.Fatalf("NewSinker() error = %v", err)
	}
	s.api = migratedTableFor(schema)
	s.query = &fakeQueryRunner{}
	return s
}

// idCapturingWriter wraps a fakeWriter to record the Row.ID values WriteRows was
// given, since fakeWriter itself only keeps the values a row carries.
type idCapturingWriter struct {
	*fakeWriter

	mu  sync.Mutex
	ids []string
}

func newIDCapturingWriter(t *testing.T) *idCapturingWriter {
	t.Helper()
	return &idCapturingWriter{fakeWriter: newFakeWriter(t)}
}

// WriteRows implements RowsWriter.
func (w *idCapturingWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	w.mu.Lock()
	for _, r := range rows {
		w.ids = append(w.ids, r.ID)
	}
	w.mu.Unlock()
	return w.fakeWriter.WriteRows(ctx, rows)
}

func (w *idCapturingWriter) recordedIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.ids...)
}

// TestMapPlanKeyChecks covers the three ways a row's keys are checked against
// the declared schema: a key the schema does not have, a declared column the
// row leaves out, and the row itself being a nil map.
func TestMapPlanKeyChecks(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType},
		{Name: "req", Type: bigquery.StringFieldType, Required: true},
	}

	t.Run("an unknown key is rejected", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"a": "x", "req": "y", "extra": 1}))
		if err == nil || !strings.Contains(err.Error(), "extra") {
			t.Errorf("marshalRow() error = %v, want it to name the unknown column", err)
		}
	})

	t.Run("a declared column missing from the row becomes NULL", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"req": "y"}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got, ok := row["a"]; !ok || got != nil {
			t.Errorf("row[a] = %#v, want NULL for a column missing from the map", row["a"])
		}
	})

	t.Run("a missing REQUIRED column is rejected", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"a": "x"}))
		if err == nil || !strings.Contains(err.Error(), "REQUIRED column") {
			t.Errorf("marshalRow() error = %v, want a rejection of the missing REQUIRED column", err)
		}
	})

	t.Run("a nil map is rejected", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any(nil)))
		if err == nil || !strings.Contains(err.Error(), "nil map[string]any row") {
			t.Errorf("marshalRow() error = %v, want a rejection of a nil row", err)
		}
	})
}

// TestMapPlanNilValueHandling covers how a present-but-nil value settles,
// which is a different question from a key being absent altogether: an
// untyped nil, a typed nil such as a nil *big.Rat, and a nil map[string]any
// under a RECORD column all become NULL, while a REQUIRED column still
// refuses NULL either way.
func TestMapPlanNilValueHandling(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "money", Type: bigquery.NumericFieldType},
		{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "a", Type: bigquery.StringFieldType},
		}},
		{Name: "req", Type: bigquery.StringFieldType, Required: true},
	}
	decl := mapDeclaration(t, schema)

	t.Run("an untyped nil becomes NULL", func(t *testing.T) {
		t.Parallel()
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"money": nil, "req": "x"}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["money"]; got != nil {
			t.Errorf("row[money] = %#v, want NULL", got)
		}
	})

	t.Run("a typed nil *big.Rat becomes NULL", func(t *testing.T) {
		t.Parallel()
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"money": (*big.Rat)(nil), "req": "x"}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["money"]; got != nil {
			t.Errorf("row[money] = %#v, want NULL", got)
		}
	})

	t.Run("a nil map[string]any for a RECORD column becomes NULL, not an error", func(t *testing.T) {
		t.Parallel()
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"inner": map[string]any(nil), "req": "x"}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["inner"]; got != nil {
			t.Errorf("row[inner] = %#v, want NULL", got)
		}
	})

	t.Run("nil for a REQUIRED column is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"req": nil}))
		if err == nil || !strings.Contains(err.Error(), "REQUIRED column") {
			t.Errorf("marshalRow() error = %v, want a rejection of NULL for a REQUIRED column", err)
		}
	})
}

// TestMapPlanDispatchOrder covers the three stages a value's conversion is
// tried in: a registered Marshalers, then the value's own FieldMarshaler, then
// the accepted type table. payload (from marshal_test.go) declares JSON
// through a FieldMarshaler, which lets the same value show all three.
func TestMapPlanDispatchOrder(t *testing.T) {
	t.Parallel()

	t.Run("a registered Marshalers wins over the value's own FieldMarshaler", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "p", Type: bigquery.StringFieldType}}
		override := MarshalFunc(bigquery.StringFieldType, func(p payload) (bigquery.Value, error) {
			return "overridden", nil
		})
		decl := mapDeclaration(t, schema, override)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"p": payload{Data: map[string]string{"k": "v"}}}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["p"]; got != "overridden" {
			t.Errorf("row[p] = %#v, want the registered marshaler's output", got)
		}
	})

	t.Run("the value's own FieldMarshaler is used when nothing is registered", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "p", Type: bigquery.JSONFieldType}}
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"p": payload{Data: map[string]string{"k": "v"}}}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["p"]; got != `{"k":"v"}` {
			t.Errorf("row[p] = %#v, want the value's own FieldMarshaler output", got)
		}
	})

	t.Run("the accepted type table is used when neither applies", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"a": "plain"}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["a"]; got != "plain" {
			t.Errorf("row[a] = %#v, want the plain string", got)
		}
	})
}

// TestMapPlanMarshalerFieldTypeMismatch checks that a registered Marshalers or
// a value's own FieldMarshaler disagreeing with the column's declared
// FieldType fails the row, rather than one silently winning.
func TestMapPlanMarshalerFieldTypeMismatch(t *testing.T) {
	t.Parallel()

	t.Run("a registered Marshalers disagreeing with the column is rejected", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "e", Type: bigquery.StringFieldType}}
		decl := mapDeclaration(t, schema, jsonMarshalers())
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"e": external{Amount: "1"}}))
		if err == nil || !strings.Contains(err.Error(), "registered marshaler") {
			t.Errorf("marshalRow() error = %v, want a rejection naming the registered marshaler mismatch", err)
		}
	})

	t.Run("a value's own FieldMarshaler disagreeing with the column is rejected", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "p", Type: bigquery.StringFieldType}}
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"p": payload{Data: map[string]string{"k": "v"}}}))
		if err == nil || !strings.Contains(err.Error(), "FieldMarshaler") {
			t.Errorf("marshalRow() error = %v, want a rejection naming the FieldMarshaler mismatch", err)
		}
	})
}

// TestMapPlanAcceptedTypes covers the normal case of every FieldType in the
// accepted type table except RECORD and REPEATED, which need a nested schema
// and get their own tests below.
func TestMapPlanAcceptedTypes(t *testing.T) {
	t.Parallel()

	rat := big.NewRat(25, 2)
	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	day := civil.Date{Year: 2026, Month: time.July, Day: 28}
	clock := civil.Time{Hour: 12, Minute: 30}
	dt := civil.DateTime{Date: day, Time: clock}

	tests := []struct {
		name  string
		field bigquery.FieldSchema
		value any
		want  bigquery.Value
	}{
		{name: "STRING", field: bigquery.FieldSchema{Type: bigquery.StringFieldType}, value: "s", want: "s"},
		{name: "GEOGRAPHY", field: bigquery.FieldSchema{Type: bigquery.GeographyFieldType}, value: "POINT(1 1)", want: "POINT(1 1)"},
		{name: "INTEGER", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: int(7), want: int64(7)},
		{name: "FLOAT from float32", field: bigquery.FieldSchema{Type: bigquery.FloatFieldType}, value: float32(1.5), want: float64(1.5)},
		{name: "FLOAT from float64", field: bigquery.FieldSchema{Type: bigquery.FloatFieldType}, value: float64(1.5), want: float64(1.5)},
		{name: "NUMERIC", field: bigquery.FieldSchema{Type: bigquery.NumericFieldType}, value: rat, want: bigquery.Value(rat)},
		{name: "BIGNUMERIC", field: bigquery.FieldSchema{Type: bigquery.BigNumericFieldType}, value: rat, want: bigquery.Value(rat)},
		{name: "BOOLEAN", field: bigquery.FieldSchema{Type: bigquery.BooleanFieldType}, value: true, want: true},
		{name: "TIMESTAMP", field: bigquery.FieldSchema{Type: bigquery.TimestampFieldType}, value: at, want: bigquery.Value(at)},
		{name: "DATE", field: bigquery.FieldSchema{Type: bigquery.DateFieldType}, value: day, want: bigquery.Value(day)},
		{name: "TIME", field: bigquery.FieldSchema{Type: bigquery.TimeFieldType}, value: clock, want: bigquery.Value(clock)},
		{name: "DATETIME", field: bigquery.FieldSchema{Type: bigquery.DateTimeFieldType}, value: dt, want: bigquery.Value(dt)},
		{name: "BYTES", field: bigquery.FieldSchema{Type: bigquery.BytesFieldType}, value: []byte("b"), want: bigquery.Value([]byte("b"))},
		{
			name:  "JSON from json.RawMessage",
			field: bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			value: json.RawMessage(`{"a":1}`),
			want:  bigquery.Value(json.RawMessage(`{"a":1}`)),
		},
		{
			name:  "JSON from string",
			field: bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			value: `{"a":1}`,
			want:  bigquery.Value(`{"a":1}`),
		},
		{
			name:  "JSON from []byte",
			field: bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			value: []byte(`{"a":1}`),
			want:  bigquery.Value([]byte(`{"a":1}`)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			field := tt.field
			field.Name = "v"
			decl := mapDeclaration(t, bigquery.Schema{&field})
			row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err != nil {
				t.Fatalf("marshalRow() error = %v", err)
			}
			if !reflect.DeepEqual(row["v"], tt.want) {
				t.Errorf("row[v] = %#v, want %#v", row["v"], tt.want)
			}
		})
	}
}

// TestMapPlanIntegerColumnAcceptsEveryGoIntegerKindAndWholeFloats checks the
// full width of Go types the accepted type table names for INTEGER.
func TestMapPlanIntegerColumnAcceptsEveryGoIntegerKindAndWholeFloats(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "v", Type: bigquery.IntegerFieldType}}
	decl := mapDeclaration(t, schema)

	tests := []struct {
		name  string
		value any
	}{
		{name: "int", value: int(7)},
		{name: "int8", value: int8(7)},
		{name: "int16", value: int16(7)},
		{name: "int32", value: int32(7)},
		{name: "int64", value: int64(7)},
		{name: "uint", value: uint(7)},
		{name: "uint8", value: uint8(7)},
		{name: "uint16", value: uint16(7)},
		{name: "uint32", value: uint32(7)},
		{name: "uint64", value: uint64(7)},
		{name: "float32 with a zero fraction", value: float32(7)},
		{name: "float64 with a zero fraction", value: float64(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err != nil {
				t.Fatalf("marshalRow() error = %v", err)
			}
			if got := row["v"]; got != int64(7) {
				t.Errorf("row[v] = %#v, want int64(7)", got)
			}
		})
	}
}

// TestMapPlanIntegerColumnRejectsOutOfRangeFloats checks the range guard added
// on top of the zero-fraction check: a float whose fractional part is zero but
// whose magnitude is beyond what int64 holds must still be rejected, naming
// both the overflow and the column, rather than converting through undefined
// behaviour.
func TestMapPlanIntegerColumnRejectsOutOfRangeFloats(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "v", Type: bigquery.IntegerFieldType}}
	decl := mapDeclaration(t, schema)

	tests := []struct {
		name  string
		value any
	}{
		{name: "a large positive float with no fractional part", value: 1e30},
		{name: "a large negative float with no fractional part", value: -1e30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err == nil {
				t.Fatal("marshalRow() error = nil, want a rejection of a float out of INT64 range")
			}
			if !strings.Contains(err.Error(), "overflows INT64") {
				t.Errorf("marshalRow() error = %v, want it to say the value overflows INT64", err)
			}
			if !strings.Contains(err.Error(), "column v") {
				t.Errorf("marshalRow() error = %v, want it to name the column", err)
			}
		})
	}
}

// TestMapPlanIntegerColumnBoundaryIsGoTypeSensitive checks the boundary at
// int64's own maximum: the value stays exact as an int64, but the same
// mathematical value as a float64 has already rounded up to 2^63 before
// marshalInteger ever sees it, so it is rejected rather than silently wrapping.
func TestMapPlanIntegerColumnBoundaryIsGoTypeSensitive(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "v", Type: bigquery.IntegerFieldType}}
	decl := mapDeclaration(t, schema)

	t.Run("int64(math.MaxInt64) is exact and accepted", func(t *testing.T) {
		t.Parallel()
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": int64(math.MaxInt64)}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		if got := row["v"]; got != int64(math.MaxInt64) {
			t.Errorf("row[v] = %#v, want int64(math.MaxInt64)", got)
		}
	})

	t.Run("float64(math.MaxInt64) has already rounded past the range", func(t *testing.T) {
		t.Parallel()
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": float64(math.MaxInt64)}))
		if err == nil {
			t.Fatal("marshalRow() error = nil, want a rejection: float64(math.MaxInt64) rounds up to 2^63")
		}
		if !strings.Contains(err.Error(), "overflows INT64") {
			t.Errorf("marshalRow() error = %v, want it to say the value overflows INT64", err)
		}
		if !strings.Contains(err.Error(), "column v") {
			t.Errorf("marshalRow() error = %v, want it to name the column", err)
		}
	})
}

// TestMapPlanAcceptedTypeRejections covers the representative ways a value can
// fail the accepted type table: the wrong Go type, a uint64 too large for
// INT64, a float with a fractional part, NaN, Inf, and a FieldType the table
// has no case for at all.
func TestMapPlanAcceptedTypeRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field bigquery.FieldSchema
		value any
		want  string
	}{
		{name: "STRING given an int", field: bigquery.FieldSchema{Type: bigquery.StringFieldType}, value: 1, want: "needs a string"},
		{name: "INTEGER given a string", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: "1", want: "INTEGER column"},
		{
			name:  "INTEGER given a uint64 overflowing int64",
			field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			value: uint64(math.MaxUint64),
			want:  "overflows",
		},
		{
			name:  "INTEGER given a float with a fractional part",
			field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			value: 1.5,
			want:  "fractional part",
		},
		{name: "INTEGER given NaN", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: math.NaN(), want: "NaN"},
		{name: "INTEGER given +Inf", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: math.Inf(1), want: "does not fit"},
		{name: "INTEGER given -Inf", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: math.Inf(-1), want: "does not fit"},
		{name: "FLOAT given an int", field: bigquery.FieldSchema{Type: bigquery.FloatFieldType}, value: 1, want: "FLOAT column"},
		{name: "NUMERIC given a float64", field: bigquery.FieldSchema{Type: bigquery.NumericFieldType}, value: 1.5, want: "*big.Rat"},
		{name: "BOOLEAN given a string", field: bigquery.FieldSchema{Type: bigquery.BooleanFieldType}, value: "true", want: "BOOLEAN column"},
		{name: "TIMESTAMP given a string", field: bigquery.FieldSchema{Type: bigquery.TimestampFieldType}, value: "2026-01-01", want: "TIMESTAMP column"},
		{name: "DATE given a string", field: bigquery.FieldSchema{Type: bigquery.DateFieldType}, value: "2026-01-01", want: "DATE column"},
		{name: "TIME given a string", field: bigquery.FieldSchema{Type: bigquery.TimeFieldType}, value: "12:00:00", want: "TIME column"},
		{name: "DATETIME given a string", field: bigquery.FieldSchema{Type: bigquery.DateTimeFieldType}, value: "2026-01-01T00:00:00", want: "DATETIME column"},
		{name: "BYTES given a string", field: bigquery.FieldSchema{Type: bigquery.BytesFieldType}, value: "b", want: "BYTES column"},
		{name: "JSON given an int", field: bigquery.FieldSchema{Type: bigquery.JSONFieldType}, value: 1, want: "JSON column"},
		{
			name:  "RECORD given a string",
			field: bigquery.FieldSchema{Type: bigquery.RecordFieldType, Schema: bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}},
			value: "x",
			want:  "RECORD column",
		},
		{name: "a FieldType the table has no case for", field: bigquery.FieldSchema{Type: bigquery.FieldType("NOPE")}, value: "x", want: "cannot write"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			field := tt.field
			field.Name = "v"
			decl := mapDeclaration(t, bigquery.Schema{&field})
			_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err == nil {
				t.Fatalf("marshalRow() error = nil, want a rejection mentioning %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("marshalRow() error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

// TestMapPlanAcceptedTypesAcceptNamedTypes checks that marshalAccepted's
// reflect.Kind-based columns accept a named type built on the same underlying
// kind, not just the exact unnamed Go type TestMapPlanAcceptedTypes uses.
func TestMapPlanAcceptedTypesAcceptNamedTypes(t *testing.T) {
	t.Parallel()

	type Env string
	type Flag bool
	type Ratio float64
	type Count int
	type Blob []byte

	tests := []struct {
		name  string
		field bigquery.FieldSchema
		value any
		want  bigquery.Value
	}{
		{name: "STRING accepts a named string type", field: bigquery.FieldSchema{Type: bigquery.StringFieldType}, value: Env("prod"), want: "prod"},
		{name: "BOOLEAN accepts a named bool type", field: bigquery.FieldSchema{Type: bigquery.BooleanFieldType}, value: Flag(true), want: true},
		{name: "FLOAT accepts a named float type", field: bigquery.FieldSchema{Type: bigquery.FloatFieldType}, value: Ratio(1.5), want: float64(1.5)},
		{name: "INTEGER accepts a named int type", field: bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, value: Count(7), want: int64(7)},
		{name: "BYTES accepts a named []byte type", field: bigquery.FieldSchema{Type: bigquery.BytesFieldType}, value: Blob("b"), want: bigquery.Value([]byte("b"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			field := tt.field
			field.Name = "v"
			decl := mapDeclaration(t, bigquery.Schema{&field})
			row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err != nil {
				t.Fatalf("marshalRow() error = %v", err)
			}
			if !reflect.DeepEqual(row["v"], tt.want) {
				t.Errorf("row[v] = %#v, want %#v", row["v"], tt.want)
			}
		})
	}
}

// TestMapPlanStrictTypesRejectNamedOrCompatibleTypes checks that the columns
// marshalAccepted still dispatches by type assertion, rather than by
// reflect.Kind, keep refusing a named type built on the same underlying
// structure as the type they require, or a named map[string]any for RECORD.
func TestMapPlanStrictTypesRejectNamedOrCompatibleTypes(t *testing.T) {
	t.Parallel()

	type namedTimestamp time.Time
	type namedDate civil.Date
	type namedClock civil.Time
	type namedDateTime civil.DateTime
	type namedRat big.Rat
	type namedRow map[string]any

	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	day := civil.Date{Year: 2026, Month: time.July, Day: 28}
	clock := civil.Time{Hour: 12, Minute: 30}
	dt := civil.DateTime{Date: day, Time: clock}
	rat := big.NewRat(25, 2)

	tests := []struct {
		name  string
		field bigquery.FieldSchema
		value any
		want  string
	}{
		{name: "TIMESTAMP rejects a named time.Time type", field: bigquery.FieldSchema{Type: bigquery.TimestampFieldType}, value: namedTimestamp(at), want: "TIMESTAMP column"},
		{name: "DATE rejects a named civil.Date type", field: bigquery.FieldSchema{Type: bigquery.DateFieldType}, value: namedDate(day), want: "DATE column"},
		{name: "TIME rejects a named civil.Time type", field: bigquery.FieldSchema{Type: bigquery.TimeFieldType}, value: namedClock(clock), want: "TIME column"},
		{name: "DATETIME rejects a named civil.DateTime type", field: bigquery.FieldSchema{Type: bigquery.DateTimeFieldType}, value: namedDateTime(dt), want: "DATETIME column"},
		{
			name:  "NUMERIC rejects a *big.Rat-compatible named type",
			field: bigquery.FieldSchema{Type: bigquery.NumericFieldType},
			value: (*namedRat)(rat),
			want:  "*big.Rat",
		},
		{
			name:  "RECORD rejects a named map[string]any type",
			field: bigquery.FieldSchema{Type: bigquery.RecordFieldType, Schema: bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}},
			value: namedRow{"a": "x"},
			want:  "RECORD column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			field := tt.field
			field.Name = "v"
			decl := mapDeclaration(t, bigquery.Schema{&field})
			_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": tt.value}))
			if err == nil {
				t.Fatalf("marshalRow() error = nil, want a rejection mentioning %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("marshalRow() error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

// TestMapPlanRegisteredMarshalerNilInnerPointerBecomesNULL checks the IsValid
// guard in marshalScalar's registered Marshalers path: a **widget whose outer
// pointer is non-nil but whose inner *widget is nil must settle as NULL rather
// than panic on reflect.Value.Interface of an invalid Value.
func TestMapPlanRegisteredMarshalerNilInnerPointerBecomesNULL(t *testing.T) {
	t.Parallel()

	type widget struct{ Name string }
	m := MarshalFunc(bigquery.StringFieldType, func(w widget) (bigquery.Value, error) {
		return w.Name, nil
	})
	schema := bigquery.Schema{{Name: "v", Type: bigquery.StringFieldType}}
	decl := mapDeclaration(t, schema, m)

	var inner *widget
	outer := &inner
	row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": outer}))
	if err != nil {
		t.Fatalf("marshalRow() error = %v", err)
	}
	if got := row["v"]; got != nil {
		t.Errorf("row[v] = %#v, want NULL for a **widget whose inner *widget is nil", got)
	}
}

// TestMapPlanRegisteredMarshalerNilInnerPointerViolatesRequired checks that a
// REQUIRED column sees the same **widget-with-nil-inner value isNilAny must
// catch before marshalScalar's own IsValid guard turns it into a silent NULL.
func TestMapPlanRegisteredMarshalerNilInnerPointerViolatesRequired(t *testing.T) {
	t.Parallel()

	type widget struct{ Name string }
	m := MarshalFunc(bigquery.StringFieldType, func(w widget) (bigquery.Value, error) {
		return w.Name, nil
	})
	schema := bigquery.Schema{{Name: "v", Type: bigquery.StringFieldType, Required: true}}
	decl := mapDeclaration(t, schema, m)

	var inner *widget
	outer := &inner
	_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": outer}))
	if err == nil {
		t.Fatal("marshalRow() error = nil, want a REQUIRED violation for a **widget whose inner *widget is nil")
	}
}

// TestMapPlanRegisteredMarshalerNilInnerPointerInRepeatedIsRejected checks
// that a REPEATED column rejects a **widget element whose inner *widget is
// nil, the same as it rejects an outright nil element.
func TestMapPlanRegisteredMarshalerNilInnerPointerInRepeatedIsRejected(t *testing.T) {
	t.Parallel()

	type widget struct{ Name string }
	m := MarshalFunc(bigquery.StringFieldType, func(w widget) (bigquery.Value, error) {
		return w.Name, nil
	})
	schema := bigquery.Schema{{Name: "v", Type: bigquery.StringFieldType, Repeated: true}}
	decl := mapDeclaration(t, schema, m)

	type widgetPtr = *widget
	var inner widgetPtr
	outer := &inner
	_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"v": []any{outer}}))
	if err == nil {
		t.Fatal("marshalRow() error = nil, want a rejection for a REPEATED element whose inner *widget is nil")
	}
}

// TestMapPlanRecordColumn covers RECORD: a nested map[string]any converts
// recursively, a repeated RECORD converts each element, and a value that is
// not a map[string]any is rejected.
func TestMapPlanRecordColumn(t *testing.T) {
	t.Parallel()

	nested := bigquery.Schema{
		{Name: "A", Type: bigquery.StringFieldType},
		{Name: "B", Type: bigquery.IntegerFieldType},
	}

	t.Run("a nested map[string]any converts recursively", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "inner", Type: bigquery.RecordFieldType, Schema: nested}}
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{
			"inner": map[string]any{"A": "a", "B": int64(1)},
		}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		want := map[string]bigquery.Value{"A": "a", "B": int64(1)}
		if !reflect.DeepEqual(row["inner"], want) {
			t.Errorf("row[inner] = %#v, want %#v", row["inner"], want)
		}
	})

	t.Run("a repeated RECORD converts each element", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "items", Type: bigquery.RecordFieldType, Repeated: true, Schema: nested}}
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{
			"items": []any{
				map[string]any{"A": "a1", "B": int64(1)},
				map[string]any{"A": "a2", "B": int64(2)},
			},
		}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		want := []bigquery.Value{
			map[string]bigquery.Value{"A": "a1", "B": int64(1)},
			map[string]bigquery.Value{"A": "a2", "B": int64(2)},
		}
		if !reflect.DeepEqual(row["items"], want) {
			t.Errorf("row[items] = %#v, want %#v", row["items"], want)
		}
	})

	t.Run("a non-map value is rejected", func(t *testing.T) {
		t.Parallel()
		schema := bigquery.Schema{{Name: "inner", Type: bigquery.RecordFieldType, Schema: nested}}
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"inner": "not a map"}))
		if err == nil || !strings.Contains(err.Error(), "map[string]any") {
			t.Errorf("marshalRow() error = %v, want a rejection naming the required type", err)
		}
	})
}

// TestMapPlanRepeatedColumn covers REPEATED: a []any of scalars converts
// element by element, a nil element is rejected rather than treated as NULL,
// and a value that is not a []any is rejected.
func TestMapPlanRepeatedColumn(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "tags", Type: bigquery.StringFieldType, Repeated: true}}

	t.Run("a []any of scalars converts element by element", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		row, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"tags": []any{"a", "b"}}))
		if err != nil {
			t.Fatalf("marshalRow() error = %v", err)
		}
		want := []bigquery.Value{"a", "b"}
		if !reflect.DeepEqual(row["tags"], want) {
			t.Errorf("row[tags] = %#v, want %#v", row["tags"], want)
		}
	})

	t.Run("a nil element is rejected, not treated as NULL", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"tags": []any{"a", nil, "c"}}))
		if err == nil || !strings.Contains(err.Error(), "NULL element") {
			t.Errorf("marshalRow() error = %v, want a rejection of the nil element", err)
		}
	})

	t.Run("something other than []any is rejected", func(t *testing.T) {
		t.Parallel()
		decl := mapDeclaration(t, schema)
		_, err := decl.plan.marshalRow(reflect.ValueOf(map[string]any{"tags": []string{"a"}}))
		if err == nil || !strings.Contains(err.Error(), "[]any") {
			t.Errorf("marshalRow() error = %v, want a rejection naming the required type", err)
		}
	})
}

// TestMapRowBatching checks that Sink reads a map[string]any the same way it
// reads a struct row: a single map is one row, and a []map[string]any is a
// batch of its elements.
func TestMapRowBatching(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}

	t.Run("a single map[string]any is one row", func(t *testing.T) {
		t.Parallel()
		writer := newFakeWriter(t)
		s := mapTestSinker(t, schema, writer)
		n, err := s.Sink(t.Context(), map[string]any{"a": "one"})
		if err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		if n != 1 {
			t.Errorf("Sink() n = %d, want 1", n)
		}
		want := []map[string]bigquery.Value{{"a": "one"}}
		if got := appendedRows(writer); !reflect.DeepEqual(got, want) {
			t.Errorf("appended rows = %#v, want %#v", got, want)
		}
	})

	t.Run("[]map[string]any is a batch", func(t *testing.T) {
		t.Parallel()
		writer := newFakeWriter(t)
		s := mapTestSinker(t, schema, writer)
		n, err := s.Sink(t.Context(), []map[string]any{{"a": "one"}, {"a": "two"}})
		if err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		if n != 2 {
			t.Errorf("Sink() n = %d, want 2", n)
		}
		want := []map[string]bigquery.Value{{"a": "one"}, {"a": "two"}}
		if got := appendedRows(writer); !reflect.DeepEqual(got, want) {
			t.Errorf("appended rows = %#v, want %#v", got, want)
		}
	})
}

// TestDeclarationFromMetadataRejectsAnIncompleteMetadata checks the two ways
// DeclarationFromMetadata itself refuses to build a plan: a nil
// *bigquery.TableMetadata and one with an empty Schema. Both are carried on the
// Declaration and surfaced by NewSinker, the way a struct tag DeclarationOf
// could not parse is.
func TestDeclarationFromMetadataRejectsAnIncompleteMetadata(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "v", Type: bigquery.StringFieldType}}
	tests := []struct {
		name       string
		md         *bigquery.TableMetadata
		marshalers []*Marshalers
		want       string
	}{
		{name: "nil metadata", md: nil, want: "table metadata is nil"},
		{name: "empty schema", md: &bigquery.TableMetadata{}, want: "table metadata has no schema"},
		{
			name:       "a marshaler built from a nil function",
			md:         &bigquery.TableMetadata{Schema: schema},
			marshalers: []*Marshalers{MarshalFunc(bigquery.StringFieldType, (func(int) (bigquery.Value, error))(nil))},
			want:       "nil function",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			decl := DeclarationFromMetadata(tt.md, tt.marshalers...)
			if decl.err == nil || !strings.Contains(decl.err.Error(), tt.want) {
				t.Fatalf("DeclarationFromMetadata() decl.err = %v, want it to mention %q", decl.err, tt.want)
			}
			if _, err := NewSinker(newFakeWriter(t), decl); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewSinker() error = %v, want it to surface DeclarationFromMetadata's error mentioning %q", err, tt.want)
			}
		})
	}
}

// TestMapRowDoesNotFillIngestionColumnsButStillGetsARowID checks the two halves
// of RowFiller having no effect on a map row: the schema declares the same
// columns IngestionMetadata would, the map never sets them, and they come back
// NULL because nothing could have called FillRow on a map. Row.ID, which
// prepare generates independently of RowFiller, still comes back non-empty and
// distinct per row.
func TestMapRowDoesNotFillIngestionColumnsButStillGetsARowID(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType},
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
	}
	writer := newIDCapturingWriter(t)
	s := mapTestSinker(t, schema, writer)

	if _, err := s.Sink(t.Context(), []map[string]any{{"a": "one"}, {"a": "two"}}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	want := []map[string]bigquery.Value{
		{"a": "one", "_ingestion_at": nil, "_ingestion_id": nil, "_ingestion_row_id": nil},
		{"a": "two", "_ingestion_at": nil, "_ingestion_id": nil, "_ingestion_row_id": nil},
	}
	if got := appendedRows(writer.fakeWriter); !reflect.DeepEqual(got, want) {
		t.Errorf("appended rows = %#v, want %#v", got, want)
	}

	ids := writer.recordedIDs()
	if len(ids) != 2 {
		t.Fatalf("recorded %d row id(s), want 2", len(ids))
	}
	if ids[0] == "" || ids[1] == "" {
		t.Error("Row.ID was empty, want prepare to generate one regardless of RowFiller")
	}
	if ids[0] == ids[1] {
		t.Error("both rows got the same Row.ID, want a distinct id per row")
	}
}
