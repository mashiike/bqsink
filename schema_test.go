package bqsink

import (
	"encoding/json"
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type simpleRow struct {
	Name  string
	Count int64
}

type allTypesRow struct {
	Str      string
	Bool     bool
	Int      int
	Int64    int64
	Uint32   uint32
	Float64  float64
	Bytes    []byte
	Time     time.Time
	Date     civil.Date
	CivTime  civil.Time
	DateTime civil.DateTime
	Rat      *big.Rat
}

type taggedRow struct {
	ID       string `bqsink:"user_id,required"`
	Optional string `bqsink:"optional"`
	Renamed  int64  `bqsink:"count"`
	Skipped  string `bqsink:"-"`
	Kept     bool   `bqsink:",required"`
	hidden   string
}

type nestedRow struct {
	A string
	B int64
}

type repeatedRow struct {
	Tags  []string
	Blobs [][]byte
	Fixed [3]int64
}

// recordRow keeps RECORD columns, which needs the "record" option now that a
// struct becomes JSON by default.
type recordRow struct {
	Inner    nestedRow   `bqsink:"inner,record"`
	InnerPtr *nestedRow  `bqsink:"inner_ptr,record"`
	Records  []nestedRow `bqsink:"records,record"`
}

// jsonRow collects the types that carry structure BigQuery has no column type
// for, all of which become JSON.
type jsonRow struct {
	Struct   nestedRow
	Records  []nestedRow
	Map      map[string]string
	AnyMap   map[string]any
	Raw      json.RawMessage
	Anything any
}

type pointerRow struct {
	Str   *string
	Num   *int64
	Tags  *[]string
	Blobs *[]byte
}

type unsignedRow struct {
	U8   uint8
	U16  uint16
	U32  uint32
	U    uint
	U64  uint64
	UPtr *uint64
}

type unsupportedRow struct {
	Bad uintptr
}

type badMapKeyRow struct {
	Bad map[int]string
}

type recordOnANonStructRow struct {
	Bad string `bqsink:"bad,record"`
}

type unknownOptionRow struct {
	A string `bqsink:"a,nope"`
}

type noExportedRow struct {
	hidden string
}

func TestInferSchema(t *testing.T) {
	t.Parallel()

	nested := bigquery.Schema{
		{Name: "A", Type: bigquery.StringFieldType},
		{Name: "B", Type: bigquery.IntegerFieldType},
	}

	tests := []struct {
		name string
		fn   func(...*Marshalers) (bigquery.Schema, error)
		want bigquery.Schema
	}{
		{
			name: "untagged fields are NULLABLE and keep the Go field name",
			fn:   InferSchema[simpleRow],
			want: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
			},
		},
		{
			name: "Go types map to BigQuery types",
			fn:   InferSchema[allTypesRow],
			want: bigquery.Schema{
				{Name: "Str", Type: bigquery.StringFieldType},
				{Name: "Bool", Type: bigquery.BooleanFieldType},
				{Name: "Int", Type: bigquery.IntegerFieldType},
				{Name: "Int64", Type: bigquery.IntegerFieldType},
				{Name: "Uint32", Type: bigquery.IntegerFieldType},
				{Name: "Float64", Type: bigquery.FloatFieldType},
				{Name: "Bytes", Type: bigquery.BytesFieldType},
				{Name: "Time", Type: bigquery.TimestampFieldType},
				{Name: "Date", Type: bigquery.DateFieldType},
				{Name: "CivTime", Type: bigquery.TimeFieldType},
				{Name: "DateTime", Type: bigquery.DateTimeFieldType},
				{Name: "Rat", Type: bigquery.NumericFieldType},
			},
		},
		{
			name: "tags rename, require and skip fields, and unexported fields drop out",
			fn:   InferSchema[taggedRow],
			want: bigquery.Schema{
				{Name: "user_id", Type: bigquery.StringFieldType, Required: true},
				{Name: "optional", Type: bigquery.StringFieldType},
				{Name: "count", Type: bigquery.IntegerFieldType},
				{Name: "Kept", Type: bigquery.BooleanFieldType, Required: true},
			},
		},
		{
			name: "slices and arrays are REPEATED, byte sequences are BYTES",
			fn:   InferSchema[repeatedRow],
			want: bigquery.Schema{
				{Name: "Tags", Type: bigquery.StringFieldType, Repeated: true},
				{Name: "Blobs", Type: bigquery.BytesFieldType, Repeated: true},
				{Name: "Fixed", Type: bigquery.IntegerFieldType, Repeated: true},
			},
		},
		{
			name: "uint and uint64 become NUMERIC, narrower unsigned types stay INTEGER",
			fn:   InferSchema[unsignedRow],
			want: bigquery.Schema{
				{Name: "U8", Type: bigquery.IntegerFieldType},
				{Name: "U16", Type: bigquery.IntegerFieldType},
				{Name: "U32", Type: bigquery.IntegerFieldType},
				{Name: "U", Type: bigquery.NumericFieldType},
				{Name: "U64", Type: bigquery.NumericFieldType},
				{Name: "UPtr", Type: bigquery.NumericFieldType},
			},
		},
		{
			name: "structures with no BigQuery column type of their own become JSON",
			fn:   InferSchema[jsonRow],
			want: bigquery.Schema{
				{Name: "Struct", Type: bigquery.JSONFieldType},
				{Name: "Records", Type: bigquery.JSONFieldType, Repeated: true},
				{Name: "Map", Type: bigquery.JSONFieldType},
				{Name: "AnyMap", Type: bigquery.JSONFieldType},
				{Name: "Raw", Type: bigquery.JSONFieldType},
				{Name: "Anything", Type: bigquery.JSONFieldType},
			},
		},
		{
			name: `the "record" option expands a struct into a RECORD instead`,
			fn:   InferSchema[recordRow],
			want: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: nested},
				{Name: "inner_ptr", Type: bigquery.RecordFieldType, Schema: nested},
				{Name: "records", Type: bigquery.RecordFieldType, Repeated: true, Schema: nested},
			},
		},
		{
			name: "pointers are followed to their element type",
			fn:   InferSchema[pointerRow],
			want: bigquery.Schema{
				{Name: "Str", Type: bigquery.StringFieldType},
				{Name: "Num", Type: bigquery.IntegerFieldType},
				{Name: "Tags", Type: bigquery.StringFieldType, Repeated: true},
				{Name: "Blobs", Type: bigquery.BytesFieldType},
			},
		},
		{
			name: "a pointer type parameter is followed to its struct",
			fn:   InferSchema[*simpleRow],
			want: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.fn()
			if err != nil {
				t.Fatalf("InferSchema() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InferSchema() mismatch\n got: %s\nwant: %s", formatSchema(got), formatSchema(tt.want))
			}
		})
	}
}

func TestInferSchemaError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(...*Marshalers) (bigquery.Schema, error)
	}{
		{name: "not a struct", fn: InferSchema[int]},
		{name: "uintptr, which is not data", fn: InferSchema[unsupportedRow]},
		{name: "a map with non-string keys", fn: InferSchema[badMapKeyRow]},
		{name: `the "record" option on a non-struct`, fn: InferSchema[recordOnANonStructRow]},
		{name: "an unknown tag option", fn: InferSchema[unknownOptionRow]},
		{name: "no exported fields", fn: InferSchema[noExportedRow]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.fn()
			if err == nil {
				t.Fatalf("InferSchema() error = nil, want an error (schema %s)", formatSchema(got))
			}
		})
	}
}

func TestParseTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		want    fieldTag
		wantErr bool
	}{
		{name: "empty tag", tag: "", want: fieldTag{}},
		{name: "name only", tag: "user_id", want: fieldTag{name: "user_id"}},
		{name: "name and required", tag: "user_id,required", want: fieldTag{name: "user_id", required: true}},
		{name: "required without a name", tag: ",required", want: fieldTag{required: true}},
		{name: "nullifzero", tag: "a,nullifzero", want: fieldTag{name: "a", nullIfZero: true}},
		{name: "record", tag: "a,record", want: fieldTag{name: "a", record: true}},
		{
			name: "record and nullifzero together",
			tag:  "a,record,nullifzero",
			want: fieldTag{name: "a", record: true, nullIfZero: true},
		},
		{name: "skip", tag: "-", want: fieldTag{skip: true}},
		{name: "an unknown option", tag: "a,nope", wantErr: true},
		{name: "required and nullifzero conflict", tag: "a,required,nullifzero", wantErr: true},
		{name: "a trailing comma is ignored", tag: "a,", want: fieldTag{name: "a"}},
		{name: "an empty option is ignored", tag: "a,,required", want: fieldTag{name: "a", required: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTag(tt.tag)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseTag(%q) error = nil, want an error", tt.tag)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTag(%q) error = %v", tt.tag, err)
			}
			if got != tt.want {
				t.Errorf("parseTag(%q) = %+v, want %+v", tt.tag, got, tt.want)
			}
		})
	}
}
