package bqsink

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"cloud.google.com/go/civil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func storageWriteSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "user_id", Type: bigquery.StringFieldType},
		{Name: "req", Type: bigquery.StringFieldType, Required: true},
		{Name: "count", Type: bigquery.IntegerFieldType},
		{Name: "rate", Type: bigquery.FloatFieldType},
		{Name: "flag", Type: bigquery.BooleanFieldType},
		{Name: "blob", Type: bigquery.BytesFieldType},
		{Name: "at", Type: bigquery.TimestampFieldType},
		{Name: "day", Type: bigquery.DateFieldType},
		{Name: "clock", Type: bigquery.TimeFieldType},
		{Name: "moment", Type: bigquery.DateTimeFieldType},
		{Name: "money", Type: bigquery.NumericFieldType},
		{Name: "huge", Type: bigquery.BigNumericFieldType},
		{Name: "doc", Type: bigquery.JSONFieldType},
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "inner", Type: bigquery.RecordFieldType, Schema: abSchema()},
	}
}

// TestRowDescriptorMapsTextualTypesToString checks the overrides that let one row
// rendering serve both transports. Without them NUMERIC and BIGNUMERIC would be
// bytes holding a BigDecimal, and DATETIME and TIME would be int64 in an encoding
// the client library does not expose.
func TestRowDescriptorMapsTextualTypesToString(t *testing.T) {
	t.Parallel()

	md, err := rowDescriptor(storageWriteSchema())
	if err != nil {
		t.Fatalf("rowDescriptor() error = %v", err)
	}

	want := map[string]protoreflect.Kind{
		"user_id": protoreflect.StringKind,
		"req":     protoreflect.StringKind,
		"count":   protoreflect.Int64Kind,
		"rate":    protoreflect.DoubleKind,
		"flag":    protoreflect.BoolKind,
		"blob":    protoreflect.BytesKind,
		"at":      protoreflect.Int64Kind,
		"day":     protoreflect.StringKind,
		"clock":   protoreflect.StringKind,
		"moment":  protoreflect.StringKind,
		"money":   protoreflect.StringKind,
		"huge":    protoreflect.StringKind,
		"doc":     protoreflect.StringKind,
		"tags":    protoreflect.StringKind,
		"inner":   protoreflect.MessageKind,
	}
	fields := md.Fields()
	if fields.Len() != len(want) {
		t.Fatalf("the descriptor has %d fields, want %d", fields.Len(), len(want))
	}
	for i := range fields.Len() {
		f := fields.Get(i)
		name := string(f.Name())
		expected, ok := want[name]
		if !ok {
			t.Errorf("unexpected proto field %q", name)
			continue
		}
		if f.Kind() != expected {
			t.Errorf("field %s kind = %s, want %s", name, f.Kind(), expected)
		}
	}
}

func TestRowDescriptorKeepsColumnNames(t *testing.T) {
	t.Parallel()

	md, err := rowDescriptor(storageWriteSchema())
	if err != nil {
		t.Fatalf("rowDescriptor() error = %v", err)
	}
	if f := md.Fields().ByName("user_id"); f == nil {
		t.Error("the descriptor has no field named user_id; the JSON keys would not match")
	}
}

// TestStorageWriteRowFitsTheDescriptor is the end-to-end check on the client side:
// the JSON bqsink renders has to be exactly what protojson can feed into the
// descriptor derived from the same schema.
func TestStorageWriteRowFitsTheDescriptor(t *testing.T) {
	t.Parallel()

	schema := storageWriteSchema()
	md, err := rowDescriptor(schema)
	if err != nil {
		t.Fatalf("rowDescriptor() error = %v", err)
	}

	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	row := map[string]bigquery.Value{
		"user_id": "u1",
		"req":     "needed",
		"count":   int64(42),
		"rate":    1.5,
		"flag":    true,
		"blob":    []byte("bytes"),
		"at":      at,
		"day":     civil.DateOf(at),
		"clock":   civil.TimeOf(at),
		"moment":  civil.DateTimeOf(at),
		"money":   big.NewRat(25, 2),
		"huge":    new(big.Rat).SetUint64(18446744073709551615),
		"doc":     `{"k":"v"}`,
		"tags":    []bigquery.Value{"a", "b"},
		"inner":   map[string]bigquery.Value{"A": "a", "B": int64(1)},
	}

	text, err := encodeStorageWriteRow(row, schema)
	if err != nil {
		t.Fatalf("encodeStorageWriteRow() error = %v", err)
	}
	message := dynamicpb.NewMessage(md)
	if err := protojson.Unmarshal(text, message); err != nil {
		t.Fatalf("protojson.Unmarshal(%s) error = %v", text, err)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	if len(data) == 0 {
		t.Error("the marshalled row is empty")
	}
}

func TestStorageWriteRendersTimestampAsMicroseconds(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	schema := bigquery.Schema{{Name: "at", Type: bigquery.TimestampFieldType}}

	storage, err := encodeStorageWriteRow(map[string]bigquery.Value{"at": at}, schema)
	if err != nil {
		t.Fatalf("encodeStorageWriteRow() error = %v", err)
	}
	if want := `{"at":1785241800000000}`; string(storage) != want {
		t.Errorf("Storage Write rendering = %s, want %s", storage, want)
	}

	load, err := encodeJSONRow(map[string]bigquery.Value{"at": at}, schema)
	if err != nil {
		t.Fatalf("encodeJSONRow() error = %v", err)
	}
	if want := `{"at":"2026-07-28T12:30:00Z"}`; string(load) != want {
		t.Errorf("load job rendering = %s, want %s", load, want)
	}
}

func TestRequiredColumnRejectsNull(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "req", Type: bigquery.StringFieldType, Required: true}}
	for _, encode := range []struct {
		name string
		fn   func(map[string]bigquery.Value, bigquery.Schema) ([]byte, error)
	}{
		{name: "load job", fn: encodeJSONRow},
		{name: "storage write", fn: encodeStorageWriteRow},
	} {
		t.Run(encode.name, func(t *testing.T) {
			t.Parallel()
			if _, err := encode.fn(map[string]bigquery.Value{"req": nil}, schema); err == nil {
				t.Fatal("error = nil, want a rejection of NULL in a REQUIRED column")
			}
		})
	}
}

func TestStorageWriteWriterOptionsPutBqsinkLast(t *testing.T) {
	t.Parallel()

	table := &bigquery.Table{ProjectID: "p", DatasetID: "d", TableID: "t"}
	md, err := rowDescriptor(abSchema())
	if err != nil {
		t.Fatalf("rowDescriptor() error = %v", err)
	}
	normalized, err := adapt.NormalizeDescriptor(md)
	if err != nil {
		t.Fatalf("NormalizeDescriptor() error = %v", err)
	}

	tests := []struct {
		name     string
		strategy *StorageWrite
		want     int
	}{
		{
			name:     "a new stream carries the type, destination and descriptor",
			strategy: &StorageWrite{},
			want:     3,
		},
		{
			name:     "an existing stream carries its name and the descriptor",
			strategy: &StorageWrite{StreamName: "projects/p/datasets/d/tables/t/streams/s"},
			want:     2,
		},
		{
			name: "the caller's options are kept alongside",
			strategy: &StorageWrite{WriterOptions: []managedwriter.WriterOption{
				managedwriter.WithTraceID("test"),
			}},
			want: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := len(tt.strategy.writerOptions(table, normalized)); got != tt.want {
				t.Errorf("options = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestJSONColumnDiffersBetweenTransports(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "doc", Type: bigquery.JSONFieldType}}
	row := map[string]bigquery.Value{"doc": `{"k":"v"}`}

	// A load job reads the column's value, so the text is embedded. The Storage
	// Write API's proto field is a string, so the text is quoted.
	load, err := encodeJSONRow(row, schema)
	if err != nil {
		t.Fatalf("encodeJSONRow() error = %v", err)
	}
	if want := `{"doc":{"k":"v"}}`; string(load) != want {
		t.Errorf("load job rendering = %s, want %s", load, want)
	}

	storage, err := encodeStorageWriteRow(row, schema)
	if err != nil {
		t.Fatalf("encodeStorageWriteRow() error = %v", err)
	}
	if want := `{"doc":"{\"k\":\"v\"}"}`; string(storage) != want {
		t.Errorf("Storage Write rendering = %s, want %s", storage, want)
	}
}

func TestJSONColumnRejectsInvalidText(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "doc", Type: bigquery.JSONFieldType}}
	if _, err := encodeJSONRow(map[string]bigquery.Value{"doc": "not json"}, schema); err == nil {
		t.Error("encodeJSONRow() error = nil, want a rejection of text that is not JSON")
	}
	if _, err := encodeJSONRow(map[string]bigquery.Value{"doc": 42}, schema); err == nil {
		t.Error("encodeJSONRow() error = nil, want a rejection of a non-text value")
	}
}
