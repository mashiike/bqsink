package bqsink

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

// payload would become a RECORD on its own, but declares a JSON column.
type payload struct {
	Data map[string]string
}

func (payload) BigQueryFieldType() bigquery.FieldType { return bigquery.JSONFieldType }

func (p payload) MarshalBigQueryValue() (bigquery.Value, error) {
	b, err := json.Marshal(p.Data)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// code declares its column through a pointer receiver.
type code struct {
	value string
}

func (*code) BigQueryFieldType() bigquery.FieldType { return bigquery.StringFieldType }

func (c *code) MarshalBigQueryValue() (bigquery.Value, error) { return c.value, nil }

// recordDeclaring asks for a type bqsink cannot derive a nested schema for.
type recordDeclaring struct{}

func (recordDeclaring) BigQueryFieldType() bigquery.FieldType { return bigquery.RecordFieldType }

func (recordDeclaring) MarshalBigQueryValue() (bigquery.Value, error) { return nil, nil }

// external stands for a type from another package: no methods of our own.
type external struct {
	Amount string
}

type marshalerRow struct {
	Payload  payload
	PtrCode  *code
	Many     []payload
	External external
}

type recordDeclaringRow struct {
	Bad recordDeclaring
}

func jsonMarshalers() *Marshalers {
	return MarshalFunc(bigquery.JSONFieldType, func(e external) (bigquery.Value, error) {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	})
}

func TestFieldMarshalerShapesTheSchema(t *testing.T) {
	t.Parallel()

	got, err := inferSchema[marshalerRow](jsonMarshalers())
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "Payload", Type: bigquery.JSONFieldType},
		{Name: "PtrCode", Type: bigquery.StringFieldType},
		{Name: "Many", Type: bigquery.JSONFieldType, Repeated: true},
		{Name: "External", Type: bigquery.JSONFieldType},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("inferSchema() mismatch\n got: %s\nwant: %s", formatSchema(got), formatSchema(want))
	}
}

func TestAnExternalTypeIsJSONEitherWay(t *testing.T) {
	t.Parallel()

	got, err := inferSchema[marshalerRow]()
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("inferSchema() returned %d columns, want 4", len(got))
	}
	// Without a marshaler the struct still becomes JSON, since that is the default
	// for a type carrying structure. Registering one settles how it is encoded.
	if got[3].Type != bigquery.JSONFieldType {
		t.Errorf("External type = %s, want JSON by default", got[3].Type)
	}
	if got[0].Type != bigquery.JSONFieldType {
		t.Errorf("Payload type = %s, want JSON from its own FieldMarshaler", got[0].Type)
	}
}

func TestRegisteredMarshalerChangesHowAStructIsEncoded(t *testing.T) {
	t.Parallel()

	s, err := testSinker[marshalerRow](t, WithMarshalers(
		MarshalFunc(bigquery.StringFieldType, func(e external) (bigquery.Value, error) {
			return "amount=" + e.Amount, nil
		}),
	))
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if got := s.schema[3].Type; got != bigquery.StringFieldType {
		t.Errorf("External type = %s, want STRING from the registered marshaler", got)
	}
	row, err := s.plan.marshalRow(reflect.ValueOf(marshalerRow{External: external{Amount: "9"}}))
	if err != nil {
		t.Fatalf("marshalRow() error = %v", err)
	}
	if got := row["External"]; got != "amount=9" {
		t.Errorf("row[External] = %#v, want the registered conversion's output", got)
	}
}

func TestRegisteredMarshalerBeatsFieldMarshaler(t *testing.T) {
	t.Parallel()

	override := MarshalFunc(bigquery.StringFieldType, func(p payload) (bigquery.Value, error) {
		return "overridden", nil
	})
	got, err := inferSchema[marshalerRow](jsonMarshalers(), override)
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	if got[0].Type != bigquery.StringFieldType {
		t.Errorf("Payload type = %s, want STRING from the registered marshaler", got[0].Type)
	}
}

func TestMarshalersRegisteredLastWins(t *testing.T) {
	t.Parallel()

	first := MarshalFunc(bigquery.StringFieldType, func(e external) (bigquery.Value, error) {
		return "first", nil
	})
	second := MarshalFunc(bigquery.BytesFieldType, func(e external) (bigquery.Value, error) {
		return []byte("second"), nil
	})
	got, err := inferSchema[marshalerRow](first, second)
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	if got[3].Type != bigquery.BytesFieldType {
		t.Errorf("External type = %s, want BYTES from the marshaler registered last", got[3].Type)
	}
}

func TestRecordDeclaringMarshalerIsRejected(t *testing.T) {
	t.Parallel()

	t.Run("through FieldMarshaler", func(t *testing.T) {
		t.Parallel()
		if _, err := inferSchema[recordDeclaringRow](); err == nil {
			t.Fatal("inferSchema() error = nil, want a rejection of the RECORD type")
		}
	})

	t.Run("through a registered marshaler", func(t *testing.T) {
		t.Parallel()
		bad := MarshalFunc(bigquery.RecordFieldType, func(e external) (bigquery.Value, error) {
			return nil, nil
		})
		if _, err := inferSchema[marshalerRow](bad); err == nil {
			t.Fatal("inferSchema() error = nil, want a rejection of the RECORD type")
		}
	})
}

func TestMarshalFuncConverts(t *testing.T) {
	t.Parallel()

	m := jsonMarshalers()
	one, ok := m.lookup(reflect.TypeFor[external]())
	if !ok {
		t.Fatal("lookup() found no mapping for the registered type")
	}
	if one.fieldType != bigquery.JSONFieldType {
		t.Errorf("fieldType = %s, want JSON", one.fieldType)
	}

	got, err := one.marshal(external{Amount: "12.5"})
	if err != nil {
		t.Fatalf("marshal() error = %v", err)
	}
	if want := `{"Amount":"12.5"}`; got != want {
		t.Errorf("marshal() = %v, want %s", got, want)
	}

	if _, err := one.marshal("not an external"); err == nil {
		t.Error("marshal() error = nil, want a rejection of the wrong type")
	}
}

func TestMarshalersLookupFollowsPointers(t *testing.T) {
	t.Parallel()

	m := jsonMarshalers()
	if _, ok := m.lookup(reflect.TypeFor[*external]()); !ok {
		t.Error("lookup(*T) found nothing, want the mapping registered for T")
	}
	if _, ok := m.lookup(reflect.TypeFor[string]()); ok {
		t.Error("lookup() found a mapping for an unregistered type")
	}
}

func TestNilMarshalersIsEmpty(t *testing.T) {
	t.Parallel()

	var m *Marshalers
	if _, ok := m.lookup(reflect.TypeFor[external]()); ok {
		t.Error("lookup() on a nil *Marshalers found something, want nothing")
	}
	if joinMarshalers([]*Marshalers{nil, nil}) != nil {
		t.Error("joinMarshalers() of only nils should be nil")
	}
}

func TestWithMarshalersRejectsAnEmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{name: "no argument", opt: WithMarshalers()},
		{name: "only nils", opt: WithMarshalers(nil, nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testSinker[simpleRow](t, tt.opt); err == nil {
				t.Fatal("testSinker() should reject the option")
			}
		})
	}
}

func TestNewAppliesMarshalersToTheSchema(t *testing.T) {
	t.Parallel()

	s, err := testSinker[marshalerRow](t, WithMarshalers(jsonMarshalers()))
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	schema := s.schema
	if len(schema) != 4 {
		t.Fatalf("schema has %d columns, want 4", len(schema))
	}
	if schema[3].Type != bigquery.JSONFieldType {
		t.Errorf("External type = %s, want JSON from the registered marshaler", schema[3].Type)
	}
	if s.plan == nil {
		t.Error("plan = nil, want the row plan kept for writing values")
	}
}

// spelledOutRow writes the same fields as marshalerRow and spells its schema out,
// which is how a row type takes full control of its columns.
type spelledOutRow marshalerRow

func spelledOutSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "Payload", Type: bigquery.StringFieldType},
		{Name: "PtrCode", Type: bigquery.StringFieldType},
		{Name: "Many", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "External", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
	}
}

func (spelledOutRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Schema: spelledOutSchema()}
}

func TestASpelledOutSchemaOverridesTheDerivedTypes(t *testing.T) {
	t.Parallel()

	s, err := testSinker[spelledOutRow](t, WithMarshalers(jsonMarshalers()))
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if want := spelledOutSchema(); !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want the spelled out %s", formatSchema(s.schema), formatSchema(want))
	}
}

// tooNarrowRow leaves out columns its fields would write, which New has to catch
// rather than let BigQuery reject the first row.
type tooNarrowRow marshalerRow

func (tooNarrowRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Schema: bigquery.Schema{{Name: "Payload", Type: bigquery.JSONFieldType}},
	}
}

func TestASpelledOutSchemaMustCoverEveryWrittenColumn(t *testing.T) {
	t.Parallel()

	_, err := testSinker[tooNarrowRow](t, WithMarshalers(jsonMarshalers()))
	if err == nil {
		t.Fatal("testSinker() error = nil, want a rejection of the schema missing columns the struct writes")
	}
}

func TestMarshalFuncPropagatesTheConversionError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("conversion failed")
	m := MarshalFunc(bigquery.StringFieldType, func(e external) (bigquery.Value, error) {
		return nil, sentinel
	})
	one, ok := m.lookup(reflect.TypeFor[external]())
	if !ok {
		t.Fatal("lookup() found no mapping")
	}
	if _, err := one.marshal(external{}); !errors.Is(err, sentinel) {
		t.Errorf("marshal() error = %v, want the sentinel", err)
	}
}
