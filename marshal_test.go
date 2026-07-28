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

	got, err := InferSchema[marshalerRow](jsonMarshalers())
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "Payload", Type: bigquery.JSONFieldType},
		{Name: "PtrCode", Type: bigquery.StringFieldType},
		{Name: "Many", Type: bigquery.JSONFieldType, Repeated: true},
		{Name: "External", Type: bigquery.JSONFieldType},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("InferSchema() mismatch\n got: %s\nwant: %s", formatSchema(got), formatSchema(want))
	}
}

func TestAnExternalTypeIsJSONEitherWay(t *testing.T) {
	t.Parallel()

	got, err := InferSchema[marshalerRow]()
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("InferSchema() returned %d columns, want 4", len(got))
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

	s, err := New[marshalerRow](testClient(t), testRelation(), WithMarshalers(
		MarshalFunc(bigquery.StringFieldType, func(e external) (bigquery.Value, error) {
			return "amount=" + e.Amount, nil
		}),
	))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := s.Schema()[3].Type; got != bigquery.StringFieldType {
		t.Errorf("External type = %s, want STRING from the registered marshaler", got)
	}
	row, err := s.toRow(marshalerRow{External: external{Amount: "9"}})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
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
	got, err := InferSchema[marshalerRow](jsonMarshalers(), override)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
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
	got, err := InferSchema[marshalerRow](first, second)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}
	if got[3].Type != bigquery.BytesFieldType {
		t.Errorf("External type = %s, want BYTES from the marshaler registered last", got[3].Type)
	}
}

func TestRecordDeclaringMarshalerIsRejected(t *testing.T) {
	t.Parallel()

	t.Run("through FieldMarshaler", func(t *testing.T) {
		t.Parallel()
		if _, err := InferSchema[recordDeclaringRow](); err == nil {
			t.Fatal("InferSchema() error = nil, want a rejection of the RECORD type")
		}
	})

	t.Run("through a registered marshaler", func(t *testing.T) {
		t.Parallel()
		bad := MarshalFunc(bigquery.RecordFieldType, func(e external) (bigquery.Value, error) {
			return nil, nil
		})
		if _, err := InferSchema[marshalerRow](bad); err == nil {
			t.Fatal("InferSchema() error = nil, want a rejection of the RECORD type")
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
			if _, err := New[simpleRow](testClient(t), testRelation(), tt.opt); err == nil {
				t.Fatal("New() should reject the option")
			}
		})
	}
}

func TestNewAppliesMarshalersToTheSchema(t *testing.T) {
	t.Parallel()

	s, err := New[marshalerRow](testClient(t), testRelation(), WithMarshalers(jsonMarshalers()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	schema := s.Schema()
	if len(schema) != 4 {
		t.Fatalf("Schema() returned %d columns, want 4", len(schema))
	}
	if schema[3].Type != bigquery.JSONFieldType {
		t.Errorf("External type = %s, want JSON from the registered marshaler", schema[3].Type)
	}
	if s.plan == nil {
		t.Error("plan = nil, want the row plan kept for writing values")
	}
}

func TestWithSchemaOverridesTheDerivedTypes(t *testing.T) {
	t.Parallel()

	explicit := bigquery.Schema{
		{Name: "Payload", Type: bigquery.StringFieldType},
		{Name: "PtrCode", Type: bigquery.StringFieldType},
		{Name: "Many", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "External", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
	}
	s, err := New[marshalerRow](testClient(t), testRelation(),
		WithMarshalers(jsonMarshalers()),
		WithSchema(explicit),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !reflect.DeepEqual(s.Schema(), explicit) {
		t.Errorf("Schema() = %s, want the explicit %s", formatSchema(s.Schema()), formatSchema(explicit))
	}
}

func TestWithSchemaMustCoverEveryWrittenColumn(t *testing.T) {
	t.Parallel()

	tooNarrow := bigquery.Schema{{Name: "Payload", Type: bigquery.JSONFieldType}}
	_, err := New[marshalerRow](testClient(t), testRelation(),
		WithMarshalers(jsonMarshalers()),
		WithSchema(tooNarrow),
	)
	if err == nil {
		t.Fatal("New() error = nil, want a rejection of the schema missing columns the struct writes")
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
