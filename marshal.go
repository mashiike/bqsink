package bqsink

import (
	"fmt"
	"maps"
	"reflect"

	"cloud.google.com/go/bigquery"
)

// FieldMarshaler lets a Go type declare the BigQuery column it becomes,
// overriding what bqsink would otherwise derive from the Go type alone.
//
// Implementing it settles both halves of the mapping at once:
// BigQueryFieldType says what the column is in the schema, and
// MarshalBigQueryValue says what is written into it. A struct that would
// otherwise become a RECORD can therefore be written as a JSON, STRING or BYTES
// column instead.
//
//	type Payload struct{ Data map[string]string }
//
//	func (Payload) BigQueryFieldType() bigquery.FieldType { return bigquery.JSONFieldType }
//
//	func (p Payload) MarshalBigQueryValue() (bigquery.Value, error) {
//		b, err := json.Marshal(p.Data)
//		return string(b), err
//	}
//
// Either a value or a pointer receiver works. Use Marshalers instead for types
// whose definition is out of reach, such as those from another package.
//
// RECORD is not accepted, because its nested schema cannot be derived from a
// field type alone; spell such a column out in BigQueryTableMetadata.
type FieldMarshaler interface {
	// BigQueryFieldType returns the type of the column this value becomes.
	BigQueryFieldType() bigquery.FieldType

	// MarshalBigQueryValue returns the value to write into that column.
	MarshalBigQueryValue() (bigquery.Value, error)
}

// Marshalers is a list of per-type overrides, built with MarshalFunc and passed
// to DeclarationOf or DeclarationFromMetadata.
//
// Despite the name it is not a collection of FieldMarshaler. FieldMarshaler is
// implemented by a type on its own behalf; Marshalers registers mappings from the
// outside, for types whose definition cannot be changed. A registered mapping
// wins over a type's own FieldMarshaler, since the caller asked for it
// explicitly.
//
// A nil *Marshalers is equivalent to an empty list.
type Marshalers struct {
	byType   map[reflect.Type]*typeMarshaler
	buildErr error
}

type typeMarshaler struct {
	fieldType bigquery.FieldType
	marshal   func(any) (bigquery.Value, error)
}

// MarshalFunc constructs a type-specific marshaler that writes values of type T
// into a column of fieldType, converting them with fn.
//
// T is inferred from fn, so it never has to be written out. It must not be a
// pointer type: a mapping registered for T already covers a *T field, and
// registering *T instead would never be found. Registering the same type twice
// keeps the mapping registered last.
//
// RECORD is not a usable fieldType, because its nested schema cannot be derived
// from a field type alone; spell such a column out in BigQueryTableMetadata.
//
// A problem with the arguments is reported when DeclarationOf or
// DeclarationFromMetadata evaluates the declaration, since a constructor cannot
// return an error and stay usable inline; it surfaces from NewSinker as the
// Declaration's own error.
func MarshalFunc[T any](fieldType bigquery.FieldType, fn func(T) (bigquery.Value, error)) *Marshalers {
	goType := reflect.TypeFor[T]()
	if goType.Kind() == reflect.Pointer {
		return &Marshalers{buildErr: fmt.Errorf(
			"bqsink: MarshalFunc was given the pointer type %s; register %s instead, which also covers %s fields",
			goType, goType.Elem(), goType)}
	}
	if fn == nil {
		return &Marshalers{buildErr: fmt.Errorf("bqsink: MarshalFunc for %s was given a nil function", goType)}
	}
	return &Marshalers{byType: map[reflect.Type]*typeMarshaler{
		goType: {
			fieldType: fieldType,
			marshal: func(v any) (bigquery.Value, error) {
				typed, ok := v.(T)
				if !ok {
					return nil, fmt.Errorf("the marshaler for %s was given a %T", goType, v)
				}
				return fn(typed)
			},
		},
	}}
}

// err reports a problem recorded while building the list.
func (s *Marshalers) err() error {
	if s == nil {
		return nil
	}
	return s.buildErr
}

// merge copies other's mappings in, letting the incoming ones win.
func (s *Marshalers) merge(other *Marshalers) {
	if other == nil {
		return
	}
	if other.buildErr != nil && s.buildErr == nil {
		s.buildErr = other.buildErr
	}
	if len(other.byType) == 0 {
		return
	}
	if s.byType == nil {
		s.byType = make(map[reflect.Type]*typeMarshaler, len(other.byType))
	}
	maps.Copy(s.byType, other.byType)
}

// lookup finds the mapping for t, following pointers so that a mapping
// registered for T also covers a *T field.
func (s *Marshalers) lookup(t reflect.Type) (*typeMarshaler, bool) {
	if s == nil || s.byType == nil {
		return nil, false
	}
	m, ok := s.byType[deref(t)]
	return m, ok
}

// joinMarshalers folds a list into one, ignoring nil entries.
func joinMarshalers(marshalers []*Marshalers) *Marshalers {
	var joined *Marshalers
	for _, m := range marshalers {
		if m == nil {
			continue
		}
		if joined == nil {
			joined = &Marshalers{}
		}
		joined.merge(m)
	}
	return joined
}

// fieldMarshalerOf reports whether values of t declare their own column, reaching
// the method set through a zero value so that both receiver forms are found.
func fieldMarshalerOf(t reflect.Type) (FieldMarshaler, bool) {
	m, ok := reflect.New(deref(t)).Interface().(FieldMarshaler)
	return m, ok
}

func marshalViaFieldMarshaler(fv reflect.Value) (bigquery.Value, error) {
	v := derefValue(fv)
	if !v.IsValid() {
		return nil, nil
	}
	if m, ok := v.Interface().(FieldMarshaler); ok {
		return m.MarshalBigQueryValue()
	}
	if v.CanAddr() {
		if m, ok := v.Addr().Interface().(FieldMarshaler); ok {
			return m.MarshalBigQueryValue()
		}
	}
	ptr := reflect.New(v.Type())
	ptr.Elem().Set(v)
	m, ok := ptr.Interface().(FieldMarshaler)
	if !ok {
		return nil, fmt.Errorf("%s does not implement FieldMarshaler", v.Type())
	}
	return m.MarshalBigQueryValue()
}
