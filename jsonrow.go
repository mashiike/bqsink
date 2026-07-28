package bqsink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// Scale limits BigQuery puts on its decimal types, used when a column does not
// state a scale of its own.
const (
	numericScale    = 9
	bigNumericScale = 38
)

// rowDialect says which transport a row is being rendered for. The two disagree
// on two columns: a load job reads a TIMESTAMP as RFC 3339 text and a JSON column
// as an embedded JSON value, while the Storage Write API's proto fields are an
// int64 of microseconds and a string holding JSON text.
type rowDialect int

const (
	loadJobDialect rowDialect = iota
	storageWriteDialect
)

// encodeJSONRow renders a row as the JSON object a load job reads.
//
// The schema decides how a value is rendered, not the Go type alone: a *big.Rat
// bound for BIGNUMERIC keeps more digits than one bound for NUMERIC. Rendering
// from the Go type alone would silently truncate the former.
func encodeJSONRow(row map[string]bigquery.Value, schema bigquery.Schema) ([]byte, error) {
	return encodeRow(row, schema, loadJobDialect)
}

// encodeStorageWriteRow renders a row as the JSON protojson feeds into the proto
// message the Storage Write API expects.
func encodeStorageWriteRow(row map[string]bigquery.Value, schema bigquery.Schema) ([]byte, error) {
	return encodeRow(row, schema, storageWriteDialect)
}

func encodeRow(row map[string]bigquery.Value, schema bigquery.Schema, dialect rowDialect) ([]byte, error) {
	object, err := jsonObject(row, schema, dialect)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(object); err != nil {
		return nil, fmt.Errorf("bqsink: encode row as JSON: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func jsonObject(row map[string]bigquery.Value, schema bigquery.Schema, dialect rowDialect) (map[string]any, error) {
	byName := make(map[string]*bigquery.FieldSchema, len(schema))
	for _, f := range schema {
		byName[f.Name] = f
	}
	object := make(map[string]any, len(row))
	for name, value := range row {
		field, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("bqsink: the schema has no column %q", name)
		}
		rendered, err := jsonValue(value, field, dialect)
		if err != nil {
			return nil, fmt.Errorf("bqsink: column %s: %w", name, err)
		}
		object[name] = rendered
	}
	return object, nil
}

func jsonValue(value bigquery.Value, field *bigquery.FieldSchema, dialect rowDialect) (any, error) {
	if value == nil {
		if field.Required {
			// Storage Write would otherwise fail at marshal time with only the
			// proto field's name to go on.
			return nil, fmt.Errorf("a REQUIRED column cannot be NULL")
		}
		return nil, nil
	}
	if field.Repeated {
		elements, ok := value.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("a repeated column needs a []bigquery.Value, got %T", value)
		}
		out := make([]any, len(elements))
		for i, element := range elements {
			rendered, err := jsonElement(element, field, dialect)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out[i] = rendered
		}
		return out, nil
	}
	return jsonElement(value, field, dialect)
}

func jsonElement(value bigquery.Value, field *bigquery.FieldSchema, dialect rowDialect) (any, error) {
	if value == nil {
		return nil, nil
	}
	if field.Type == bigquery.RecordFieldType {
		nested, ok := value.(map[string]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("a RECORD column needs a map[string]bigquery.Value, got %T", value)
		}
		return jsonObject(nested, field.Schema, dialect)
	}
	if field.Type == bigquery.JSONFieldType {
		return jsonColumnValue(value, dialect)
	}
	switch v := value.(type) {
	case time.Time:
		if dialect == storageWriteDialect {
			return v.UnixMicro(), nil
		}
		return v.Format(time.RFC3339Nano), nil
	case civil.Date:
		return v.String(), nil
	case civil.Time:
		return v.String(), nil
	case civil.DateTime:
		return v.String(), nil
	case *big.Rat:
		return decimalString(v, field)
	case big.Rat:
		return decimalString(&v, field)
	}
	return value, nil
}

// jsonColumnValue renders a JSON column. A load job reads the column's value as
// JSON, so the text is embedded rather than quoted; quoting it stores the text as
// a JSON string instead of the value it represents. The Storage Write API's proto
// field is a string and takes the text as is.
func jsonColumnValue(value bigquery.Value, dialect rowDialect) (any, error) {
	var text []byte
	switch v := value.(type) {
	case json.RawMessage:
		text = v
	case string:
		text = []byte(v)
	case []byte:
		text = v
	default:
		return nil, fmt.Errorf("a JSON column needs JSON text, got %T", value)
	}
	if len(text) == 0 {
		return nil, nil
	}
	if dialect == storageWriteDialect {
		return string(text), nil
	}
	if !json.Valid(text) {
		return nil, fmt.Errorf("a JSON column was given text that is not valid JSON: %s", text)
	}
	return json.RawMessage(text), nil
}

// decimalString renders a rational as the decimal text BigQuery reads. The
// encoding/json default is a fraction such as "25/2", which BigQuery rejects,
// so this cannot be left to the encoder.
func decimalString(r *big.Rat, field *bigquery.FieldSchema) (string, error) {
	if r == nil {
		return "", fmt.Errorf("a nil *big.Rat cannot be written")
	}
	if r.IsInt() {
		return r.Num().String(), nil
	}
	return r.FloatString(int(decimalScale(field))), nil
}

// decimalScale is the number of fractional digits a column keeps, taken from the
// column where it says so and from the type's limit otherwise.
func decimalScale(field *bigquery.FieldSchema) int64 {
	if field.Scale > 0 {
		return field.Scale
	}
	if field.Type == bigquery.BigNumericFieldType {
		return bigNumericScale
	}
	return numericScale
}
