package bqsink

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Struct tags bqsink reads. Go's convention for a tag string is a concatenation of
// space-separated key:"value" pairs, so the physical layout of the table lives in
// keys of its own rather than crowding into the column's own tag.
//
//	type AccessLog struct {
//		Timestamp time.Time `bqsink:"timestamp,required" partition:"day"`
//		UserID    string    `bqsink:"user_id" cluster:"1"`
//		Amount    *big.Rat  `bqsink:"amount" description:"billed amount, including tax"`
//	}
const (
	// TagKey describes the column itself: its name and how its value is treated.
	TagKey = "bqsink"

	// PartitionTagKey makes the column the table's partitioning column. Its value
	// is the granularity, optionally followed by "require" to demand a partition
	// filter on every query.
	PartitionTagKey = "partition"

	// ClusterTagKey makes the column one of the table's clustering columns. Its
	// value is the position, counting from 1, since the order decides how well
	// BigQuery can prune.
	ClusterTagKey = "cluster"

	// DescriptionTagKey documents the column, or the table itself when it is on an
	// embedded TableMeta. It is a key of its own because a description may contain
	// the commas and spaces a comma-separated tag cannot.
	DescriptionTagKey = "description"

	// LabelsTagKey carries the table's labels as a "key=value,key=value" list. It
	// is only read from an embedded TableMeta, since a label describes the table
	// rather than any one column.
	LabelsTagKey = "labels"
)

// TableMeta lets a row type settle what describes the table as a whole, through
// tags on the embedded field, so that a description or a set of labels needs no
// BigQueryTableMetadata method.
//
// It contributes no column. Embed it as a direct field of the row type:
//
//	type AccessLog struct {
//		bqsink.TableMeta `description:"one row per request" labels:"team=data,env=prod"`
//
//		Timestamp time.Time `bqsink:"timestamp,required" partition:"day"`
//		UserID    string    `bqsink:"user_id"`
//	}
//
// Only DescriptionTagKey and LabelsTagKey are read here. Anything describing a
// column, including the column tag itself, is rejected rather than ignored, since
// TableMeta has no column to apply it to. Declaring the same thing here and in
// BigQueryTableMetadata is an error rather than one silently winning.
type TableMeta struct{}

// tableMetaTag is what an embedded TableMeta says about the table.
type tableMetaTag struct {
	description string
	labels      map[string]string
}

// tableMetaOf reads the tags on an embedded TableMeta.
//
// Only a direct field of the row type counts. A TableMeta reached through another
// embedded struct is left alone, so that what the table is can be read off the row
// type itself rather than assembled from the types it embeds.
func tableMetaOf(t reflect.Type) (tableMetaTag, error) {
	marker := reflect.TypeFor[TableMeta]()
	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.Anonymous || sf.Type != marker {
			continue
		}
		for _, key := range []string{TagKey, PartitionTagKey, ClusterTagKey} {
			if _, ok := sf.Tag.Lookup(key); ok {
				return tableMetaTag{}, fmt.Errorf(
					"the embedded TableMeta carries the %q tag, which describes a column and TableMeta is not one", key)
			}
		}
		out := tableMetaTag{description: sf.Tag.Get(DescriptionTagKey)}
		if v, ok := sf.Tag.Lookup(LabelsTagKey); ok {
			labels, err := parseLabels(v)
			if err != nil {
				return tableMetaTag{}, err
			}
			out.labels = labels
		}
		return out, nil
	}
	return tableMetaTag{}, nil
}

// parseLabels reads a "key=value,key=value" list.
//
// Neither separator needs escaping: BigQuery documents a label's key and value as
// holding only lowercase letters, digits, underscores and dashes, so a comma or an
// equals sign cannot appear inside one. Whether the characters are allowed is left
// to BigQuery, which owns that rule; what is checked here is the shape of the list.
// A value may be empty, which BigQuery allows, but a key may not.
func parseLabels(v string) (map[string]string, error) {
	labels := make(map[string]string)
	for _, pair := range strings.Split(v, ",") {
		key, value, ok := strings.Cut(pair, "=")
		switch {
		case !ok:
			return nil, fmt.Errorf("the %q tag holds %q, want key=value", LabelsTagKey, pair)
		case key == "":
			return nil, fmt.Errorf("the %q tag holds %q, whose key is empty", LabelsTagKey, pair)
		}
		if _, duplicate := labels[key]; duplicate {
			return nil, fmt.Errorf("the %q tag names %q twice", LabelsTagKey, key)
		}
		labels[key] = value
	}
	return labels, nil
}

// maxClusteringColumns is BigQuery's limit, confirmed by asking it to create a
// table with five: "5 clustering fields specified, exceeding the limit of 4".
const maxClusteringColumns = 4

// InferSchema derives a BigQuery schema from T's struct tags, honouring the
// given per-type overrides.
//
// It differs from bigquery.InferSchema in two ways that matter in practice:
// columns are NULLABLE by default, and the "bqsink" tag is read instead of
// "bigquery". Mark a column REQUIRED with `bqsink:",required"`.
//
// The tag's first element renames the column; an empty one keeps the Go field
// name verbatim, with no conversion to snake_case. A tag of `bqsink:"-"` drops
// the field, so it appears in neither the schema nor the rows written. Unexported
// fields are always dropped.
//
// An embedded struct's fields are promoted into the outer struct, following the
// rules of encoding/json: a shallower field hides a deeper one of the same name,
// an explicit tag breaks a tie at equal depth, an unresolved tie removes that one
// column while leaving the rest promoted, and the columns come out in field
// declaration order. Naming an embedded field in its tag makes it a column of its
// own rather than something to descend into. An embedded type with no exported
// fields, such as a sync.Mutex, therefore contributes no columns at all.
//
// The options after the name are:
//
//	required    the column is REQUIRED rather than NULLABLE
//	nullifzero  a zero value is written as NULL
//	record      a struct expands into a RECORD rather than becoming JSON
//	date        a time.Time becomes a DATE rather than a TIMESTAMP
//	datetime    a time.Time becomes a DATETIME
//	time        a time.Time becomes a TIME
//
// "required" and "nullifzero" cannot be combined, since a REQUIRED column cannot
// hold NULL. Neither can two options that name the column's type, so "record" and
// the three below exclude one another.
//
// "date", "datetime" and "time" each drop what the column does not record: the
// time of day, the UTC offset, and the date. They read the value's own location,
// so the calendar a column records is chosen by handing over a time.Time already
// in it, and no separate timezone option is needed:
//
//	Day time.Time `bqsink:"day,date"`  // time.Now().In(jst) records the Tokyo date
//
// They are the only options that change a column's type from the Go type's own,
// because dropping a component is the whole conversion: there is no rounding to
// choose and nothing that can fail. A conversion needing either, such as a float64
// written to an INTEGER column, belongs in a FieldMarshaler or MarshalFunc where
// the caller states the policy. Only time.Time takes them; a named type whose
// underlying type is time.Time does not, since bqsink cannot see through it.
//
// "nullifzero" decides what counts as zero the way the "omitzero" option of
// encoding/json/v2 does: through an IsZero method where the type has one, and by
// the zero Go value otherwise. That is what makes a zero time.Time recognisable.
// On a repeated column it means no elements, so both a nil and an empty slice
// become NULL; without it they become an empty array, which BigQuery keeps
// distinct from NULL.
//
// Separate tag keys describe the table rather than the column:
//
//	partition:"day"           partition by this column, by day
//	partition:"hour,require"  by hour, and demand a partition filter
//	cluster:"1"               the first clustering column
//	description:"..."         document the column
//
// Go types map to BigQuery types as follows.
//
//	STRING     string
//	BOOL       bool
//	INTEGER    int, int8, int16, int32, int64, uint8, uint16, uint32
//	FLOAT      float32, float64
//	BYTES      []byte
//	TIMESTAMP  time.Time
//	DATE       civil.Date, or a time.Time tagged "date"
//	TIME       civil.Time, or a time.Time tagged "time"
//	DATETIME   civil.DateTime, or a time.Time tagged "datetime"
//	NUMERIC    big.Rat, uint, uint64
//	JSON       a struct, a map with string keys, json.RawMessage, or any
//
// uint and uint64 become NUMERIC rather than INTEGER because BigQuery's INTEGER
// is INT64, which is signed and cannot hold the upper half of a uint64. BIGINT
// does not help, being an alias of INT64. The column is then no longer an integer
// type, so prefer int64 where the values allow it.
//
// A slice or array becomes a REPEATED field of its element type, except that a
// slice of bytes becomes BYTES. Pointers are followed, so *string is a NULLABLE
// STRING; a pointer does not by itself make a column NULLABLE, since that is
// already the default.
//
// A type that carries structure BigQuery has no column type for becomes JSON.
// That covers structs, maps with string keys, json.RawMessage and any. Keeping a
// struct out of a JSON column and expanding it into a RECORD takes the "record"
// option: `bqsink:"inner,record"`. JSON leaves the shape inside the column free,
// so adding a field to a nested struct needs no migration, while a RECORD keeps
// the columnar layout that lets BigQuery read one nested field without scanning
// the rest.
//
// A json.RawMessage is written through unchanged, since it already holds JSON
// text. Everything else is encoded with encoding/json, without escaping HTML, so
// that a URL stays readable rather than arriving full of &.
//
// A type that implements FieldMarshaler, or one registered through MarshalFunc,
// takes the column type it declares instead of any of the above.
//
// Types with no representation at all, including uint, uint64, uintptr, a map
// with non-string keys, channels and functions, produce an error. Give the row
// type a BigQueryTableMetadata method spelling the schema out for columns none of
// this can express.
func InferSchema[T any](marshalers ...*Marshalers) (bigquery.Schema, error) {
	plan, err := buildRowPlan(reflect.TypeFor[T](), joinMarshalers(marshalers))
	if err != nil {
		return nil, err
	}
	return plan.schema(), nil
}

// isRepeated reports whether t becomes a REPEATED field. A sequence of bytes
// does not: it becomes BYTES, matching bigquery.InferSchema.
func isRepeated(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		return !isByteSequence(t)
	}
	return false
}

func isByteSequence(t reflect.Type) bool {
	return t.Elem().Kind() == reflect.Uint8
}

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

type fieldTag struct {
	name       string
	required   bool
	nullIfZero bool
	record     bool
	skip       bool

	// timeType names the column a time.Time becomes when it is not the TIMESTAMP
	// it defaults to. It holds the option as written, which is a key of
	// timeColumnTypes.
	timeType string

	// The following come from tag keys of their own and describe the table.
	partition     bigquery.TimePartitioningType
	requireFilter bool
	cluster       int
	description   string
}

// parseFieldTags reads every tag key bqsink understands from one field.
func parseFieldTags(tag reflect.StructTag) (fieldTag, error) {
	f, err := parseTag(tag.Get(TagKey))
	if err != nil {
		return fieldTag{}, err
	}
	if f.skip {
		// A dropped field has no column, so nothing else about it applies.
		return f, nil
	}
	if v, ok := tag.Lookup(PartitionTagKey); ok {
		if err := f.parsePartition(v); err != nil {
			return fieldTag{}, err
		}
	}
	if v, ok := tag.Lookup(ClusterTagKey); ok {
		position, err := strconv.Atoi(v)
		if err != nil {
			return fieldTag{}, fmt.Errorf("the %q tag is %q, want a position counting from 1", ClusterTagKey, v)
		}
		if position < 1 || position > maxClusteringColumns {
			return fieldTag{}, fmt.Errorf("the %q tag is %d, want a position from 1 to %d",
				ClusterTagKey, position, maxClusteringColumns)
		}
		f.cluster = position
	}
	f.description = tag.Get(DescriptionTagKey)
	return f, nil
}

// parsePartition reads a granularity, optionally followed by "require".
func (f *fieldTag) parsePartition(v string) error {
	granularity, rest, _ := strings.Cut(v, ",")
	switch strings.ToLower(granularity) {
	case "", "day":
		f.partition = bigquery.DayPartitioningType
	case "hour":
		f.partition = bigquery.HourPartitioningType
	case "month":
		f.partition = bigquery.MonthPartitioningType
	case "year":
		f.partition = bigquery.YearPartitioningType
	default:
		return fmt.Errorf("the %q tag is %q, want day, hour, month or year", PartitionTagKey, granularity)
	}
	for rest != "" {
		var opt string
		opt, rest, _ = strings.Cut(rest, ",")
		switch strings.ToLower(opt) {
		case "":
		case "require":
			f.requireFilter = true
		default:
			return fmt.Errorf("the %q tag has the unknown option %q", PartitionTagKey, opt)
		}
	}
	return nil
}

func parseTag(s string) (fieldTag, error) {
	if s == "-" {
		return fieldTag{skip: true}, nil
	}
	name, opts, _ := strings.Cut(s, ",")
	tag := fieldTag{name: name}
	for opts != "" {
		var opt string
		opt, opts, _ = strings.Cut(opts, ",")
		switch opt {
		case "":
		case "required":
			tag.required = true
		case "nullifzero":
			tag.nullIfZero = true
		case "record":
			tag.record = true
		default:
			if _, ok := timeColumnTypes[opt]; !ok {
				return fieldTag{}, fmt.Errorf("unknown tag option %q", opt)
			}
			if tag.timeType != "" {
				return fieldTag{}, fmt.Errorf("the %q and %q options conflict: a column has one type", tag.timeType, opt)
			}
			tag.timeType = opt
		}
	}
	if tag.required && tag.nullIfZero {
		return fieldTag{}, errors.New(`the "required" and "nullifzero" options conflict: a REQUIRED column cannot hold NULL`)
	}
	if tag.record && tag.timeType != "" {
		return fieldTag{}, fmt.Errorf("the %q and %q options conflict: a column has one type", "record", tag.timeType)
	}
	return tag, nil
}
