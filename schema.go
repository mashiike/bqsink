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
