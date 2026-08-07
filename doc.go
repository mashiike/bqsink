// Package bqsink writes rows to BigQuery and keeps the destination table's
// schema in sync with a schema declared in Go code.
//
// The declaration is the source of truth: the real table follows it, and bqsink
// never infers a schema from the data being written. What the table looks like is
// said by the row type and nowhere else — its struct tags, and its
// BigQueryTableMetadata method for what tags cannot express. No Option describes
// the table, since a row type carries the domain knowledge that gives its columns
// meaning, and two places to say what a table is means two answers to keep
// agreeing.
//
// # Writing rows
//
// Writing takes two things. A RowsWriter holds what writing depends on — the table,
// the connection, the transport, how a transient failure is retried — and a Sinker
// holds what none of that changes: the declaration, bringing the real table in line
// with it, and turning a row into columns.
//
//	w, err := (&bqsink.LoadJobs{}).NewWriter(client, relation)
//	if err != nil {
//		return err
//	}
//	defer w.Close(ctx)
//
//	s, err := bqsink.NewSinker(w, bqsink.DeclarationOf[AccessLog]())
//	if err != nil {
//		return err
//	}
//	n, err := s.Sink(ctx, logs)
//
// The declaration reaches NewSinker rather than being picked up from the first
// batch, so a mistake in it is reported before anything is written. Nothing contacts
// BigQuery until the first Sink, which is what reconciles the real table with the
// declaration and hands the writer the settled schema.
//
// Sink returns a non-nil error whenever n is fewer than the rows it was given, so
// that rows[n:] are exactly the ones that did not. What counts as done there is
// the writer's own promise: how many reached BigQuery for a writer promising
// delivery, and only how many reached its own buffer for one promising
// acceptance instead, such as a LoadJobsWriter with LoadJobs.FlushRows set —
// what becomes of those once a job carries them is for FlushRows or Close to
// report, not Sink. A Sinker itself buffers nothing between calls — the rows
// handed to one Sink are the batch — though the writer it hands them to may.
// Closing belongs to the writer, since that is what holds a connection, and a
// Sinker has nothing waiting.
//
// The Options settle how bqsink behaves around the declaration: what to do about a
// difference between it and the real table, and what gets logged. How rows travel and
// how a failed write is retried are settled on the writer instead.
//
// # Declaring the columns
//
// A row type's struct tags describe its columns, and the per-type overrides passed
// to DeclarationOf or DeclarationFromMetadata refine how their values are written.
//
// This differs from bigquery.InferSchema in two ways that matter in practice:
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
// # How Go types become columns
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
// Types with no representation at all, including uintptr, a map with non-string
// keys, channels and functions, produce an error. Give the row type a
// BigQueryTableMetadata method spelling the schema out for columns none of this
// can express.
package bqsink
