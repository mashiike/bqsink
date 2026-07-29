package bqsink

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
)

// discardLogger is what a test hands to code that logs when the logging is not
// what the test is about.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// recorder keeps the records bqsink emitted, so that a test can look at what was
// logged rather than at where it went.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func recordingLogger() (*recorder, *slog.Logger) {
	r := &recorder{}
	return r, slog.New(recordingHandler{recorder: r})
}

// recordingHandler is one view of a recorder. Attributes added with Logger.With
// are held here, so that a record carries them the way a real handler's would.
type recordingHandler struct {
	recorder *recorder
	attrs    []slog.Attr
}

func (h recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordingHandler) Handle(_ context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	out.AddAttrs(h.attrs...)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(a)
		return true
	})
	h.recorder.mu.Lock()
	defer h.recorder.mu.Unlock()
	h.recorder.records = append(h.recorder.records, out)
	return nil
}

func (h recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	next = append(next, h.attrs...)
	next = append(next, attrs...)
	return recordingHandler{recorder: h.recorder, attrs: next}
}

func (h recordingHandler) WithGroup(string) slog.Handler { return h }

func (r *recorder) all() []slog.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

func (r *recorder) matching(level slog.Level, message string) []slog.Record {
	var found []slog.Record
	for _, rec := range r.all() {
		if rec.Level == level && rec.Message == message {
			found = append(found, rec)
		}
	}
	return found
}

// only returns the one record with the given level and message, failing the test
// when there is not exactly one.
func (r *recorder) only(t *testing.T, level slog.Level, message string) slog.Record {
	t.Helper()
	found := r.matching(level, message)
	if len(found) != 1 {
		t.Fatalf("%d record(s) of %s %q, want 1; logged: %s", len(found), level, message, r.summary())
	}
	return found[0]
}

// first returns the earliest record with the given level and message.
func (r *recorder) first(t *testing.T, level slog.Level, message string) slog.Record {
	t.Helper()
	found := r.matching(level, message)
	if len(found) == 0 {
		t.Fatalf("no record of %s %q; logged: %s", level, message, r.summary())
	}
	return found[0]
}

func (r *recorder) count(level slog.Level, message string) int {
	return len(r.matching(level, message))
}

// summary describes everything logged, for a failure message.
func (r *recorder) summary() string {
	records := r.all()
	if len(records) == 0 {
		return "(nothing)"
	}
	out := make([]string, len(records))
	for i, rec := range records {
		out[i] = rec.Level.String() + " " + rec.Message
	}
	return "[" + joinWithSemicolons(out) + "]"
}

func joinWithSemicolons(parts []string) string {
	var out string
	for i, p := range parts {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}

// stringsOf reads an attribute holding a list of column names.
func stringsOf(t *testing.T, rec slog.Record, key string) []string {
	t.Helper()
	var found []string
	var ok bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key != key {
			return true
		}
		names, is := a.Value.Any().([]string)
		if !is {
			t.Fatalf("attribute %q = %v of type %T, want a []string", key, a.Value, a.Value.Any())
		}
		found, ok = names, true
		return false
	})
	if !ok {
		t.Fatalf("no attribute %q on %s %q", key, rec.Level, rec.Message)
	}
	return found
}

func stringOf(t *testing.T, rec slog.Record, key string) string {
	t.Helper()
	value := valueOf(t, rec, key)
	if value.Kind() != slog.KindString {
		t.Fatalf("attribute %q = %v of kind %s, want a string", key, value, value.Kind())
	}
	return value.String()
}

func intOf(t *testing.T, rec slog.Record, key string) int64 {
	t.Helper()
	value := valueOf(t, rec, key)
	if value.Kind() != slog.KindInt64 {
		t.Fatalf("attribute %q = %v of kind %s, want an integer", key, value, value.Kind())
	}
	return value.Int64()
}

// valueOf reaches an attribute without formatting it, so that a helper can insist
// on its kind rather than accept whatever String() makes of it.
func valueOf(t *testing.T, rec slog.Record, key string) slog.Value {
	t.Helper()
	var found slog.Value
	var ok bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key != key {
			return true
		}
		found, ok = a.Value, true
		return false
	})
	if !ok {
		t.Fatalf("no attribute %q on %s %q", key, rec.Level, rec.Message)
	}
	return found
}

func TestWithLoggerRejectsNil(t *testing.T) {
	t.Parallel()

	if _, err := New[simpleRow](testClient(t), testRelation(), WithLogger(nil)); err == nil {
		t.Fatal("New() error = nil, want WithLogger to reject a nil logger")
	}
}

func TestEveryRecordNamesTheRelation(t *testing.T) {
	t.Parallel()

	rec, logger := recordingLogger()
	s := newTestSinker[simpleRow](t, &fakeTable{metadataErr: notFoundErr()}, WithLogger(logger))
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	records := rec.all()
	if len(records) == 0 {
		t.Fatal("nothing was logged, want at least the created table")
	}
	// The project comes from the client, so the attribute names the table in full
	// even though testRelation leaves it out.
	const want = "test-project.test_dataset.test_table"
	for _, record := range records {
		if got := stringOf(t, record, "relation"); got != want {
			t.Errorf("relation = %q on %s %q, want %q", got, record.Level, record.Message, want)
		}
	}
}

func TestMigrateLogsWhatItChanged(t *testing.T) {
	t.Parallel()

	t.Run("creating the table", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		s := newTestSinker[simpleRow](t, &fakeTable{metadataErr: notFoundErr()}, WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelInfo, "created the table")
		if got := stringsOf(t, record, "columns"); !slices.Equal(got, []string{"Name", "Count"}) {
			t.Errorf("columns = %v, want [Name Count]", got)
		}
	})

	t.Run("adding a column", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		fake := migratedTableFor(bigquery.Schema{{Name: "Name", Type: bigquery.StringFieldType}})
		s := newTestSinker[simpleRow](t, fake, WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelInfo, "brought the table's schema up to the declaration")
		if got := stringsOf(t, record, "added_columns"); !slices.Equal(got, []string{"Count"}) {
			t.Errorf("added_columns = %v, want [Count]", got)
		}
	})

	t.Run("dropping a column is a warning", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		fake := migratedTableFor(bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
			{Name: "legacy", Type: bigquery.StringFieldType},
		})
		s := newTestSinker[simpleRow](t, fake, WithMigration(SyncAllColumns{}), WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelWarn, "dropped columns, and the data they held is gone")
		if got := stringsOf(t, record, "columns"); !slices.Equal(got, []string{"legacy"}) {
			t.Errorf("columns = %v, want [legacy]", got)
		}
	})

	t.Run("a table that already agrees says so at debug level", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		fake := migratedTableFor(bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
		})
		s := newTestSinker[simpleRow](t, fake, WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		rec.only(t, slog.LevelDebug, "the table needs no change")
		for _, record := range rec.all() {
			if record.Level >= slog.LevelInfo {
				t.Errorf("logged %s %q, want nothing above debug when there is nothing to do",
					record.Level, record.Message)
			}
		}
	})
}

func TestStrategyLogsTheDifferenceItLeaves(t *testing.T) {
	t.Parallel()

	// The table has a column the declaration does not, which is the difference each
	// strategy treats differently.
	tableWithLegacy := func() *fakeTable {
		return migratedTableFor(bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
			{Name: "legacy", Type: bigquery.StringFieldType},
		})
	}

	t.Run("MigrationNone warns that it reconciles nothing", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		s := newTestSinker[simpleRow](t, tableWithLegacy(), WithMigration(MigrationNone{}), WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelWarn, "the table differs from the declaration and MigrationNone leaves it as it is")
		if got := stringsOf(t, record, "undeclared_columns"); !slices.Equal(got, []string{"legacy"}) {
			t.Errorf("undeclared_columns = %v, want [legacy]", got)
		}
	})

	t.Run("AppendNewColumns says which columns it keeps", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		s := newTestSinker[simpleRow](t, tableWithLegacy(), WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelInfo, "the table has columns the declaration does not, and AppendNewColumns keeps them")
		if got := stringsOf(t, record, "columns"); !slices.Equal(got, []string{"legacy"}) {
			t.Errorf("columns = %v, want [legacy]", got)
		}
	})

	t.Run("SyncAllColumns warns about an ignored column that differs", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		s := newTestSinker[simpleRow](t, tableWithLegacy(),
			WithMigration(SyncAllColumns{IgnoreColumns: []string{"legacy"}}),
			WithLogger(logger),
		)
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelWarn,
			"IgnoreColumns holds columns that differ from the declaration, and they are left as they are")
		if got := stringsOf(t, record, "columns"); !slices.Equal(got, []string{"legacy"}) {
			t.Errorf("columns = %v, want [legacy]", got)
		}
	})

	t.Run("an ignored column the table agrees on is not worth a record", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		fake := migratedTableFor(bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
		})
		s := newTestSinker[simpleRow](t, fake,
			WithMigration(SyncAllColumns{IgnoreColumns: []string{"legacy"}}),
			WithLogger(logger),
		)
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		for _, record := range rec.all() {
			if record.Level >= slog.LevelInfo {
				t.Errorf("logged %s %q, want nothing above debug", record.Level, record.Message)
			}
		}
	})
}

const retryMessage = "retrying after a failure a later attempt may get past"

func TestRetryLogsOnlyTheFailuresItSwallows(t *testing.T) {
	t.Parallel()

	t.Run("a failure the retry gets past is logged", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		writer := &flakyRowWriter{failAppends: 2, appendErr: unavailableErr()}
		s := newTestSinker[nestedRow](t, migratedTable(),
			WithMigration(AppendNewColumns{}),
			WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
			WithLogger(logger),
		)
		if err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		warnings := rec.matching(slog.LevelWarn, retryMessage)
		if len(warnings) != 2 {
			t.Fatalf("%d retry warning(s), want 2 for the two swallowed failures; logged: %s", len(warnings), rec.summary())
		}
		if got := stringOf(t, warnings[0], "operation"); got != "append" {
			t.Errorf("operation = %q, want %q", got, "append")
		}
		// The attempt named is the one that failed, not the one about to run, so the
		// two swallowed failures are attempts 1 and 2 of three.
		for i, warning := range warnings {
			if got, want := intOf(t, warning, "attempt"), int64(i+1); got != want {
				t.Errorf("attempt = %d on warning %d, want %d", got, i, want)
			}
		}
	})

	t.Run("the failure that is returned is not logged as well", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		writer := &flakyRowWriter{failAppends: 99, appendErr: unavailableErr()}
		s := newTestSinker[nestedRow](t, migratedTable(),
			WithMigration(AppendNewColumns{}),
			WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
			WithLogger(logger),
		)
		if err := s.Sink(t.Context(), nestedRow{}); err == nil {
			t.Fatal("Sink() error = nil, want the last failure")
		}
		// Every attempt but the last was followed by another, and the last one's
		// failure is the one Sink returned.
		if got := rec.count(slog.LevelWarn, retryMessage); got != migrateMaxRetries {
			t.Errorf("%d retry warning(s), want %d for %d attempts; logged: %s",
				got, migrateMaxRetries, migrateMaxRetries+1, rec.summary())
		}
	})

	t.Run("a failure that is not retried is not logged", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		writer := &flakyRowWriter{failAppends: 1, appendErr: forbiddenErr()}
		s := newTestSinker[nestedRow](t, migratedTable(),
			WithMigration(AppendNewColumns{}),
			WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
			WithLogger(logger),
		)
		if err := s.Sink(t.Context(), nestedRow{}); err == nil {
			t.Fatal("Sink() error = nil, want the permission failure")
		}
		if got := rec.count(slog.LevelWarn, retryMessage); got != 0 {
			t.Errorf("%d retry warning(s), want none; logged: %s", got, rec.summary())
		}
	})

	t.Run("Migrate names itself as the operation", func(t *testing.T) {
		t.Parallel()
		rec, logger := recordingLogger()
		fake := migratedTableFor(bigquery.Schema{{Name: "Name", Type: bigquery.StringFieldType}})
		fake.updateErrs = []error{etagErr(), nil}
		s := newTestSinker[simpleRow](t, fake, WithLogger(logger))
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
		record := rec.only(t, slog.LevelWarn, retryMessage)
		if got := stringOf(t, record, "operation"); got != "migrate" {
			t.Errorf("operation = %q, want %q", got, "migrate")
		}
	})
}

func TestTheWriterIsHandedTheLogger(t *testing.T) {
	t.Parallel()

	_, logger := recordingLogger()
	writer := &fakeRowWriter{}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
		WithLogger(logger),
	)
	if err := s.Sink(t.Context(), nestedRow{A: "a"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.openedLogger == nil {
		t.Error("Open was handed a nil logger, want the one the Sinker holds")
	}
}

// TestWithoutALoggerNothingIsLogged is not parallel: it replaces the default
// logger to prove bqsink does not write to it uninvited.
func TestWithoutALoggerNothingIsLogged(t *testing.T) {
	rec, logger := recordingLogger()
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })

	s := newTestSinker[simpleRow](t, &fakeTable{metadataErr: notFoundErr()})
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if got := rec.all(); len(got) != 0 {
		t.Errorf("%d record(s) reached the default logger, want none; logged: %s", len(got), rec.summary())
	}
}
