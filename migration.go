package bqsink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConflictReason says why a difference between the declared schema and the real
// table cannot be reconciled.
type ConflictReason int

const (
	// ConflictType means the column's type differs. BigQuery does not allow a
	// column's type to be changed by patching the table.
	ConflictType ConflictReason = iota

	// ConflictRepeated means one side is REPEATED and the other is not.
	ConflictRepeated

	// ConflictRequired means the declaration asks for REQUIRED where the table has
	// NULLABLE. Only the opposite direction is allowed.
	ConflictRequired

	// ConflictNested means the fields inside a RECORD differ. bqsink reports this
	// instead of migrating it, so that it cannot pass unnoticed.
	ConflictNested
)

// String implements fmt.Stringer.
func (r ConflictReason) String() string {
	switch r {
	case ConflictType:
		return "type"
	case ConflictRepeated:
		return "repeated"
	case ConflictRequired:
		return "required"
	case ConflictNested:
		return "nested"
	}
	return fmt.Sprintf("ConflictReason(%d)", int(r))
}

// SchemaConflict describes a difference BigQuery cannot reconcile by patching
// the table's metadata, or one bqsink does not implement.
type SchemaConflict struct {
	// Name is the column's name.
	Name string

	// Reason says what kind of difference this is.
	Reason ConflictReason

	// Want is the column as declared.
	Want *bigquery.FieldSchema

	// Got is the column as the table has it.
	Got *bigquery.FieldSchema
}

// String implements fmt.Stringer.
func (c SchemaConflict) String() string {
	switch c.Reason {
	case ConflictType:
		return fmt.Sprintf("%s: declared %s, table has %s", c.Name, c.Want.Type, c.Got.Type)
	case ConflictRepeated:
		return fmt.Sprintf("%s: declared repeated=%t, table has repeated=%t", c.Name, c.Want.Repeated, c.Got.Repeated)
	case ConflictRequired:
		return fmt.Sprintf("%s: declared REQUIRED, table has NULLABLE", c.Name)
	case ConflictNested:
		return fmt.Sprintf("%s: RECORD fields differ, declared %s, table has %s",
			c.Name, fieldNames(c.Want.Schema), fieldNames(c.Got.Schema))
	}
	return c.Name
}

func fieldNames(schema bigquery.Schema) string {
	return "[" + strings.Join(namesOf(schema), " ") + "]"
}

// namesOf lists the columns' names, which both a conflict's message and a log
// record describe a schema by.
func namesOf(schema bigquery.Schema) []string {
	names := make([]string, len(schema))
	for i, f := range schema {
		names[i] = f.Name
	}
	return names
}

// SchemaDiff lists how a declared schema differs from a real table's.
type SchemaDiff struct {
	// Added holds columns the declaration has and the table lacks.
	Added bigquery.Schema

	// Removed names columns the table has and the declaration lacks.
	Removed []string

	// Relaxed names columns the table marks REQUIRED where the declaration says
	// NULLABLE. BigQuery allows this direction.
	Relaxed []string

	// Conflicts holds differences that cannot be reconciled.
	Conflicts []SchemaConflict
}

// Empty reports whether the declared schema and the table already agree.
func (d SchemaDiff) Empty() bool {
	return len(d.Added) == 0 && len(d.Removed) == 0 && len(d.Relaxed) == 0 && len(d.Conflicts) == 0
}

// mentions returns the named columns d has something to say about, so that a
// strategy can report which of the columns it leaves alone actually differ.
func (d SchemaDiff) mentions(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	named := make(map[string]bool, len(names))
	for _, name := range names {
		named[name] = true
	}
	seen := make(map[string]bool, len(names))
	var out []string
	add := func(name string) {
		if named[name] && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	for _, f := range d.Added {
		add(f.Name)
	}
	for _, name := range d.Removed {
		add(name)
	}
	for _, name := range d.Relaxed {
		add(name)
	}
	for _, c := range d.Conflicts {
		add(c.Name)
	}
	return out
}

// Without returns d with every mention of the named columns dropped, letting a
// strategy leave columns it does not manage alone.
func (d SchemaDiff) Without(names []string) SchemaDiff {
	if len(names) == 0 {
		return d
	}
	ignored := make(map[string]bool, len(names))
	for _, name := range names {
		ignored[name] = true
	}
	var out SchemaDiff
	for _, f := range d.Added {
		if !ignored[f.Name] {
			out.Added = append(out.Added, f)
		}
	}
	for _, name := range d.Removed {
		if !ignored[name] {
			out.Removed = append(out.Removed, name)
		}
	}
	for _, name := range d.Relaxed {
		if !ignored[name] {
			out.Relaxed = append(out.Relaxed, name)
		}
	}
	for _, c := range d.Conflicts {
		if !ignored[c.Name] {
			out.Conflicts = append(out.Conflicts, c)
		}
	}
	return out
}

// DiffSchema compares want, the declared schema, with got, the schema the table
// currently has.
//
// Adding and dropping columns is only resolved at the top level. A change inside
// a RECORD lands in Conflicts rather than being migrated.
func DiffSchema(want, got bigquery.Schema) SchemaDiff {
	inTable := make(map[string]*bigquery.FieldSchema, len(got))
	for _, f := range got {
		inTable[f.Name] = f
	}
	declared := make(map[string]bool, len(want))
	var diff SchemaDiff
	for _, w := range want {
		declared[w.Name] = true
		g, ok := inTable[w.Name]
		if !ok {
			diff.Added = append(diff.Added, w)
			continue
		}
		if reason, conflicted := conflictBetween(w, g); conflicted {
			diff.Conflicts = append(diff.Conflicts, SchemaConflict{Name: w.Name, Reason: reason, Want: w, Got: g})
			continue
		}
		if !w.Required && g.Required {
			diff.Relaxed = append(diff.Relaxed, w.Name)
		}
	}
	for _, g := range got {
		if !declared[g.Name] {
			diff.Removed = append(diff.Removed, g.Name)
		}
	}
	return diff
}

// conflictBetween reports whether reconciling the declared field w with the
// field g the table already has needs a change BigQuery does not allow, or one
// bqsink does not implement.
func conflictBetween(w, g *bigquery.FieldSchema) (ConflictReason, bool) {
	switch {
	case w.Type != g.Type:
		return ConflictType, true
	case w.Repeated != g.Repeated:
		return ConflictRepeated, true
	case w.Required && !g.Required:
		return ConflictRequired, true
	case w.Type == bigquery.RecordFieldType && !sameNestedSchema(w.Schema, g.Schema):
		return ConflictNested, true
	}
	return ConflictType, false
}

// sameNestedSchema reports whether two RECORD schemas describe the same fields.
// Names, types and modes are compared recursively; field order is not.
func sameNestedSchema(want, got bigquery.Schema) bool {
	if len(want) != len(got) {
		return false
	}
	inGot := make(map[string]*bigquery.FieldSchema, len(got))
	for _, f := range got {
		inGot[f.Name] = f
	}
	for _, w := range want {
		g, ok := inGot[w.Name]
		if !ok {
			return false
		}
		if w.Type != g.Type || w.Repeated != g.Repeated || w.Required != g.Required {
			return false
		}
		if w.Type == bigquery.RecordFieldType && !sameNestedSchema(w.Schema, g.Schema) {
			return false
		}
	}
	return true
}

// TableState describes the destination table as the migration found it.
type TableState struct {
	// Exists reports whether the table exists.
	Exists bool

	// Diff is how the declared schema differs from the table's. It is zero when
	// Exists is false.
	Diff SchemaDiff
}

// SchemaChange is what a MigrationStrategy asks bqsink to do.
type SchemaChange struct {
	// CreateTable asks for the table to be created. The other fields are ignored
	// when it is set, since a created table already matches the declaration.
	CreateTable bool

	// AddColumns holds columns to add. BigQuery only accepts NULLABLE and REPEATED
	// columns on an existing table.
	AddColumns bigquery.Schema

	// RelaxColumns names columns to turn from REQUIRED into NULLABLE.
	RelaxColumns []string

	// DropColumns names columns to drop with an ALTER TABLE statement, which
	// destroys the data they hold irreversibly. They are dropped after AddColumns
	// and RelaxColumns have been applied, so that a failure in between leaves the
	// table holding more than the declaration asks for rather than less.
	DropColumns []string
}

// Empty reports whether there is nothing to do.
func (c SchemaChange) Empty() bool {
	return !c.CreateTable && len(c.AddColumns) == 0 && len(c.RelaxColumns) == 0 && len(c.DropColumns) == 0
}

// MigrationStrategy decides what to do about the difference between the declared
// schema and the real table.
//
// Plan touches nothing: bqsink reads the table's state, asks the strategy what to
// change, and applies the answer. An implementation therefore needs no BigQuery
// access and can be tested on its own.
//
// logger is the one WithLogger settled on and is never nil. It is there for the
// one thing the answer cannot express: a difference the strategy decided not to
// reconcile. Since bqsink is for keeping a table in step with the declaration,
// leaving a difference alone is worth a record, and the SchemaChange returned no
// longer mentions it.
type MigrationStrategy interface {
	Plan(state TableState, logger *slog.Logger) (SchemaChange, error)
}

// MigrationNone leaves an existing table's schema untouched.
//
// It is not the default: AppendNewColumns is, since following the declaration is
// what bqsink is for. Choose this to write to a table something else owns.
type MigrationNone struct {
	// CreateIfMissing creates the table when it does not exist.
	CreateIfMissing bool
}

// Plan implements MigrationStrategy.
//
// It asks for no change beyond creating a missing table, but still reports
// conflicts, because writing to a table whose columns disagree with the
// declaration would fail anyway.
func (m MigrationNone) Plan(state TableState, logger *slog.Logger) (SchemaChange, error) {
	if !state.Exists {
		return SchemaChange{CreateTable: m.CreateIfMissing}, nil
	}
	if err := conflictError(state.Diff.Conflicts); err != nil {
		return SchemaChange{}, err
	}
	if !state.Diff.Empty() {
		logger.Warn("the table differs from the declaration and MigrationNone leaves it as it is",
			slog.Any("missing_columns", namesOf(state.Diff.Added)),
			slog.Any("undeclared_columns", state.Diff.Removed),
			slog.Any("columns_to_relax", state.Diff.Relaxed))
	}
	return SchemaChange{}, nil
}

// AppendNewColumns adds columns the declaration has and the table lacks, and
// relaxes REQUIRED columns the declaration marks NULLABLE. Columns the table has
// and the declaration lacks are left alone.
//
// AppendNewColumns{CreateIfMissing: true} is the default strategy. Neither change
// it makes destroys anything, which is why it is safe to have on by default; drops
// need SyncAllColumns, which has to be asked for.
type AppendNewColumns struct {
	// CreateIfMissing creates the table when it does not exist.
	CreateIfMissing bool
}

// Plan implements MigrationStrategy.
func (a AppendNewColumns) Plan(state TableState, logger *slog.Logger) (SchemaChange, error) {
	if !state.Exists {
		return SchemaChange{CreateTable: a.CreateIfMissing}, nil
	}
	if err := conflictError(state.Diff.Conflicts); err != nil {
		return SchemaChange{}, err
	}
	if len(state.Diff.Removed) > 0 {
		logger.Info("the table has columns the declaration does not, and AppendNewColumns keeps them",
			slog.Any("columns", state.Diff.Removed))
	}
	return SchemaChange{
		AddColumns:   state.Diff.Added,
		RelaxColumns: state.Diff.Relaxed,
	}, nil
}

// SyncAllColumns does what AppendNewColumns does and additionally drops columns
// the table has and the declaration lacks.
type SyncAllColumns struct {
	// IgnoreColumns names columns bqsink does not manage. They are neither
	// dropped nor reported as conflicts, which suits columns another system owns.
	IgnoreColumns []string

	// CreateIfMissing creates the table when it does not exist.
	CreateIfMissing bool
}

// Validate implements Validator.
func (s SyncAllColumns) Validate() error {
	for i, name := range s.IgnoreColumns {
		if name == "" {
			return fmt.Errorf("SyncAllColumns: IgnoreColumns[%d] is empty", i)
		}
	}
	return nil
}

// Plan implements MigrationStrategy.
func (s SyncAllColumns) Plan(state TableState, logger *slog.Logger) (SchemaChange, error) {
	if !state.Exists {
		return SchemaChange{CreateTable: s.CreateIfMissing}, nil
	}
	diff := state.Diff.Without(s.IgnoreColumns)
	if err := conflictError(diff.Conflicts); err != nil {
		return SchemaChange{}, err
	}
	if ignored := state.Diff.mentions(s.IgnoreColumns); len(ignored) > 0 {
		logger.Warn("IgnoreColumns holds columns that differ from the declaration, and they are left as they are",
			slog.Any("columns", ignored))
	}
	return SchemaChange{
		AddColumns:   diff.Added,
		RelaxColumns: diff.Relaxed,
		DropColumns:  diff.Removed,
	}, nil
}

func conflictError(conflicts []SchemaConflict) error {
	if len(conflicts) == 0 {
		return nil
	}
	parts := make([]string, len(conflicts))
	for i, c := range conflicts {
		parts[i] = c.String()
	}
	return fmt.Errorf("%w: %s", ErrSchemaConflict, strings.Join(parts, "; "))
}

// mergeSchema returns got with change's new columns appended and its relaxed
// columns turned NULLABLE. The order of the existing columns is preserved, which
// BigQuery requires of a schema patch.
func mergeSchema(got bigquery.Schema, change SchemaChange) bigquery.Schema {
	relax := make(map[string]bool, len(change.RelaxColumns))
	for _, name := range change.RelaxColumns {
		relax[name] = true
	}
	next := make(bigquery.Schema, 0, len(got)+len(change.AddColumns))
	for _, f := range got {
		copied := *f
		if relax[copied.Name] {
			copied.Required = false
		}
		next = append(next, &copied)
	}
	for _, f := range change.AddColumns {
		copied := *f
		next = append(next, &copied)
	}
	return next
}

// migrateMaxRetries caps how many times a concurrent change is retried, on top
// of the first attempt. gax deliberately leaves the retry count to the caller.
const migrateMaxRetries = 4

var migrateBackoff = gax.Backoff{
	Initial:    200 * time.Millisecond,
	Max:        5 * time.Second,
	Multiplier: 2,
}

// DefaultRetryPolicy returns the policy bqsink uses unless WithRetryPolicy
// replaces it: a failure that a later attempt could get past is retried up to
// four times, waiting between 200ms and 5s with jitter, and any other error is
// returned immediately.
//
// It covers both a concurrent change to the table during the migration and a transient
// failure while writing.
func DefaultRetryPolicy() gax.Retryer {
	return &attemptLimiter{
		retryer: gax.OnErrorFunc(migrateBackoff, isRetryable),
		max:     migrateMaxRetries,
	}
}

// attemptLimiter stops retrying after max retries, wrapping a Retryer that
// decides whether an error is retryable at all.
type attemptLimiter struct {
	retryer gax.Retryer
	retries int
	max     int
}

// Retry implements gax.Retryer.
func (l *attemptLimiter) Retry(err error) (time.Duration, bool) {
	if l.retries >= l.max {
		return 0, false
	}
	l.retries++
	return l.retryer.Retry(err)
}

func (s *Sinker) migrate(ctx context.Context) error {
	md, err := s.api.Metadata(ctx)
	var state TableState
	switch {
	case err == nil:
		state = TableState{Exists: true, Diff: DiffSchema(s.schema, md.Schema)}
		s.logger.DebugContext(ctx, "compared the declaration with the table",
			slog.Any("missing_columns", namesOf(state.Diff.Added)),
			slog.Any("undeclared_columns", state.Diff.Removed),
			slog.Any("columns_to_relax", state.Diff.Relaxed),
			slog.Int("conflicts", len(state.Diff.Conflicts)))
	case isNotFound(err):
		md = nil
		s.logger.DebugContext(ctx, "the table does not exist yet")
	default:
		return fmt.Errorf("bqsink: read metadata of %s: %w", s.relation, err)
	}
	change, err := s.strategy.Plan(state, s.logger)
	if err != nil {
		return fmt.Errorf("bqsink: migrate %s: %w", s.relation, err)
	}
	return s.apply(ctx, md, change)
}

func (s *Sinker) apply(ctx context.Context, md *bigquery.TableMetadata, change SchemaChange) error {
	if md == nil {
		if !change.CreateTable {
			return fmt.Errorf("bqsink: %s: %w", s.relation, ErrTableMissing)
		}
		return s.createTable(ctx)
	}
	if change.Empty() {
		s.logger.DebugContext(ctx, "the table needs no change")
		return nil
	}
	// Columns are added before any are dropped, so that a failure in between
	// leaves the table holding more than the declaration asks for rather than
	// less. The migration reports the failure either way.
	if err := s.patchSchema(ctx, md, change); err != nil {
		return err
	}
	if len(change.DropColumns) == 0 {
		return nil
	}
	return s.dropColumns(ctx, change.DropColumns)
}

func (s *Sinker) createTable(ctx context.Context) error {
	if err := s.api.Create(ctx, s.newTableMetadata()); err != nil {
		return fmt.Errorf("bqsink: create %s: %w", s.relation, err)
	}
	s.logger.InfoContext(ctx, "created the table", slog.Any("columns", namesOf(s.schema)))
	return nil
}

func (s *Sinker) newTableMetadata() *bigquery.TableMetadata {
	md := &bigquery.TableMetadata{}
	if s.metadata != nil {
		copied := *s.metadata
		md = &copied
	}
	md.Schema = s.schema
	clearReadOnlyTableMetadata(md)
	return md
}

// clearReadOnlyTableMetadata zeroes the fields BigQuery populates itself.
// DeclarationFromMetadata's natural input is a table's own fetched metadata,
// which carries them, and TableDefiner's GoDoc already promises they are
// ignored; Create is where that promise has to be kept, since
// bigquery.TableMetadata carries no separate type for what may be created
// from what may only be read.
func clearReadOnlyTableMetadata(md *bigquery.TableMetadata) {
	md.FullID = ""
	md.Type = ""
	md.CreationTime = time.Time{}
	md.LastModifiedTime = time.Time{}
	md.NumBytes = 0
	md.NumLongTermBytes = 0
	md.NumRows = 0
	md.SnapshotDefinition = nil
	md.CloneDefinition = nil
	md.StreamingBuffer = nil
	md.ETag = ""
}

func (s *Sinker) patchSchema(ctx context.Context, md *bigquery.TableMetadata, change SchemaChange) error {
	if len(change.AddColumns) == 0 && len(change.RelaxColumns) == 0 {
		return nil
	}
	if required := requiredNames(change.AddColumns); len(required) > 0 {
		return fmt.Errorf("%w: cannot add REQUIRED columns to the existing table %s: %s",
			ErrSchemaConflict, s.relation, strings.Join(required, ", "))
	}
	next := mergeSchema(md.Schema, change)
	if _, err := s.api.Update(ctx, bigquery.TableMetadataToUpdate{Schema: next}, md.ETag); err != nil {
		return fmt.Errorf("bqsink: update schema of %s: %w", s.relation, err)
	}
	s.logger.InfoContext(ctx, "brought the table's schema up to the declaration",
		slog.Any("added_columns", namesOf(change.AddColumns)),
		slog.Any("relaxed_columns", change.RelaxColumns))
	return nil
}

// dropColumns removes columns with an ALTER TABLE statement, which destroys their
// data irreversibly. Only SyncAllColumns asks for this, and only for columns the
// declaration no longer has and IgnoreColumns does not protect.
func (s *Sinker) dropColumns(ctx context.Context, names []string) error {
	drops := make([]string, len(names))
	for i, name := range names {
		if err := checkColumnName(name); err != nil {
			return fmt.Errorf("bqsink: refusing to drop from %s: %w", s.relation, err)
		}
		drops[i] = fmt.Sprintf("DROP COLUMN `%s`", name)
	}
	sql := fmt.Sprintf("ALTER TABLE %s %s", s.relation.quoted(), strings.Join(drops, ", "))
	if err := s.query.run(ctx, sql); err != nil {
		return fmt.Errorf("bqsink: drop columns [%s] from %s: %w",
			strings.Join(names, ", "), s.relation, err)
	}
	// Warn rather than Info: this is the one thing bqsink does that destroys data,
	// and there is no undoing it.
	s.logger.WarnContext(ctx, "dropped columns, and the data they held is gone",
		slog.Any("columns", names))
	return nil
}

// checkColumnName rejects anything outside the characters BigQuery allows in a
// column name, since the name is interpolated into a DDL statement.
func checkColumnName(name string) error {
	if name == "" {
		return errors.New("a column name is empty")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return fmt.Errorf("column name %q contains %q, which BigQuery does not allow", name, r)
		}
	}
	return nil
}

func requiredNames(schema bigquery.Schema) []string {
	var names []string
	for _, f := range schema {
		if f.Required {
			names = append(names, f.Name)
		}
	}
	return names
}

func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound
}

// isConcurrentChange reports whether err says another process changed the table
// between the moment its metadata was read and the moment the change was
// applied. Reading the metadata again and reapplying resolves it.
func isConcurrentChange(err error) bool {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	}
	return false
}

// isRetryable reports whether a later attempt could get past err: a concurrent
// change to the table, a rate limit, or a transient failure on BigQuery's side.
//
// Both transports are covered, since a load job reports over HTTP while the
// Storage Write API reports gRPC status codes.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if isConcurrentChange(err) {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted,
		codes.Internal, codes.Aborted:
		return true
	}
	return false
}
