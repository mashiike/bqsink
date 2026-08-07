package bqsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
)

// Row is one row on its way to BigQuery.
type Row struct {
	// ID identifies the row. It is what a transport names the row by when it has
	// something to say about it, and it is the value the _ingestion_row_id column
	// gets when the row type fills that column in.
	//
	// It plays no part in reporting what could not be written: the count a
	// WriteResult returns is what says that.
	ID string

	// Values are the columns to write.
	Values map[string]bigquery.Value
}

// RowsWriter sends rows to a single BigQuery table.
//
// A writer holds everything writing depends on: the table it writes to, the
// connection it goes through, how a transient failure is retried, and the rows a
// buffering transport has not sent yet. A Sinker holds what none of that changes —
// the declaration, reconciling the table with it, and turning rows into columns —
// so that the two are settled apart from one another.
//
// An implementation must be safe for concurrent use.
type RowsWriter interface {
	// Relation names the table this writer writes to.
	//
	// It is the writer's own answer rather than something it repeats back, so a
	// writer bound to a stream that already exists reports the table that stream
	// belongs to.
	Relation() Relation

	// Client returns the BigQuery client to reconcile the table through, or nil
	// when the writer is not connected to BigQuery at all.
	//
	// A nil client leaves the real table unreadable, so NewSinker refuses every
	// migration strategy but MigrationNone.
	Client() *bigquery.Client

	// BindSchema hands over the declared schema, once it has been reconciled with
	// the real table and before the first WriteRows. It is called once.
	//
	// A transport that derives something from the schema, such as a proto
	// descriptor, does it here and keeps it for the writer's lifetime.
	BindSchema(ctx context.Context, schema bigquery.Schema) error

	// WriteRows hands rows over and returns what will say whether they landed.
	//
	// A non-nil error means the rows were not taken at all. Otherwise they are the
	// writer's, and a writer that buffers may keep them past this call.
	WriteRows(ctx context.Context, rows []Row) (WriteResult, error)

	// Close releases what the writer holds, having settled the rows it still has.
	// Rows a WriteResult was promised for but which were never sent are sent here,
	// so closing is the last chance to decide their fate.
	Close(ctx context.Context) error
}

// WriteResult says what the writer that returned it is promising about the rows
// of that call, and the promise is not the same for every writer.
//
// A writer that sends rows itself and waits for BigQuery to accept them, such as
// StorageWriter, promises delivery: its WriteResult is not resolved until Wait
// is called, and n then says how many of the rows landed.
//
// A writer that instead buffers rows of its own, such as LoadJobsWriter with
// LoadJobs.FlushRows set, promises acceptance: its WriteResult is already
// resolved by the time WriteRows returns, and n says how many rows were taken
// into the buffer, not how many have landed. What becomes of them once a job
// carries them is FlushRows's or Close's to report, not this WriteResult's.
//
// A LoadJobsWriter with FlushRows unset submits a job for every call's rows on
// the spot, so its WriteResult promises delivery too, already resolved with
// that job's outcome by the time WriteRows returns.
//
// Whichever it promises, a WriteResult that reports fewer rows than were placed
// in its care always comes with a non-nil error, whether those rows were handed
// to WriteRows or held in a buffer FlushRows submits.
type WriteResult interface {
	// Wait returns what the WriteResult above promises. A result already
	// resolved returns it at once; a writer that defers delivery, such as
	// StorageWriter, has Wait do the waiting.
	//
	// Cancelling ctx before a deferred result resolves reports that
	// cancellation rather than the rows' actual fate, and does not take the rows
	// back: they may still land.
	Wait(ctx context.Context) (n int, err error)
}

// LoggerBindable is implemented by a writer with something to log. NewSinker
// hands it the logger WithLogger settled on, already carrying the relation.
type LoggerBindable interface {
	BindLogger(logger *slog.Logger)
}

// Flusher is implemented by a writer that buffers rows of its own, letting a
// caller send what it holds without waiting for enough rows to arrive on
// their own.
type Flusher interface {
	// FlushRows submits the rows buffered so far as one job and returns a
	// WriteResult already resolved with the outcome.
	FlushRows(ctx context.Context) (WriteResult, error)
}

var _ Flusher = (*LoadJobsWriter)(nil)

// ResolvedResult returns a WriteResult that has already settled, which is what a
// transport writing its rows before it returns hands back.
func ResolvedResult(n int, err error) WriteResult {
	return resolvedResult{n: n, err: err}
}

type resolvedResult struct {
	n   int
	err error
}

func (r resolvedResult) Wait(context.Context) (int, error) {
	return r.n, r.err
}

// Stager puts the rows somewhere a load job can read them, instead of uploading
// them with the job itself.
//
// Staging through Cloud Storage suits large batches: the upload is a plain object
// write that can be retried on its own, and the load job then reads from a URI
// rather than carrying the data.
type Stager interface {
	// Stage writes rows and returns the URI a load job should read. The returned
	// cleanup, when not nil, removes what Stage created and is called once the
	// load job has finished, whether or not it succeeded.
	Stage(ctx context.Context, rows []byte) (uri string, cleanup func(context.Context) error, err error)
}

// LoadJobs writes rows by rendering them as newline delimited JSON and submitting
// a BigQuery load job. It is the settings a LoadJobsWriter is made from.
//
// A load job is all or nothing: the rows of one job either all land or, having
// been retried under RetryPolicy, none do. Rows are never left half written.
//
// Rows are uploaded with the load job itself unless Staging is set. Set it to
// bqgcs.Staging to put them in Cloud Storage first, which suits large batches.
type LoadJobs struct {
	// Staging, when set, writes the rows through a Stager and has the load job
	// read them from there instead of carrying them itself.
	Staging Stager

	// RetryPolicy decides how a load job that failed in a way a later attempt
	// could get past is retried. The zero value means DefaultRetryPolicy; set it
	// to a policy of your own to change that, and note that returning a bare
	// gax.OnErrorFunc places no limit on the number of attempts.
	//
	// It is called once per load job, because a gax.Retryer carries the state of
	// its backoff and cannot be reused.
	RetryPolicy func() gax.Retryer

	// FlushRows gathers rows across calls into a buffer and submits them as one
	// load job once this many are held, rather than submitting a job per
	// WriteRows. A table's daily job quota is what makes that worth doing:
	// writing a row at a time without it submits a load job each time.
	//
	// The zero value submits a job per WriteRows, and that call's WriteResult
	// reports the job's own outcome.
	//
	// With FlushRows set, WriteRows itself only ever reports that the rows were
	// taken into the buffer, not that a job has run. A submission triggered by
	// the buffer reaching this many rows, or one a later call to FlushRows or
	// Close makes, is reported by whichever of those methods makes it, not by
	// the WriteResult of the WriteRows call that filled the buffer.
	//
	// A buffer reaching this many rows is submitted by the call that filled it,
	// which is what keeps the rows held here bounded, and FlushRows or Close
	// submits what is left. The call that fills the buffer blocks until that
	// job finishes, under RetryPolicy, before returning.
	FlushRows int
}

func (w *LoadJobs) retryPolicy() func() gax.Retryer {
	if w.RetryPolicy == nil {
		return DefaultRetryPolicy
	}
	return w.RetryPolicy
}

// Validate implements Validator.
func (w *LoadJobs) Validate() error {
	if w.FlushRows < 0 {
		return fmt.Errorf("LoadJobs: FlushRows is %d, which is not a number of rows", w.FlushRows)
	}
	if v, ok := w.Staging.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("LoadJobs: %w", err)
		}
	}
	return nil
}

// NewWriter returns a writer that loads rows into the table relation names.
//
// It does not contact BigQuery: a load job is the first thing that does, and the
// schema it carries arrives with BindSchema. An empty ProjectID on relation is
// filled in from the client.
func (w *LoadJobs) NewWriter(client *bigquery.Client, relation Relation) (*LoadJobsWriter, error) {
	if client == nil {
		return nil, errors.New("bqsink: LoadJobs.NewWriter: client is nil")
	}
	if err := relation.validate(); err != nil {
		return nil, err
	}
	if relation.ProjectID == "" {
		relation.ProjectID = client.Project()
	}
	if err := w.Validate(); err != nil {
		return nil, fmt.Errorf("bqsink: %w", err)
	}
	table := relation.table(client)
	var loader jobLoader = tableLoader{table: table}
	if w.Staging != nil {
		loader = stagedLoader{table: table, stager: w.Staging}
	}
	return &LoadJobsWriter{
		relation:    relation,
		client:      client,
		loader:      loader,
		retryPolicy: w.retryPolicy(),
		flushRows:   w.FlushRows,
		logger:      slog.New(slog.DiscardHandler),
	}, nil
}

// LoadJobsWriter writes rows to one BigQuery table with load jobs.
//
// Its WriteRows never defers what it reports: with LoadJobs.FlushRows unset, it
// submits a load job for the call's own rows on the spot and reports that job's
// outcome. With FlushRows set, it instead reports that the rows were accepted
// into a buffer held here, and what becomes of them once a job carries them is
// FlushRows's or Close's to report, not WriteRows's.
//
// A buffer reaching FlushRows is submitted by the call that filled it, which is
// what keeps the rows held here bounded. A failure of that submission is kept
// until FlushRows or Close next runs, rather than handed to any particular
// caller.
type LoadJobsWriter struct {
	relation    Relation
	client      *bigquery.Client
	loader      jobLoader
	retryPolicy func() gax.Retryer
	flushRows   int

	mu      sync.Mutex
	logger  *slog.Logger
	schema  bigquery.Schema
	buf     []Row
	pending []error
	closed  bool

	// wg lets Close wait for a submission still in flight rather than return
	// before it lands. See .claude/rules/transports.md for the Add/Wait
	// ordering this depends on.
	wg sync.WaitGroup
}

// Relation implements RowsWriter.
func (w *LoadJobsWriter) Relation() Relation { return w.relation }

// Client implements RowsWriter.
func (w *LoadJobsWriter) Client() *bigquery.Client { return w.client }

// BindLogger implements LoggerBindable.
func (w *LoadJobsWriter) BindLogger(logger *slog.Logger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logger = logger
}

// BindSchema implements RowsWriter. The schema is what a load job declares, so
// nothing can be written before it arrives.
func (w *LoadJobsWriter) BindSchema(_ context.Context, schema bigquery.Schema) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(schema) == 0 {
		return errors.New("bqsink: LoadJobsWriter: the schema has no columns")
	}
	if w.schema != nil {
		return errors.New("bqsink: LoadJobsWriter: a schema is already bound")
	}
	w.schema = schema
	return nil
}

// WriteRows implements RowsWriter.
//
// With LoadJobs.FlushRows unset, it submits a load job for these rows on the
// spot, under RetryPolicy, and the WriteResult it returns is already resolved
// with that job's outcome.
//
// With FlushRows set, it appends the rows to the buffer and returns a
// WriteResult resolved with their acceptance: len(rows) and a nil error,
// whether or not the append goes on to submit a job. When appending fills the
// buffer to FlushRows, this call also submits it on the spot, under
// RetryPolicy, and blocks until that job finishes before returning, but a
// failure of that submission is kept for FlushRows or Close to report rather
// than reflected in the WriteResult returned here.
func (w *LoadJobsWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	if len(rows) == 0 {
		return ResolvedResult(0, nil), nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, fmt.Errorf("bqsink: LoadJobsWriter: %w", ErrWriterClosed)
	}
	if w.schema == nil {
		w.mu.Unlock()
		return nil, errors.New("bqsink: LoadJobsWriter: no schema is bound yet")
	}
	schema, logger := w.schema, w.logger
	if w.flushRows == 0 {
		w.mu.Unlock()
		if err := w.load(ctx, rows, schema, logger); err != nil {
			return ResolvedResult(0, err), nil
		}
		return ResolvedResult(len(rows), nil), nil
	}
	w.buf = append(w.buf, rows...)
	var batch []Row
	if len(w.buf) >= w.flushRows {
		batch = w.buf
		w.buf = nil
		w.wg.Add(1)
	}
	w.mu.Unlock()
	if batch != nil {
		defer w.wg.Done()
		if err := w.load(ctx, batch, schema, logger); err != nil {
			w.mu.Lock()
			w.pending = append(w.pending, fmt.Errorf("bqsink: %d row(s) did not land: %w", len(batch), err))
			w.mu.Unlock()
		}
	}
	return ResolvedResult(len(rows), nil), nil
}

// FlushRows implements Flusher. It submits whatever rows the buffer holds as
// one load job, under RetryPolicy, and returns a WriteResult already resolved
// with that job's outcome: n is how many of the rows this call itself sent
// landed, which is 0 when the job fails.
//
// A submission WriteRows made on its own, by filling the buffer to FlushRows,
// is not reported when it happens: that call's own WriteResult already
// reported acceptance. Its outcome is kept until the next call to FlushRows or
// to Close, which folds it into the error returned here and then clears it, so
// it is reported exactly once — and only there: a caller that lets this
// WriteResult go without calling Wait on it loses that outcome for good, even
// though the rows behind it were never this call's own to report. Unlike
// Close, FlushRows does not wait for a submission still in flight, so that
// outcome may instead be left for whichever of the two runs next.
//
// The outcome of this call's own submission, in contrast, is carried solely by
// the WriteResult returned here: it is never folded into pending, so it is
// never reported by a later FlushRows or by Close either. A caller that lets
// this WriteResult go without calling Wait on it has no way left to learn
// whether these rows landed.
func (w *LoadJobsWriter) FlushRows(ctx context.Context) (WriteResult, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, fmt.Errorf("bqsink: LoadJobsWriter: %w", ErrWriterClosed)
	}
	batch := w.buf
	w.buf = nil
	pending := w.pending
	w.pending = nil
	schema, logger := w.schema, w.logger
	if len(batch) > 0 {
		w.wg.Add(1)
	}
	w.mu.Unlock()

	n := 0
	var jobErr error
	if len(batch) > 0 {
		defer w.wg.Done()
		if err := w.load(ctx, batch, schema, logger); err != nil {
			jobErr = fmt.Errorf("bqsink: %d row(s) did not land: %w", len(batch), err)
		} else {
			n = len(batch)
		}
	}
	err := jobErr
	if len(pending) > 0 {
		err = errors.Join(append(pending, jobErr)...)
	}
	return ResolvedResult(n, err), nil
}

// Close waits for a submission still in flight, whether WriteRows started it
// on its own by filling the buffer to FlushRows or a FlushRows call started
// it, then submits whatever the buffer still holds and releases the writer.
// Closing twice in sequence is harmless: the second call finds nothing left to
// submit or report and returns nil. Two concurrent calls are not so evenly
// matched: whichever observes the writer already closed returns nil at once,
// without waiting for the other to finish, so it learns nothing about the
// outcome that call goes on to report.
//
// ctx bounds only the submission this call makes of whatever the buffer still
// holds; it does not bound the wait for a submission already in flight, so
// Close can run past ctx's own deadline if that submission is retrying under
// a RetryPolicy with no bound of its own.
//
// What it folds into the error it returns, though, is narrower than what it
// waits for: only a WriteRows submission's outcome is kept in pending, so
// only that is reported here. A FlushRows call's own submission is never
// folded into pending; its outcome was carried solely by the WriteResult that
// call returned, and if nothing ever called Wait on it, Close does not
// recover it either — that outcome is lost to every caller, not just the one
// that made the FlushRows call.
func (w *LoadJobsWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	batch := w.buf
	w.buf = nil
	schema, logger := w.schema, w.logger
	w.mu.Unlock()

	w.wg.Wait()

	w.mu.Lock()
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()

	var errs []error
	errs = append(errs, pending...)
	if len(batch) > 0 {
		if err := w.load(ctx, batch, schema, logger); err != nil {
			errs = append(errs, fmt.Errorf("bqsink: %d row(s) did not land: %w", len(batch), err))
		}
	}
	return errors.Join(errs...)
}

func (w *LoadJobsWriter) load(ctx context.Context, rows []Row, schema bigquery.Schema, logger *slog.Logger) error {
	var buf bytes.Buffer
	for i := range rows {
		line, err := encodeJSONRow(rows[i].Values, schema)
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	logger.DebugContext(ctx, "loading rows",
		slog.Int("rows", len(rows)),
		slog.Int("bytes", buf.Len()))
	return retrying(ctx, logger, "load", w.retryPolicy, func(ctx context.Context) error {
		return w.loader.load(ctx, buf.Bytes(), schema, logger)
	})
}

// jobLoader submits the rows and waits for BigQuery to finish. It exists so that
// the writer can be tested without BigQuery.
//
// The logger is passed in rather than held, because a writer is given its logger
// after it is built.
type jobLoader interface {
	load(ctx context.Context, rows []byte, schema bigquery.Schema, logger *slog.Logger) error
}

type tableLoader struct {
	table *bigquery.Table
}

// load submits a load job with an explicit schema and CreateNever, leaving the
// table's shape entirely to the migration.
func (l tableLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema, logger *slog.Logger) error {
	source := bigquery.NewReaderSource(bytes.NewReader(rows))
	source.SourceFormat = bigquery.JSON
	source.Schema = schema
	return runLoader(ctx, l.table, source, logger)
}

// runLoader submits the load job and waits for it, with an explicit schema and
// CreateNever so that the table's shape is left entirely to the migration.
func runLoader(ctx context.Context, table *bigquery.Table, source bigquery.LoadSource, logger *slog.Logger) error {
	loader := table.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.CreateDisposition = bigquery.CreateNever
	started := time.Now()
	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("bqsink: submit load job for %s: %w", table.FullyQualifiedName(), err)
	}
	// The wait below takes seconds to minutes, so the job is worth reporting before
	// it rather than only once it is over.
	logger.InfoContext(ctx, "submitted a load job", slog.String("job", job.ID()))
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("bqsink: wait for load job %s: %w", job.ID(), err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("bqsink: load job %s failed: %w", job.ID(), err)
	}
	// Measured from before the submission, since what the caller waited for is the
	// whole thing and not the job's own runtime.
	logger.InfoContext(ctx, "the load job finished",
		slog.String("job", job.ID()),
		slog.Duration("elapsed", time.Since(started)))
	return nil
}

// stagedLoader hands the rows to a Stager and points the load job at the URI it
// returns.
type stagedLoader struct {
	table  *bigquery.Table
	stager Stager
}

func (l stagedLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema, logger *slog.Logger) error {
	uri, cleanup, err := l.stager.Stage(ctx, rows)
	if err != nil {
		return fmt.Errorf("bqsink: stage rows for %s: %w", l.table.FullyQualifiedName(), err)
	}
	logger.DebugContext(ctx, "staged the rows for a load job",
		slog.String("uri", uri),
		slog.Int("bytes", len(rows)))
	if cleanup != nil {
		defer func() {
			// The rows are already loaded or already reported as undelivered, so a
			// failure to tidy up is not worth overriding that with; it is only logged,
			// and what is left behind needs removing by hand.
			if cerr := cleanup(ctx); cerr != nil {
				logger.WarnContext(ctx, "could not remove the staged rows",
					slog.String("uri", uri),
					slog.Any("error", cerr))
			}
		}()
	}
	source := bigquery.NewGCSReference(uri)
	source.SourceFormat = bigquery.JSON
	source.Schema = schema
	return runLoader(ctx, l.table, source, logger)
}
