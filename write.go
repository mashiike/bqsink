package bqsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	// writer's until the WriteResult settles, and a writer that buffers may keep
	// them past this call.
	WriteRows(ctx context.Context, rows []Row) (WriteResult, error)

	// Close releases what the writer holds, having settled the rows it still has.
	// Rows a WriteResult was promised for but which were never sent are sent here,
	// so closing is the last chance to decide their fate.
	Close(ctx context.Context) error
}

// WriteResult says whether the rows of one WriteRows reached BigQuery.
type WriteResult interface {
	// Wait returns how many of that call's rows landed, and a non-nil error
	// whenever that is fewer than the call was given, so that rows[n:] of it are
	// exactly the rows that did not land.
	//
	// Waiting is also what sends rows a buffering transport still holds, which is
	// the choice it offers: wait at once to know they landed, or wait later to let
	// more rows share one job.
	//
	// Cancelling ctx does not take the rows back, and where the transport gathers
	// rows from several calls it does not stop the job either: whichever call sends
	// a batch lends the job its own ctx, so cancelling that one fails the batch for
	// everyone whose rows are in it. A caller that cannot afford that gives every
	// Sink the same ctx, or writes without FlushRows so that no batch is shared.
	Wait(ctx context.Context) (n int, err error)
}

// LoggerBindable is implemented by a writer with something to log. NewSinker
// hands it the logger WithLogger settled on, already carrying the relation.
type LoggerBindable interface {
	BindLogger(logger *slog.Logger)
}

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

	// FlushRows gathers rows across calls and submits them as one load job once
	// this many are held, rather than submitting a job per WriteRows. A table's
	// daily job quota is what makes that worth doing: writing a row at a time
	// without it submits a load job each time.
	//
	// The zero value submits a job per WriteRows.
	//
	// Waiting on a WriteResult submits the batch its rows are in, so what a batch
	// ends up carrying depends on when the rows are waited for. Several goroutines
	// writing at once share a job, which each of them then waits for: the rows of
	// every call that joined the batch before one of them waited travel together,
	// and none of the calls gives up its own answer for it. One goroutine waiting
	// on each call in turn gets a job per call, since there is nobody else to
	// gather rows from; it is Flush that gathers rows for a caller like that, or
	// waiting on the results later rather than at once.
	//
	// A batch reaching this many rows is submitted by the call that filled it,
	// which is what keeps the rows held here bounded, and Flush or Close submits
	// what is left.
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

// LoadJobsWriter writes rows with BigQuery load jobs.
//
// Where FlushRows asks for it, rows gather here until a job's worth of them has
// arrived. What that means for a caller is settled by when it waits on the
// WriteResults: waiting at once submits a job per call, and waiting later lets
// the rows of several calls travel together.
//
// Nothing is serialised on a lock while a job runs, so concurrent calls submit
// concurrent jobs, and a job carries the rows of every call that filled its
// batch. A failure is reported to each of those calls, and to Close when no
// caller ever asked.
type LoadJobsWriter struct {
	relation    Relation
	client      *bigquery.Client
	loader      jobLoader
	retryPolicy func() gax.Retryer
	flushRows   int

	mu       sync.Mutex
	idle     *sync.Cond
	inflight int
	logger   *slog.Logger
	schema   bigquery.Schema
	open     *loadBatch
	failed   []*loadBatch
	closed   bool
}

// settled returns the condition Close waits on for the jobs still running. It is
// made on first use so that a writer built as a struct literal, which the tests
// do, needs nothing of the sort. Its caller holds mu, which is what it guards.
func (w *LoadJobsWriter) settled() *sync.Cond {
	if w.idle == nil {
		w.idle = sync.NewCond(&w.mu)
	}
	return w.idle
}

// loadBatch is the rows of one load job, shared by the WriteResults of every
// WriteRows that put rows in it.
//
// claimed is what keeps a batch to one job: whoever wins it submits, and everyone
// else waits for done. err is only read once done is closed.
//
// unread counts the WriteResults of this batch that have not reported its outcome
// yet, so that a failure is still Close's to report while any one of the calls that
// filled the batch has not asked. One caller waiting does not answer for another.
type loadBatch struct {
	rows    []Row
	claimed atomic.Bool
	done    chan struct{}
	err     error
	unread  atomic.Int32
}

func newLoadBatch() *loadBatch {
	return &loadBatch{done: make(chan struct{})}
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
// The rows join the batch being gathered, and the result returned settles when
// that batch's load job has finished. A batch reaching FlushRows is submitted by
// the call that filled it, so the rows held here stay bounded.
func (w *LoadJobsWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	if len(rows) == 0 {
		return ResolvedResult(0, nil), nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, errors.New("bqsink: LoadJobsWriter: the writer is closed")
	}
	if w.schema == nil {
		w.mu.Unlock()
		return nil, errors.New("bqsink: LoadJobsWriter: no schema is bound yet")
	}
	if w.open == nil {
		w.open = newLoadBatch()
	}
	batch := w.open
	batch.rows = append(batch.rows, rows...)
	batch.unread.Add(1)
	result := &loadResult{writer: w, batch: batch, rows: len(rows)}
	if len(batch.rows) < w.flushRows {
		w.mu.Unlock()
		return result, nil
	}
	w.open = nil
	w.mu.Unlock()
	w.submit(ctx, batch)
	return result, nil
}

// Flush submits the rows FlushRows is holding back, without waiting on each
// WriteResult in turn. It is what a caller gathering rows over time calls, from a
// ticker or once a batch is worth sending.
//
// It returns the load job's error, which is the same one the WriteResults of the
// rows it sent report.
func (w *LoadJobsWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	batch := w.open
	w.open = nil
	w.mu.Unlock()
	if batch == nil {
		return nil
	}
	return w.sendAndWait(ctx, batch)
}

// Close submits what is still held and reports what nothing else would.
//
// Two things are reported here. Rows FlushRows was holding back are sent, since
// closing is the last chance to send them. And a load job that failed goes into
// the error too while any of the calls whose rows were in it has not waited on its
// WriteResult, because that call would otherwise never be told. One caller waiting
// does not answer for another sharing the batch, so the same failure can reach both
// a Wait and Close.
//
// A job another goroutine is still running is waited for first, so that its
// outcome is not missed by closing at the wrong moment. Closing twice is harmless.
func (w *LoadJobsWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	batch := w.open
	w.open = nil
	w.mu.Unlock()

	var errs []error
	if batch != nil {
		if err := w.sendAndWait(ctx, batch); err != nil {
			errs = append(errs, err)
		}
	}
	w.mu.Lock()
	// A job another goroutine started is waited for here rather than left behind:
	// its failure reaches w.failed only when it finishes, and a Close that returned
	// before then would be the last chance to report it going by unnoticed.
	for w.inflight > 0 {
		w.settled().Wait()
	}
	unread := retainUnread(w.failed)
	w.failed = nil
	w.mu.Unlock()
	for _, b := range unread {
		errs = append(errs, fmt.Errorf("bqsink: %d row(s) did not land and their result was never waited for: %w",
			len(b.rows), b.err))
	}
	return errors.Join(errs...)
}

// settle sends the batch if it is still gathering rows and returns its outcome,
// which is what a WriteResult reports.
func (w *LoadJobsWriter) settle(ctx context.Context, batch *loadBatch) error {
	w.mu.Lock()
	if w.open == batch {
		// No more rows join a batch on its way out.
		w.open = nil
	}
	w.mu.Unlock()
	return w.sendAndWait(ctx, batch)
}

// sendAndWait submits the batch unless someone else already is, and then waits
// for whoever did.
func (w *LoadJobsWriter) sendAndWait(ctx context.Context, batch *loadBatch) error {
	w.submit(ctx, batch)
	select {
	case <-batch.done:
		return batch.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// submit runs the batch's load job, once. A caller that does not win the batch
// returns straight away and waits for the one that did.
func (w *LoadJobsWriter) submit(ctx context.Context, batch *loadBatch) {
	if !batch.claimed.CompareAndSwap(false, true) {
		return
	}
	w.mu.Lock()
	schema, logger := w.schema, w.logger
	w.inflight++
	w.mu.Unlock()
	err := w.load(ctx, batch.rows, schema, logger)
	// Settled before the batch is put anywhere anyone else can reach it, so that
	// Close never reads the outcome of a batch that has none yet, and closing done
	// last is what releases the waiters.
	batch.err = err
	w.mu.Lock()
	if err != nil {
		// Kept so that Close can report a failure no WriteResult was ever asked
		// about. Batches every result has read are dropped rather than piling up.
		w.failed = append(retainUnread(w.failed), batch)
	}
	w.inflight--
	if w.inflight == 0 {
		w.settled().Broadcast()
	}
	w.mu.Unlock()
	close(batch.done)
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

// retainUnread drops the batches every WriteResult of which has reported the
// outcome, leaving the ones somebody was never told about.
func retainUnread(batches []*loadBatch) []*loadBatch {
	kept := batches[:0]
	for _, b := range batches {
		if b.unread.Load() > 0 {
			kept = append(kept, b)
		}
	}
	return kept
}

// loadResult reports the outcome of the batch its rows went into.
//
// read keeps this one call's share of the batch's unread count to a single
// decrement, so that waiting twice does not make Close think another caller has
// been told.
type loadResult struct {
	writer *LoadJobsWriter
	batch  *loadBatch
	rows   int
	read   atomic.Bool
}

// Wait implements WriteResult. It submits the batch these rows are in when
// nothing else has, which is what makes waiting at once equivalent to writing
// without a buffer at all.
func (r *loadResult) Wait(ctx context.Context) (int, error) {
	err := r.writer.settle(ctx, r.batch)
	if err != nil && ctx.Err() != nil && !isBatchSettled(r.batch) {
		// The wait gave up rather than the batch, so this call has not been told
		// what became of its rows and Close still owes it an answer.
		return 0, err
	}
	if r.read.CompareAndSwap(false, true) {
		r.batch.unread.Add(-1)
	}
	if err != nil {
		return 0, err
	}
	return r.rows, nil
}

func isBatchSettled(batch *loadBatch) bool {
	select {
	case <-batch.done:
		return true
	default:
		return false
	}
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
