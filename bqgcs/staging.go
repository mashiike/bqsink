// Package bqgcs stages bqsink's rows in Cloud Storage so that a load job reads
// them from a URI rather than carrying them.
//
// It lives in its own package so that using bqsink does not pull in
// cloud.google.com/go/storage.
package bqgcs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
)

// Staging writes each batch of rows to an object in a bucket and has the load job
// read it from there.
//
// It implements bqsink.Stager.
type Staging struct {
	// Client is the Cloud Storage client the objects are written with.
	Client *storage.Client

	// Bucket names the bucket the objects are written to.
	Bucket string

	// Prefix is put in front of every object name, so that a lifecycle rule can
	// cover the staged files.
	Prefix string

	// Keep leaves a staged object in place after the load job has read it. It is
	// off by default, so an object is removed once it is no longer needed.
	Keep bool

	counter atomic.Uint64
}

// Validate reports whether the staging is configured well enough to use. bqsink
// calls it when the strategy carrying it is applied.
func (s *Staging) Validate() error {
	if s.Client == nil {
		return errors.New("bqgcs: Staging: Client is nil")
	}
	if s.Bucket == "" {
		return errors.New("bqgcs: Staging: Bucket is empty")
	}
	if strings.HasPrefix(s.Bucket, "gs://") {
		return fmt.Errorf("bqgcs: Staging: Bucket is %q, want the name alone without the gs:// scheme", s.Bucket)
	}
	return nil
}

// Stage writes rows to a new object and returns its URI, along with a cleanup that
// deletes the object unless Keep is set.
func (s *Staging) Stage(ctx context.Context, rows []byte) (string, func(context.Context) error, error) {
	if err := s.Validate(); err != nil {
		return "", nil, err
	}
	object := s.Client.Bucket(s.Bucket).Object(s.objectName())
	uri := fmt.Sprintf("gs://%s/%s", s.Bucket, object.ObjectName())

	w := object.NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(rows); err != nil {
		// Close releases the upload; its error is not what went wrong here.
		_ = w.Close()
		return "", nil, fmt.Errorf("bqgcs: write %s: %w", uri, err)
	}
	if err := w.Close(); err != nil {
		return "", nil, fmt.Errorf("bqgcs: finish writing %s: %w", uri, err)
	}

	cleanup := func(ctx context.Context) error {
		if s.Keep {
			return nil
		}
		if err := object.Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("bqgcs: delete %s: %w", uri, err)
		}
		return nil
	}
	return uri, cleanup, nil
}

// objectName builds a name unique to this batch, so that concurrent writers and
// repeated attempts never collide.
func (s *Staging) objectName() string {
	name := fmt.Sprintf("%d-%d.json", time.Now().UnixNano(), s.counter.Add(1))
	if s.Prefix == "" {
		return name
	}
	return strings.TrimSuffix(s.Prefix, "/") + "/" + name
}
