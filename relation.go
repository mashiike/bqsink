package bqsink

import (
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Relation identifies the BigQuery table a Sinker writes to.
//
// ProjectID may be left empty, in which case New fills it in from the project
// of the bigquery.Client it is given.
type Relation struct {
	ProjectID string
	DatasetID string
	TableID   string
}

// ParseRelation parses a table reference written in standard SQL notation.
//
// Both "project.dataset.table" and "dataset.table" are accepted; the latter
// leaves ProjectID empty. Splitting on "." is unambiguous because BigQuery
// allows only letters, digits and underscores in dataset and table names, and
// project IDs contain no dots either.
func ParseRelation(s string) (Relation, error) {
	var r Relation
	switch parts := strings.Split(s, "."); len(parts) {
	case 3:
		r = Relation{ProjectID: parts[0], DatasetID: parts[1], TableID: parts[2]}
	case 2:
		r = Relation{DatasetID: parts[0], TableID: parts[1]}
	default:
		return Relation{}, fmt.Errorf("bqsink: %q is not a table reference of the form project.dataset.table", s)
	}
	if err := r.validate(); err != nil {
		return Relation{}, fmt.Errorf("%w: %q", err, s)
	}
	return r, nil
}

// String returns the relation in standard SQL notation, omitting the project
// when ProjectID is empty.
func (r Relation) String() string {
	if r.ProjectID == "" {
		return r.DatasetID + "." + r.TableID
	}
	return r.ProjectID + "." + r.DatasetID + "." + r.TableID
}

// quoted returns the relation as a quoted identifier for a DDL statement.
func (r Relation) quoted() string {
	return fmt.Sprintf("`%s.%s.%s`", r.ProjectID, r.DatasetID, r.TableID)
}

func (r Relation) validate() error {
	if r.DatasetID == "" {
		return errors.New("bqsink: relation has no dataset")
	}
	if r.TableID == "" {
		return errors.New("bqsink: relation has no table")
	}
	return nil
}

func (r Relation) table(client *bigquery.Client) *bigquery.Table {
	return client.DatasetInProject(r.ProjectID, r.DatasetID).Table(r.TableID)
}
