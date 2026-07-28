package bqsink

import "testing"

func TestParseRelation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    Relation
		wantErr bool
	}{
		{
			name: "three parts fill every field",
			in:   "proj.ds.tbl",
			want: Relation{ProjectID: "proj", DatasetID: "ds", TableID: "tbl"},
		},
		{
			name: "two parts leave the project empty",
			in:   "ds.tbl",
			want: Relation{DatasetID: "ds", TableID: "tbl"},
		},
		{
			name: "a hyphenated project id survives",
			in:   "my-project-123.ds.tbl",
			want: Relation{ProjectID: "my-project-123", DatasetID: "ds", TableID: "tbl"},
		},
		{name: "a bare table name", in: "tbl", wantErr: true},
		{name: "four parts", in: "a.b.c.d", wantErr: true},
		{name: "an empty string", in: "", wantErr: true},
		{name: "an empty dataset", in: "proj..tbl", wantErr: true},
		{name: "an empty table", in: "proj.ds.", wantErr: true},
		{name: "an empty table in the two part form", in: "ds.", wantErr: true},
		{name: "an empty dataset in the two part form", in: ".tbl", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRelation(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseRelation(%q) = %+v, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRelation(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseRelation(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestRelationString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		r    Relation
		want string
	}{
		{
			name: "with a project",
			r:    Relation{ProjectID: "proj", DatasetID: "ds", TableID: "tbl"},
			want: "proj.ds.tbl",
		},
		{
			name: "without a project",
			r:    Relation{DatasetID: "ds", TableID: "tbl"},
			want: "ds.tbl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.r.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			back, err := ParseRelation(tt.want)
			if err != nil {
				t.Fatalf("ParseRelation(%q) error = %v", tt.want, err)
			}
			if back != tt.r {
				t.Errorf("ParseRelation(String()) = %+v, want %+v", back, tt.r)
			}
		})
	}
}
