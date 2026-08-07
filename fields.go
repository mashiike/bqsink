package bqsink

import (
	"fmt"
	"reflect"
	"slices"
)

// candidateField is a struct field found while walking a type, before name
// collisions between embedding levels have been settled.
type candidateField struct {
	sf    reflect.StructField
	tag   fieldTag
	name  string
	index []int
	depth int

	// tagged reports whether the column name came from a tag rather than the Go
	// field name, which breaks a tie between fields at the same depth.
	tagged bool
}

// collectFields lists the columns a struct type contributes, promoting the fields
// of embedded structs into the outer struct.
//
// The walk follows the rules of encoding/json: a breadth-first search that
// descends into embedded structs, where a shallower field hides a deeper one of
// the same name, an explicit tag breaks a tie at equal depth, and an unresolved
// tie removes every field involved. An embedded field carrying a name in its tag
// is a column of its own rather than something to descend into.
func collectFields(t reflect.Type) ([]candidateField, error) {
	var found []candidateField
	visited := map[reflect.Type]bool{t: true}
	queue := []candidateField{{sf: reflect.StructField{Type: t}}}

	for len(queue) > 0 {
		level := queue
		queue = nil
		next := map[reflect.Type]bool{}
		for _, parent := range level {
			st := deref(parent.sf.Type)
			for i := range st.NumField() {
				sf := st.Field(i)
				tag, err := parseFieldTags(sf.Tag)
				if err != nil {
					return nil, fmt.Errorf("bqsink: field %s: %w", sf.Name, err)
				}
				if tag.skip {
					continue
				}
				ft := deref(sf.Type)
				if sf.Anonymous {
					if !sf.IsExported() && ft.Kind() != reflect.Struct {
						continue
					}
					// An embedded struct is descended into unless its tag names a
					// column, in which case it becomes that column.
					if ft.Kind() == reflect.Struct && tag.name == "" {
						if visited[ft] || next[ft] {
							continue
						}
						next[ft] = true
						queue = append(queue, candidateField{
							sf:    sf,
							index: append(slices.Clone(parent.index), i),
						})
						continue
					}
				} else if !sf.IsExported() {
					continue
				}
				name := tag.name
				if name == "" {
					name = sf.Name
				}
				found = append(found, candidateField{
					sf:     sf,
					tag:    tag,
					name:   name,
					index:  append(slices.Clone(parent.index), i),
					depth:  len(parent.index),
					tagged: tag.name != "",
				})
			}
		}
		for t := range next {
			visited[t] = true
		}
	}
	return resolveCollisions(found), nil
}

// resolveCollisions keeps, for each column name, the single field that wins by
// depth or by carrying an explicit tag, and drops the name entirely when neither
// settles it.
//
// The survivors come back in field index order, which puts a promoted column where
// its embedded struct is declared, matching the order encoding/json emits.
func resolveCollisions(found []candidateField) []candidateField {
	byName := make(map[string][]candidateField, len(found))
	for _, f := range found {
		byName[f.name] = append(byName[f.name], f)
	}
	kept := make([]candidateField, 0, len(found))
	for _, f := range found {
		winner, ok := winnerFor(byName[f.name])
		if !ok || !slices.Equal(winner.index, f.index) {
			continue
		}
		kept = append(kept, f)
	}
	slices.SortFunc(kept, func(a, b candidateField) int {
		return slices.Compare(a.index, b.index)
	})
	return kept
}

func winnerFor(candidates []candidateField) (candidateField, bool) {
	if len(candidates) == 1 {
		return candidates[0], true
	}
	shallowest := candidates[0].depth
	for _, c := range candidates[1:] {
		if c.depth < shallowest {
			shallowest = c.depth
		}
	}
	var atDepth []candidateField
	for _, c := range candidates {
		if c.depth == shallowest {
			atDepth = append(atDepth, c)
		}
	}
	if len(atDepth) == 1 {
		return atDepth[0], true
	}
	var tagged []candidateField
	for _, c := range atDepth {
		if c.tagged {
			tagged = append(tagged, c)
		}
	}
	if len(tagged) == 1 {
		return tagged[0], true
	}
	return candidateField{}, false
}
