// Copyright Timothy Rochford 2019

package timeseries

import (
	"fmt"
	"strconv"
	"time"
)

// DataType represents the Observation data value
type DataType float64

// TagKey is used to identify what an Observation relates to.
type TagKey int16

// TagValue is used to identify what an Observation relates to.
type TagValue DataType

// Tag is used to assign information to an Observation
type Tag struct {
	Key   TagKey
	Value TagValue
}

// Observation represents a Value at a point in time for an observable object.
type Observation struct {
	// Tags are user defined Key/value pairs associated with the observation.
	Tags []Tag
	// timeOffset from bucket start time
	TimeOffset time.Duration
	// next is for internal use.
	next *Observation
}

// NewObservation creates a new Observation event
func NewObservation(tags []Tag) (Observation, error) {
	if len(tags) < 1 {
		return Observation{}, fmt.Errorf("Must be at least 1 tag in an observation")
	}
	ownTags := make([]Tag, len(tags), len(tags))
	copy(ownTags, tags)
	return Observation{ownTags, 0, nil}, nil
}

func (ob Observation) String() string {
	var str string

	str = str + ", "
	for _, tag := range ob.Tags {
		str = str + "Tag(" + strconv.FormatInt(int64(tag.Key), 10) + ")=" + strconv.FormatInt(int64(tag.Value), 10)
		str = str + ", "
	}
	return str
}
