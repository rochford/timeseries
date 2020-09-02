// Copyright Timothy Rochford 2019

package timeseries

import (
	"encoding/gob"
	"io"
	"log"
	"time"
)

// Bucket contains observations starting at timeStart
// TODO: should duplicate observation time offsets be allowed? For stock quotes,
// multiple observation events may happen at the same time.
type Bucket struct {
	//  Key and TimeStart form a database key. The Key is a TagValue.
	Key              TagValue
	TimeStart        time.Time
	firstObservation *Observation
	lastObservation  *Observation
	// Array of observations that will be filled based on linked list contents.
	Observations []Observation
}

func (b *Bucket) writeBucketBlob(w io.Writer) error {
	var current *Observation
	for current = b.firstObservation; current.next != nil; current = current.next {
		b.Observations = append(b.Observations, *current)
	}

	enc := gob.NewEncoder(w)
	err := enc.Encode(b)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return err
}

// readBucketBlob from an io.reader source. The bucket is returned as a pointer.
func readBucketBlob(r io.Reader) (*Bucket, error) {
	var current Bucket

	dec := gob.NewDecoder(r)
	err := dec.Decode(&current)
	if err != nil {
		return nil, err
	}
	log.Println("decoded Bucket")
	log.Println(current)
	log.Println("Timestart:", current.TimeStart)
	return &current, nil
}
