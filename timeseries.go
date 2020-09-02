// Copyright Timothy Rochford 2018

package timeseries

import (
	"fmt"
	"io"
	"log"
	"time"
)

// TimeSeries represents a sequence of Observation events
type TimeSeries struct {
	// MetricName the name of the time series data. E.g stock.quote
	// Metrics may include several Observerable objects.
	MetricName     string
	timeZone       *time.Location
	timeStart      time.Time
	bucketDuration time.Duration
	/* Buckets associated with a Key. As map structures return a copy of the
	 * value, the value type needs to be a pointer to the slice of buckets.
	 */
	bucketsForKey map[TagValue][]*Bucket
}

// ReadBucket from an io.reader source. The bucket is returned as a pointer.
func (ts *TimeSeries) ReadBucket(r io.Reader) (*Bucket, error) {
	return readBucketBlob(r)
}

// InsertBucket adds a pointer to a bucket to the time series. This is useful When
// reading buckets from a disk and making the timeseries database
func (ts *TimeSeries) InsertBucket(b *Bucket) error {
	if buckets, ok := ts.bucketsForKey[b.Key]; ok {
		buckets = append(buckets, b)
		return nil
	}
	// b.key was not present in the map, insert it
	newSlice := []*Bucket{b}
	ts.bucketsForKey[b.Key] = newSlice
	return nil
}

// NewTimeSeries creates a new TimeSeries
func NewTimeSeries(metricName string, bucketDuration time.Duration) *TimeSeries {
	ts := new(TimeSeries)
	ts.timeZone, _ = time.LoadLocation("Europe/Helsinki")
	log.Println("timeZone=", ts.timeZone.String())
	ts.MetricName = metricName
	ts.bucketDuration = bucketDuration
	// TODO: How can the capacity of the buckets be known at this point in time?
	ts.bucketsForKey = make(map[TagValue][]*Bucket, 0)
	return ts
}

func (ts *TimeSeries) numberOfBuckets() int {
	count := 0
	for _, buckets := range ts.bucketsForKey {
		count += len(buckets)
	}
	return count
}

// Flush buckets to a writer.
func (ts *TimeSeries) Flush(w io.Writer) {
	for _, buckets := range ts.bucketsForKey {
		for _, bucket := range buckets {
			_ = bucket.writeBucketBlob(w)
		}
	}
}

func (ts *TimeSeries) findBucket(obseravableKey TagValue, timestamp time.Time) (*Bucket, error) {
	log.Println("findBucket for timestamp:", timestamp)
	if buckets, ok := ts.bucketsForKey[obseravableKey]; ok {
		for _, bucket := range buckets {
			if bucket.Key != obseravableKey {
				continue
			}
			if bucket.TimeStart.Add(ts.bucketDuration).After(timestamp) &&
				timestamp.After(bucket.TimeStart) {
				log.Println("Found bucket: Key:", bucket.Key, "", bucket.TimeStart)
				return bucket, nil
			}
		}
	}
	return &Bucket{}, fmt.Errorf("No existing bucket found")
}

// AddPoint adds Observation to TimeSeries
func (ts *TimeSeries) AddPoint(ob Observation, timestamp time.Time) error {
	/*	if timestamp.Location() != ts.timeZone {
		return fmt.Errorf("timestamp timezone does not match timeseries timezone")
	}*/
	log.Println("AddPoint")
	var err error
	var b *Bucket
	if b, err = ts.findBucket(ob.Tags[0].Value, timestamp); err != nil {
		// calculate the startTime of the bucket.
		startTime := timestamp.Truncate(ts.bucketDuration)
		log.Printf("Bucket startTime: %s", startTime.String())
		ob.TimeOffset = timestamp.Sub(startTime)
		log.Println("ob.TimeOffset", ob.TimeOffset)

		// create a new bucket. The first TagValue will be used as the bucket Key.
		newBucket := Bucket{ob.Tags[0].Value, startTime, &ob, &ob, make([]Observation, 0, 5)}
		ts.bucketsForKey[ob.Tags[0].Value] = append(ts.bucketsForKey[ob.Tags[0].Value], &newBucket)
		return nil
	}

	// bucket exists, insert in bucket

	ob.TimeOffset = timestamp.Sub(b.TimeStart)
	log.Println("ob.TimeOffset", ob.TimeOffset)

	// ob time is after all other observations in the bucket
	if ob.TimeOffset >= b.lastObservation.TimeOffset {
		b.lastObservation.next = &ob
		b.lastObservation = &ob
		b.lastObservation.next = nil
		return nil
	}
	// ob time is before all other observations in the bucket
	if ob.TimeOffset < b.firstObservation.TimeOffset {
		tmpObservation := b.firstObservation
		b.firstObservation = &ob
		b.firstObservation.next = tmpObservation
		return nil
	}

	// ob time is somewhere in the middle
	it := b.firstObservation
	for {
		if it == nil {
			break
		}
		if it.next != nil && ob.TimeOffset < it.next.TimeOffset {
			tmp := it.next
			it.next = &ob
			ob.next = tmp
			break
		}
		it = it.next
	}

	return nil
}
