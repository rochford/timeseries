// Copyright Timothy Rochford 2018

package timeseries

import (
	"bytes"
	"log"
	"testing"
	"time"
)

const (
	MetricName = "stock.quote"
)
const (
	TagKeyStockName = iota
	TagKeyStockPrice
)
const (
	TagValueStockAlpha = iota
	TagValueStockBravo
)

func TestBasicTimeSeries(t *testing.T) {
	const Observations = 3
	ts := NewTimeSeries(MetricName, time.Hour)
	loc := time.UTC
	timestamp := time.Date(1970, 1, 7, 13, 01, 0, 0, loc)
	observation := TagValue(100.0)

	for i := 0; i < Observations; i++ {
		var tagValues []Tag
		tagValues = make([]Tag, 0, 5)
		tagValues = append(tagValues, Tag{TagKeyStockName, TagValueStockAlpha})
		tagValues = append(tagValues, Tag{TagKeyStockPrice, observation})
		obs, _ := NewObservation(tagValues)
		ts.AddPoint(obs, timestamp)
		timestamp = timestamp.Add(time.Second)
		observation = observation + 1
	}

	if ts.numberOfBuckets() != 1 {
		t.Fatalf("Incorrect number of buckets: %d", ts.numberOfBuckets())
	}

	var tagValues []Tag
	tagValues = make([]Tag, 0, 5)
	tagValues = append(tagValues, Tag{TagKeyStockName, TagValueStockAlpha})
	tagValues = append(tagValues, Tag{TagKeyStockPrice, observation})
	obs2, _ := NewObservation(tagValues)
	timestamp2 := timestamp.Add(time.Hour * 1)
	ts.AddPoint(obs2, timestamp2)
	if ts.numberOfBuckets() != 2 {
		t.Fatalf("Incorrect number of buckets: %d", ts.numberOfBuckets())
	}

	var blob bytes.Buffer
	ts.bucketsForKey[TagValueStockAlpha][0].writeBucketBlob(&blob)
	log.Println(blob)
}

func TestOutOfOrderSeries(t *testing.T) {
	t.Log("start TestOutOfOrderSeries")
	ts := NewTimeSeries(MetricName, time.Hour)
	loc := time.UTC
	observation := TagValue(100.0)

	timestamp := time.Date(1978, 1, 7, 13, 01, 0, 0, loc)
	timestamp2 := timestamp.Add(time.Second * 1)
	timestamp3 := timestamp.Add(time.Second * 2)
	timestamp4 := timestamp.Add(time.Second * 3)

	tagValues := make([]Tag, 0, 5)
	tag := Tag{TagKeyStockName, TagValueStockAlpha}
	tagValues = append(tagValues, tag)
	tagValues = append(tagValues, Tag{TagKeyStockPrice, observation})

	obs1, _ := NewObservation(tagValues)
	obs2, _ := NewObservation(tagValues)
	obs3, _ := NewObservation(tagValues)
	obs4, _ := NewObservation(tagValues)
	t.Log("start TestOutOfOrderSeries add obs 4: ", obs4, timestamp4)
	ts.AddPoint(obs4, timestamp4)
	t.Log("start TestOutOfOrderSeries add obs 1", obs1, timestamp)
	ts.AddPoint(obs1, timestamp)
	t.Log("start TestOutOfOrderSeries add obs 2", obs2, timestamp2)
	ts.AddPoint(obs2, timestamp2)
	t.Log("start TestOutOfOrderSeries add obs 3", obs3, timestamp3)
	ts.AddPoint(obs3, timestamp3)
	if ts.numberOfBuckets() != 1 {
		t.Fatalf("Incorrect number of buckets: %d", ts.numberOfBuckets())
	}
	t.Log("TestOutOfOrderSeries done")
}

func TestObservationTags(t *testing.T) {
	const Observations = 3
	observation := TagValue(100.0)

	tagValues := make([]Tag, 0, 5)

	_, err := NewObservation(tagValues)

	if err == nil {
		t.Fatalf("Incorrect error: %s", err.Error())
	}

	tagValues = append(tagValues, Tag{TagKeyStockName, TagValueStockAlpha})
	tagValues = append(tagValues, Tag{TagKeyStockPrice, observation})
	_, err = NewObservation(tagValues)
	_, err = NewObservation(tagValues)
	if err != nil {
		t.Fatalf("No Error expected: %s", err.Error())
	}

}

func TestMultipleObservablesCorrectBucket(t *testing.T) {
	t.Log("start TestOutOfOrderSeries")
	ts := NewTimeSeries(MetricName, time.Hour)
	loc := time.UTC
	observation := TagValue(100.0)

	timestamp := time.Date(1978, 1, 7, 13, 01, 0, 0, loc)

	tagValuesForAlpha := make([]Tag, 0, 5)
	tagValuesForAlpha = append(tagValuesForAlpha, Tag{TagKeyStockName, TagValueStockAlpha})
	tagValuesForAlpha = append(tagValuesForAlpha, Tag{TagKeyStockPrice, observation})

	tagValuesForBravo := make([]Tag, 0, 5)
	tagValuesForBravo = append(tagValuesForBravo, Tag{TagKeyStockName, TagValueStockBravo})
	tagValuesForBravo = append(tagValuesForBravo, Tag{TagKeyStockPrice, observation})

	obsAlpha, _ := NewObservation(tagValuesForAlpha)
	obsBravo, _ := NewObservation(tagValuesForBravo)

	ts.AddPoint(obsAlpha, timestamp)
	ts.AddPoint(obsBravo, timestamp)

	if ts.numberOfBuckets() != 2 {
		t.Fatalf("Incorrect number of buckets: %d", ts.numberOfBuckets())
	}
}

func BenchmarkInOrderFillBucket(t *testing.B) {
	const Observations = 1000 * 60 * 60
	ts := NewTimeSeries(MetricName, time.Hour)
	loc := time.UTC
	timestamp := time.Date(1970, 1, 7, 13, 01, 0, 0, loc)
	observation := TagValue(0.0)

	tagValues := make([]Tag, 0, 5)
	tagValues = append(tagValues, Tag{TagKeyStockName, TagValueStockAlpha})
	for i := 0; i < Observations; i++ {
		obs, _ := NewObservation(tagValues)
		tagValues = append(tagValues, Tag{TagKeyStockPrice, observation})
		ts.AddPoint(obs, timestamp)
		timestamp = timestamp.Add(time.Millisecond)
		observation = observation + 0.001
	}
}

func TestObservationsMustBeInSameTimeZone(t *testing.T) {
	ts := NewTimeSeries(MetricName, time.Hour)
	loc, _ := time.LoadLocation("Europe/Helsinki")
	timestamp := time.Date(1970, 1, 7, 13, 01, 0, 0, loc)

	var tagValues []Tag
	tagValues = make([]Tag, 0, 5)
	tagValues = append(tagValues, Tag{TagKeyStockName, TagValueStockAlpha})
	tagValues = append(tagValues, Tag{TagKeyStockPrice, TagValue(100.0)})
	obs, _ := NewObservation(tagValues)

	if err := ts.AddPoint(obs, timestamp); err == nil {
		t.Fatal("Not allowed to use different timeZones in timestamps.")
	}

}
