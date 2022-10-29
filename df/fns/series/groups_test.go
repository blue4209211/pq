package series

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {

	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4})
	assert.Equal(t, int64(10), Sum(s1).GetAsInt())

	s2 := inmemory.NewDoubleSeries(&[]float64{1, 2, 3, 4})
	assert.Equal(t, float64(10), Sum(s2).GetAsDouble())

}

func TestMin(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4})
	assert.Equal(t, int64(1), Min(s1).GetAsInt())

	s2 := inmemory.NewDoubleSeries(&[]float64{1, 2, 3, 4})
	assert.Equal(t, float64(1), Min(s2).GetAsDouble())
}

func TestMax(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4})
	assert.Equal(t, int64(4), Max(s1).GetAsInt())

	s2 := inmemory.NewDoubleSeries(&[]float64{1, 2, 3, 4})
	assert.Equal(t, float64(4), Max(s2).GetAsDouble())
}

func TestMean(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4})
	assert.Equal(t, float64(2.5), Mean(s1).GetAsDouble())
}

func TestMedian(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4})
	assert.Equal(t, float64(2.5), Median(s1).GetAsDouble())

	s1 = inmemory.NewIntSeries(&[]int64{1, 2, 3, 4, 5})
	assert.Equal(t, float64(3), Median(s1).GetAsDouble())
}

func TestDescribe(t *testing.T) {
}

func TestCountDistinctValues(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4, 1, 1})
	s2 := CountDistinctValues(s1)
	assert.Equal(t, 4, len(s2))
	assert.Equal(t, int64(3), s2["1"])
}

func TestUnion(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4, 1, 1})
	s2 := inmemory.NewIntSeries(&[]int64{5, 6})
	s3 := Union(s1, s2, true)
	assert.Equal(t, int64(8), s3.Len())

	s3 = Union(s1, s2, false)
	assert.Equal(t, int64(6), s3.Len())

}

func TestIntersection(t *testing.T) {
	s1 := inmemory.NewIntSeries(&[]int64{1, 2, 3, 4, 1, 1})
	s2 := inmemory.NewIntSeries(&[]int64{5, 1})
	s3 := Intersection(s1, s2)
	assert.Equal(t, int64(1), s3.Len())

}

func TestSubstract(t *testing.T) {
}

func TestCountNotNil(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{
		inmemory.NewIntValue(1),
		inmemory.NewIntValue(2),
		inmemory.NewValue(df.IntegerFormat, nil),
	}, df.IntegerFormat)
	v := CountNotNil(s1)
	assert.Equal(t, v, int64(2))
}

func TestCovariance(t *testing.T) {
}
