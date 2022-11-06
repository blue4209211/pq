package num

import (
	"testing"

	"github.com/blue4209211/pq/df/fns/series"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestIsBetweenInt(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := IsBetweenInt(s1, 2, 3, series.BetweenIncludeBoth)
	assert.Equal(t, int64(2), s2.Len())
}

func TestIsBetweenDouble(t *testing.T) {
	s1 := inmemory.NewDoubleSeriesVarArg(1, 2, 3, 4)
	s2 := IsBetweenDouble(s1, 2, 3, series.BetweenIncludeBoth)
	assert.Equal(t, int64(2), s2.Len())
}

func TestIsCompareInt(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := IsCompareInt(s1, int64(2), series.Equal)
	assert.Equal(t, int64(1), s2.Len())
}

func TestIsCompareDouble(t *testing.T) {
	s1 := inmemory.NewDoubleSeriesVarArg(1, 2, 3, 4)
	s2 := IsCompareDouble(s1, float64(2), series.Equal)
	assert.Equal(t, int64(1), s2.Len())
}
