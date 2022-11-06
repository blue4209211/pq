package series

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestHasAny(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := HasAny(s1, int64(1))
	assert.Equal(t, int64(1), s2.Len())
}

func TestHasNotAny(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := HasNotAny(s1, int64(1), int64(2), int64(3), int64(4))
	assert.Equal(t, int64(0), s2.Len())
}

func TestHasNil(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{inmemory.NewIntValueConst(1), inmemory.NewIntValueConst(2), inmemory.NewIntValue(nil)}, df.IntegerFormat)
	s2 := HasNil(s1)
	assert.Equal(t, int64(1), s2.Len())
}

func TestHasNotNil(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{inmemory.NewIntValueConst(1), inmemory.NewIntValueConst(2), inmemory.NewIntValue(nil)}, df.IntegerFormat)
	s2 := HasNotNil(s1)
	assert.Equal(t, int64(2), s2.Len())
}
