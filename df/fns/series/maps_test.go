package series

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestMask(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := Mask(s1, df.IntegerFormat, map[any]any{
		int64(1): int64(11),
	})
	assert.Equal(t, int64(11), s2.Get(0).GetAsInt())
}

func TestAsType(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := AsType(s1, df.StringFormat)
	assert.Equal(t, "1", s2.Get(0).GetAsString())
}
