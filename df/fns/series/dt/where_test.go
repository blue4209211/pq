package dt

import (
	"testing"
	"time"

	"github.com/blue4209211/pq/df/fns/series"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestIsBetween(t *testing.T) {
	s1 := inmemory.NewDatetimeSeries(&[]time.Time{
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	})
	s2 := IsBetween(s1, time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC), time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC), series.BetweenIncludeBoth)
	assert.Equal(t, int64(2), s2.Len())
}

func TestIsCompare(t *testing.T) {
	s1 := inmemory.NewDatetimeSeries(&[]time.Time{
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	})
	s2 := IsCompare(s1, time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC), series.GreaterThan)
	assert.Equal(t, int64(2), s2.Len())
}
