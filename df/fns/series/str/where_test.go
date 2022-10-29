package str

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestIsContains(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("1"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := IsContains(s1, "1")
	assert.Equal(t, "1", s2.Get(0).GetAsString())
	assert.Equal(t, int64(1), s2.Len())
}

func TestIsStartsWith(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("1"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := IsStartsWith(s1, "1")
	assert.Equal(t, "1", s2.Get(0).GetAsString())
	assert.Equal(t, int64(1), s2.Len())
}

func TestIsEndsWith(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("1"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := IsEndsWith(s1, "1")
	assert.Equal(t, "1", s2.Get(0).GetAsString())
	assert.Equal(t, int64(1), s2.Len())
}
