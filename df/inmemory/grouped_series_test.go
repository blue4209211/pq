package inmemory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupedSeries(t *testing.T) {

	data := []string{
		"abc", "def", "geh", "ijk", "lmn", "abc",
	}

	s := NewStringSeries(&data)
	//group
	sg := NewGroupedSeries(s)
	assert.Equal(t, sg.Len(), int64(5))
	assert.Equal(t, len(sg.GetKeys()), 5)
	assert.Equal(t, sg.Get(NewStringValue("abc")).Len(), int64(2))
}
