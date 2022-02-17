package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"

	"github.com/stretchr/testify/assert"
)

func TestNewDataFrameRow(t *testing.T) {

	data := []any{1, 2.0, "abc", false}
	r := NewDataFrameRow(df.NewSchema([]df.Column{
		{Name: "c1", Format: df.IntegerFormat},
		{Name: "c2", Format: df.DoubleFormat},
		{Name: "c3", Format: df.StringFormat},
		{Name: "c4", Format: df.BoolFormat},
	}), data)

	assert.Equal(t, data, r.Data())
	assert.Equal(t, len(data), r.Len())
	for i, c := range data {
		assert.Equal(t, c, r.Get(i))
	}
}
