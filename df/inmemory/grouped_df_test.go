package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestGroupedDf(t *testing.T) {

	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, &[]df.Series{
		NewIntSeries(&[]int64{1, 2, 3, 4, 1}),
		NewDoubleSeries(&[]float64{1, 2, 3, 4, 1}),
		NewStringSeries(&[]string{"a1", "a2", "a3", "a4", "a1"}),
	})

	//group
	sg := NewGroupedDf(data, "c1")
	assert.Equal(t, sg.Len(), int64(4))
	assert.Equal(t, len(sg.GetKeys()), 4)
	assert.Equal(t, sg.GetGroupColumns(), []string{"c1"})
	assert.Equal(t, sg.Get(NewRowFromMap(&map[string]df.Value{"c1": NewIntValue(1)})).Len(), int64(2))

	sg2 := sg.Where(func(r df.Row, df df.DataFrame) bool {
		return r.Get(0).GetAsInt() != 1
	})
	assert.Equal(t, sg2.Len(), int64(3))
	assert.Equal(t, nil, sg2.Get(NewRowFromMap(&map[string]df.Value{"c1": NewIntValue(1)})))

}
