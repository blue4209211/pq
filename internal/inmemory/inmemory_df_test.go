package inmemory

import (
	"fmt"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryDfColOps(t *testing.T) {
	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, []df.DataFrameSeries{
		NewIntSeries([]int64{1, 2, 3, 4}),
		NewDoubleSeries([]float64{1, 2, 3, 4}),
		NewStringSeries([]string{"a1", "a2", "a3", "a4"}),
	})

	assert.Equal(t, "df1", data.Name())
	assert.Equal(t, int64(4), data.Len())
	assert.Equal(t, df.DoubleFormat, data.Column(1).Schema())
	assert.Equal(t, 3.0, data.Column(1).Get(2))

	data2, err := data.AddColumn("c4", NewBoolSeries([]bool{true, true, false, false}))
	assert.Nil(t, err)
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, 3, data.Schema().Len())

	data2, err = data2.RenameColumnByName("c4", "c44", true)
	assert.Nil(t, err)
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, "c44", data2.Schema().Get(3).Name)

	data2 = data2.RemoveColumnByName("c44")
	fmt.Println(data2)
	assert.Equal(t, 3, data2.Schema().Len())

}

func TestInMemoryDfRowOps(t *testing.T) {
	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, []df.DataFrameSeries{
		NewIntSeries([]int64{1, 2, 3, 4}),
		NewDoubleSeries([]float64{1, 2, 3, 4}),
		NewStringSeries([]string{"a1", "a2", "a3", "a4"}),
	})

	filteredData := data.Filter(func(r df.DataFrameRow) bool {
		return r.GetByName("c3") == "a1"
	})

	assert.Equal(t, int64(1), filteredData.Len())
	assert.Equal(t, int64(4), data.Len())

	sortedData := data.SortByName(df.SortByName{Column: "c1", Order: df.SortOrderDESC})

	assert.Equal(t, data.Len(), sortedData.Len())
	assert.Equal(t, int64(4), sortedData.Get(0).Get(0))
	assert.Equal(t, int64(4), data.Len())

}
