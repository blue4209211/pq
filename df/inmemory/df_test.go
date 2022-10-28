package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryDfColOps(t *testing.T) {
	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, &[]df.Series{
		NewIntSeries(&[]int64{1, 2, 3, 4}),
		NewDoubleSeries(&[]float64{1, 2, 3, 4}),
		NewStringSeries(&[]string{"a1", "a2", "a3", "a4"}),
	})

	assert.Equal(t, "df1", data.Name())
	assert.Equal(t, int64(4), data.Len())
	assert.Equal(t, df.DoubleFormat, data.GetSeries(1).Schema().Format)
	assert.Equal(t, 3.0, data.GetSeries(1).Get(2).Get())

	data2, err := data.AddSeries("c4", NewBoolSeries(&[]bool{true, true, false, false}))
	assert.Nil(t, err)
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, 3, data.Schema().Len())

	data2, err = data2.RenameSeriesByName("c4", "c44", true)
	assert.Nil(t, err)
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, "c44", data2.Schema().Get(3).Name)

	data2 = data2.RemoveSeriesByName("c44")
	assert.Equal(t, 3, data2.Schema().Len())

}

func TestInMemoryDfRowOps(t *testing.T) {
	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, &[]df.Series{
		NewIntSeries(&[]int64{1, 2, 3, 4}),
		NewDoubleSeries(&[]float64{1, 2, 3, 4}),
		NewStringSeries(&[]string{"a1", "a2", "a3", "a4"}),
	})

	filteredData := data.WhereRow(func(r df.Row) bool {
		return r.GetByName("c3").GetAsString() == "a1"
	})

	assert.Equal(t, int64(1), filteredData.Len())
	assert.Equal(t, int64(4), data.Len())

	sortedData := data.SortByName(df.SortByName{Series: "c1", Order: df.SortOrderDESC})

	assert.Equal(t, data.Len(), sortedData.Len())
	assert.Equal(t, int64(4), sortedData.GetRow(0).Get(0).Get())
	assert.Equal(t, int64(4), data.Len())
}
