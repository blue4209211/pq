package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryDf(t *testing.T) {
	data := NewDataframeWithNameFromSeries("df1", []string{"c1", "c2", "c3"}, &[]df.Series{
		NewIntSeriesVarArg(1, 2, 3, 4),
		NewDoubleSeriesVarArg(1, 2, 3, 4),
		NewStringSeriesVarArg("a1", "a2", "a3", "a4"),
	})

	// name
	assert.Equal(t, "df1", data.Name())
	// len
	assert.Equal(t, int64(4), data.Len())
	assert.Equal(t, df.DoubleFormat, data.GetSeries(1).Schema().Format)
	//GetSeriesByName
	assert.Equal(t, 3.0, data.GetSeries(1).Get(2).Get())
	// AddSeries
	data2 := data.AddSeries("c4", NewBoolSeriesVarArg(true, true, false, false))
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, 3, data.Schema().Len())

	// RenameSeriesByName/RenameSeries
	data2 = data2.RenameSeriesByName("c4", "c44", true)
	assert.Equal(t, 4, data2.Schema().Len())
	assert.Equal(t, "c44", data2.Schema().Get(3).Name)

	// RemoveSeriesByName/RemoveSeries
	data2 = data2.RemoveSeriesByName("c44")
	assert.Equal(t, 3, data2.Schema().Len())

	// WhereRow
	filteredData := data.WhereRow(func(r df.Row) bool {
		return r.GetByName("c3").GetAsString() == "a1"
	})
	assert.Equal(t, int64(1), filteredData.Len())
	assert.Equal(t, int64(4), data.Len())

	// SortByName
	sortedData := data.SortByName(df.SortByName{Series: "c1", Order: df.SortOrderDESC})
	assert.Equal(t, data.Len(), sortedData.Len())
	assert.Equal(t, int64(4), sortedData.GetRow(0).Get(0).Get())
	assert.Equal(t, int64(4), data.Len())

	// SelectSeriesByName
	selectedSeries := data.SelectBySeriesName("c1", "c3")
	assert.Equal(t, int64(4), selectedSeries.Len())
	assert.Equal(t, 2, selectedSeries.Schema().Len())

	// MapRow
	mappedSchema := df.NewSchema([]df.SeriesSchema{{Name: "m1", Format: df.IntegerFormat}})
	mapped := data.MapRow(mappedSchema, func(r df.Row) df.Row {
		return NewRowFromMap(&map[string]df.Value{
			"m1": NewIntValueConst(r.GetAsInt(0)),
		})
	})
	assert.Equal(t, int64(4), mapped.Len())
	assert.Equal(t, 1, mapped.Schema().Len())

	// FlatMapRow
	mapped = data.FlatMapRow(mappedSchema, func(r df.Row) []df.Row {
		return []df.Row{
			NewRowFromMap(&map[string]df.Value{
				"m1": NewIntValueConst(r.GetAsInt(0)),
			}),
			NewRowFromMap(&map[string]df.Value{
				"m1": NewIntValueConst(r.GetAsInt(0)),
			}),
		}
	})
	assert.Equal(t, int64(8), mapped.Len())
	assert.Equal(t, 1, mapped.Schema().Len())

	// ForEachRow
	data.ForEachRow(func(r df.Row) {
		assert.Equal(t, 3, r.Schema().Len())
	})

	// UpdateSeries/UpdateSeriesByName
	updatedData := data.UpdateSeriesByName("c1", data.GetSeriesByName("c1").Map(df.StringFormat, func(v df.Value) df.Value {
		return NewStringValueConst(v.GetAsString())
	}))
	s1 := updatedData.Schema().GetByName("c1")
	assert.Equal(t, df.StringFormat, s1.Format)
	assert.Equal(t, 3, updatedData.Schema().Len())

	// Limit
	limitedData := data.Limit(1, 2)
	assert.Equal(t, int64(2), limitedData.Len())

	// Rename
	renamedData := data.Rename("renamed", true)
	assert.Equal(t, "renamed", data.Name())
	assert.Equal(t, "renamed", renamedData.Name())

	// Group
	grouped := data.Group("c1")
	assert.Equal(t, int64(4), grouped.Len())

	// Append
	appended := data.Append(data)
	assert.Equal(t, int64(8), appended.Len())

	// Join
	equiJoined := data.Join(appended.Schema(), appended, df.JoinEqui, map[string]string{"c1": "c1"}, func(r1, r2 df.Row) []df.Row {
		return []df.Row{r1}
	})
	assert.Equal(t, int64(8), equiJoined.Len())

	crossJoined := data.Join(appended.Schema(), appended, df.JoinCross, map[string]string{}, func(r1, r2 df.Row) []df.Row {
		return []df.Row{r1}
	})
	assert.Equal(t, int64(32), crossJoined.Len())
}

func TestDfMapPar(t *testing.T) {
	s1 := NewIntRangeSeries(100000)
	s2 := NewIntRangeSeries(100000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})
	d.(*inmemoryDataFrame).partitions = 10
	d = d.MapRow(d.Schema(), func(r df.Row) df.Row {
		schema := d.Schema()
		return NewRow(&schema, &[]df.Value{NewIntValueConst(r.Get(0).GetAsInt() * 2), NewIntValueConst(r.Get(0).GetAsInt() + 2)})
	})

	assert.Equal(t, int64(100000), d.Len())
}

func TestDfWherePar(t *testing.T) {
	s1 := NewIntRangeSeries(100000)
	s2 := NewIntRangeSeries(100000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})
	d.(*inmemoryDataFrame).partitions = 10

	d = d.WhereRow(func(r df.Row) bool {
		return r.Get(0).GetAsInt()%2 == 0
	})
	assert.Equal(t, int64(50000), d.Len())
}

func TestDfIntersection(t *testing.T) {
	s1 := NewIntRangeSeries(100000)
	s2 := NewIntRangeSeries(100000)
	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})

	s3 := NewIntRangeSeries(100)
	s4 := NewIntRangeSeries(100)
	d2 := NewDataframeWithNameFromSeries("df2", []string{"s1", "s2"}, &[]df.Series{s3, s4})

	d = d.Intersection(d2)
	assert.Equal(t, int64(100), d.Len())
}

func TestDfExcept(t *testing.T) {
	s1 := NewIntRangeSeries(100000)
	s2 := NewIntRangeSeries(100000)
	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})

	s3 := NewIntRangeSeries(100)
	s4 := NewIntRangeSeries(100)
	d2 := NewDataframeWithNameFromSeries("df2", []string{"s1", "s2"}, &[]df.Series{s3, s4})

	d = d.Except(d2)
	assert.Equal(t, int64(100000-100), d.Len())
}
