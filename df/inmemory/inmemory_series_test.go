package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewStringSeries(t *testing.T) {

	data := []string{
		"abc", "def", "geh", "ijk", "lmn", "abc",
	}

	s := NewStringSeries(data)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.DataFrameSeriesValue) bool {
		return i.Get() == "abc"
	})
	assert.Equal(t, int64(2), sf.Len())

	sm := s.Map(df.StringFormat, func(i df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return NewDataFrameSeriesStringValue(i.GetAsString() + "1")
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+"1", sm.Get(1))

	sfm := s.FlatMap(df.StringFormat, func(i df.DataFrameSeriesValue) []df.DataFrameSeriesValue {
		return []df.DataFrameSeriesValue{
			NewDataFrameSeriesStringValue(i.GetAsString() + "1"),
			NewDataFrameSeriesStringValue(i.GetAsString() + "2"),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+"2", sfm.Get(1))

	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, "lmn", ss.Get(0))

}

func TestNewIntSeries(t *testing.T) {
	data := []int64{
		1, 2, 3, 4, 5, 1,
	}

	s := NewIntSeries(data)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.DataFrameSeriesValue) bool {
		return i.GetAsInt() == int64(1)
	})
	assert.Equal(t, int64(2), sf.Len())

	sm := s.Map(df.IntegerFormat, func(i df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return NewDataFrameSeriesIntValue(i.GetAsInt() + 10)
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+10, sm.Get(1))

	sfm := s.FlatMap(df.IntegerFormat, func(i df.DataFrameSeriesValue) []df.DataFrameSeriesValue {
		return []df.DataFrameSeriesValue{
			NewDataFrameSeriesIntValue(i.GetAsInt() + 10),
			NewDataFrameSeriesIntValue(i.GetAsInt() + 20),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+20, sfm.Get(1))

	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, int64(5), ss.Get(0))
}

func TestNewBoolSeries(t *testing.T) {
	data := []bool{
		true, false, true, false, true, false,
	}

	s := NewBoolSeries(data)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.DataFrameSeriesValue) bool {
		return i.GetAsBool() == true
	})
	assert.Equal(t, int64(3), sf.Len())

	sm := s.Map(df.BoolFormat, func(i df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return NewDataFrameSeriesBoolValue(!i.GetAsBool())
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, true, sm.Get(1))

	sfm := s.FlatMap(df.BoolFormat, func(i df.DataFrameSeriesValue) []df.DataFrameSeriesValue {
		return []df.DataFrameSeriesValue{
			NewDataFrameSeriesBoolValue(i.GetAsBool()),
			NewDataFrameSeriesBoolValue(i.GetAsBool()),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, true, sfm.Get(1))

	sd := s.Distinct()
	assert.Equal(t, int64(2), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, true, ss.Get(0))

}

func TestNewDoubleSeries(t *testing.T) {
	data := []float64{
		1, 2, 3, 4, 5, 1,
	}

	s := NewDoubleSeries(data)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.DataFrameSeriesValue) bool {
		return i.GetAsDouble() == float64(1)
	})
	assert.Equal(t, int64(2), sf.Len())

	sm := s.Map(df.DoubleFormat, func(i df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return NewDataFrameSeriesDoubleValue(i.GetAsDouble() + 10)
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+10, sm.Get(1))

	sfm := s.FlatMap(df.IntegerFormat, func(i df.DataFrameSeriesValue) []df.DataFrameSeriesValue {
		return []df.DataFrameSeriesValue{
			NewDataFrameSeriesDoubleValue(i.GetAsDouble() + 10),
			NewDataFrameSeriesDoubleValue(i.GetAsDouble() + 10),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+20, sfm.Get(1))

	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, float64(5), ss.Get(0))
}
