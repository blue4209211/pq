package inmemory

import (
	"sort"
	"time"

	"github.com/blue4209211/pq/df"
)

//TODO create series for different types

type inmemoryDataFrameSeries struct {
	schema df.SeriesSchema
	data   []df.DataFrameSeriesValue
}

func (t *inmemoryDataFrameSeries) Schema() df.SeriesSchema {
	return t.schema
}

func (t *inmemoryDataFrameSeries) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrameSeries) Get(i int64) df.DataFrameSeriesValue {
	return t.data[i]
}

func (t *inmemoryDataFrameSeries) ForEach(f func(df.DataFrameSeriesValue)) {
	for _, d := range t.data {
		f(d)
	}
}

func (t *inmemoryDataFrameSeries) Where(f func(df.DataFrameSeriesValue) bool) df.DataFrameSeries {
	data := make([]df.DataFrameSeriesValue, 0, len(t.data))
	for _, d := range t.data {
		if f(d) {
			data = append(data, d)
		}
	}
	return NewValueSeries(data, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Select(b df.DataFrameSeries) df.DataFrameSeries {
	if b.Schema().Format != df.BoolFormat {
		panic("Only bool series supported")
	}
	data := make([]df.DataFrameSeriesValue, 0, len(t.data))
	seriesLength := b.Len()
	for i, d := range t.data {
		if int64(i) < seriesLength && b.Get(int64(i)).GetAsBool() {
			data = append(data, d)
		}
	}
	return NewValueSeries(data, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Map(s df.DataFrameSeriesFormat, f func(df.DataFrameSeriesValue) df.DataFrameSeriesValue) df.DataFrameSeries {
	data := make([]df.DataFrameSeriesValue, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d))
	}
	return NewValueSeries(data, s)
}

func (t *inmemoryDataFrameSeries) FlatMap(s df.DataFrameSeriesFormat, f func(df.DataFrameSeriesValue) []df.DataFrameSeriesValue) df.DataFrameSeries {
	data := make([]df.DataFrameSeriesValue, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d)...)
	}
	return NewValueSeries(data, s)
}

func (t *inmemoryDataFrameSeries) Reduce(f func(df.DataFrameSeriesValue, df.DataFrameSeriesValue) df.DataFrameSeriesValue, startValue df.DataFrameSeriesValue) df.DataFrameSeriesValue {
	finalValue := startValue
	for _, d := range t.data {
		finalValue = f(finalValue, d)
	}
	return finalValue
}

//TODO use maps{}
func (t *inmemoryDataFrameSeries) Distinct() df.DataFrameSeries {
	data := make([]df.DataFrameSeriesValue, 0, len(t.data))
	for _, d := range t.data {
		found := false
		for _, v := range data {
			if v.Get() == d.Get() {
				found = true
				break
			}
		}
		if !found {
			data = append(data, d)
		}
	}
	return NewValueSeries(data, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Copy() df.DataFrameSeries {
	v := make([]df.DataFrameSeriesValue, t.Len())
	copy(v, t.data)

	return NewValueSeries(v, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Limit(offset int, size int) df.DataFrameSeries {
	return NewValueSeries(t.data[offset:offset+size], t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Sort(order df.SortOrder) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(t.data))
	copy(d, t.data)

	if t.schema.Format == df.IntegerFormat {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsInt() < d[j].GetAsInt()
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsInt() > d[j].GetAsInt()
			})
		}
	} else if t.schema.Format == df.DoubleFormat {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsDouble() < d[j].GetAsDouble()
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsDouble() > d[j].GetAsDouble()
			})
		}
	} else if t.schema.Format == df.StringFormat {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsString() < d[j].GetAsString()
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsString() > d[j].GetAsString()
			})
		}
	} else if t.schema.Format == df.BoolFormat {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return !d[i].GetAsBool()
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsBool()
			})
		}
	} else if t.schema.Format == df.DateTimeFormat {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsDatetime().Before(d[j].GetAsDatetime())
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].GetAsDatetime().After(d[j].GetAsDatetime())
			})
		}
	}

	return NewValueSeries(d, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Join(schema df.DataFrameSeriesFormat, series df.DataFrameSeries, jointype df.JoinType, f func(df.DataFrameSeriesValue, df.DataFrameSeriesValue) []df.DataFrameSeriesValue) (s df.DataFrameSeries) {
	val := []df.DataFrameSeriesValue{}
	if jointype == df.JoinLeft || jointype == df.JoinReft || jointype == df.JoinEqui {
		min := int64(len(t.data))
		if series.Len() < min {
			min = series.Len()
		}
		for i := int64(0); i < min; i++ {
			val = append(val, f(t.Get(i), series.Get(i))...)
		}
		if jointype == df.JoinLeft {
			for i := int64(min); i < int64(len(t.data)); i++ {
				val = append(val, f(t.Get(i), nil)...)
			}
		} else if jointype == df.JoinReft {
			for i := int64(min); i < int64(len(t.data)); i++ {
				val = append(val, f(nil, series.Get(i))...)
			}
		}
	} else if jointype == df.JoinCross {
		for i := int64(0); i < t.Len(); i++ {
			for j := int64(0); j < series.Len(); j++ {
				val = append(val, f(t.Get(i), series.Get(j))...)
			}
		}
	}
	return NewValueSeries(val, schema)
}

func (t *inmemoryDataFrameSeries) Append(s df.DataFrameSeries) df.DataFrameSeries {
	if t.Schema().Format != s.Schema().Format {
		panic("types are not same")
	}
	dv := make([]df.DataFrameSeriesValue, t.Len())
	copy(dv, t.data)
	for i := int64(0); i < s.Len(); i++ {
		dv = append(dv, s.Get(i))
	}
	return NewValueSeries(dv, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Group() df.DataFrameGroupedSeries {
	return NewGroupedSeries(t)
}

//
func NewNamedSeries(data []any, colFormat df.DataFrameSeriesFormat, colName string) df.DataFrameSeries {
	data2 := make([]df.DataFrameSeriesValue, len(data))
	for i, v := range data {
		data2[i] = NewDataFrameSeriesValue(colFormat, v)
	}
	return NewValueSeriesWihNameAndCopy(data2, colFormat, colName, false)
}

// NewSeries returns a column of given type
func NewSeries(data []any, colSchema df.DataFrameSeriesFormat) df.DataFrameSeries {
	return NewNamedSeries(data, colSchema, "")
}

// NewSeries returns a column of given type
func NewValueSeries(data []df.DataFrameSeriesValue, colSchema df.DataFrameSeriesFormat) df.DataFrameSeries {
	return NewValueSeriesWihNameAndCopy(data, colSchema, "", false)
}

func NewValueSeriesWihNameAndCopy(data []df.DataFrameSeriesValue, colFormat df.DataFrameSeriesFormat, colName string, dataCopy bool) df.DataFrameSeries {
	data2 := data
	if dataCopy {
		data2 = make([]df.DataFrameSeriesValue, len(data))
		copy(data2, data)
	}
	return &inmemoryDataFrameSeries{schema: df.SeriesSchema{Name: colName, Format: colFormat}, data: data2}
}

// NewStringSeries returns a column of type string
func NewStringSeries(data []string) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(data))
	for i, e := range data {
		d[i] = NewDataFrameSeriesStringValue(e)
	}
	return NewValueSeries(d, df.StringFormat)
}

// NewIntSeries returns a column of type int
func NewIntSeries(data []int64) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(data))
	for i, e := range data {
		d[i] = NewDataFrameSeriesIntValue(e)
	}
	return NewValueSeries(d, df.IntegerFormat)
}

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []bool) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(data))
	for i, e := range data {
		d[i] = NewDataFrameSeriesBoolValue(e)
	}
	return NewValueSeries(d, df.BoolFormat)
}

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []float64) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(data))
	for i, e := range data {
		d[i] = NewDataFrameSeriesDoubleValue(e)
	}
	return NewValueSeries(d, df.DoubleFormat)
}

// NewDoubleSeries returns a column of type double
func NewDatetimeSeries(data []time.Time) df.DataFrameSeries {
	d := make([]df.DataFrameSeriesValue, len(data))
	for i, e := range data {
		d[i] = NewDataFrameSeriesDatetimeValue(e)
	}
	return NewValueSeries(d, df.DateTimeFormat)
}
