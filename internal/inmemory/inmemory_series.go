package inmemory

import (
	"reflect"
	"sort"
	"time"

	"github.com/blue4209211/pq/df"
)

//TODO create series for different types

type inmemoryDataFrameSeries struct {
	schema df.SeriesSchema
	data   []any
}

func (t *inmemoryDataFrameSeries) Schema() df.SeriesSchema {
	return t.schema
}

func (t *inmemoryDataFrameSeries) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrameSeries) Get(i int64) df.DataFrameSeriesValue {
	return NewDataFrameSeriesValue(t.schema.Format, t.data[i])
}

func (t *inmemoryDataFrameSeries) ForEach(f func(df.DataFrameSeriesValue)) {
	for _, d := range t.data {
		f(NewDataFrameSeriesValue(t.schema.Format, d))
	}
}

func (t *inmemoryDataFrameSeries) Where(f func(df.DataFrameSeriesValue) bool) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		if f(NewDataFrameSeriesValue(t.schema.Format, d)) {
			data = append(data, d)
		}
	}
	return NewNamedSeries(data, t.schema.Format, t.schema.Name)
}

func (t *inmemoryDataFrameSeries) Select(b df.DataFrameSeries) df.DataFrameSeries {
	if b.Schema().Format != df.BoolFormat {
		panic("Only bool series supported")
	}
	data := make([]any, 0, len(t.data))
	seriesLength := b.Len()
	for i, d := range t.data {
		if int64(i) < seriesLength && b.Get(int64(i)).GetAsBool() {
			data = append(data, d)
		}
	}
	return NewNamedSeries(data, t.schema.Format, t.schema.Name)
}

func (t *inmemoryDataFrameSeries) Map(s df.DataFrameSeriesFormat, f func(df.DataFrameSeriesValue) df.DataFrameSeriesValue) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(NewDataFrameSeriesValue(t.schema.Format, d)).Get())
	}
	return NewSeries(data, s)
}

func (t *inmemoryDataFrameSeries) FlatMap(s df.DataFrameSeriesFormat, f func(df.DataFrameSeriesValue) []df.DataFrameSeriesValue) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		for _, k := range f(NewDataFrameSeriesValue(t.schema.Format, d)) {
			data = append(data, k)
		}
	}
	return NewSeries(data, s)
}

func (t *inmemoryDataFrameSeries) Reduce(f func(df.DataFrameSeriesValue, df.DataFrameSeriesValue) df.DataFrameSeriesValue, startValue df.DataFrameSeriesValue) df.DataFrameSeriesValue {
	finalValue := startValue
	for _, d := range t.data {
		finalValue = f(finalValue, NewDataFrameSeriesValue(t.schema.Format, d))
	}
	return finalValue
}

//TODO use maps{}
func (t *inmemoryDataFrameSeries) Distinct() df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		found := false
		for _, v := range data {
			if v == d {
				found = true
				break
			}
		}
		if !found {
			data = append(data, d)
		}
	}
	return NewNamedSeries(data, t.schema.Format, t.schema.Name)
}

func (t *inmemoryDataFrameSeries) Copy() df.DataFrameSeries {
	v := make([]any, t.Len())
	copy(v, t.data)

	return NewNamedSeries(v, t.schema.Format, t.schema.Name+"_Copy")
}

func (t *inmemoryDataFrameSeries) Limit(offset int, size int) df.DataFrameSeries {
	return NewNamedSeries(t.data[offset:offset+size], t.schema.Format, t.schema.Name)
}

func (t *inmemoryDataFrameSeries) Sort(order df.SortOrder) df.DataFrameSeries {
	d := make([]any, len(t.data))
	copy(d, t.data)

	if t.schema.Format.Type() == reflect.Int64 {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(int64) < d[j].(int64)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(int64) > d[j].(int64)
			})
		}
	} else if t.schema.Format.Type() == reflect.Float64 {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(float64) < d[j].(float64)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(float64) > d[j].(float64)
			})
		}
	} else if t.schema.Format.Type() == reflect.String {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(string) < d[j].(string)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(string) > d[j].(string)
			})
		}
	} else if t.schema.Format.Type() == reflect.Bool {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return !d[i].(bool)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(bool)
			})
		}
	}

	return NewNamedSeries(d, t.schema.Format, t.schema.Name)
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
	dv := make([]any, t.Len())
	copy(dv, t.data)
	for i := int64(0); i < s.Len(); i++ {
		dv = append(dv, s.Get(i).Get())
	}
	return NewSeries(dv, t.schema.Format)
}

func (t *inmemoryDataFrameSeries) Group() df.DataFrameGroupedSeries {
	return NewGroupedSeries(t)
}

//
func NewNamedSeries(data []any, colFormat df.DataFrameSeriesFormat, colName string) df.DataFrameSeries {
	colSchema := df.SeriesSchema{Name: colName, Format: colFormat}
	return &inmemoryDataFrameSeries{schema: colSchema, data: data}
}

// NewSeries returns a column of given type
func NewSeries(data []any, colSchema df.DataFrameSeriesFormat) df.DataFrameSeries {
	return NewNamedSeries(data, colSchema, "")
}

// NewSeries returns a column of given type
func NewValueSeries(data []df.DataFrameSeriesValue, colSchema df.DataFrameSeriesFormat) df.DataFrameSeries {
	val := []any{}
	for _, v := range data {
		val = append(val, v.Get())
	}
	return NewNamedSeries(val, colSchema, "")
}

// NewStringSeries returns a column of type string
func NewStringSeries(data []string) df.DataFrameSeries {
	d := make([]any, len(data))
	for i, e := range data {
		d[i] = e
	}
	return NewSeries(d, df.StringFormat)
}

// NewIntSeries returns a column of type int
func NewIntSeries(data []int64) df.DataFrameSeries {
	d := make([]any, len(data))
	for i, e := range data {
		d[i] = e
	}
	return NewSeries(d, df.IntegerFormat)
}

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []bool) df.DataFrameSeries {
	d := make([]any, len(data))
	for i, e := range data {
		d[i] = e
	}
	return NewSeries(d, df.BoolFormat)
}

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []float64) df.DataFrameSeries {
	d := make([]any, len(data))
	for i, e := range data {
		d[i] = e
	}
	return NewSeries(d, df.DoubleFormat)
}

// NewDoubleSeries returns a column of type double
func NewDatetimeSeries(data []time.Time) df.DataFrameSeries {
	d := make([]any, len(data))
	for i, e := range data {
		d[i] = e
	}
	return NewSeries(d, df.DateTimeFormat)
}
