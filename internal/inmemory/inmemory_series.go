package inmemory

import (
	"reflect"
	"sort"

	"github.com/blue4209211/pq/df"
)

//TODO create series for different types

type inmemoryDataFrameSeries struct {
	schema df.DataFrameSeriesFormat
	data   []any
}

func (t *inmemoryDataFrameSeries) Schema() df.DataFrameSeriesFormat {
	return t.schema
}

func (t *inmemoryDataFrameSeries) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrameSeries) Get(i int64) any {
	return t.data[i]
}

func (t *inmemoryDataFrameSeries) ForEach(f func(any)) {
	for _, d := range t.data {
		f(d)
	}
}

func (t *inmemoryDataFrameSeries) Filter(f func(any) bool) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		if f(d) {
			data = append(data, d)
		}
	}
	return NewSeries(data, t.schema)
}

func (t *inmemoryDataFrameSeries) Map(s df.DataFrameSeriesFormat, f func(any) any) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d))
	}
	return NewSeries(data, s)
}

func (t *inmemoryDataFrameSeries) FlatMap(s df.DataFrameSeriesFormat, f func(any) []any) df.DataFrameSeries {
	data := make([]any, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d)...)
	}
	return NewSeries(data, s)
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
	return NewSeries(data, t.schema)
}

func (t *inmemoryDataFrameSeries) Limit(offset int, size int) df.DataFrameSeries {
	return NewSeries(t.data[offset:offset+size], t.schema)
}

func (t *inmemoryDataFrameSeries) Sort(order df.SortOrder) df.DataFrameSeries {
	d := make([]any, len(t.data), len(t.data))
	for i, e := range t.data {
		d[i] = e
	}

	if t.schema.Type() == reflect.Int64 {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(int64) < d[j].(int64)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(int64) > d[j].(int64)
			})
		}
	} else if t.schema.Type() == reflect.Float64 {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(float64) < d[j].(float64)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(float64) > d[j].(float64)
			})
		}
	} else if t.schema.Type() == reflect.String {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(string) < d[j].(string)
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(string) > d[j].(string)
			})
		}
	} else if t.schema.Type() == reflect.Bool {
		if order == df.SortOrderASC {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(bool) == false
			})
		} else {
			sort.Slice(d, func(i, j int) bool {
				return d[i].(bool) == true
			})
		}
	}

	return NewSeries(d, t.schema)
}

// NewSeries returns a column of given type
func NewSeries(data []any, columnType df.DataFrameSeriesFormat) df.DataFrameSeries {
	return &inmemoryDataFrameSeries{schema: columnType, data: data}
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
