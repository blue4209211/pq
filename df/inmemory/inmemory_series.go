package inmemory

import (
	"sort"
	"time"

	"github.com/blue4209211/pq/df"
)

//TODO create series for different types

type inmemorySeries struct {
	schema df.SeriesSchema
	data   []df.Value
}

func (t *inmemorySeries) Schema() df.SeriesSchema {
	return t.schema
}

func (t *inmemorySeries) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemorySeries) Get(i int64) df.Value {
	return t.data[i]
}

func (t *inmemorySeries) ForEach(f func(df.Value)) {
	for _, d := range t.data {
		f(d)
	}
}

func (t *inmemorySeries) Where(f func(df.Value) bool) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		if f(d) {
			data = append(data, d)
		}
	}
	return NewValueSeries(data, t.schema.Format)
}

func (t *inmemorySeries) Select(b df.Series) df.Series {
	if b.Schema().Format != df.BoolFormat {
		panic("Only bool series supported")
	}
	data := make([]df.Value, 0, len(t.data))
	seriesLength := b.Len()
	for i, d := range t.data {
		if int64(i) < seriesLength && b.Get(int64(i)).GetAsBool() {
			data = append(data, d)
		}
	}
	return NewValueSeries(data, t.schema.Format)
}

func (t *inmemorySeries) Map(s df.Format, f func(df.Value) df.Value) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d))
	}
	return NewValueSeries(data, s)
}

func (t *inmemorySeries) FlatMap(s df.Format, f func(df.Value) []df.Value) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d)...)
	}
	return NewValueSeries(data, s)
}

func (t *inmemorySeries) Reduce(f func(df.Value, df.Value) df.Value, startValue df.Value) df.Value {
	finalValue := startValue
	for _, d := range t.data {
		finalValue = f(finalValue, d)
	}
	return finalValue
}

//TODO use maps{}
func (t *inmemorySeries) Distinct() df.Series {
	data := make([]df.Value, 0, len(t.data))
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

func (t *inmemorySeries) Copy() df.Series {
	v := make([]df.Value, t.Len())
	copy(v, t.data)

	return NewValueSeries(v, t.schema.Format)
}

func (t *inmemorySeries) Limit(offset int, size int) df.Series {
	return NewValueSeries(t.data[offset:offset+size], t.schema.Format)
}

func (t *inmemorySeries) Sort(order df.SortOrder) df.Series {
	d := make([]df.Value, len(t.data))
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

func (t *inmemorySeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) (s df.Series) {
	val := []df.Value{}
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

func (t *inmemorySeries) Append(s df.Series) df.Series {
	if t.Schema().Format != s.Schema().Format {
		panic("types are not same")
	}
	dv := make([]df.Value, t.Len())
	copy(dv, t.data)
	for i := int64(0); i < s.Len(); i++ {
		dv = append(dv, s.Get(i))
	}
	return NewValueSeries(dv, t.schema.Format)
}

func (t *inmemorySeries) Group() df.GroupedSeries {
	return NewGroupedSeries(t)
}

//
func NewNamedSeries(data []any, colFormat df.Format, colName string) df.Series {
	data2 := make([]df.Value, len(data))
	for i, v := range data {
		data2[i] = NewValue(colFormat, v)
	}
	return NewValueSeriesWihNameAndCopy(data2, colFormat, colName, false)
}

// NewSeries returns a column of given type
func NewSeries(data []any, colSchema df.Format) df.Series {
	return NewNamedSeries(data, colSchema, "")
}

// NewSeries returns a column of given type
func NewValueSeries(data []df.Value, colSchema df.Format) df.Series {
	return NewValueSeriesWihNameAndCopy(data, colSchema, "", false)
}

func NewValueSeriesWihNameAndCopy(data []df.Value, colFormat df.Format, colName string, dataCopy bool) df.Series {
	data2 := data
	if dataCopy {
		data2 = make([]df.Value, len(data))
		copy(data2, data)
	}
	return &inmemorySeries{schema: df.SeriesSchema{Name: colName, Format: colFormat}, data: data2}
}

// NewStringSeries returns a column of type string
func NewStringSeries(data []string) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewStringValue(e)
	}
	return NewValueSeries(d, df.StringFormat)
}

// NewIntSeries returns a column of type int
func NewIntSeries(data []int64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewIntValue(e)
	}
	return NewValueSeries(d, df.IntegerFormat)
}

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []bool) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewBoolValue(e)
	}
	return NewValueSeries(d, df.BoolFormat)
}

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []float64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDoubleValue(e)
	}
	return NewValueSeries(d, df.DoubleFormat)
}

// NewDoubleSeries returns a column of type double
func NewDatetimeSeries(data []time.Time) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDatetimeValue(e)
	}
	return NewValueSeries(d, df.DateTimeFormat)
}
