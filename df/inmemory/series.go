package inmemory

import (
	"sort"

	"github.com/blue4209211/pq/df"
)

type genericSeries struct {
	schema df.SeriesSchema
	data   []df.Value
}

func (t *genericSeries) Schema() df.SeriesSchema {
	return t.schema
}

func (t *genericSeries) Len() int64 {
	return int64(len(t.data))
}

func (t *genericSeries) Get(i int64) df.Value {
	return (t.data[i])
}

func (t *genericSeries) ForEach(f func(df.Value)) {
	for _, d := range t.data {
		f(d)
	}
}

func (t *genericSeries) Where(f func(df.Value) bool) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		if f(d) {
			data = append(data, d)
		}
	}
	return NewSeries(data, t.schema.Format)
}

func (t *genericSeries) Select(b df.Series) df.Series {
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
	return NewSeries(data, t.schema.Format)
}

func (t *genericSeries) Map(s df.Format, f func(df.Value) df.Value) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d))
	}
	return NewSeries(data, s)
}

func (t *genericSeries) FlatMap(s df.Format, f func(df.Value) []df.Value) df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		data = append(data, f(d)...)
	}
	return NewSeries(data, s)
}

func (t *genericSeries) Reduce(f func(df.Value, df.Value) df.Value, startValue df.Value) df.Value {
	finalValue := startValue
	for _, d := range t.data {
		finalValue = f(finalValue, d)
	}
	return finalValue
}

func (t *genericSeries) Distinct() df.Series {
	data := make([]df.Value, 0, len(t.data))
	for _, d := range t.data {
		found := false
		for _, v := range data {
			if v.Get() == (d).Get() {
				found = true
				break
			}
		}
		if !found {
			data = append(data, d)
		}
	}
	return NewSeries(data, t.schema.Format)
}

func (t *genericSeries) Copy() df.Series {
	v := make([]df.Value, t.Len())
	copy(v, t.data)

	return NewSeries(v, t.schema.Format)
}

func (t *genericSeries) Limit(offset int, size int) df.Series {
	data := t.data[offset : offset+size]
	return NewSeries(data, t.schema.Format)
}

func (t *genericSeries) Sort(order df.SortOrder) df.Series {
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

	return NewSeries(d, t.schema.Format)
}

func (t *genericSeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) (s df.Series) {
	val := []df.Value{}
	if jointype == df.JoinLeft || jointype == df.JoinRight || jointype == df.JoinEqui {
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
		} else if jointype == df.JoinRight {
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
	return NewSeries(val, schema)
}

func (t *genericSeries) Append(s df.Series) df.Series {
	if t.Schema().Format != s.Schema().Format {
		panic("types are not same")
	}
	dv := make([]df.Value, t.Len())
	copy(dv, t.data)
	for i := int64(0); i < s.Len(); i++ {
		dv = append(dv, s.Get(i))
	}
	return NewSeries(dv, t.schema.Format)
}

func (t *genericSeries) Group() df.GroupedSeries {
	return NewGroupedSeries(t)
}

// NewSeries returns a column of given type
func NewSeries(data []df.Value, colSchema df.Format) df.Series {
	return NewSeriesWihNameAndCopy(data, colSchema, "", false)
}

func NewSeriesWihNameAndCopy(data []df.Value, colFormat df.Format, colName string, dataCopy bool) df.Series {
	data2 := data
	if dataCopy {
		data2 = make([]df.Value, len(data))
		copy(data2, data)
	}
	return &genericSeries{schema: df.SeriesSchema{Name: colName, Format: colFormat}, data: data2}
}
