package inmemory

import (
	"fmt"
	"sort"
	"sync"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/log"
	"github.com/samber/lo"
)

type genericSeries struct {
	schema     df.SeriesSchema
	data       []df.Value
	partitions int
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
	if t.partitions < 2 {
		for _, d := range t.data {
			f(d)
		}
	} else {
		var wg sync.WaitGroup
		wg.Add(t.partitions)
		length := len(t.data) / t.partitions
		for part := 0; part < t.partitions; part++ {
			start := part * length
			end := start + length
			if end > length {
				end = length
			}
			go func(start, end int) {
				defer wg.Done()
				for k := start; k < end; k++ {
					f(t.data[k])
				}
			}(start, end)
		}
		wg.Wait()
	}
}

func (t *genericSeries) Where(f func(df.Value) bool) df.Series {
	if t.partitions < 2 {
		data := make([]df.Value, 0, len(t.data))
		for i := int64(0); i < t.Len(); i++ {
			if f(t.data[i]) {
				data = append(data, t.data[i])
			}
		}
		return NewSeries(data, t.schema.Format)
	} else {
		data := make([]df.Value, 0, len(t.data))
		var wg sync.WaitGroup
		wg.Add(t.partitions)
		length := len(t.data) / t.partitions
		mutex := sync.Mutex{}

		for part := 0; part < t.partitions; part++ {
			start := part * length
			end := start + length
			if end > length {
				end = length
			}
			go func(start, end, part int) {
				defer wg.Done()
				data2 := []df.Value{}
				for k := start; k < end; k++ {
					if f(t.data[k]) {
						data2 = append(data2, t.data[k])
					}
				}
				mutex.Lock()
				data = append(data, data2...)
				mutex.Unlock()
			}(start, end, part)
		}
		wg.Wait()
		return NewSeries(data, t.schema.Format)
	}
}

func (t *genericSeries) Select(e df.Expr) (s df.Series) {
	log.Debug("select called", e.OpType(), e.Name())

	var expressionData df.Series
	if e.Parent() != nil {
		expressionData = t.Select(e.Parent())
	} else {
		if e.Const() != nil {
			return NewConstSeries(e.Const(), int(t.Len()))
		} else if e.Col() != "" {
			panic("only const is supported in series expression")
		}
	}

	if e.OpType() == df.ExprTypeFilter {
		log.Debug("filter called")
		args := []df.Value{}
		if len(e.FilterOp().Args()) > 0 {
			args = lo.Map(e.FilterOp().Args(), func(v df.Expr, i int) df.Value {
				return v.Const()
			})
		}
		return expressionData.Where(func(v df.Value) bool {
			return e.FilterOp().ApplyFilter(v, args...)
		})
	} else if e.OpType() == df.ExprTypeMap {
		args := []df.Value{}
		if len(e.MapOp().Args()) > 0 {
			args = lo.Map(e.MapOp().Args(), func(v df.Expr, i int) df.Value {
				return v.Const()
			})
		}
		return expressionData.Map(e.MapOp().ReturnFormat(), func(v df.Value) df.Value {
			return e.MapOp().ApplyMap(v, args...)
		})
	} else if e.OpType() == "" {
		return t
	} else {
		panic(fmt.Sprintf("unsupported opType %s, opName %s ", e.OpType(), e.Name()))
	}
}

func (t *genericSeries) Map(s df.Format, f func(df.Value) df.Value) df.Series {
	if t.partitions < 2 {
		data := make([]df.Value, len(t.data))
		for i := 0; i < int(t.Len()); i++ {
			data[i] = f(t.data[i])
		}
		return NewSeries(data, s)
	} else {
		data := make([]df.Value, len(t.data))
		var wg sync.WaitGroup
		wg.Add(t.partitions)
		length := len(t.data) / t.partitions
		for part := 0; part < t.partitions; part++ {
			start := part * length
			end := start + length
			if end > length {
				end = length
			}
			go func(start, end int) {
				defer wg.Done()
				for k := start; k < end; k++ {
					data[k] = f(t.data[k])
				}
			}(start, end)
		}
		wg.Wait()
		return NewSeries(data, s)
	}
}

func (t *genericSeries) FlatMap(s df.Format, f func(df.Value) []df.Value) df.Series {
	if t.partitions < 2 {
		data := make([]df.Value, 0, len(t.data))
		for _, d := range t.data {
			data = append(data, f(d)...)
		}
		return NewSeries(data, s)
	} else {
		data := make([]df.Value, 0, len(t.data))
		var wg sync.WaitGroup
		wg.Add(t.partitions)
		length := len(t.data) / t.partitions
		mutex := sync.Mutex{}

		for part := 0; part < t.partitions; part++ {
			start := part * length
			end := start + length
			if end > length {
				end = length
			}
			go func(start, end, part int) {
				defer wg.Done()
				data2 := []df.Value{}
				for k := start; k < end; k++ {
					data2 = append(data2, f(data[k])...)
				}
				mutex.Lock()
				data = append(data, data2...)
				mutex.Unlock()
			}(start, end, part)
		}
		wg.Wait()
		return NewSeries(data, t.schema.Format)
	}
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

func (t *genericSeries) WhenNil(v1 df.Value) df.Series {
	return t.Map(t.schema.Format, func(v df.Value) df.Value {
		if v.IsNil() {
			return v1
		}
		return v
	})
}

func (t *genericSeries) When(data map[any]df.Value) df.Series {
	return t.Map(t.schema.Format, func(v df.Value) df.Value {
		v1, ok := data[v.Get()]
		if ok {
			return v1
		}
		return v
	})
}

func (t *genericSeries) AsFormat(f df.Format) df.Series {
	return t.Map(f, func(v df.Value) df.Value {
		v1, err := f.Convert(v.Get())
		if err != nil {
			panic(fmt.Sprintf("unable to convert - %v", v.Get()))
		}
		return NewValue(f, v1)
	})
}

func (t *genericSeries) Expr() df.Expr {
	switch t.schema.Format {
	case df.BoolFormat:
		return NewBoolExpr()
	case df.IntegerFormat:
		return NewIntExpr()
	case df.DoubleFormat:
		return NewDoubleExpr()
	case df.StringFormat:
		return NewStringExpr()
	case df.DateTimeFormat:
		return NewDatetimeExpr()
	default:
		panic("unsupported format")
	}
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

func (t *genericSeries) Intersection(s df.Series) df.Series {
	if s.Schema().Format != s.Schema().Format {
		panic("formats should match")
	}
	return t.Join(df.StringFormat, s, df.JoinCross, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		if dfsv1.Get() == dfsv2.Get() {
			return append(r, NewValue(s.Schema().Format, dfsv1.Get()))
		}
		return r
	}).Distinct()

}

func (t *genericSeries) Substract(s df.Series) df.Series {
	if s.Schema().Format != s.Schema().Format {
		panic("formats should match")
	}
	return t.Join(df.StringFormat, s, df.JoinLeft, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		if dfsv2 == nil {
			return []df.Value{dfsv1}
		}
		return r
	}).Distinct()
}

func (t *genericSeries) Union(s df.Series) df.Series {
	return t.Append(s)
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

func NewConstSeries(data df.Value, n int) df.Series {
	s := make([]df.Value, n)
	for i := 0; i < n; i++ {
		s[i] = data
	}

	return &genericSeries{schema: df.SeriesSchema{Name: "", Format: data.Schema()}, data: s}
}
