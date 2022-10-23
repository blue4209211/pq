package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrameSeries struct {
	data map[any][]any
	typ  df.DataFrameSeriesFormat
}

func (t *inmemoryGroupedDataFrameSeries) Get(index any) (d df.DataFrameSeries) {
	r, ok := t.data[index]
	if ok {
		d = NewSeries(r, t.typ)
	} else {
		d = NewSeries([]any{}, t.typ)
	}

	return d
}

func (t *inmemoryGroupedDataFrameSeries) GetKeys() (d []any) {
	d = make([]any, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedDataFrameSeries) ForEach(f func(any, df.DataFrameSeries)) {
	for k, v := range t.data {
		f(k, NewSeries(v, t.typ))
	}
}

func (t *inmemoryGroupedDataFrameSeries) Map(schema df.DataFrameSeriesFormat, f func(any, df.DataFrameSeries) df.DataFrameSeries) (d df.DataFrameGroupedSeries) {
	d1 := map[any][]any{}
	for k, v := range t.data {
		nv := f(k, NewSeries(v, t.typ))
		ns := make([]any, 0, nv.Len())
		nv.ForEach(func(f df.DataFrameSeriesValue) {
			ns = append(ns, f.Get())
		})
		d1[k] = ns
	}
	return &inmemoryGroupedDataFrameSeries{data: d1, typ: schema}
}

func (t *inmemoryGroupedDataFrameSeries) Filter(f func(any, df.DataFrameSeries) bool) (d df.DataFrameGroupedSeries) {
	d1 := map[any][]any{}
	for k, v := range t.data {
		if f(k, NewSeries(v, t.typ)) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedDataFrameSeries{data: d1, typ: t.typ}
}

func NewGroupedSeries(data df.DataFrameSeries) df.DataFrameGroupedSeries {
	gd := map[any][]any{}

	data.ForEach(func(dfsv df.DataFrameSeriesValue) {
		k := gd[dfsv.Get()]
		gd[dfsv.Get()] = append(k, dfsv.Get())
	})

	return &inmemoryGroupedDataFrameSeries{data: gd, typ: data.Schema().Format}
}
