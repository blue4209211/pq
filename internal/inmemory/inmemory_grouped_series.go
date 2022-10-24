package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrameSeries struct {
	data map[df.DataFrameSeriesValue]df.DataFrameSeries
	typ  df.DataFrameSeriesFormat
}

func (t *inmemoryGroupedDataFrameSeries) Get(index df.DataFrameSeriesValue) (d df.DataFrameSeries) {
	return t.data[index]
}

func (t *inmemoryGroupedDataFrameSeries) GetKeys() (d []df.DataFrameSeriesValue) {
	d = make([]df.DataFrameSeriesValue, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedDataFrameSeries) ForEach(f func(df.DataFrameSeriesValue, df.DataFrameSeries)) {
	for k, v := range t.data {
		f(k, v)
	}
}

func (t *inmemoryGroupedDataFrameSeries) Map(schema df.DataFrameSeriesFormat, f func(df.DataFrameSeriesValue, df.DataFrameSeries) df.DataFrameSeries) (d df.DataFrameGroupedSeries) {
	d1 := map[df.DataFrameSeriesValue]df.DataFrameSeries{}
	for k, v := range t.data {
		nv := f(k, v)
		d1[k] = nv
	}
	return &inmemoryGroupedDataFrameSeries{data: d1, typ: schema}
}

func (t *inmemoryGroupedDataFrameSeries) Filter(f func(df.DataFrameSeriesValue, df.DataFrameSeries) bool) (d df.DataFrameGroupedSeries) {
	d1 := map[df.DataFrameSeriesValue]df.DataFrameSeries{}
	for k, v := range t.data {
		if f(k, v) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedDataFrameSeries{data: d1, typ: t.typ}
}

func NewGroupedSeries(data df.DataFrameSeries) df.DataFrameGroupedSeries {
	gd := map[df.DataFrameSeriesValue]df.DataFrameSeries{}
	gdv := map[df.DataFrameSeriesValue][]any{}

	data.ForEach(func(dfsv df.DataFrameSeriesValue) {
		k := gdv[dfsv]
		gdv[dfsv] = append(k, dfsv.Get())
	})

	for k, v := range gdv {
		gd[k] = NewSeries(v, data.Schema().Format)
	}

	return &inmemoryGroupedDataFrameSeries{data: gd, typ: data.Schema().Format}
}
