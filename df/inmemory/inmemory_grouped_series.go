package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedSeries struct {
	data   map[any]df.Series
	format df.Format
}

func (t *inmemoryGroupedSeries) Get(index df.Value) (d df.Series) {
	return t.data[index.Get()]
}

func (t *inmemoryGroupedSeries) GetKeys() (d []df.Value) {
	d = make([]df.Value, 0, len(t.data))
	for k := range t.data {
		d = append(d, NewValue(t.format, k))
	}
	return d
}

func (t *inmemoryGroupedSeries) ForEach(f func(df.Value, df.Series)) {
	for k, v := range t.data {
		f(NewValue(t.format, k), v)
	}
}

func (t *inmemoryGroupedSeries) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryGroupedSeries) Map(f func(df.Value, df.Series) df.Series) (d df.GroupedSeries) {
	d1 := map[any]df.Series{}
	for k, v := range t.data {
		nv := f(NewValue(t.format, k), v)
		d1[k] = nv
	}
	return &inmemoryGroupedSeries{data: d1, format: t.format}
}

func (t *inmemoryGroupedSeries) Where(f func(df.Value, df.Series) bool) (d df.GroupedSeries) {
	d1 := map[any]df.Series{}
	for k, v := range t.data {
		if f(NewValue(t.format, k), v) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedSeries{data: d1, format: t.format}
}

func NewGroupedSeries(data df.Series) df.GroupedSeries {
	gd := map[any]df.Series{}
	gdv := map[any][]df.Value{}

	(data).ForEach(func(dfsv df.Value) {
		k := gdv[dfsv.Get()]
		gdv[dfsv.Get()] = append(k, dfsv)
	})

	for k, v := range gdv {
		gd[k] = NewSeries(&v, (data).Schema().Format)
	}

	return &inmemoryGroupedSeries{data: gd, format: data.Schema().Format}
}
