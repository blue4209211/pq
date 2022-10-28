package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedSeries struct {
	data map[df.Value]df.Series
}

func (t *inmemoryGroupedSeries) Get(index df.Value) (d df.Series) {
	return t.data[index]
}

func (t *inmemoryGroupedSeries) GetKeys() (d []df.Value) {
	d = make([]df.Value, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedSeries) ForEach(f func(df.Value, df.Series)) {
	for k, v := range t.data {
		f(k, v)
	}
}

func (t *inmemoryGroupedSeries) Map(f func(df.Value, df.Series) df.Series) (d df.GroupedSeries) {
	d1 := map[df.Value]df.Series{}
	for k, v := range t.data {
		nv := f(k, v)
		d1[k] = nv
	}
	return &inmemoryGroupedSeries{data: d1}
}

func (t *inmemoryGroupedSeries) Where(f func(df.Value, df.Series) bool) (d df.GroupedSeries) {
	d1 := map[df.Value]df.Series{}
	for k, v := range t.data {
		if f(k, v) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedSeries{data: d1}
}

func NewGroupedSeries(data df.Series) df.GroupedSeries {
	gd := map[df.Value]df.Series{}
	gdv := map[df.Value][]df.Value{}

	(data).ForEach(func(dfsv df.Value) {
		k := gdv[dfsv]
		gdv[dfsv] = append(k, dfsv)
	})

	for k, v := range gdv {
		gd[k] = NewValueSeries(&v, (data).Schema().Format)
	}

	return &inmemoryGroupedSeries{data: gd}
}
