package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func Where(s df.DataFrameSeries, f df.DataFrameSeriesFormat, v map[any]any) (r df.DataFrameSeries) {
	r = s.Map(f, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		k, ok := v[sv.Get()]
		if ok {
			return inmemory.NewDataFrameSeriesValue(f, k)
		}
		return sv
	})
	return r
}

func AsType(s df.DataFrameSeries, t df.DataFrameSeriesFormat) (r df.DataFrameSeries) {
	return s.Map(t, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		v, e := t.Convert(dfsv.Get())
		if e != nil {
			v = nil
		}
		return inmemory.NewDataFrameSeriesValue(t, v)
	})
}
