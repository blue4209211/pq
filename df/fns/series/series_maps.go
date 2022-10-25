package series

import (
	"fmt"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func MWhere(s df.DataFrameSeries, f df.DataFrameSeriesFormat, v map[any]any) (r df.DataFrameSeries) {
	r = s.Map(f, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		k, ok := v[sv.Get()]
		if ok {
			return inmemory.NewDataFrameSeriesValue(f, k)
		}
		return sv
	})
	return r
}

func MAsType(s df.DataFrameSeries, t df.DataFrameSeriesFormat) (r df.DataFrameSeries) {
	return s.Map(t, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		v, e := t.Convert(dfsv.Get())
		if e != nil {
			v = nil
		}
		return inmemory.NewDataFrameSeriesValue(t, v)
	})
}

func MSeriesConcat(s df.DataFrameSeries, s1 df.DataFrameSeries, sep string) (r df.DataFrameSeries) {
	r = s.Join(df.StringFormat, s1, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesStringValue(fmt.Sprintf("%v%v%v", dfsv1.Get(), sep, dfsv2.Get())))
	})
	return r
}
