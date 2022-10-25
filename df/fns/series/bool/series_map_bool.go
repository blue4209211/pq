package bool

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func WhereNil(s df.DataFrameSeries, v bool) (r df.DataFrameSeries) {
	if s.Schema().Format != df.BoolFormat {
		panic("only supported for bool format")
	}
	r = s.Map(df.BoolFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesBoolValue(v)
		}
		return inmemory.NewDataFrameSeriesBoolValue(sv.GetAsBool())
	})

	return r
}

func Not(bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesBoolValue(!dfsv.GetAsBool())
	})
}

func And(bs df.DataFrameSeries, v bool) (r df.DataFrameSeries) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesBoolValue(dfsv.GetAsBool() && v)
	})
}

func Or(bs df.DataFrameSeries, v bool) (r df.DataFrameSeries) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesBoolValue(dfsv.GetAsBool() || v)
	})
}

func AndSeries(s df.DataFrameSeries, bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesBoolValue(dfsv1.GetAsBool() && dfsv2.GetAsBool()))
	})

	return r

}

func OrSeries(s df.DataFrameSeries, bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesBoolValue(dfsv1.GetAsBool() || dfsv2.GetAsBool()))
	})
	return r
}
