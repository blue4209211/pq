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
