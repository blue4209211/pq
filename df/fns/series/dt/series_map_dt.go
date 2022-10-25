package dt

import (
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func WhereNil(s df.DataFrameSeries, v time.Time) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesDatetimeValue(v)
		}
		return inmemory.NewDataFrameSeriesDatetimeValue(sv.GetAsDatetime())
	})

	return r
}

func Year(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Year()))
	})

	return r
}

func Month(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Month()))
	})

	return r
}

func Day(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Day()))
	})

	return r
}

func Hour(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Hour()))
	})

	return r
}

func Minute(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Minute()))
	})

	return r
}

func Second(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().Second()))
	})

	return r
}

func UnixMilli(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(int64(sv.GetAsDatetime().UnixMilli()))
	})

	return r
}

func AddDate(s df.DataFrameSeries, y int, m int, d int) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesDatetimeValue(sv.GetAsDatetime().AddDate(y, m, d))
	})

	return r
}

func AddTime(s df.DataFrameSeries, h time.Duration, m time.Duration, sec time.Duration) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		dt := sv.GetAsDatetime()
		if h != 0 {
			dt = dt.Add(time.Hour * h)
		}
		if m != 0 {
			dt = dt.Add(time.Minute * m)
		}
		if m != 0 {
			dt = dt.Add(time.Second * sec)
		}
		return inmemory.NewDataFrameSeriesDatetimeValue(dt)
	})

	return r
}

func Parse(s df.DataFrameSeries, pattern string) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for string format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		dt, _ := time.Parse(pattern, sv.GetAsString())
		return inmemory.NewDataFrameSeriesDatetimeValue(dt)
	})

	return r
}

func Format(s df.DataFrameSeries, pattern string) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesStringValue(sv.GetAsDatetime().Format(pattern))
	})

	return r
}

func ToUnixMilli(s df.DataFrameSeries, pattern string) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesIntValue(sv.GetAsDatetime().UnixMilli())
	})

	return r
}

func FromUnixMilli(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for int format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesDatetimeValue(time.UnixMilli(sv.GetAsInt()))
	})

	return r
}
