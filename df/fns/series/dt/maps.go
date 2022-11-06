package dt

import (
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func MaskNil(s df.Series, v time.Time) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.Value) df.Value {
		if sv.Get() == nil {
			return inmemory.NewDatetimeValue(&v)
		}
		return inmemory.NewDatetimeValueConst(sv.GetAsDatetime())
	})

	return r
}

func Year(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Year()))
	})

	return r
}

func Month(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Month()))
	})

	return r
}

func Day(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Day()))
	})

	return r
}

func Hour(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Hour()))
	})

	return r
}

func Minute(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Minute()))
	})

	return r
}

func Second(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().Second()))
	})

	return r
}

func UnixMilli(s df.Series) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(int64(sv.GetAsDatetime().UnixMilli()))
	})

	return r
}

func AddDate(s df.Series, y int, m int, d int) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewDatetimeValueConst(sv.GetAsDatetime().AddDate(y, m, d))
	})

	return r
}

func AddTime(s df.Series, h time.Duration, m time.Duration, sec time.Duration) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
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
		return inmemory.NewDatetimeValue(&dt)
	})

	return r
}

func Parse(s df.Series, pattern string) (r df.Series) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for string format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.Value) df.Value {
		dt, _ := time.Parse(pattern, sv.GetAsString())
		return inmemory.NewDatetimeValue(&dt)
	})

	return r
}

func Format(s df.Series, pattern string) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.StringFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewStringValue(nil)
		}
		return inmemory.NewStringValueConst(sv.GetAsDatetime().Format(pattern))
	})

	return r
}

func ToUnixMilli(s df.Series, pattern string) (r df.Series) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewIntValue(nil)
		}
		return inmemory.NewIntValueConst(sv.GetAsDatetime().UnixMilli())
	})

	return r
}

func FromUnixMilli(s df.Series) (r df.Series) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	r = s.Map(df.DateTimeFormat, func(sv df.Value) df.Value {
		if sv == nil || sv.IsNil() {
			return inmemory.NewDatetimeValue(nil)
		}
		return inmemory.NewDatetimeValueConst(time.UnixMilli(sv.GetAsInt()))
	})

	return r
}
