package series

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

type MNumOp string

const (
	MNumMulOp MNumOp = "mul"
	MNumAddOp MNumOp = "add"
	MNumSubOp MNumOp = "sub"
	MNumDivOp MNumOp = "div"
	MNumPowOp MNumOp = "pow"
)

func MNumIntOp(s df.DataFrameSeries, v int64, op MNumOp) (r df.DataFrameSeries) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	switch op {
	case MNumMulOp:
		r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsInt()
			return inmemory.NewDataFrameSeriesIntValue(i * v)
		})
	case MNumAddOp:
		r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsInt()
			return inmemory.NewDataFrameSeriesIntValue(i + v)
		})
	case MNumSubOp:
		r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsInt()
			return inmemory.NewDataFrameSeriesIntValue(i - v)
		})
	case MNumDivOp:
		r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsInt()
			return inmemory.NewDataFrameSeriesIntValue(i / v)
		})
	case MNumPowOp:
		r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsInt()
			return inmemory.NewDataFrameSeriesIntValue(int64(math.Pow(float64(i), float64(v))))
		})
	}

	return r
}

func MNumDoubleOp(s df.DataFrameSeries, v float64, op MNumOp) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	switch op {
	case MNumMulOp:
		r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsDouble()
			return inmemory.NewDataFrameSeriesDoubleValue(i * v)
		})
	case MNumAddOp:
		r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsDouble()
			return inmemory.NewDataFrameSeriesDoubleValue(i + v)
		})
	case MNumSubOp:
		r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsDouble()
			return inmemory.NewDataFrameSeriesDoubleValue(i - v)
		})
	case MNumDivOp:
		r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsDouble()
			return inmemory.NewDataFrameSeriesDoubleValue(i / v)
		})
	case MNumPowOp:
		r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
			i := sv.GetAsDouble()
			return inmemory.NewDataFrameSeriesDoubleValue(math.Pow(i, v))
		})
	}
	return r
}

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

func MWhereNil(s df.DataFrameSeries, f df.DataFrameSeriesFormat, v any) (r df.DataFrameSeries) {
	switch f {
	case df.BoolFormat:
		return MBoolWhereNil(s, v.(bool))
	case df.IntegerFormat:
		return MIntWhereNil(s, v.(int64))
	case df.StringFormat:
		return MStrWhereNil(s, v.(string))
	case df.DoubleFormat:
		return MDoubleWhereNil(s, v.(float64))
	case df.DateTimeFormat:
		return MDatetimeWhereNil(s, v.(time.Time))
	default:
		panic("invalid format")
	}
}

func MStrWhereNil(s df.DataFrameSeries, v string) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesStringValue(v)
		}
		return inmemory.NewDataFrameSeriesStringValue(sv.GetAsString())
	})
	return r
}

func MDoubleWhereNil(s df.DataFrameSeries, v float64) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesDoubleValue(v)
		}
		return inmemory.NewDataFrameSeriesDoubleValue(sv.GetAsDouble())
	})
	return r
}

func MIntWhereNil(s df.DataFrameSeries, v int64) (r df.DataFrameSeries) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesIntValue(v)
		}
		return inmemory.NewDataFrameSeriesIntValue(sv.GetAsInt())
	})

	return r
}

func MBoolWhereNil(s df.DataFrameSeries, v bool) (r df.DataFrameSeries) {
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

func MDatetimeWhereNil(s df.DataFrameSeries, v time.Time) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}
	r = s.Map(df.BoolFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesDatetimeValue(v)
		}
		return inmemory.NewDataFrameSeriesDatetimeValue(sv.GetAsDatetime())
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

func MStrConcat(s df.DataFrameSeries, v string, vs string) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, dfsv.GetAsString()+vs+v)
	})
}

func MStrSubstring(s df.DataFrameSeries, start int, end int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, dfsv.GetAsString()[start:end])
	})
}

func MStrUpper(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ToUpper(dfsv.GetAsString()))
	})
}

func MStrLower(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ToLower(dfsv.GetAsString()))
	})
}

func MStrTitle(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Title(dfsv.GetAsString()))
	})
}

func MStrReplaceAll(s df.DataFrameSeries, match string, replcae string) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ReplaceAll(dfsv.GetAsString(), match, replcae))
	})
}

func MStrReplace(s df.DataFrameSeries, match string, replcae string, n int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Replace(dfsv.GetAsString(), match, replcae, n))
	})
}

func MStrTrim(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimSpace(dfsv.GetAsString()))
	})
}

func MStrSplit(s df.DataFrameSeries, sep string, index int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Split(dfsv.GetAsString(), sep)[index])
	})
}

func MSeriesConcat(s df.DataFrameSeries, s1 df.DataFrameSeries, sep string) (r df.DataFrameSeries) {
	r = s.Join(df.StringFormat, s1, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesStringValue(fmt.Sprintf("%v%v%v", dfsv1.Get(), sep, dfsv2.Get())))
	})
	return r
}

func MSeriesNumOp(s df.DataFrameSeries, s2 df.DataFrameSeries, op MNumOp) (r df.DataFrameSeries) {
	if s.Schema().Format != s2.Schema().Format {
		panic("both formats are not same")
	}

	if s.Schema().Format != df.DoubleFormat || s.Schema().Format != df.IntegerFormat {
		panic("only int and double formats supported")
	}

	sf := s.Schema().Format
	switch op {
	case MNumMulOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)*dfsv2.Get().(int64)))
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)*dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumAddOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)+dfsv2.Get().(int64)))
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)+dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumSubOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)-dfsv2.Get().(int64)))
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)-dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumDivOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)/dfsv2.Get().(int64)))
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)/dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumPowOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, int64(math.Pow(float64(dfsv1.Get().(int64)), float64(dfsv2.Get().(int64))))))
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, math.Pow(dfsv1.Get().(float64), dfsv2.Get().(float64))))
			}
			return r
		})
	default:
		panic("unsupported operation")
	}

	return r
}
