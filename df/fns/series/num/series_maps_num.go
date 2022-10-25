package num

import (
	"math"
	"strconv"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

type MNumOp string

const (
	MNumMulOp MNumOp = "mul"
	MNumAddOp MNumOp = "add"
	MNumSubOp MNumOp = "sub"
	MNumDivOp MNumOp = "div"
	MNumPowOp MNumOp = "pow"
)

func IntOp(s df.DataFrameSeries, v int64, op MNumOp) (r df.DataFrameSeries) {
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

func DoubleOp(s df.DataFrameSeries, v float64, op MNumOp) (r df.DataFrameSeries) {
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

func WhereNilDouble(s df.DataFrameSeries, v float64) (r df.DataFrameSeries) {
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

func WhereNilInt(s df.DataFrameSeries, v int64) (r df.DataFrameSeries) {
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

func ParseInt(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for str format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		i, _ := strconv.Atoi(sv.GetAsString())
		return inmemory.NewDataFrameSeriesIntValue(int64(i))
	})

	return r
}

func ParseDouble(s df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for str format")
	}
	r = s.Map(df.DoubleFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		i, _ := strconv.ParseFloat(sv.GetAsString(), 64)
		return inmemory.NewDataFrameSeriesDoubleValue(i)
	})

	return r
}

func NumOp(s df.DataFrameSeries, s2 df.DataFrameSeries, op MNumOp) (r df.DataFrameSeries) {
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
			case df.DoubleFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)*dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumAddOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)+dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)+dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumSubOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)-dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)-dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumDivOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(int64)/dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, dfsv1.Get().(float64)/dfsv2.Get().(float64)))
			}
			return r
		})
	case MNumPowOp:
		r = s.Join(df.StringFormat, s2, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, int64(math.Pow(float64(dfsv1.Get().(int64)), float64(dfsv2.Get().(int64))))))
			case df.DoubleFormat:
				return append(r, inmemory.NewDataFrameSeriesValue(sf, math.Pow(dfsv1.Get().(float64), dfsv2.Get().(float64))))
			}
			return r
		})
	default:
		panic("unsupported operation")
	}

	return r
}
