package num

import (
	"math"
	"strconv"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

type NumOp string

const (
	NumMulOp NumOp = "mul"
	NumAddOp NumOp = "add"
	NumSubOp NumOp = "sub"
	NumDivOp NumOp = "div"
	NumModOp NumOp = "mod"
	NumPowOp NumOp = "pow"
)

func IntOp(s df.Series, v int64, op NumOp) (r df.Series) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	switch op {
	case NumMulOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(i * v)
		})
	case NumAddOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(i + v)
		})
	case NumSubOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(i - v)
		})
	case NumDivOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(i / v)
		})
	case NumModOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(i % v)
		})
	case NumPowOp:
		r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsInt()
			return inmemory.NewIntValueConst(int64(math.Pow(float64(i), float64(v))))
		})
	}

	return r
}

func DoubleOp(s df.Series, v float64, op NumOp) (r df.Series) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	switch op {
	case NumMulOp:
		r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsDouble()
			return inmemory.NewDoubleValueConst(i * v)
		})
	case NumAddOp:
		r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsDouble()
			return inmemory.NewDoubleValueConst(i + v)
		})
	case NumSubOp:
		r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsDouble()
			return inmemory.NewDoubleValueConst(i - v)
		})
	case NumDivOp:
		r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsDouble()
			return inmemory.NewDoubleValueConst(i / v)
		})
	case NumPowOp:
		r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
			if sv == nil || sv.IsNil() {
				return sv
			}
			i := sv.GetAsDouble()
			return inmemory.NewDoubleValueConst(math.Pow(i, v))
		})
	}
	return r
}

func MaskNilDouble(s df.Series, v float64) (r df.Series) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
		if sv.IsNil() {
			return inmemory.NewDoubleValueConst(v)
		}
		return sv
	})
	return r
}

func MaskNilInt(s df.Series, v int64) (r df.Series) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		if sv.IsNil() {
			return inmemory.NewIntValueConst(v)
		}
		return sv
	})

	return r
}

func ParseInt(s df.Series) (r df.Series) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for str format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.Value) df.Value {
		i, _ := strconv.Atoi(sv.GetAsString())
		return inmemory.NewIntValueConst(int64(i))
	})

	return r
}

func ParseDouble(s df.Series) (r df.Series) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for str format")
	}
	r = s.Map(df.DoubleFormat, func(sv df.Value) df.Value {
		i, _ := strconv.ParseFloat(sv.GetAsString(), 64)
		return inmemory.NewDoubleValueConst(i)
	})

	return r
}

func NumOpSeries(s df.Series, s2 df.Series, op NumOp) (r df.Series) {
	if s.Schema().Format != s2.Schema().Format {
		panic("both formats are not same")
	}

	if !(s.Schema().Format == df.DoubleFormat || s.Schema().Format == df.IntegerFormat) {
		panic("only int and double formats supported")
	}

	sf := s.Schema().Format
	switch op {
	case NumMulOp:
		r = s.Join(sf, s2, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(int64)*dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(float64)*dfsv2.Get().(float64)))
			}
			return r
		})
	case NumAddOp:
		r = s.Join(sf, s2, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(int64)+dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(float64)+dfsv2.Get().(float64)))
			}
			return r
		})
	case NumSubOp:
		r = s.Join(sf, s2, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(int64)-dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(float64)-dfsv2.Get().(float64)))
			}
			return r
		})
	case NumDivOp:
		r = s.Join(sf, s2, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(int64)/dfsv2.Get().(int64)))
			case df.DoubleFormat:
				return append(r, inmemory.NewValue(sf, dfsv1.Get().(float64)/dfsv2.Get().(float64)))
			}
			return r
		})
	case NumPowOp:
		r = s.Join(sf, s2, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
			switch sf {
			case df.IntegerFormat:
				return append(r, inmemory.NewValue(sf, int64(math.Pow(float64(dfsv1.Get().(int64)), float64(dfsv2.Get().(int64))))))
			case df.DoubleFormat:
				return append(r, inmemory.NewValue(sf, math.Pow(dfsv1.Get().(float64), dfsv2.Get().(float64))))
			}
			return r
		})
	default:
		panic("unsupported operation")
	}

	return r
}
