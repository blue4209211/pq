package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
	"golang.org/x/exp/slices"
)

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []*float64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDoubleValue(e)
	}
	return NewSeries(d, df.DoubleFormat)
}

func NewDoubleSeriesVarArg(data ...float64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewDoubleValue(&e2)
	}
	return NewSeries(d, df.DoubleFormat)
}

type doubleVal struct {
	data *float64
}

func (t *doubleVal) Schema() df.Format {
	return df.DoubleFormat
}

func (t *doubleVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *doubleVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *doubleVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *doubleVal) GetAsDouble() (r float64) {
	return *t.data
}

func (t *doubleVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *doubleVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Datetime Value")
	}
	return v.(time.Time)
}

func (t *doubleVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *doubleVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewDoubleValue(data *float64) df.Value {
	return &doubleVal{data: data}
}

func NewDoubleValueConst(data float64) df.Value {
	return &doubleVal{data: &data}
}

type doubleExpr struct {
	name     string
	col      string
	val      df.Value
	parent   df.Expr
	typ      df.ExprOpType
	mapOp    df.ExprMapOp
	filterOp df.ExprFilterOp
	reduceOp df.ExprReduceOp
}

func (t *doubleExpr) FilterOp() df.ExprFilterOp {
	return t.filterOp
}

func (t *doubleExpr) MapOp() df.ExprMapOp {
	return t.mapOp
}

func (t *doubleExpr) ReduceOp() df.ExprReduceOp {
	return t.reduceOp
}

func (t *doubleExpr) Name() string {
	return t.name
}

func (t *doubleExpr) Col() string {
	return t.col
}

func (t *doubleExpr) Const() df.Value {
	return t.val
}

func (t *doubleExpr) OpType() df.ExprOpType {
	if t.typ == "" {
		if t.filterOp != nil {
			return df.ExprTypeFilter
		} else if t.mapOp != nil {
			return df.ExprTypeMap
		} else if t.reduceOp != nil {
			return df.ExprTypeReduce
		}
	}
	return t.typ
}

func (t *doubleExpr) Alias(a string) df.Expr {
	return &doubleExpr{name: a, parent: t}
}

func (t *doubleExpr) Parent() df.Expr {
	return t.parent
}

func (t *doubleExpr) AsFormat(f df.Format) df.Expr {
	return &doubleExpr{name: "cast", parent: t, typ: df.ExprTypeMap, mapOp: newExpMapOp(f, func(v df.Value, args ...df.Value) df.Value {
		a, err := f.Convert(v.Get())
		if err != nil {
			panic("unable to convert data")
		}
		return NewValue(f, a)
	})}
}

func (t *doubleExpr) Op(e df.DoubleSeriesExpr, o df.ExprNumOp) df.DoubleSeriesExpr {
	if o == df.ExprNumOpSum {
		return &doubleExpr{name: "+", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.DoubleFormat, nil)
			}
			o := v.GetAsDouble() + args[0].GetAsDouble()
			return NewDoubleValue(&o)
		}, e)}
	} else if o == df.ExprNumOpMinus {
		return &doubleExpr{name: "-", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.DoubleFormat, nil)
			}
			o := v.GetAsDouble() - args[0].GetAsDouble()
			return NewDoubleValue(&o)
		}, e)}
	} else if o == df.ExprNumOpMul {
		return &doubleExpr{name: "*", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.DoubleFormat, nil)
			}
			o := v.GetAsDouble() * args[0].GetAsDouble()
			return NewDoubleValue(&o)
		}, e)}
	} else if o == df.ExprNumOpDiv {
		return &doubleExpr{name: "/", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.DoubleFormat, nil)
			}
			o := v.GetAsDouble() / args[0].GetAsDouble()
			return NewDoubleValue(&o)
		}, e)}
	} else {
		panic("unknown operation")
	}
}

func (t *doubleExpr) OpConst(e float64, o df.ExprNumOp) df.DoubleSeriesExpr {
	return t.Op(NewDoubleConstExpr(e), o)
}

func (t *doubleExpr) NonNil() (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "is not null", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.IsNil()
	})}
}
func (t *doubleExpr) WhenConst(val map[float64]float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "when", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		k, ok := val[v.GetAsDouble()]
		if ok {
			return NewDoubleValue(&k)
		}
		return v
	})}
}
func (t *doubleExpr) WhenNil(val df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "whennil", col: "", parent: t, mapOp: newExpMapOp(df.DoubleFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		return v
	}, val)}
}

func (t *doubleExpr) WhenNilConst(val float64) (d df.DoubleSeriesExpr) {
	return t.WhenNil(NewDoubleConstExpr(val))
}

func (t *doubleExpr) Between(e, e2 df.DoubleSeriesExpr, e3 df.ExprBetweenInclude) (d df.DoubleSeriesExpr) {
	if e3 == df.ExprBetweenIncludeNeighter {
		return &doubleExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDouble() > args[0].GetAsDouble() && v.GetAsDouble() < args[1].GetAsDouble()
		}, e, e2)}
	} else if e3 == df.ExprBetweenIncludeBoth {
		return &doubleExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDouble() >= args[0].GetAsDouble() && v.GetAsDouble() <= args[1].GetAsDouble()
		}, e, e2)}
	} else if e3 == df.ExprBetweenIncludeLeft {
		return &doubleExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDouble() >= args[0].GetAsDouble() && v.GetAsDouble() < args[1].GetAsDouble()
		}, e, e2)}
	} else if e3 == df.ExprBetweenIncludeRight {
		return &doubleExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDouble() > args[0].GetAsDouble() && v.GetAsDouble() <= args[1].GetAsDouble()
		}, e, e2)}
	} else {
		panic("unknown operation")
	}
}

func (t *doubleExpr) BetweenConst(e, e2 float64, e3 df.ExprBetweenInclude) (d df.DoubleSeriesExpr) {
	return t.Between(NewDoubleConstExpr(e), NewDoubleConstExpr(e2), e3)
}

func (t *doubleExpr) InConst(e ...float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return slices.Contains(e, v.GetAsDouble())
	})}
}

func (t *doubleExpr) NotInConst(e ...float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "not in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return !slices.Contains(e, v.GetAsDouble())
	})}
}

func (t *doubleExpr) Eq(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.Equals(args[0])
	}, e)}
}

func (t *doubleExpr) Lt(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "<", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDouble() < args[0].GetAsDouble()
	}, e)}
}

func (t *doubleExpr) Gt(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: ">", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDouble() > args[0].GetAsDouble()
	}, e)}
}

func (t *doubleExpr) Le(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "<=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDouble() <= args[0].GetAsDouble()
	}, e)}
}

func (t *doubleExpr) Ge(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDouble() >= args[0].GetAsDouble()
	}, e)}
}

func (t *doubleExpr) Ne(e df.DoubleSeriesExpr) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.Equals(args[0])
	}, e)}
}

func (t *doubleExpr) EqConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.GetAsDouble() == e
	})}
}

func (t *doubleExpr) LtConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "<", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsDouble() < e
	})}
}

func (t *doubleExpr) GtConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: ">", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsDouble() > e
	})}
}

func (t *doubleExpr) LeConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "<=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsDouble() <= e
	})}
}

func (t *doubleExpr) GeConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDouble() >= e
	})}
}

func (t *doubleExpr) NeConst(e float64) (d df.DoubleSeriesExpr) {
	return &doubleExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.GetAsDouble() != e
	})}
}

func (t *doubleExpr) AggSum() df.DoubleSeriesExpr {
	return &doubleExpr{name: "sum", col: "", parent: t, reduceOp: newExpReduceOp(df.DoubleFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() && v1.IsNil() {
			return NewDoubleValueConst(0)
		} else if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else {
			return NewDoubleValueConst(v.GetAsDouble() + v1.GetAsDouble())
		}

	})}
}

func (t *doubleExpr) AggMean() df.DoubleSeriesExpr {
	return t.AggSum()

}

func (t *doubleExpr) AggMin() df.DoubleSeriesExpr {
	return &doubleExpr{name: "min", col: "", parent: t, reduceOp: newExpReduceOp(df.DoubleFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else if v.GetAsDouble() < v1.GetAsDouble() {
			return v
		} else {
			return v1
		}

	})}
}

func (t *doubleExpr) AggMax() df.DoubleSeriesExpr {
	return &doubleExpr{name: "max", col: "", parent: t, reduceOp: newExpReduceOp(df.DoubleFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else if v.GetAsDouble() > v1.GetAsDouble() {
			return v
		} else {
			return v1
		}

	})}
}

func NewDoubleExpr() df.DoubleSeriesExpr {
	return &doubleExpr{name: "root", col: ""}
}

func NewDoubleColExpr(col string) df.DoubleSeriesExpr {
	return &doubleExpr{name: "col", col: col}
}

func NewDoubleConstExpr(col float64) df.DoubleSeriesExpr {
	return &doubleExpr{name: "const", val: NewDoubleValue(&col)}
}
