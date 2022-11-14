package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
	"golang.org/x/exp/slices"
)

// NewIntSeries returns a column of type int
func NewIntSeries(data []*int64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewIntValue(e)
	}
	return NewSeries(d, df.IntegerFormat)
}

func NewIntSeriesVarArg(data ...int64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewIntValue(&e2)
	}
	return NewSeries(d, df.IntegerFormat)
}

func NewIntRangeSeries(end int64, args ...int64) df.Series {
	start := int64(0)
	step := int64(1)
	if len(args) > 0 {
		start = args[0]
	}
	if len(args) > 1 {
		step = args[1]
	}
	d := make([]df.Value, (end-start)/step)
	for i := start; i < end; i = i + step {
		i2 := i
		d[i] = NewIntValue(&i2)
	}
	return NewSeries(d, df.IntegerFormat)
}

type intVal struct {
	data *int64
}

func (t *intVal) Schema() df.Format {
	return df.IntegerFormat
}

func (t *intVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *intVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *intVal) GetAsInt() (r int64) {
	return *t.data
}

func (t *intVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *intVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *intVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Datetime Value")
	}
	return v.(time.Time)
}

func (t *intVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *intVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewIntValue(data *int64) df.Value {
	return &intVal{data: data}
}

func NewIntValueConst(data int64) df.Value {
	return &intVal{data: &data}
}

type intExpr struct {
	name     string
	col      string
	val      df.Value
	parent   df.Expr
	typ      df.ExprOpType
	mapOp    df.ExprMapOp
	filterOp df.ExprFilterOp
	reduceOp df.ExprReduceOp
}

func (t *intExpr) FilterOp() df.ExprFilterOp {
	return t.filterOp
}

func (t *intExpr) MapOp() df.ExprMapOp {
	return t.mapOp
}

func (t *intExpr) ReduceOp() df.ExprReduceOp {
	return t.reduceOp
}

func (t *intExpr) Name() string {
	return t.name
}

func (t *intExpr) Col() string {
	return t.col
}

func (t *intExpr) Const() df.Value {
	return t.val
}

func (t *intExpr) OpType() df.ExprOpType {
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

func (t *intExpr) Alias(a string) df.Expr {
	return &intExpr{name: a, parent: t}
}

func (t *intExpr) Parent() df.Expr {
	return t.parent
}

func (t *intExpr) AsFormat(f df.Format) df.Expr {
	return &intExpr{name: "cast", col: "", val: nil, parent: t, mapOp: newExpMapOp(f, func(v df.Value, args ...df.Value) df.Value {
		a, err := f.Convert(v.Get())
		if err != nil {
			panic("unable to convert data")
		}
		return NewValue(f, a)
	})}
}

func (t *intExpr) Op(e df.IntSeriesExpr, o df.ExprNumOp) df.IntSeriesExpr {
	if o == df.ExprNumOpSum {
		return &intExpr{name: "+", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.IntegerFormat, nil)
			}
			o := v.GetAsInt() + args[0].GetAsInt()
			return NewIntValue(&o)
		}, e)}
	} else if o == df.ExprNumOpMinus {
		return &intExpr{name: "-", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.IntegerFormat, nil)
			}
			o := v.GetAsInt() - args[0].GetAsInt()
			return NewIntValue(&o)
		}, e)}
	} else if o == df.ExprNumOpMul {
		return &intExpr{name: "*", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.IntegerFormat, nil)
			}
			o := v.GetAsInt() * args[0].GetAsInt()
			return NewIntValue(&o)
		}, e)}
	} else if o == df.ExprNumOpDiv {
		return &intExpr{name: "/", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() || args[0].IsNil() {
				return NewValue(df.IntegerFormat, nil)
			}
			o := v.GetAsInt() / args[0].GetAsInt()
			return NewIntValue(&o)
		}, e)}
	} else {
		panic("unknown operation")
	}
}

func (t *intExpr) OpConst(e int64, o df.ExprNumOp) df.IntSeriesExpr {
	return t.Op(NewIntConstExpr(e), o)
}

func (t *intExpr) NonNil() (d df.IntSeriesExpr) {
	return &intExpr{name: "is not null", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.IsNil()
	})}
}

func (t *intExpr) WhenConst(val map[int64]int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "when", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		k, ok := val[v.GetAsInt()]
		if ok {
			return NewIntValue(&k)
		}
		return v
	})}
}

func (t *intExpr) WhenNil(val df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: "whennill", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		return v
	}, val)}
}

func (t *intExpr) WhenNilConst(val int64) (d df.IntSeriesExpr) {
	return t.WhenNil(NewIntConstExpr(val))
}

func (t *intExpr) Between(e, e1 df.IntSeriesExpr, e3 df.ExprBetweenInclude) (d df.IntSeriesExpr) {
	if e3 == df.ExprBetweenIncludeNeighter {
		return &intExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsInt() > args[0].GetAsInt() && v.GetAsInt() < args[1].GetAsInt()
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeBoth {
		return &intExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsInt() >= args[0].GetAsInt() && v.GetAsInt() <= args[1].GetAsInt()
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeLeft {
		return &intExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsInt() >= args[0].GetAsInt() && v.GetAsInt() < args[1].GetAsInt()
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeRight {
		return &intExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsInt() > args[0].GetAsInt() && v.GetAsInt() <= args[1].GetAsInt()
		}, e, e1)}
	} else {
		panic("unknown operation")
	}
}

func (t *intExpr) BetweenConst(e, e1 int64, e3 df.ExprBetweenInclude) (d df.IntSeriesExpr) {
	return t.Between(NewIntConstExpr(e), NewIntConstExpr(e1), e3)
}

func (t *intExpr) InConst(e ...int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return slices.Contains(e, v.GetAsInt())
	})}
}

func (t *intExpr) NotInConst(e ...int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "not in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return !slices.Contains(e, v.GetAsInt())
	})}
}

func (t *intExpr) Eq(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.Equals(args[0])
	}, e)}
}

func (t *intExpr) Lt(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: "<", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsInt() < args[0].GetAsInt()
	}, e)}
}

func (t *intExpr) Gt(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: ">", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsInt() > args[0].GetAsInt()
	}, e)}
}

func (t *intExpr) Le(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: "<=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsInt() <= args[0].GetAsInt()
	}, e)}
}

func (t *intExpr) Ge(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsInt() >= args[0].GetAsInt()
	}, e)}
}

func (t *intExpr) Ne(e df.IntSeriesExpr) (d df.IntSeriesExpr) {
	return &intExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.Equals(args[0])
	}, e)}
}

func (t *intExpr) EqConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.GetAsInt() == e
	})}
}

func (t *intExpr) LtConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "<", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsInt() < e
	})}
}

func (t *intExpr) GtConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: ">", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsInt() > e
	})}
}

func (t *intExpr) LeConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "<=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsInt() <= e
	})}
}

func (t *intExpr) GeConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.GetAsInt() >= e
	})}
}

func (t *intExpr) NeConst(e int64) (d df.IntSeriesExpr) {
	return &intExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.GetAsInt() != e
	})}
}

func (t *intExpr) AggSum() df.IntSeriesExpr {
	return &intExpr{name: "sum", col: "", parent: t, reduceOp: newExpReduceOp(df.DoubleFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() && v1.IsNil() {
			return NewIntValueConst(0)
		} else if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else {
			return NewIntValueConst(v.GetAsInt() + v1.GetAsInt())
		}

	})}
}

func (t *intExpr) AggMean() df.DoubleSeriesExpr {
	return t.AggSum().AsFormat(df.DoubleFormat).(df.DoubleSeriesExpr)
}

func (t *intExpr) AggMin() df.IntSeriesExpr {
	return &intExpr{name: "min", col: "", parent: t, reduceOp: newExpReduceOp(df.IntegerFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else if v.GetAsInt() < v1.GetAsInt() {
			return v
		} else {
			return v1
		}

	})}
}
func (t *intExpr) AggMax() df.IntSeriesExpr {
	return &intExpr{name: "max", col: "", parent: t, reduceOp: newExpReduceOp(df.IntegerFormat, NewDoubleValueConst(0), func(v, v1 df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v1
		} else if v1.IsNil() {
			return v
		} else if v.GetAsInt() > v1.GetAsInt() {
			return v
		} else {
			return v1
		}

	})}
}

func NewIntExpr() df.IntSeriesExpr {
	return &intExpr{name: "root", col: ""}
}

func NewIntColExpr(col string) df.IntSeriesExpr {
	return &intExpr{name: "col", col: col}
}

func NewIntConstExpr(col int64) df.IntSeriesExpr {
	return &intExpr{name: "const", val: NewIntValue(&col)}
}
