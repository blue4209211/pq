package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
	"golang.org/x/exp/slices"
)

// NewDatetimeSeries returns a column of type double
func NewDatetimeSeries(data []*time.Time) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDatetimeValue(e)
	}
	return NewSeries(d, df.DateTimeFormat)
}

func NewDatetimeSeriesVarArg(data ...time.Time) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewDatetimeValue(&e2)
	}
	return NewSeries(d, df.DateTimeFormat)
}

type timeVal struct {
	data *time.Time
}

func (t *timeVal) Schema() df.Format {
	return df.DateTimeFormat
}

func (t *timeVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *timeVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *timeVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *timeVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *timeVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *timeVal) GetAsDatetime() (r time.Time) {
	return *t.data
}

func (t *timeVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *timeVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewDatetimeValue(data *time.Time) df.Value {
	return &timeVal{data: data}
}

func NewDatetimeValueConst(data time.Time) df.Value {
	return &timeVal{data: &data}
}

type datetimeExpr struct {
	name     string
	col      string
	val      df.Value
	parent   df.Expr
	typ      df.ExprOpType
	mapOp    df.ExprMapOp
	filterOp df.ExprFilterOp
	reduceOp df.ExprReduceOp
}

func (t *datetimeExpr) FilterOp() df.ExprFilterOp {
	return t.filterOp
}

func (t *datetimeExpr) MapOp() df.ExprMapOp {
	return t.mapOp
}

func (t *datetimeExpr) ReduceOp() df.ExprReduceOp {
	return t.reduceOp
}

func (t *datetimeExpr) Name() string {
	return t.name
}

func (t *datetimeExpr) Col() string {
	return t.col
}

func (t *datetimeExpr) Const() df.Value {
	return t.val
}

func (t *datetimeExpr) OpType() df.ExprOpType {
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

func (t *datetimeExpr) Alias(a string) df.Expr {
	return &datetimeExpr{name: a, parent: t}
}

func (t *datetimeExpr) Parent() df.Expr {
	return t.parent
}

func (t *datetimeExpr) AsFormat(f df.Format) df.Expr {
	return &datetimeExpr{name: "cast", parent: t, typ: df.ExprTypeMap, mapOp: newExpMapOp(f, func(v df.Value, args ...df.Value) df.Value {
		a, err := f.Convert(v.Get())
		if err != nil {
			panic("unable to convert data")
		}
		return NewValue(f, a)
	})}
}

func (t *datetimeExpr) NonNil() df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "not null", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.IsNil()
	})}

}

func (t *datetimeExpr) WhenConst(val map[time.Time]time.Time) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "when", col: "", parent: t, mapOp: newExpMapOp(df.DateTimeFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		k, ok := val[v.GetAsDatetime()]
		if ok {
			return NewDatetimeValueConst(k)
		}
		return v
	})}
}

func (t *datetimeExpr) WhenNil(val df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "whennill", col: "", parent: t, mapOp: newExpMapOp(df.DateTimeFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		return v
	}, val)}
}

func (t *datetimeExpr) WhenNilConst(val time.Time) df.DatetimeSeriesExpr {
	return t.WhenNil(NewDatetimeConstExpr(val))
}

func (t *datetimeExpr) Year() df.IntSeriesExpr {
	return &intExpr{name: "year", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Year()))
	})}
}

func (t *datetimeExpr) Month() df.IntSeriesExpr {
	return &intExpr{name: "month", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Month()))
	})}
}

func (t *datetimeExpr) Day() df.IntSeriesExpr {
	return &intExpr{name: "day", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Day()))
	})}
}
func (t *datetimeExpr) Hour() df.IntSeriesExpr {
	return &intExpr{name: "hour", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Hour()))
	})}
}

func (t *datetimeExpr) Minute() df.IntSeriesExpr {
	return &intExpr{name: "minute", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Minute()))
	})}
}

func (t *datetimeExpr) Second() df.IntSeriesExpr {
	return &intExpr{name: "second", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(int64(v.GetAsDatetime().Second()))
	})}
}

func (t *datetimeExpr) UnixMilli() df.IntSeriesExpr {
	return &intExpr{name: "second", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValue(nil)
		}
		return NewIntValueConst(v.GetAsDatetime().UnixMilli())
	})}
}

func (t *datetimeExpr) AddDate(y, m, d df.IntSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "adddate", col: "", parent: t, mapOp: newExpMapOp(df.DateTimeFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		y1 := 0
		if !args[0].IsNil() {
			y1 = int(args[0].GetAsInt())
		}
		m1 := 0
		if !args[1].IsNil() {
			m1 = int(args[1].GetAsInt())
		}
		d1 := 0
		if !args[2].IsNil() {
			d1 = int(args[2].GetAsInt())
		}
		return NewDatetimeValueConst(v.GetAsDatetime().AddDate(y1, m1, d1))
	}, y, m, d)}
}

func (t *datetimeExpr) AddDateConst(y, m, d int64) df.DatetimeSeriesExpr {
	return t.AddDate(NewIntConstExpr(y), NewIntConstExpr(m), NewIntConstExpr(d))
}

func (t *datetimeExpr) AddTime(h, m, s df.IntSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "addtime", col: "", parent: t, mapOp: newExpMapOp(df.DateTimeFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		dt := v.GetAsDatetime()
		if !args[0].IsNil() {
			dt = dt.Add(time.Hour * time.Duration(args[0].GetAsInt()))
		}
		if !args[1].IsNil() {
			dt = dt.Add(time.Minute * time.Duration(args[1].GetAsInt()))
		}
		if !args[2].IsNil() {
			dt = dt.Add(time.Second * time.Duration(args[2].GetAsInt()))
		}
		return NewDatetimeValueConst(dt)
	}, h, m, s)}
}

func (t *datetimeExpr) AddTimeConst(h, m, s int64) df.DatetimeSeriesExpr {
	return t.AddTime(NewIntConstExpr(h), NewIntConstExpr(m), NewIntConstExpr(s))
}

func (t *datetimeExpr) Format(h df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "datetime_format", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(v.GetAsDatetime().Format(args[0].GetAsString()))
	}, h)}
}

func (t *datetimeExpr) FormatConst(h string) df.StringSeriesExpr {
	return t.Format(NewStringConstExpr(h))
}

func (t *datetimeExpr) Between(e, e1 df.DatetimeSeriesExpr, e3 df.ExprBetweenInclude) df.DatetimeSeriesExpr {
	if e3 == df.ExprBetweenIncludeNeighter {
		return &datetimeExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDatetime().After(args[0].GetAsDatetime()) && v.GetAsDatetime().Before(args[1].GetAsDatetime())
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeBoth {
		return &datetimeExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return (v.GetAsDatetime().After(args[0].GetAsDatetime()) || v.Equals(args[0])) && (v.GetAsDatetime().Before(args[1].GetAsDatetime()) || v.Equals(args[1]))
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeLeft {
		return &datetimeExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return (v.GetAsDatetime().After(args[0].GetAsDatetime()) || v.Equals(args[0])) && v.GetAsDatetime().Before(args[1].GetAsDatetime())
		}, e, e1)}
	} else if e3 == df.ExprBetweenIncludeRight {
		return &datetimeExpr{name: "between", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
			if v.IsNil() || args[0].IsNil() || args[1].IsNil() {
				return false
			}
			return v.GetAsDatetime().After(args[0].GetAsDatetime()) && (v.GetAsDatetime().Before(args[1].GetAsDatetime()) || v.Equals(args[1]))
		}, e, e1)}
	} else {
		panic("unknown operation")
	}

}

func (t *datetimeExpr) BetweenConst(s, e time.Time, t1 df.ExprBetweenInclude) df.DatetimeSeriesExpr {
	return t.Between(NewDatetimeConstExpr(s), NewDatetimeConstExpr(e), t1)
}

func (t *datetimeExpr) InConst(e ...time.Time) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return slices.Contains(e, v.GetAsDatetime())
	})}
}

func (t *datetimeExpr) NotInConst(e ...time.Time) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "not in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return !slices.Contains(e, v.GetAsDatetime())
	})}
}

func (t *datetimeExpr) Eq(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.Equals(args[0])
	}, e)}
}

func (t *datetimeExpr) EqConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Eq(NewDatetimeConstExpr(e))
}

func (t *datetimeExpr) Lt(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDatetime().Before(args[0].GetAsDatetime())
	}, e)}
}

func (t *datetimeExpr) LtConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Lt(NewDatetimeConstExpr(e))
}

func (t *datetimeExpr) Gt(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDatetime().After(args[0].GetAsDatetime())
	}, e)}
}

func (t *datetimeExpr) GtConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Gt(NewDatetimeConstExpr(e))
}

func (t *datetimeExpr) Le(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDatetime().Before(args[0].GetAsDatetime()) || v.Equals(args[0])
	}, e)}
}

func (t *datetimeExpr) LeConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Le(NewDatetimeConstExpr(e))
}

func (t *datetimeExpr) Ge(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: ">=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() || args[0].IsNil() {
			return false
		}
		return v.GetAsDatetime().After(args[0].GetAsDatetime()) || v.Equals(args[0])
	}, e)}
}

func (t *datetimeExpr) GeConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Ge(NewDatetimeConstExpr(e))
}

func (t *datetimeExpr) Ne(e df.DatetimeSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.Equals(args[0])
	}, e)}
}

func (t *datetimeExpr) NeConst(e time.Time) df.DatetimeSeriesExpr {
	return t.Ne(NewDatetimeConstExpr(e))
}

func NewDatetimeExpr() df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "root", col: ""}
}

func NewDatetimeColExpr(col string) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "col", col: col}
}

func NewDatetimeConstExpr(col time.Time) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "const", val: NewDatetimeValueConst(col)}
}
