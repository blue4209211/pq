package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []*bool) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewBoolValue(e)
	}
	return NewSeries(d, df.BoolFormat)
}

func NewBoolSeriesVarArg(data ...bool) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewBoolValue(&e2)
	}
	return NewSeries(d, df.BoolFormat)
}

type boolVal struct {
	data *bool
}

func (t *boolVal) Schema() df.Format {
	return df.BoolFormat
}

func (t *boolVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *boolVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *boolVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *boolVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *boolVal) GetAsBool() (r bool) {
	return *t.data
}

func (t *boolVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(time.Time)
}

func (t *boolVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *boolVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewBoolValue(data *bool) df.Value {
	return &boolVal{data: data}
}

func NewBoolValueConst(data bool) df.Value {
	return &boolVal{data: &data}
}

type boolExpr struct {
	name     string
	col      string
	val      df.Value
	parent   df.Expr
	typ      df.ExprOpType
	mapOp    df.ExprMapOp
	filterOp df.ExprFilterOp
	reduceOp df.ExprReduceOp
}

func (t *boolExpr) FilterOp() df.ExprFilterOp {
	return t.filterOp
}

func (t *boolExpr) MapOp() df.ExprMapOp {
	return t.mapOp
}

func (t *boolExpr) ReduceOp() df.ExprReduceOp {
	return t.reduceOp
}

func (t *boolExpr) Name() string {
	return t.name
}

func (t *boolExpr) Col() string {
	return t.col
}

func (t *boolExpr) Const() df.Value {
	return t.val
}

func (t *boolExpr) OpType() df.ExprOpType {
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

func (t *boolExpr) Alias(a string) df.Expr {
	return &intExpr{name: a, parent: t}
}

func (t *boolExpr) Parent() df.Expr {
	return t.parent
}

func (t *boolExpr) AsFormat(f df.Format) df.Expr {
	return &intExpr{name: "cast", col: "", val: nil, parent: t, mapOp: newExpMapOp(f, func(v df.Value, args ...df.Value) df.Value {
		a, err := f.Convert(v.Get())
		if err != nil {
			panic("unable to convert data")
		}
		return NewValue(f, a)
	})}
}

func (t *boolExpr) And(val df.BoolSeriesExpr) df.BoolSeriesExpr {
	return &boolExpr{name: "and", col: "", parent: t, mapOp: newExpMapOp(df.BoolFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() || args[0].IsNil() {
			return NewBoolValueConst(false)
		}
		return NewBoolValueConst(v.GetAsBool() && args[0].GetAsBool())
	}, val)}
}

func (t *boolExpr) AndConst(val bool) df.BoolSeriesExpr {
	return t.And(NewBoolConstExpr(val))
}

func (t *boolExpr) Or(val df.BoolSeriesExpr) df.BoolSeriesExpr {
	return &boolExpr{name: "or", col: "", parent: t, mapOp: newExpMapOp(df.BoolFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() || args[0].IsNil() {
			return NewBoolValueConst(false)
		}
		return NewBoolValueConst(v.GetAsBool() || args[0].GetAsBool())
	}, val)}
}
func (t *boolExpr) OrConst(val bool) df.BoolSeriesExpr {
	return t.Or(NewBoolConstExpr(val))
}

func (t *boolExpr) Not() df.BoolSeriesExpr {
	return &boolExpr{name: "not", col: "", parent: t, mapOp: newExpMapOp(df.BoolFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() || args[0].IsNil() {
			return NewBoolValueConst(false)
		}
		return NewBoolValueConst(!v.GetAsBool())
	})}
}

func (t *boolExpr) Eq(e df.BoolSeriesExpr) df.BoolSeriesExpr {
	return &boolExpr{name: "==", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return v.Equals(args[0])
	}, e)}
}

func (t *boolExpr) EqConst(val bool) df.BoolSeriesExpr {
	return t.Eq(NewBoolConstExpr(val))
}

func (t *boolExpr) NonNil() df.BoolSeriesExpr {
	return &boolExpr{name: "not null", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.IsNil()
	})}
}

func (t *boolExpr) WhenNil(val df.BoolSeriesExpr) df.BoolSeriesExpr {
	return &boolExpr{name: "whennill", col: "", parent: t, mapOp: newExpMapOp(df.BoolFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		return v
	}, val)}
}

func (t *boolExpr) WhenNilConst(val bool) df.BoolSeriesExpr {
	return t.WhenNil(NewBoolConstExpr(val))
}

func NewBoolExpr() df.BoolSeriesExpr {
	return &boolExpr{name: "root", col: ""}
}

func NewBoolColExpr(col string) df.BoolSeriesExpr {
	return &boolExpr{name: "col", col: col}
}

func NewBoolConstExpr(col bool) df.BoolSeriesExpr {
	return &boolExpr{name: "const", val: NewBoolValue(&col)}
}
