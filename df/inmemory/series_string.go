package inmemory

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
)

// NewStringSeries returns a column of type string
func NewStringSeries(data []*string) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewStringValue(e)
	}
	return NewSeries(d, df.StringFormat)
}

func NewStringSeriesVarArg(data ...string) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewStringValue(&e2)
	}
	return NewSeries(d, df.StringFormat)
}

type stringVal struct {
	data *string
}

func (t *stringVal) Schema() df.Format {
	return df.StringFormat
}

func (t *stringVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *stringVal) GetAsString() (r string) {
	return *t.data
}

func (t *stringVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *stringVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *stringVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *stringVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(time.Time)
}

func (t *stringVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *stringVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewStringValue(data *string) df.Value {
	return &stringVal{data: data}
}

func NewStringValueConst(data string) df.Value {
	return &stringVal{data: &data}
}

type stringExpr struct {
	name     string
	col      string
	val      df.Value
	parent   df.Expr
	typ      df.ExprOpType
	mapOp    df.ExprMapOp
	filterOp df.ExprFilterOp
	reduceOp df.ExprReduceOp
}

func (t *stringExpr) FilterOp() df.ExprFilterOp {
	return t.filterOp
}

func (t *stringExpr) MapOp() df.ExprMapOp {
	return t.mapOp
}

func (t *stringExpr) ReduceOp() df.ExprReduceOp {
	return t.reduceOp
}

func (t *stringExpr) Name() string {
	return t.name
}

func (t *stringExpr) Col() string {
	return t.col
}

func (t *stringExpr) Const() df.Value {
	return t.val
}

func (t *stringExpr) OpType() df.ExprOpType {
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

func (t *stringExpr) Alias(a string) df.Expr {
	return &intExpr{name: a, parent: t}
}

func (t *stringExpr) Parent() df.Expr {
	return t.parent
}

func (t *stringExpr) AsFormat(f df.Format) df.Expr {
	return &intExpr{name: "cast", parent: t, typ: df.ExprTypeMap, mapOp: newExpMapOp(f, func(v df.Value, args ...df.Value) df.Value {
		a, err := f.Convert(v.Get())
		if err != nil {
			panic("unable to convert data")
		}
		return NewValue(f, a)
	})}
}

func (t *stringExpr) Concat(s df.StringSeriesExpr, e ...df.StringSeriesExpr) df.StringSeriesExpr {
	args := []df.Expr{s}
	for _, v := range e {
		args = append(args, v)
	}
	return &stringExpr{name: "concat", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if len(args) < 2 {
			return v
		}
		data := []string{}
		if v.IsNil() {
			data = append(data, "")
		} else {
			data = append(data, v.GetAsString())
		}
		for _, v := range args[1:] {
			if v.IsNil() {
				data = append(data, "")
			} else {
				data = append(data, v.GetAsString())
			}
		}
		return NewStringValueConst(strings.Join(data, args[0].GetAsString()))
	}, args...)}
}

func (t *stringExpr) ConcatConst(s string, e ...string) df.StringSeriesExpr {
	args := lo.Map(e, func(s string, i int) df.StringSeriesExpr {
		return NewStringConstExpr(s)
	})
	return t.Concat(NewStringConstExpr(s), args...)
}

func (t *stringExpr) NonNil() df.StringSeriesExpr {
	return &stringExpr{name: "is not null", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		return !v.IsNil()
	})}
}

func (t *stringExpr) Substring(start, end df.IntSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "substring", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(v.GetAsString()[args[0].GetAsInt():args[1].GetAsInt()])
	}, start, end)}
}

func (t *stringExpr) SubstringConst(start, end int64) df.StringSeriesExpr {
	if start > end {
		panic(fmt.Sprintf("start (%v) cannot be greater than end (%v)", start, end))
	}
	return t.Substring(NewIntConstExpr(start), NewIntConstExpr(end))
}

func (t *stringExpr) Upper() df.StringSeriesExpr {
	return &stringExpr{name: "upper", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.ToUpper(v.GetAsString()))
	})}
}

func (t *stringExpr) Lower() df.StringSeriesExpr {
	return &stringExpr{name: "lower", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.ToLower(v.GetAsString()))
	})}
}

func (t *stringExpr) Title() df.StringSeriesExpr {
	return &stringExpr{name: "title", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.Title(v.GetAsString()))
	})}
}

func (t *stringExpr) ReplaceAll(match, replcae df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "replaceAll", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.ReplaceAll(v.GetAsString(), args[0].GetAsString(), args[1].GetAsString()))
	}, match, replcae)}
}

func (t *stringExpr) ReplaceAllConst(match, replace string) df.StringSeriesExpr {
	return t.ReplaceAll(NewStringConstExpr(match), NewStringConstExpr(replace))
}

func (t *stringExpr) Replace(match, replcae df.StringSeriesExpr, n df.IntSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "replace", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.Replace(v.GetAsString(), args[0].GetAsString(), args[1].GetAsString(), int(args[2].GetAsInt())))
	}, match, replcae, n)}
}

func (t *stringExpr) ReplaceConst(match, replcae string, n int) df.StringSeriesExpr {
	return t.Replace(NewStringConstExpr(match), NewStringConstExpr(replcae), NewIntConstExpr(int64(n)))
}

func (t *stringExpr) Trim() df.StringSeriesExpr {
	return &stringExpr{name: "trim", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.TrimSpace(v.GetAsString()))
	})}
}

func (t *stringExpr) RTrim() df.StringSeriesExpr {
	return &stringExpr{name: "rtrim", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.TrimRight(v.GetAsString(), " \t\f\v"))
	})}
}

func (t *stringExpr) LTrim() df.StringSeriesExpr {
	return &stringExpr{name: "ltrim", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.TrimLeft(v.GetAsString(), " \t\f\v"))
	})}
}

func (t *stringExpr) Split(sep df.StringSeriesExpr, index df.IntSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "split", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.Split(v.GetAsString(), args[0].GetAsString())[args[1].GetAsInt()])
	}, sep, index)}
}

func (t *stringExpr) SplitConst(sep string, index int) df.StringSeriesExpr {
	return t.Split(NewStringConstExpr(sep), NewIntConstExpr(int64(index)))
}

func (t *stringExpr) Extract(pattern df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "extract", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		exp, err := regexp.Compile(args[0].GetAsString())
		if err != nil {
			panic(err)
		}

		return NewStringValueConst(exp.FindString(v.GetAsString()))
	}, pattern)}
}

func (t *stringExpr) ExtractConst(pattern string) df.StringSeriesExpr {
	return t.Extract(NewStringConstExpr(pattern))
}

func (t *stringExpr) Repeat(n df.IntSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "repeat", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.Repeat(v.GetAsString(), int(args[0].GetAsInt())))
	}, n)}
}

func (t *stringExpr) RepeatConst(n int) df.StringSeriesExpr {
	return t.Repeat(NewIntConstExpr(int64(n)))
}

func (t *stringExpr) TrimSuffix(s df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "suffix", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.TrimSuffix(v.GetAsString(), args[0].GetAsString()))
	}, s)}
}

func (t *stringExpr) TrimSuffixConst(s string) df.StringSeriesExpr {
	return t.TrimSuffix(NewStringConstExpr(s))
}

func (t *stringExpr) TrimPrefix(p df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "suffix", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		return NewStringValueConst(strings.TrimPrefix(v.GetAsString(), args[0].GetAsString()))
	}, p)}
}

func (t *stringExpr) TrimPrefixConst(p string) df.StringSeriesExpr {
	return t.TrimPrefix(NewStringConstExpr(p))
}

func (t *stringExpr) WhenConst(val map[string]string) df.StringSeriesExpr {
	return &stringExpr{name: "when", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return v
		}
		k, ok := val[v.GetAsString()]
		if ok {
			return NewStringValueConst(k)
		}
		return v
	})}
}

func (t *stringExpr) WhenNil(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "whennill", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		return v
	}, val)}
}

func (t *stringExpr) WhenNilConst(val string) df.StringSeriesExpr {
	return t.WhenNil(NewStringConstExpr(val))
}

func (t *stringExpr) ParseDatetime(fmt df.StringSeriesExpr) df.DatetimeSeriesExpr {
	return &datetimeExpr{name: "parse_datetime", col: "", parent: t, mapOp: newExpMapOp(df.StringFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return args[0]
		}
		dt, _ := time.Parse(args[0].GetAsString(), v.GetAsString())
		return NewDatetimeValueConst(dt)
	}, fmt)}
}

func (t *stringExpr) Len() df.IntSeriesExpr {
	return &intExpr{name: "length", col: "", parent: t, mapOp: newExpMapOp(df.IntegerFormat, func(v df.Value, args ...df.Value) df.Value {
		if v.IsNil() {
			return NewIntValueConst(0)
		}
		return NewIntValueConst(int64(len(v.GetAsString())))
	})}
}

func (t *stringExpr) ParseDatetimeConst(fmt string) df.DatetimeSeriesExpr {
	return t.ParseDatetime(NewStringConstExpr(fmt))
}

func (t *stringExpr) InConst(e ...string) df.StringSeriesExpr {
	return &stringExpr{name: "in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return slices.Contains(e, v.GetAsString())
	})}
}

func (t *stringExpr) NotInConst(e ...string) df.StringSeriesExpr {
	return &stringExpr{name: "not in", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return slices.Contains(e, v.GetAsString())
	})}
}

func (t *stringExpr) Contains(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "contains", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return strings.Contains(v.GetAsString(), args[0].GetAsString())
	}, val)}
}

func (t *stringExpr) ContainsConst(val string) df.StringSeriesExpr {
	return t.Contains(NewStringConstExpr(val))
}

func (t *stringExpr) StartsWith(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "startswith", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return strings.HasPrefix(v.GetAsString(), args[0].GetAsString())
	}, val)}
}

func (t *stringExpr) StartsWithConst(val string) df.StringSeriesExpr {
	return t.StartsWith(NewStringConstExpr(val))
}

func (t *stringExpr) EndsWith(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "endswith", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return strings.HasSuffix(v.GetAsString(), args[0].GetAsString())
	}, val)}
}

func (t *stringExpr) EndsWithConst(val string) df.StringSeriesExpr {
	return t.EndsWith(NewStringConstExpr(val))
}

func (t *stringExpr) Eq(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return v.Equals(args[0])
	}, val)}
}

func (t *stringExpr) EqConst(val string) df.StringSeriesExpr {
	return t.Eq(NewStringConstExpr(val))
}

func (t *stringExpr) Ne(val df.StringSeriesExpr) df.StringSeriesExpr {
	return &stringExpr{name: "!=", col: "", parent: t, filterOp: newExpFilterOp(func(v df.Value, args ...df.Value) bool {
		if v.IsNil() {
			return false
		}
		return !v.Equals(args[0])
	}, val)}
}

func (t *stringExpr) NeConst(val string) df.StringSeriesExpr {
	return t.Ne(NewStringConstExpr(val))
}

func NewStringExpr() df.StringSeriesExpr {
	return &stringExpr{name: "root", col: ""}
}

func NewStringColExpr(col string) df.StringSeriesExpr {
	return &stringExpr{name: "col", col: col}
}

func NewStringConstExpr(col string) df.StringSeriesExpr {
	return &stringExpr{name: "const", val: NewStringValue(&col)}
}
