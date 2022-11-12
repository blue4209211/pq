package df

import (
	"time"
)

type ExprOpType string

const (
	ExprTypeFilter = "filter"
	ExprTypeMap    = "map"
	ExprTypeReduce = "reduce"
)

type Expr interface {
	Name() string
	Col() string
	Const() Value
	Alias(a string) Expr
	AsFormat(f Format) Expr
	OpType() ExprOpType
	Parent() Expr
	FilterOp() ExprFilterOp
	MapOp() ExprMapOp
	ReduceOp() ExprReduceOp
}

type ExprFilterOp interface {
	ApplyFilter(v Value, args ...Value) bool
	Args() []Expr
}

type ExprMapOp interface {
	ApplyMap(v Value, args ...Value) Value
	ReturnFormat() Format
	Args() []Expr
}

type ExprReduceOp interface {
	InitValue() Value
	ApplyReduce(v, v1 Value, args ...Value) Value
	ReturnFormat() Format
	Args() []Expr
}

type ExprBetweenInclude string

const (
	ExprBetweenIncludeLeft     ExprBetweenInclude = "left"
	ExprBetweenIncludeRight    ExprBetweenInclude = "right"
	ExprBetweenIncludeBoth     ExprBetweenInclude = "both"
	ExprBetweenIncludeNeighter ExprBetweenInclude = "neighter"
)

type ExprNumOp string

const (
	ExprNumOpSum   ExprNumOp = "+"
	ExprNumOpMinus ExprNumOp = "-"
	ExprNumOpMul   ExprNumOp = "*"
	ExprNumOpDiv   ExprNumOp = "/"
)

type DoubleSeriesExpr interface {
	Expr
	Op(e DoubleSeriesExpr, o ExprNumOp) DoubleSeriesExpr
	OpConst(e float64, o ExprNumOp) DoubleSeriesExpr

	NonNil() DoubleSeriesExpr
	WhenConst(val map[float64]float64) DoubleSeriesExpr

	WhenNil(val DoubleSeriesExpr) DoubleSeriesExpr
	WhenNilConst(val float64) DoubleSeriesExpr

	Between(e, e2 DoubleSeriesExpr, e3 ExprBetweenInclude) DoubleSeriesExpr
	BetweenConst(e, e2 float64, e3 ExprBetweenInclude) DoubleSeriesExpr

	InConst(e ...float64) DoubleSeriesExpr
	NotInConst(e ...float64) DoubleSeriesExpr

	Eq(e DoubleSeriesExpr) DoubleSeriesExpr
	EqConst(e float64) DoubleSeriesExpr

	Lt(e DoubleSeriesExpr) DoubleSeriesExpr
	LtConst(e float64) DoubleSeriesExpr

	Gt(e DoubleSeriesExpr) DoubleSeriesExpr
	GtConst(e float64) DoubleSeriesExpr

	Le(e DoubleSeriesExpr) DoubleSeriesExpr
	LeConst(e float64) DoubleSeriesExpr

	Ge(e DoubleSeriesExpr) DoubleSeriesExpr
	GeConst(e float64) DoubleSeriesExpr

	Ne(e DoubleSeriesExpr) DoubleSeriesExpr
	NeConst(e float64) DoubleSeriesExpr

	AggSum() DoubleSeriesExpr
	AggMean() DoubleSeriesExpr
	AggMin() DoubleSeriesExpr
	AggMax() DoubleSeriesExpr
}

type IntSeriesExpr interface {
	Expr

	Op(e IntSeriesExpr, o ExprNumOp) IntSeriesExpr
	OpConst(e int64, o ExprNumOp) IntSeriesExpr

	NonNil() IntSeriesExpr

	WhenConst(val map[int64]int64) IntSeriesExpr

	WhenNil(val IntSeriesExpr) IntSeriesExpr
	WhenNilConst(val int64) IntSeriesExpr

	Between(e, e1 IntSeriesExpr, e3 ExprBetweenInclude) IntSeriesExpr
	BetweenConst(e, e2 int64, e3 ExprBetweenInclude) IntSeriesExpr

	InConst(e ...int64) IntSeriesExpr
	NotInConst(e ...int64) IntSeriesExpr

	Eq(e IntSeriesExpr) IntSeriesExpr
	EqConst(e int64) IntSeriesExpr

	Lt(e IntSeriesExpr) IntSeriesExpr
	LtConst(e int64) IntSeriesExpr

	Gt(e IntSeriesExpr) IntSeriesExpr
	GtConst(e int64) IntSeriesExpr

	Le(e IntSeriesExpr) IntSeriesExpr
	LeConst(e int64) IntSeriesExpr

	Ge(e IntSeriesExpr) IntSeriesExpr
	GeConst(e int64) IntSeriesExpr

	Ne(e IntSeriesExpr) IntSeriesExpr
	NeConst(e int64) IntSeriesExpr

	AggSum() IntSeriesExpr
	AggMean() DoubleSeriesExpr
	AggMin() IntSeriesExpr
	AggMax() IntSeriesExpr
}

type StringSeriesExpr interface {
	Expr

	Concat(s StringSeriesExpr, e ...StringSeriesExpr) StringSeriesExpr
	ConcatConst(s string, e ...string) StringSeriesExpr

	NonNil() StringSeriesExpr

	Substring(start, end IntSeriesExpr) StringSeriesExpr
	SubstringConst(start, end int64) StringSeriesExpr

	Upper() StringSeriesExpr
	Lower() StringSeriesExpr
	Title() StringSeriesExpr

	ReplaceAll(match, replcae StringSeriesExpr) StringSeriesExpr
	ReplaceAllConst(match, replcae string) StringSeriesExpr

	Replace(match, replcae StringSeriesExpr, n IntSeriesExpr) StringSeriesExpr
	ReplaceConst(match, replcae string, n int) StringSeriesExpr

	Trim() StringSeriesExpr
	RTrim() StringSeriesExpr
	LTrim() StringSeriesExpr

	Split(sep StringSeriesExpr, index IntSeriesExpr) StringSeriesExpr
	SplitConst(sep string, index int) StringSeriesExpr

	Extract(pattern StringSeriesExpr) StringSeriesExpr
	ExtractConst(pattern string) StringSeriesExpr

	Repeat(n IntSeriesExpr) StringSeriesExpr
	RepeatConst(n int) StringSeriesExpr

	TrimSuffix(s StringSeriesExpr) StringSeriesExpr
	TrimSuffixConst(s string) StringSeriesExpr

	TrimPrefix(p StringSeriesExpr) StringSeriesExpr
	TrimPrefixConst(p string) StringSeriesExpr

	WhenConst(val map[string]string) StringSeriesExpr

	WhenNil(val StringSeriesExpr) StringSeriesExpr
	WhenNilConst(val string) StringSeriesExpr

	ParseDatetime(fmt StringSeriesExpr) DatetimeSeriesExpr
	ParseDatetimeConst(fmt string) DatetimeSeriesExpr

	InConst(e ...string) StringSeriesExpr
	NotInConst(e ...string) StringSeriesExpr

	Contains(val StringSeriesExpr) StringSeriesExpr
	ContainsConst(val string) StringSeriesExpr

	StartsWith(val StringSeriesExpr) StringSeriesExpr
	StartsWithConst(val string) StringSeriesExpr

	EndsWith(val StringSeriesExpr) StringSeriesExpr
	EndsWithConst(val string) StringSeriesExpr

	Eq(e StringSeriesExpr) StringSeriesExpr
	EqConst(e string) StringSeriesExpr

	Ne(e StringSeriesExpr) StringSeriesExpr
	NeConst(e string) StringSeriesExpr

	Len() IntSeriesExpr
}

type DatetimeSeriesExpr interface {
	Expr

	WhenConst(val map[time.Time]time.Time) DatetimeSeriesExpr

	NonNil() DatetimeSeriesExpr

	WhenNil(val DatetimeSeriesExpr) DatetimeSeriesExpr
	WhenNilConst(val time.Time) DatetimeSeriesExpr

	Year() IntSeriesExpr
	Month() IntSeriesExpr
	Day() IntSeriesExpr
	Hour() IntSeriesExpr
	Minute() IntSeriesExpr
	Second() IntSeriesExpr
	UnixMilli() IntSeriesExpr

	AddDate(y, m, d IntSeriesExpr) DatetimeSeriesExpr
	AddDateConst(y, m, d int64) DatetimeSeriesExpr

	AddTime(h, m, s IntSeriesExpr) DatetimeSeriesExpr
	AddTimeConst(h, m, s int64) DatetimeSeriesExpr

	Format(h StringSeriesExpr) StringSeriesExpr
	FormatConst(h string) StringSeriesExpr

	Between(s, e DatetimeSeriesExpr, t ExprBetweenInclude) DatetimeSeriesExpr
	BetweenConst(s, e time.Time, t ExprBetweenInclude) DatetimeSeriesExpr

	InConst(e ...time.Time) DatetimeSeriesExpr
	NotInConst(e ...time.Time) DatetimeSeriesExpr

	Eq(e DatetimeSeriesExpr) DatetimeSeriesExpr
	EqConst(e time.Time) DatetimeSeriesExpr

	Lt(e DatetimeSeriesExpr) DatetimeSeriesExpr
	LtConst(e time.Time) DatetimeSeriesExpr

	Gt(e DatetimeSeriesExpr) DatetimeSeriesExpr
	GtConst(e time.Time) DatetimeSeriesExpr

	Le(e DatetimeSeriesExpr) DatetimeSeriesExpr
	LeConst(e time.Time) DatetimeSeriesExpr

	Ge(e DatetimeSeriesExpr) DatetimeSeriesExpr
	GeConst(e time.Time) DatetimeSeriesExpr

	Ne(e DatetimeSeriesExpr) DatetimeSeriesExpr
	NeConst(e time.Time) DatetimeSeriesExpr
}

type BoolSeriesExpr interface {
	Expr

	And(val BoolSeriesExpr) BoolSeriesExpr
	AndConst(val bool) BoolSeriesExpr

	Or(val BoolSeriesExpr) BoolSeriesExpr
	OrConst(val bool) BoolSeriesExpr

	Not() BoolSeriesExpr

	Eq(BoolSeriesExpr) BoolSeriesExpr
	EqConst(val bool) BoolSeriesExpr

	NonNil() BoolSeriesExpr
	WhenNil(val BoolSeriesExpr) BoolSeriesExpr
	WhenNilConst(val bool) BoolSeriesExpr
}
