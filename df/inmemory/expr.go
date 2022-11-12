package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type exprMapOp struct {
	typ  df.Format
	fn   func(v df.Value, args ...df.Value) df.Value
	args []df.Expr
}

func (t *exprMapOp) ReturnFormat() df.Format {
	return t.typ
}

func (t *exprMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value {
	return t.fn(v, args...)
}

func (t *exprMapOp) Args() []df.Expr {
	return t.args
}

func newExpMapOp(typ df.Format, fn func(v df.Value, args ...df.Value) df.Value, argExpr ...df.Expr) df.ExprMapOp {
	return &exprMapOp{typ: typ, fn: fn, args: argExpr}
}

type exprFilterOp struct {
	fn   func(v df.Value, args ...df.Value) bool
	args []df.Expr
}

func (t *exprFilterOp) ApplyFilter(v df.Value, args ...df.Value) bool {
	return t.fn(v, args...)
}

func (t *exprFilterOp) Args() []df.Expr {
	return t.args
}

func newExpFilterOp(fn func(v df.Value, args ...df.Value) bool, argExpr ...df.Expr) df.ExprFilterOp {
	return &exprFilterOp{fn: fn, args: argExpr}
}

type exprReduceOp struct {
	fn     func(v, v1 df.Value, arg ...df.Value) df.Value
	intVal df.Value
	fmt    df.Format
	args   []df.Expr
	merge  func(v, v1 df.Value) df.Value
}

func (t *exprReduceOp) ApplyReduce(v, v1 df.Value, args ...df.Value) df.Value {
	return t.fn(v, v1, args...)
}

func (t *exprReduceOp) ApplyMerge(v, v1 df.Value) df.Value {
	return t.merge(v, v1)
}

func (t *exprReduceOp) InitValue() df.Value {
	return t.intVal
}

func (t *exprReduceOp) ReturnFormat() df.Format {
	return t.fmt
}

func (t *exprReduceOp) Args() []df.Expr {
	return t.args
}

func newExpReduceOp(fmt df.Format, initVal df.Value, fn func(v, v1 df.Value, arg ...df.Value) df.Value, argExpr ...df.Expr) df.ExprReduceOp {
	return &exprReduceOp{fn: fn, args: argExpr, intVal: initVal, fmt: fmt}
}
