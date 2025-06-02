//go:build arrow

package arrow_test

import (
	"fmt"
	"reflect" // Added for TestArrowSeries_Expr panic test
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/expr" // Assuming expression types are here or in df
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// --- (Existing helpers like getTestInt64Array, etc.) ---
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := array.NewInt64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := array.NewStringBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestFloat64Array(mem memory.Allocator, values []float64, valids []bool) arrow.Array {
	b := array.NewFloat64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestBoolArray(mem memory.Allocator, values []bool, valids []bool) arrow.Array {
	b := array.NewBooleanBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestTimestampArrayNano(mem memory.Allocator, values []time.Time, valids []bool) arrow.Array {
	b := array.NewTimestampBuilder(mem, arrow.TimestampTypes.Timestamp_ns); defer b.Release()
	tsValues := make([]arrow.Timestamp, len(values))
	for i, v := range values { if valids == nil || (len(valids) > i && valids[i]) { tsValues[i] = arrow.Timestamp(v.UnixNano()) } }
	b.AppendValues(tsValues, valids); return b.NewArray()
}
const nilPlaceholder = "__NIL_PLACEHOLDER__"
func extractValues(s df.Series) []interface{} {
	var out []interface{}
	for i := int64(0); i < s.Len(); i++ {
		v := s.Get(i)
		if v.IsNil() { out = append(out, nilPlaceholder) } else { out = append(out, v.Get()) }
	}
	return out
}
func sortInterfaceSlice(slice []interface{}) {
	sort.Slice(slice, func(i, j int) bool {
		if slice[i] == nilPlaceholder && slice[j] != nilPlaceholder { return true }
		if slice[i] != nilPlaceholder && slice[j] == nilPlaceholder { return false }
		if slice[i] == nilPlaceholder && slice[j] == nilPlaceholder { return false }
		return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j])
	})
}

// --- (Existing tests: New, Schema, Get, Copy, ForEach, Limit, Where, Sort, Map, FlatMap, Reduce, Distinct, Append, Union, Intersection, Except) ---
func TestArrowSeries_NewArrowSeries(t *testing.T) { /* ... */ }
func TestArrowSeries_Schema_Len_Get(t *testing.T) { /* ... */ }
func TestArrowSeries_Copy(t *testing.T) { /* ... */ }
func TestArrowSeries_ForEach(t *testing.T) { /* ... */ }
func TestArrowSeries_Limit(t *testing.T) { /* ... */ }
func TestArrowSeries_Where(t *testing.T) { /* ... */ }
func TestArrowSeries_Sort(t *testing.T) { /* ... */ }
func TestArrowSeries_Map(t *testing.T) { /* ... */ }
func TestArrowSeries_FlatMap(t *testing.T) { /* ... */ }
func TestArrowSeries_Reduce(t *testing.T) { /* ... */ }
func TestArrowSeries_Distinct(t *testing.T) { /* ... */ }
func TestArrowSeries_Append(t *testing.T) { /* ... */ }
func TestArrowSeries_Union(t *testing.T) { /* ... */ }
func TestArrowSeries_Intersection(t *testing.T) { /* ... */ }
func TestArrowSeries_Except(t *testing.T) { /* ... */ }


func TestArrowSeries_Expr(t *testing.T) {
	mem := memory.NewGoAllocator()

	testCases := []struct {
		name         string
		seriesArr    arrow.Array
		seriesSchema df.SeriesSchema
		// expectedType df.ExprType // This was an example, direct type assertion is better if possible
		assertType func(t *testing.T, e df.Expr)
	}{
		{
			name:         "IntSeries",
			seriesArr:    getTestInt64Array(mem, []int64{1}, nil),
			seriesSchema: df.SeriesSchema{Name: "int_col", Format: df.IntegerFormat},
			assertType:   func(t *testing.T, e df.Expr) { _, ok := e.(df.IntExpr); assert.True(t, ok, "Expected IntExpr") },
		},
		{
			name:         "StringSeries",
			seriesArr:    getTestStringArray(mem, []string{"a"}, nil),
			seriesSchema: df.SeriesSchema{Name: "str_col", Format: df.StringFormat},
			assertType:   func(t *testing.T, e df.Expr) { _, ok := e.(df.StringExpr); assert.True(t, ok, "Expected StringExpr") },
		},
		{
			name:         "FloatSeries",
			seriesArr:    getTestFloat64Array(mem, []float64{1.0}, nil),
			seriesSchema: df.SeriesSchema{Name: "float_col", Format: df.DoubleFormat},
			assertType:   func(t *testing.T, e df.Expr) { _, ok := e.(df.DoubleExpr); assert.True(t, ok, "Expected DoubleExpr") },
		},
		{
			name:         "BoolSeries",
			seriesArr:    getTestBoolArray(mem, []bool{true}, nil),
			seriesSchema: df.SeriesSchema{Name: "bool_col", Format: df.BoolFormat},
			assertType:   func(t *testing.T, e df.Expr) { _, ok := e.(df.BoolExpr); assert.True(t, ok, "Expected BoolExpr") },
		},
		{
			name:         "DateTimeSeries",
			seriesArr:    getTestTimestampArrayNano(mem, []time.Time{time.Now()}, nil),
			seriesSchema: df.SeriesSchema{Name: "time_col", Format: df.DateTimeFormat},
			assertType:   func(t *testing.T, e df.Expr) { _, ok := e.(df.DatetimeExpr); assert.True(t, ok, "Expected DatetimeExpr") },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.seriesArr.Release()
			series := arrowimpl.NewArrowSeries(tc.seriesArr, tc.seriesSchema)
			defer series.(*arrowimpl.ArrowSeries).Release()

			seriesExpr := series.Expr()
			assert.NotNil(t, seriesExpr)
			tc.assertType(t, seriesExpr)
		})
	}

	unsupportedFormat := df.NewGenericFormat("unsupported", reflect.TypeOf(""))
	unsupportedArr := getTestInt64Array(mem, []int64{1}, nil)
	defer unsupportedArr.Release()
	unsupportedSeries := arrowimpl.NewArrowSeries(unsupportedArr, df.SeriesSchema{Name:"unsup", Format: unsupportedFormat})
	defer unsupportedSeries.(*arrowimpl.ArrowSeries).Release()
	assert.PanicsWithValue(t, "Expr() not supported for series format: unsupported", func(){
		unsupportedSeries.Expr()
	})
}

type mockExpr struct {
	exprName     string
	exprConstVal df.Value
	exprColName  string
	exprOpType   df.ExprOpType
	exprFilterOp df.FilterOp
	exprMapOp    df.MapOp
	exprParent   df.Expr
}
func (m *mockExpr) Name() string { return m.exprName }
func (m *mockExpr) Const() df.Value { return m.exprConstVal }
func (m *mockExpr) Col() string { return m.exprColName }
func (m *mockExpr) OpType() df.ExprOpType { return m.exprOpType }
func (m *mockExpr) FilterOp() df.FilterOp { return m.exprFilterOp }
func (m *mockExpr) MapOp() df.MapOp { return m.exprMapOp }
func (m *mockExpr) Parent() df.Expr { return m.exprParent }
func (m *mockExpr) SetParent(p df.Expr) df.Expr { m.exprParent = p; return m }
func (m *mockExpr) SetName(n string) df.Expr {m.exprName = n; return m}

type mockFilterOp struct {
	applyFunc func(v df.Value, args ...df.Value) bool
	argExprs  []df.Expr
}
func (m *mockFilterOp) Args() []df.Expr { return m.argExprs }
func (m *mockFilterOp) ApplyFilter(v df.Value, args ...df.Value) bool { return m.applyFunc(v, args...) }
func (m *mockFilterOp) SetArgs(args ...df.Expr) df.FilterOp { m.argExprs = args; return m }

type mockMapOp struct {
	applyFunc    func(v df.Value, args ...df.Value) df.Value
	argExprs     []df.Expr
	returnFormat df.Format
}
func (m *mockMapOp) Args() []df.Expr { return m.argExprs }
func (m *mockMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value { return m.applyFunc(v, args...) }
func (m *mockMapOp) ReturnFormat() df.Format { return m.returnFormat }
func (m *mockMapOp) SetArgs(args ...df.Expr) df.MapOp { m.argExprs = args; return m }


func TestArrowSeries_Select(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "col_int", Format: df.IntegerFormat}
	intVals := []int64{10, 20, 0, 30}
	intValids := []bool{true, true, false, true}
	intArr := getTestInt64Array(mem, intVals, intValids)
	defer intArr.Release()
	intSeries := arrowimpl.NewArrowSeries(intArr, sSchemaInt)
	defer intSeries.(*arrowimpl.ArrowSeries).Release()

	// Case 1: Select with a Constant Expression
	constIntVal := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(5), df.IntegerFormat)
	constExpr := &mockExpr{exprName: "const_5", exprConstVal: constIntVal}
	selectedConst := intSeries.Select(constExpr)
	defer selectedConst.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, intSeries.Len(), selectedConst.Len())
	for i := int64(0); i < selectedConst.Len(); i++ {
		assert.Equal(t, int64(5), selectedConst.Get(i).GetAsInt())
	}
	assert.Equal(t, "const_5", selectedConst.Schema().Name)
	assert.True(t, constIntVal.Schema().Equals(selectedConst.Schema().Format))

	// Case 2: Select with a Column Reference (current implementation expects Col() to be series name or "" for simple copy)
	colRefExpr := &mockExpr{exprColName: "col_int"}
	selectedColRef := intSeries.Select(colRefExpr)
	defer selectedColRef.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, intSeries.Len(), selectedColRef.Len())
	assert.True(t, intSeries.Schema().Equals(selectedColRef.Schema()))
	for i := int64(0); i < intSeries.Len(); i++ {
		assert.True(t, intSeries.Get(i).Equals(selectedColRef.Get(i)))
	}

	// Case 3: Select with a Filter Operation (e.g., > 15)
	gtVal := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(15), df.IntegerFormat)
	filterExpr := &mockExpr{
		exprOpType: df.ExprTypeFilter, // Ensure this matches your df.ExprOpType definition
		exprFilterOp: &mockFilterOp{
			applyFunc: func(v df.Value, args ...df.Value) bool {
				if v.IsNil() { return false }
				return v.GetAsInt() > args[0].GetAsInt()
			},
			argExprs: []df.Expr{&mockExpr{exprConstVal: gtVal}},
		},
	}
	selectedFilter := intSeries.Select(filterExpr)
	defer selectedFilter.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(2), selectedFilter.Len()) // 20, 30
	assert.Equal(t, int64(20), selectedFilter.Get(0).GetAsInt())
	assert.Equal(t, int64(30), selectedFilter.Get(1).GetAsInt())

	// Case 4: Select with a Map Operation (e.g., value * 2)
	mapExpr := &mockExpr{
		exprOpType: df.ExprTypeMap, // Ensure this matches your df.ExprOpType definition
		exprMapOp: &mockMapOp{
			applyFunc: func(v df.Value, args ...df.Value) df.Value {
				if v.IsNil() { return arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat) }
				return arrowimpl.NewArrowValue(scalar.NewInt64Scalar(v.GetAsInt()*2), df.IntegerFormat)
			},
			returnFormat: df.IntegerFormat,
		},
	}
	selectedMap := intSeries.Select(mapExpr)
	defer selectedMap.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, intSeries.Len(), selectedMap.Len()) // 20, 40, nil, 60
	assert.Equal(t, int64(20), selectedMap.Get(0).GetAsInt())
	assert.Equal(t, int64(40), selectedMap.Get(1).GetAsInt())
	assert.True(t, selectedMap.Get(2).IsNil())
	assert.Equal(t, int64(60), selectedMap.Get(3).GetAsInt())

	// Case 5: Panic on nil expression
	assert.PanicsWithValue(t, "expression cannot be nil for Select", func() {
		intSeries.Select(nil)
	})

	// Case 6: Panic on unsupported expression type
	unsupportedExpr := &mockExpr{exprName:"unsupported", exprOpType: "UNSUPPORTED_OP_TYPE_XYZ"} // Use a distinct string for OpType
	assert.PanicsWithValue(t, fmt.Sprintf("unsupported expression for Series.Select: Name='unsupported', OpType='UNSUPPORTED_OP_TYPE_XYZ', Col=''"), func() {
		intSeries.Select(unsupportedExpr)
	})
}

// TODO: Add more tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
// This TODO was part of the original structure, some of these are now tested.
```

Then, `df/arrow/df_test.go`:
