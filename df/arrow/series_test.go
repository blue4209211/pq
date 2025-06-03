//go:build arrow

package arrow_test

import (
	"fmt"
	"reflect"
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
	"github.com/blue4209211/pq/df/expr"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// --- Helper functions ---
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
// const nilPlaceholder = "__NIL_PLACEHOLDER__" // Defined in df_test.go, accessible in same package
func extractValues(s df.Series) []interface{} {
	var out []interface{}
	if s == nil { return out }
	for i := 0; i < s.Len(); i++ { // Use int for s.Len()
		v := s.Get(i) // Use int for s.Get()
		if v.IsNil() { out = append(out, nilPlaceholder) } else { out = append(out, v.Get()) }
	}
	return out
}
func sortInterfaceSlice(slice []interface{}) {
	sort.Slice(slice, func(i, j int) bool {
		if slice[i] == nilPlaceholder && slice[j] != nilPlaceholder { return true }
		if slice[i] != nilPlaceholder && slice[j] == nilPlaceholder { return false }
		if slice[i] == nilPlaceholder && slice[j] == nilPlaceholder { return false } // Consistent sort for two nils
		return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j])
	})
}
type mockValue struct { df.Value; data any; mockSchema df.Format; mockIsNil bool }
func (m *mockValue) Get() any { return m.data }
func (m *mockValue) IsNil() bool { return m.mockIsNil }
func (m *mockValue) Schema() df.Format { return m.mockSchema }
func (m *mockValue) GetAsInt() int64 { if i,ok := m.data.(int64); ok {return i}; panic("mockValue not int") }
func (m *mockValue) GetAsString() string { if s,ok := m.data.(string); ok {return s}; panic("mockValue not string") }


// --- Existing tests ---
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
	seriesName := "my_series"
	sSchema := df.SeriesSchema{Name: seriesName, Format: df.IntegerFormat}
	arr := getTestInt64Array(mem, []int64{1, 2, 3}, nil); defer arr.Release()
	s := arrowimpl.NewArrowSeries(arr, sSchema); defer s.(*arrowimpl.ArrowSeries).Release()

	expr := s.Expr()
	assert.NotNil(t, expr, "Expr() should not return nil")

	assert.Equal(t, df.ColNameExpr, expr.OpType(), "Expression type should be ColNameExpr")
	assert.Equal(t, seriesName, expr.Col(), "Expression column name should match series name")
	assert.Equal(t, seriesName, expr.Name(), "Expression name should match series name by default for ColExpr")

	unnamedSchema := df.SeriesSchema{Name: "", Format: df.IntegerFormat}
	// Need a new array for unnamedSeries as 'arr' is released by 's'
	arrForUnnamed := getTestInt64Array(mem, []int64{1}, nil); defer arrForUnnamed.Release()
	unnamedSeries := arrowimpl.NewArrowSeries(arrForUnnamed, unnamedSchema); defer unnamedSeries.Release()
	assert.PanicsWithValue(t, "Expr: cannot create a column expression for an unnamed series", func() {
		unnamedSeries.Expr()
	}, "Expr() on unnamed series should panic")
}

func TestArrowSeries_Select(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchema := df.SeriesSchema{Name: "test_series_for_select", Format: df.IntegerFormat, Nullable: true} // Made nullable for WhenNil test
	arr := getTestInt64Array(mem, []int64{1, 2, 3, 0, 5}, []bool{true, true, true, false, true}); defer arr.Release()
	s := arrowimpl.NewArrowSeries(arr, sSchema); defer s.(*arrowimpl.ArrowSeries).Release()


	type mockSeriesExpr struct {
		df.Expr
		parentExpr df.Expr; opType df.ExprOpType; mapOp df.MapOp
		filterOp   df.FilterOp; exprName string; colName string
		constVal   df.Value
	}
	func (m *mockSeriesExpr) Parent() df.Expr { return m.parentExpr }
	func (m *mockSeriesExpr) OpType() df.ExprOpType { return m.opType }
	func (m *mockSeriesExpr) MapOp() df.MapOp { return m.mapOp }
	func (m *mockSeriesExpr) FilterOp() df.FilterOp { return m.filterOp }
	func (m *mockSeriesExpr) Name() string { return m.exprName }
	func (m *mockSeriesExpr) SetName(n string) df.Expr { m.exprName = n; return m }
	func (m *mockSeriesExpr) Col() string { return m.colName }
	func (m *mockSeriesExpr) Const() df.Value { return m.constVal }
	func (m *mockSeriesExpr) SetParent(p df.Expr) df.Expr { m.parentExpr = p; return m }

	type mockSeriesMapOp struct {
		df.MapOp; opName string; args   []df.Expr
	}
	func (m *mockSeriesMapOp) Name() string { return m.opName }
	func (m *mockSeriesMapOp) Args() []df.Expr { return m.args }
	func (m *mockSeriesMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value { panic("not used by kernel path") }
	func (m *mockSeriesMapOp) ReturnFormat() df.Format { panic("not used by kernel path") }
	func (m *mockSeriesMapOp) SetArgs(args ...df.Expr) df.MapOp { m.args = args; return m}

	type mockSeriesFilterOp struct {
		df.FilterOp; opName string; args   []df.Expr
	}
	func (m *mockSeriesFilterOp) Name() string { return m.opName }
	func (m *mockSeriesFilterOp) Args() []df.Expr { return m.args }
	func (m *mockSeriesFilterOp) ApplyFilter(v df.Value, args ...df.Value) bool { panic("not used by kernel path") }
	func (m *mockSeriesFilterOp) SetArgs(args ...df.Expr) df.FilterOp {m.args = args; return m}

	newLitExpr := func(val df.Value, name string) df.Expr {
		return &mockSeriesExpr{opType: df.LiteralExpr, constVal: val, exprName: name}
	}

	t.Run("ArithmeticOps", func(t *testing.T) {
		addExpr := &mockSeriesExpr{
			opType:   df.ExprTypeMap,
			mapOp:    &mockSeriesMapOp{opName: "OpConst_Add", args: []df.Expr{newLitExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(5), df.IntegerFormat), "lit_5")}},
			exprName: "added_5",
		}
		sAdded := s.Select(addExpr); defer sAdded.Release()
		expectedAdd := []interface{}{int64(6), int64(7), int64(8), nilPlaceholder, int64(10)}
		assert.Equal(t, expectedAdd, extractValues(sAdded), "Integer Add")
	})

	t.Run("ComparisonOps", func(t *testing.T) {
		eqExpr := &mockSeriesExpr{
			opType:   df.ExprTypeFilter,
			filterOp: &mockSeriesFilterOp{opName: "OpFilter_EqConst", args: []df.Expr{newLitExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(3), df.IntegerFormat), "lit_3")}},
			exprName: "is_eq_3",
		}
		sEq := s.Select(eqExpr); defer sEq.Release()
		expectedEq := []interface{}{false, false, true, nilPlaceholder, false}
		assert.Equal(t, expectedEq, extractValues(sEq), "Integer Equals")
	})

	t.Run("WhenNilConstOp", func(t *testing.T) {
		fillVal := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(99), df.IntegerFormat)
		whenNilExpr := &mockSeriesExpr{
			opType:   df.ExprTypeMap,
			mapOp:    &mockSeriesMapOp{opName: "WhenNilConst", args: []df.Expr{newLitExpr(fillVal, "lit_99")}},
			exprName: "nils_filled",
		}
		sFilled := s.Select(whenNilExpr); defer sFilled.Release()
		expectedFilled := []interface{}{int64(1), int64(2), int64(3), int64(99), int64(5)}
		assert.Equal(t, expectedFilled, extractValues(sFilled), "WhenNilConst")
		assert.False(t, sFilled.Schema().Nullable)
	})

	t.Run("ChainedOps", func(t *testing.T) {
		add5Expr := &mockSeriesExpr{
			parentExpr: nil, opType: df.ExprTypeMap,
			mapOp:    &mockSeriesMapOp{opName: "OpConst_Add", args: []df.Expr{newLitExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(5), df.IntegerFormat), "lit_5_add")}},
			exprName: "s_plus_5",
		}
		eq10Expr := &mockSeriesExpr{
			parentExpr: add5Expr, opType: df.ExprTypeFilter,
			filterOp:   &mockSeriesFilterOp{opName: "OpFilter_EqConst", args: []df.Expr{newLitExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(10), df.IntegerFormat), "lit_10_eq")}},
			exprName:   "s_plus_5_eq_10",
		}
		sChained := s.Select(eq10Expr); defer sChained.Release()
		expectedChained := []interface{}{false, false, false, nilPlaceholder, true}
		assert.Equal(t, expectedChained, extractValues(sChained), "Chained Add then Eq")
	})
}

func TestArrowSeries_Join(t *testing.T) {
	mem := memory.NewGoAllocator()

	s1Schema := df.SeriesSchema{Name: "s1_int", Format: df.IntegerFormat}
	s1Arr := getTestInt64Array(mem, []int64{1, 2}, nil); defer s1Arr.Release()
	s1 := arrowimpl.NewArrowSeries(s1Arr, s1Schema); defer s1.Release()

	s2Schema := df.SeriesSchema{Name: "s2_str", Format: df.StringFormat}
	s2Arr := getTestStringArray(mem, []string{"a", "b", "c"}, nil); defer s2Arr.Release()
	s2 := arrowimpl.NewArrowSeries(s2Arr, s2Schema); defer s2.Release()

	s3Schema := df.SeriesSchema{Name: "s3_int", Format: df.IntegerFormat}
	s3Arr := getTestInt64Array(mem, []int64{10, 20, 30}, nil); defer s3Arr.Release()
	s3 := arrowimpl.NewArrowSeries(s3Arr, s3Schema); defer s3.Release()

	s4Schema := df.SeriesSchema{Name: "s4_int", Format: df.IntegerFormat}
	s4Arr := getTestInt64Array(mem, []int64{1,2}, nil); defer s4Arr.Release()
	s4 := arrowimpl.NewArrowSeries(s4Arr, s4Schema); defer s4.Release()

	t.Run("JoinCross", func(t *testing.T) {
		fUserCross := func(v1, v2 df.Value) []df.Value {
			resStr := fmt.Sprintf("%d-%s", v1.GetAsInt(), v2.GetAsString())
			return []df.Value{arrowimpl.NewArrowValue(scalar.NewStringScalar(resStr), df.StringFormat)}
		}
		resultSeries := s1.Join(df.StringFormat, s2, df.JoinCross, fUserCross); defer resultSeries.Release()
		expected := []interface{}{"1-a", "1-b", "1-c", "2-a", "2-b", "2-c"}
		actual := extractValues(resultSeries)
		assert.Equal(t, expected, actual, "Cross Join content mismatch")
		assert.Equal(t, df.StringFormat, resultSeries.Schema().Format)
		assert.Equal(t, s1.Schema().Name, resultSeries.Schema().Name)

		fUserCrossMulti := func(v1, v2 df.Value) []df.Value {
			res1 := fmt.Sprintf("%d-%s-copy1", v1.GetAsInt(), v2.GetAsString())
			res2 := fmt.Sprintf("%d-%s-copy2", v1.GetAsInt(), v2.GetAsString())
			return []df.Value{ arrowimpl.NewArrowValue(scalar.NewStringScalar(res1), df.StringFormat), arrowimpl.NewArrowValue(scalar.NewStringScalar(res2), df.StringFormat)}
		}
		resultMultiSeries := s1.Join(df.StringFormat, s2, df.JoinCross, fUserCrossMulti); defer resultMultiSeries.Release()
		assert.Equal(t, s1.Len()*s2.Len()*2, resultMultiSeries.Len())

		emptySArr := getTestInt64Array(mem, []int64{}, nil); defer emptySArr.Release()
		emptyS := arrowimpl.NewArrowSeries(emptySArr, s1Schema); defer emptyS.Release()
		resEmpty1 := s1.Join(df.StringFormat, emptyS, df.JoinCross, fUserCross); defer resEmpty1.Release()
		assert.Equal(t, 0, resEmpty1.Len())
		resEmpty2 := emptyS.Join(df.StringFormat, s2, df.JoinCross, fUserCross); defer resEmpty2.Release()
		assert.Equal(t, 0, resEmpty2.Len())
	})

	t.Run("JoinEqui", func(t *testing.T) {
		fUserEquiSum := func(v1, v2 df.Value) []df.Value {
			sum := v1.GetAsInt() + v2.GetAsInt()
			return []df.Value{arrowimpl.NewArrowValue(scalar.NewInt64Scalar(sum), df.IntegerFormat)}
		}
		resultSeries := s1.Join(df.IntegerFormat, s4, df.JoinEqui, fUserEquiSum); defer resultSeries.Release()
		expected := []interface{}{int64(2), int64(4)}
		assert.Equal(t, expected, extractValues(resultSeries))
		assert.Equal(t, df.IntegerFormat, resultSeries.Schema().Format)

		emptyS1Arr := getTestInt64Array(mem, []int64{}, nil); defer emptyS1Arr.Release()
		emptyS1 := arrowimpl.NewArrowSeries(emptyS1Arr, s1Schema); defer emptyS1.Release()
		emptyS2Arr := getTestInt64Array(mem, []int64{}, nil); defer emptyS2Arr.Release()
		emptyS2 := arrowimpl.NewArrowSeries(emptyS2Arr, s4Schema); defer emptyS2.Release()
		resEmpty := emptyS1.Join(df.IntegerFormat, emptyS2, df.JoinEqui, fUserEquiSum); defer resEmpty.Release()
		assert.Equal(t, 0, resEmpty.Len())

		assert.PanicsWithValue(t, fmt.Sprintf("Join (Equi): series lengths must be equal. Self: %d, Other: %d", s1.Len(), s3.Len()), func() {
			s1.Join(df.IntegerFormat, s3, df.JoinEqui, fUserEquiSum)
		})
	})

	t.Run("UnsupportedJoinTypes", func(t *testing.T) {
		fUserDummy := func(v1,v2 df.Value) []df.Value { return nil }
		unsupported := []df.JoinType{df.JoinLeft, df.JoinOuter, df.JoinRight, df.JoinLeftAnti, df.JoinLeftSemi, df.JoinRightAnti, df.JoinRightSemi}
		for _, jt := range unsupported {
			assert.PanicsWithValue(t, fmt.Sprintf("JoinType '%s' not supported for arrowSeries.Join", jt), func() {
				s1.Join(df.StringFormat, s2, jt, fUserDummy)
			}, "Unsupported join type %s should panic", jt)
		}
	})

	t.Run("PanicConditions", func(t *testing.T) {
		fUserDummy := func(v1,v2 df.Value) []df.Value { return nil }
		assert.PanicsWithValue(t, "Join: function f cannot be nil", func(){ s1.Join(df.StringFormat, s2, df.JoinCross, nil)})
		assert.PanicsWithValue(t, "Join: outputFormat cannot be nil or UnknownFormat", func(){ s1.Join(nil, s2, df.JoinCross, fUserDummy)})
		assert.PanicsWithValue(t, "Join: outputFormat cannot be nil or UnknownFormat", func(){ s1.Join(df.UnknownFormat, s2, df.JoinCross, fUserDummy)})
		assert.PanicsWithValue(t, "Join: otherRaw series cannot be nil", func(){ s1.Join(df.StringFormat, nil, df.JoinCross, fUserDummy)})

		fUserBadReturn := func(v1,v2 df.Value) []df.Value {
			return []df.Value{arrowimpl.NewArrowValue(scalar.NewStringScalar("not_an_int"), df.StringFormat)}
		}
		assert.Panics(t, func(){s1.Join(df.IntegerFormat, s4, df.JoinEqui, fUserBadReturn)}, "Panic on bad value from fUser for output format")
	})
}


func TestArrowSeries_AsFormat(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int", Format: df.IntegerFormat}
	sSchemaStr := df.SeriesSchema{Name: "s_str", Format: df.StringFormat}
	sSchemaFloat := df.SeriesSchema{Name: "s_float", Format: df.DoubleFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 30}, []bool{true, false, true}); defer arr1.Release()
	s1Int := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1Int.(*arrowimpl.ArrowSeries).Release()

	s1AsStr := s1Int.AsFormat(df.StringFormat);	defer s1AsStr.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, s1Int.Len(), s1AsStr.Len())
	assert.Equal(t, df.StringFormat.Name(), s1AsStr.Schema().Format.Name())
	assert.Equal(t, "10", s1AsStr.Get(0).GetAsString())
	assert.True(t, s1AsStr.Get(1).IsNil())
	assert.Equal(t, "30", s1AsStr.Get(2).GetAsString())

	arr2 := getTestStringArray(mem, []string{"100", "0", "300"}, []bool{true, false, true}); defer arr2.Release() // "0" is nil string
	s2Str := arrowimpl.NewArrowSeries(arr2, sSchemaStr); defer s2Str.(*arrowimpl.ArrowSeries).Release()
	s2AsInt := s2Str.AsFormat(df.IntegerFormat); defer s2AsInt.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(100), s2AsInt.Get(0).GetAsInt())
	assert.True(t, s2AsInt.Get(1).IsNil()) // nil string to nil int
	assert.Equal(t, int64(300), s2AsInt.Get(2).GetAsInt())

	arr3 := getTestStringArray(mem, []string{"abc"}, nil); defer arr3.Release()
	s3Str := arrowimpl.NewArrowSeries(arr3, sSchemaStr); defer s3Str.(*arrowimpl.ArrowSeries).Release()
	assert.Panics(t, func() { s3Str.AsFormat(df.IntegerFormat) }, "Cast non-numeric string to int should panic")

	arr4 := getTestFloat64Array(mem, []float64{10.1, 0.0, 10.9, 30.5}, []bool{true, false, true, true}); defer arr4.Release()
	s4Float := arrowimpl.NewArrowSeries(arr4, sSchemaFloat); defer s4Float.(*arrowimpl.ArrowSeries).Release()
	s4AsInt := s4Float.AsFormat(df.IntegerFormat);	defer s4AsInt.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(10), s4AsInt.Get(0).GetAsInt())
	assert.True(t, s4AsInt.Get(1).IsNil())
	assert.Equal(t, int64(10), s4AsInt.Get(2).GetAsInt())
	assert.Equal(t, int64(30), s4AsInt.Get(3).GetAsInt())

	s1AsIntCopy := s1Int.AsFormat(df.IntegerFormat);	defer s1AsIntCopy.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, s1Int.Len(), s1AsIntCopy.Len())
	assert.True(t, df.IntegerFormat.Equals(s1AsIntCopy.Schema().Format))
	assert.Equal(t, extractValues(s1Int), extractValues(s1AsIntCopy))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyAsStr := sEmpty.AsFormat(df.StringFormat);	defer sEmptyAsStr.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyAsStr.Len())

	assert.PanicsWithValue(t, "AsFormat: targetFormat cannot be nil", func() { s1Int.AsFormat(nil) })
}


func TestArrowSeries_WhenNil_Series(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int_wn", Format: df.IntegerFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 30, 0}, []bool{true, false, true, false}); defer arr1.Release()
	s1 := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1.(*arrowimpl.ArrowSeries).Release()

	fillVal1 := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(99), df.IntegerFormat)
	s1Filled1 := s1.WhenNil(fillVal1);	defer s1Filled1.(*arrowimpl.ArrowSeries).Release()
	expected1 := []interface{}{int64(10), int64(99), int64(30), int64(99)}
	assert.Equal(t, expected1, extractValues(s1Filled1))

	fillValNil := arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat)
	s1FilledNil := s1.WhenNil(fillValNil);	defer s1FilledNil.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1FilledNil))

	arrNoNil := getTestInt64Array(mem, []int64{1,2,3}, nil); defer arrNoNil.Release()
	sNoNil := arrowimpl.NewArrowSeries(arrNoNil, sSchemaInt); defer sNoNil.(*arrowimpl.ArrowSeries).Release()
	sNoNilFilled := sNoNil.WhenNil(fillVal1);	defer sNoNilFilled.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(sNoNil), extractValues(sNoNilFilled))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyFilled := sEmpty.WhenNil(fillVal1);	defer sEmptyFilled.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyFilled.Len())

	assert.PanicsWithValue(t, "WhenNil: fillValue cannot be nil (can be a nil df.Value though)", func() { s1.WhenNil(nil) })

	fillValStr := arrowimpl.NewArrowValue(scalar.NewStringScalar("abc"), df.StringFormat)
	assert.Panics(t, func() { s1.WhenNil(fillValStr) })
}

func TestArrowSeries_When_Series(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int_when", Format: df.IntegerFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 20, 10, 30, 0}, []bool{true, false, true, true, true, false})
	defer arr1.Release()
	s1 := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1.(*arrowimpl.ArrowSeries).Release()

	replaceMap1 := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewInt64Scalar(100), df.IntegerFormat),
		nil:       arrowimpl.NewArrowValue(scalar.NewInt64Scalar(99), df.IntegerFormat),
	}
	s1Replaced1 := s1.When(replaceMap1);	defer s1Replaced1.(*arrowimpl.ArrowSeries).Release()
	expected1 := []interface{}{int64(100), int64(99), int64(20), int64(100), int64(30), int64(99)}
	assert.Equal(t, expected1, extractValues(s1Replaced1))

	replaceMap2 := map[any]df.Value{
		int64(20): arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat),
	}
	s1Replaced2 := s1.When(replaceMap2);	defer s1Replaced2.(*arrowimpl.ArrowSeries).Release()
	expected2 := []interface{}{int64(10), nilPlaceholder, nilPlaceholder, int64(10), int64(30), nilPlaceholder}
	assert.Equal(t, expected2, extractValues(s1Replaced2))

	replaceMapNoMatch := map[any]df.Value{ int64(999): arrowimpl.NewArrowValue(scalar.NewInt64Scalar(1000), df.IntegerFormat) }
	s1NoMatch := s1.When(replaceMapNoMatch);	defer s1NoMatch.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1NoMatch))

	s1EmptyMap := s1.When(map[any]df.Value{});	defer s1EmptyMap.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1EmptyMap))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyReplaced := sEmpty.When(replaceMap1);	defer sEmptyReplaced.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyReplaced.Len())

	replaceMapCast := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewStringScalar("1000"), df.StringFormat),
	}
	s1ReplacedCast := s1.When(replaceMapCast);	defer s1ReplacedCast.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(1000), s1ReplacedCast.Get(0).GetAsInt())
	assert.Equal(t, int64(1000), s1ReplacedCast.Get(3).GetAsInt())
	assert.True(t, s1ReplacedCast.Get(1).IsNil())

	replaceMapBadCast := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewStringScalar("not-an-int"), df.StringFormat),
	}
	assert.Panics(t, func() { s1.When(replaceMapBadCast) })
}

// TODO: Add more tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
// This TODO was part of the original structure, some of these are now tested.

[end of df/arrow/series_test.go]
