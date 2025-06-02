//go:build arrow

package arrow_test

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"
	"reflect"

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
func getTestDataFrameArrowSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)
}
func getTestDataFrameRecord(mem memory.Allocator, schema *arrow.Schema) arrow.Record {
	b := array.NewRecordBuilder(mem, schema); defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 0, 300}, []bool{true, false, true})
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 0}, []bool{true, true, false})
	return b.NewRecord()
}
func getBaseTestDf(t *testing.T, mem memory.Allocator) df.DataFrame {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_a", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, nil,
	)
	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"row1", "row2", "row3"}, nil)
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	record := rb.NewRecord()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	return arrowimpl.NewArrowDataFrame("test_df", record, dfSchema)
}
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := array.NewInt64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := array.NewStringBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}

const nilPlaceholder = "__NIL_PLACEHOLDER__"

func dfToSliceOfInterfaceSlices(dataFrame df.DataFrame) [][]interface{} {
	var result [][]interface{}
	if dataFrame == nil || dataFrame.Len() == 0 { return result }
	for r := int64(0); r < dataFrame.Len(); r++ {
		row := dataFrame.GetRow(r); var rowData []interface{}
		for c := 0; c < row.Len(); c++ {
			val := row.Get(c)
			if val.IsNil() { rowData = append(rowData, nilPlaceholder) } else { rowData = append(rowData, val.Get()) }
		}
		result = append(result, rowData)
	}
	return result
}

func sortSliceOfInterfaceSlices(slice [][]interface{}) {
	sort.Slice(slice, func(i, j int) bool { return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j]) })
}

// --- Mock df.Expr, df.Value, df.MapOp (redefine or ensure accessible if in another _test.go file) ---
type mockExpr struct {
	exprName     string
	exprConstVal df.Value
	exprColName  string
	exprOpType   df.ExprOpType
	exprMapOp    df.MapOp
	exprParent   df.Expr
}
func (m *mockExpr) Name() string { return m.exprName }
func (m *mockExpr) Const() df.Value { return m.exprConstVal }
func (m *mockExpr) Col() string { return m.exprColName }
func (m *mockExpr) OpType() df.ExprOpType { return m.exprOpType }
func (m *mockExpr) FilterOp() df.FilterOp { return nil }
func (m *mockExpr) MapOp() df.MapOp { return m.exprMapOp }
func (m *mockExpr) Parent() df.Expr { return m.exprParent }
func (m *mockExpr) SetParent(p df.Expr) df.Expr { m.exprParent = p; return m }
func (m *mockExpr) SetName(n string) df.Expr {m.exprName = n; return m}

type mockMapOp struct {
	applyFunc    func(v df.Value, args ...df.Value) df.Value
	argExprs     []df.Expr
	returnFormat df.Format
}
func (m *mockMapOp) Args() []df.Expr { return m.argExprs }
func (m *mockMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value { return m.applyFunc(v, args...) }
func (m *mockMapOp) ReturnFormat() df.Format { return m.returnFormat }
func (m *mockMapOp) SetArgs(args ...df.Expr) df.MapOp { m.argExprs = args; return m }


// --- Existing tests ---
func TestArrowDataFrame_NewArrowDataFrame(t *testing.T) { /* ... */ }
func TestArrowDataFrame_NewArrowDataFrameFromArrays(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Accessors(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Limit(t *testing.T) { /* ... */ }
func TestArrowDataFrame_SelectBySeriesIndex(t *testing.T) { /* ... */ }
func TestArrowDataFrame_SelectBySeriesName(t *testing.T) { /* ... */ }
func TestArrowDataFrame_WhereRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Sort(t *testing.T) { /* ... */ }
func TestArrowDataFrame_AddSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_RemoveSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_RenameSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_GetSeriesExprByName(t *testing.T) { /* ... */ }
func TestArrowDataFrame_MapRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_FlatMapRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Distinct(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Append(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Union(t *testing.T) { /* ... */ }
func TestArrowDataFrame_WhenNil(t *testing.T) { /* ... */ }
func TestArrowDataFrame_When(t *testing.T) { /* ... */ }
func TestArrowDataFrame_UpdateSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Join_EquiJoin(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Join_CrossJoin_Partial(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Intersection(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Except(t *testing.T) { /* ... */ }


func TestArrowDataFrame_Select_Advanced(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_a", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_c", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"r1", "r2", "r3"}, []bool{true, true, true})
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 0, 30}, []bool{true, false, true})
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 0}, []bool{true, true, false})
	record := rb.NewRecord(); defer record.Release()
	baseDf := arrowimpl.NewArrowDataFrame("select_adv_test", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Select existing columns
	exprColA := &mockExpr{exprName: "res_A", exprColName: "col_a"}
	exprColC := &mockExpr{exprName: "res_C", exprColName: "col_c"}
	selectedCols := baseDf.Select(exprColA, exprColC);	defer selectedCols.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, baseDf.Len(), selectedCols.Len())
	assert.Equal(t, 2, selectedCols.Schema().Len())
	assert.Equal(t, "res_A", selectedCols.Schema().Get(0).Name)
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, selectedCols.Schema().(*arrowimpl.ArrowDataFrameSchema).InternalArrowSchema().Field(0).Type))
	assert.Equal(t, "r1", selectedCols.GetValue(0,0).GetAsString())
	assert.True(t, selectedCols.GetValue(2,1).IsNil())

	// Case 2: Select constant values
	constStrVal := arrowimpl.NewArrowValue(scalar.NewStringScalar("const_str"), df.StringFormat)
	exprConstStr := &mockExpr{exprName: "LiteralStr", exprConstVal: constStrVal}
	constIntVal := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(999), df.IntegerFormat)
	exprConstInt := &mockExpr{exprName: "LiteralInt", exprConstVal: constIntVal}
	selectedConsts := baseDf.Select(exprConstStr, exprConstInt);	defer selectedConsts.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, baseDf.Len(), selectedConsts.Len())
	for r := int64(0); r < selectedConsts.Len(); r++ {
		assert.Equal(t, "const_str", selectedConsts.GetValue(r,0).GetAsString())
		assert.Equal(t, int64(999), selectedConsts.GetValue(r,1).GetAsInt())
	}

	// Case 3: Select simple single-column transformation (map op)
	baseColBExpr := &mockExpr{exprName: "col_b_base", exprColName: "col_b"}
	mapOpDouble := &mockMapOp{
		applyFunc: func(v df.Value, args ...df.Value) df.Value {
			if v.IsNil() { return arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat) }
			return arrowimpl.NewArrowValue(scalar.NewInt64Scalar(v.GetAsInt()*2), df.IntegerFormat)
		},
		returnFormat: df.IntegerFormat,
	}
	exprMapColB := &mockExpr{exprName: "col_b_doubled", exprParent: baseColBExpr, exprOpType: df.ExprTypeMap, exprMapOp: mapOpDouble}
	selectedMapped := baseDf.Select(exprMapColB);	defer selectedMapped.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(20), selectedMapped.GetValue(0,0).GetAsInt())
	assert.True(t, selectedMapped.GetValue(1,0).IsNil())
	assert.Equal(t, int64(60), selectedMapped.GetValue(2,0).GetAsInt())

	// Case 4: Mixed expressions
	exprColA_forName := &mockExpr{exprName: "col_a_alias", exprColName: "col_a"}
	selectedMixed := baseDf.Select(exprColA_forName, exprConstInt, exprMapColB);	defer selectedMixed.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, 3, selectedMixed.Schema().Len())
	assert.Equal(t, "col_a_alias", selectedMixed.Schema().Get(0).Name)
	assert.Equal(t, "LiteralInt", selectedMixed.Schema().Get(1).Name)
	assert.Equal(t, "col_b_doubled", selectedMixed.Schema().Get(2).Name)
	assert.Equal(t, "r1", selectedMixed.GetValue(0,0).GetAsString())
	assert.Equal(t, int64(999), selectedMixed.GetValue(0,1).GetAsInt())
	assert.Equal(t, int64(20), selectedMixed.GetValue(0,2).GetAsInt())

	// Case 5: No expressions (empty select)
	selectedEmptyExpr := baseDf.Select();	defer selectedEmptyExpr.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, baseDf.Len(), selectedEmptyExpr.Len())
	assert.Equal(t, 0, selectedEmptyExpr.Schema().Len())

	// Case 6: DataFrame with 0 rows
	emptyRec := array.NewRecord(schema, nil, 0); defer emptyRec.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_select_df", emptyRec, dfSchema);	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()
	selectedFromEmpty := emptyDf.Select(exprColA, exprConstStr);	defer selectedFromEmpty.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), selectedFromEmpty.Len())
	assert.Equal(t, 2, selectedFromEmpty.Schema().Len())

	// Case 7: Panic on nil expression in list
	assert.PanicsWithValue(t, "Select: expression at index 0 is nil", func() { baseDf.Select(nil) })
	assert.PanicsWithValue(t, "Select: expression at index 1 is nil", func() { baseDf.Select(exprColA, nil) })

	// Case 8: Panic on unsupported expression
	unsupportedExpr := &mockExpr{exprName: "bad_expr", exprOpType: "SOME_OTHER_OP"}
	assert.PanicsWithValue(t, fmt.Sprintf("Select: expression '%s' (type: %s, col: %s) is not supported in this DataFrame.Select version", unsupportedExpr.Name(), unsupportedExpr.OpType(), unsupportedExpr.Col()), func() {
		baseDf.Select(unsupportedExpr)
	})
}

// TODO: Add tests for df.go (This was the original comment in the file)
