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
func getTestFloat64Array(mem memory.Allocator, values []float64, valids []bool) arrow.Array {
	b := array.NewFloat64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
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

// --- Mock df.Expr, df.Value, df.MapOp ---
type mockExpr struct {
	exprName     string; exprConstVal df.Value; exprColName  string
	exprOpType   df.ExprOpType; exprMapOp    df.MapOp; exprParent   df.Expr
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
	argExprs     []df.Expr; returnFormat df.Format
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
func TestArrowDataFrame_Select_Advanced(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Rename_DataFrame(t *testing.T) { /* ... */ }
func TestArrowDataFrame_AsFormat(t *testing.T) { /* ... */ }


func TestArrowDataFrame_ForEachRow(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "active", Type: arrow.PrimitiveTypes.Boolean, Nullable: true},
		}, nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)

	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "", "C"}, []bool{true, false, true})
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 20, 0}, []bool{true, true, false})
	rb.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	record := rb.NewRecord(); defer record.Release()
	baseDf := arrowimpl.NewArrowDataFrame("foreach_test", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Iterate over non-empty DataFrame
	var processedRows [][]interface{}
	var rowCount int64
	baseDf.ForEachRow(func(row df.Row) {
		rowCount++
		nameVal := row.GetByName("name")
		valueVal := row.GetByName("value")
		activeVal := row.GetByName("active")

		var nameStr, valStr, activeStr string
		if nameVal.IsNil() { nameStr = "nil" } else { nameStr = nameVal.GetAsString() }
		if valueVal.IsNil() { valStr = "nil" } else { valStr = strconv.FormatInt(valueVal.GetAsInt(), 10) }
		if activeVal.IsNil() { activeStr = "nil" } else { activeStr = strconv.FormatBool(activeVal.GetAsBool()) }

		processedRows = append(processedRows, []interface{}{nameStr, valStr, activeStr})
	})

	assert.Equal(t, baseDf.Len(), rowCount, "Number of callback executions should match row count")
	expectedProcessed := [][]interface{}{
		{"A", "10", "true"},
		{"nil", "20", "false"},
		{"C", "nil", "true"},
	}
	assert.Equal(t, expectedProcessed, processedRows, "Data processed by ForEachRow")

	// Case 2: Iterate over an empty DataFrame
	emptyRec := array.NewRecord(schema, nil, 0); defer emptyRec.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_foreach", emptyRec, dfSchema)
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	emptyRowCount := 0
	emptyDf.ForEachRow(func(row df.Row) {
		emptyRowCount++
	})
	assert.Equal(t, 0, emptyRowCount, "Callback should not execute for empty DataFrame")

	// Case 3: Iterate over DataFrame with 0 columns but >0 rows
	schema0Col := arrow.NewSchema([]arrow.Field{}, nil)
	dfSchema0Col := arrowimpl.NewArrowDataFrameSchema(schema0Col).(*arrowimpl.ArrowDataFrameSchema)
	rec0Col := array.NewRecord(schema0Col, nil, 3); defer rec0Col.Release()
	df0Col := arrowimpl.NewArrowDataFrame("0col_foreach", rec0Col, dfSchema0Col)
	defer df0Col.(*arrowimpl.ArrowDataFrame).Release()

	count0ColRows := 0
	df0Col.ForEachRow(func(row df.Row) {
		assert.Equal(t, 0, row.Len(), "Row length should be 0 for 0-column DataFrame")
		count0ColRows++
	})
	assert.Equal(t, 3, count0ColRows, "Callback should execute for each 'empty' row in 0-column DataFrame")

	// Case 4: Panic if function f is nil
	assert.PanicsWithValue(t, "ForEachRow: function f cannot be nil", func() {
		baseDf.ForEachRow(nil)
	})
}

// TODO: Add tests for df.go (This was the original comment in the file)
