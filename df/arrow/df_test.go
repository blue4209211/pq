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
// func TestArrowDataFrame_Except(t *testing.T) { /* ... */ } // Will be replaced by TestArrowDataFrame_Except_KernelBased
func TestArrowDataFrame_Select_Advanced(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Rename_DataFrame(t *testing.T) { /* ... */ }
func TestArrowDataFrame_AsFormat(t *testing.T) { /* ... */ }
func TestArrowDataFrame_ForEachRow(t *testing.T) { /* ... */ }


func TestArrowDataFrame_Except_KernelBased(t *testing.T) {
	mem := memory.NewGoAllocator()

	schemaL := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable:true}, // Made id nullable for nil key tests
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, nil,
	)
	dfSchemaL := arrowimpl.NewArrowDataFrameSchema(schemaL).(*arrowimpl.ArrowDataFrameSchema)

	lrb := array.NewRecordBuilder(mem, schemaL); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 1, 5, 0}, []bool{true, true, true, true, true, true, false})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"A_one", "A_two", "A_three", "A_four", "A_one", "", "A_nil_id"}, []bool{true, true, true, true, true, false, true})
	lrb.Field(2).(*array.Int64Builder).AppendValues([]int64{100, 0, 300, 100, 100, 500, 600}, []bool{true, false, true, true, true, true, true})
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_A_except", lRec, dfSchemaL)
	defer ldf.(*arrowimpl.ArrowDataFrame).Release()

	rrb := array.NewRecordBuilder(mem, schemaL); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3, 6, 5, 0}, []bool{true, true, true, true, false})
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"B_two", "A_three", "B_six", "", "A_nil_id_diff"}, []bool{true, true, true, false, true})
	rrb.Field(2).(*array.Int64Builder).AppendValues([]int64{2000, 300, 6000, 500, 600}, []bool{true, true, true, true, true})
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_B_except", rRec, dfSchemaL)
	defer rdf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: A.Except(B) on key "id"
	// LDF ids (distinct after internal sort for key matching by Except's Join): {nil, 1, 2, 3, 4, 5}
	// RDF ids (distinct for key matching by Except's Join): {nil, 2, 3, 5, 6}
	// IDs in LDF whose keys are NOT in RDF's keys: {1, 4}
	// Expected unique rows from LDF corresponding to these IDs:
	// (1, "A_one", 100)  (Note: LDF has two (1, "A_one", 100) rows, Distinct at end makes it one)
	// (4, "A_four", 100)
	except1 := ldf.Except(rdf, "id")
	defer except1.(*arrowimpl.ArrowDataFrame).Release()
	expectedData1 := [][]interface{}{
		{int64(1), "A_one", int64(100)},
		{int64(4), "A_four", int64(100)},
	}
	actualData1 := dfToSliceOfInterfaceSlices(except1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), int(except1.Len()), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: A.Except(B) on keys "name", "value"
	// LDF distinct (name,value) for key matching: ("A_one",100), ("A_two",nil), ("A_three",300), ("A_four",100), (nil,500), ("A_nil_id",600)
	// RDF distinct (name,value) for key matching: ("B_two",2000), ("A_three",300), ("B_six",6000), (nil,5000), ("A_nil_id_diff",600)
	// LDF (name,value) pairs NOT IN RDF's pairs:
	// ("A_one",100)
	// ("A_two",nil)
	// ("A_four",100)
	// (nil,500)  (since (nil,500) is not same as (nil,5000) in RDF)
	// ("A_nil_id",600) (since ("A_nil_id",600) is not same as ("A_nil_id_diff",600) in RDF)
	// Expected rows from LDF (after final Distinct):
	except2 := ldf.Except(rdf, "name", "value")
	defer except2.(*arrowimpl.ArrowDataFrame).Release()
	expectedData2 := [][]interface{}{
		{int64(1), "A_one", int64(100)}, // This covers both (1,A_one,100) entries in LDF
		{int64(2), "A_two", nilPlaceholder},
		{int64(4), "A_four", int64(100)},
		{int64(5), nilPlaceholder, int64(500)},
		{nilPlaceholder, "A_nil_id", int64(600)},
	}
	actualData2 := dfToSliceOfInterfaceSlices(except2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), int(except2.Len()), "Case 2: Length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Data")

	// Case 3: All rows in LDF have matching keys in RDF (A - A = empty)
	except3 := ldf.Except(ldf);	defer except3.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), except3.Len(), "Case 3: A Except A should be empty")

	// Case 4: Other DataFrame is empty (A - {} = Distinct A)
	emptyRec := array.NewRecord(schemaL, nil, 0); defer emptyRec.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_except", emptyRec, dfSchemaL);	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()
	except4 := ldf.Except(emptyDf, "id");	defer except4.(*arrowimpl.ArrowDataFrame).Release()
	expectedData4 := dfToSliceOfInterfaceSlices(ldf.Distinct())
	actualData4 := dfToSliceOfInterfaceSlices(except4)
	sortSliceOfInterfaceSlices(expectedData4); sortSliceOfInterfaceSlices(actualData4)
	assert.Equal(t, len(expectedData4), int(except4.Len()), "Case 4: Length (A Except empty)")
	assert.Equal(t, expectedData4, actualData4, "Case 4: Data (A Except empty)")

	// Case 5: Panic conditions (delegated to Join, but good to confirm for Except context)
	assert.PanicsWithValue(t, "Except: other dataframe cannot be nil", func() { ldf.Except(nil, "id") })
	schemaRDiffIdType := arrow.NewSchema( []arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil )
	dfSchemaRDiffIdType := arrowimpl.NewArrowDataFrameSchema(schemaRDiffIdType).(*arrowimpl.ArrowDataFrameSchema)
	rRecDiffIdType := array.NewRecord(schemaRDiffIdType, nil, 0); defer rRecDiffIdType.Release()
	rdfDiffIdType := arrowimpl.NewArrowDataFrame("rdfDiffIdType_except", rRecDiffIdType, dfSchemaRDiffIdType);	defer rdfDiffIdType.(*arrowimpl.ArrowDataFrame).Release()
	// This panic message comes from the Join method's key type validation.
	assert.Panics(t, func() { ldf.Except(rdfDiffIdType, "id") }, "Panic on key type mismatch for 'id' in Except")
}

// TODO: Add tests for df.go (This was the original comment in the file)
