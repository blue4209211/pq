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
	"github.com/apache/arrow/go/v14/arrow/builder"
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
// getTestInt64Array, getTestStringArray, getTestFloat64Array are defined in series_test.go or df_test.go

const nilPlaceholder = "__NIL_PLACEHOLDER__"

func dfToSliceOfInterfaceSlices(dataFrame df.DataFrame) [][]interface{} {
	var result [][]interface{}
	if dataFrame == nil || dataFrame.Len() == 0 { return result }
	for r := 0; r < dataFrame.Len(); r++ { // dataFrame.Len() is int
		row := dataFrame.GetRow(int64(r)); var rowData []interface{}
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

// Helper to create a df.Value from a Go native value and an arrow.DataType
func makeArrowValue(val interface{}, dt arrow.DataType) df.Value {
	var s scalar.Scalar
	if val == nil {
		s = scalar.NewNullScalar(dt)
	} else {
		switch dt.ID() {
		case arrow.INT64:
			s = scalar.NewInt64Scalar(val.(int64))
		case arrow.STRING:
			s = scalar.NewStringScalar(val.(string))
		case arrow.FLOAT64:
			s = scalar.NewFloat64Scalar(val.(float64))
		case arrow.BOOL:
			s = scalar.NewBooleanScalar(val.(bool))
		default:
			panic(fmt.Sprintf("unsupported type for makeArrowValue: %s", dt.Name()))
		}
	}
	dummyFormat := df.FormatWithName(dt.Name())
	if dt.ID() == arrow.INT64 { dummyFormat = df.IntegerFormat }
	if dt.ID() == arrow.STRING { dummyFormat = df.StringFormat }
	if dt.ID() == arrow.FLOAT64 { dummyFormat = df.DoubleFormat }
	return arrowimpl.NewArrowValue(s, dummyFormat)
}

type mockSeriesExpr struct {
	df.Expr
	parentExpr df.Expr
	opType     df.ExprOpType
	mapOp      df.MapOp
	filterOp   df.FilterOp
	exprName   string
	colName    string
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
	df.MapOp
	opName string
	args   []df.Expr
}
func (m *mockSeriesMapOp) Name() string { return m.opName }
func (m *mockSeriesMapOp) Args() []df.Expr { return m.args }
func (m *mockSeriesMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value { panic("not used by kernel path") }
func (m *mockSeriesMapOp) ReturnFormat() df.Format { panic("not used by kernel path") }
func (m *mockSeriesMapOp) SetArgs(args ...df.Expr) df.MapOp { m.args = args; return m}

type mockSeriesFilterOp struct {
	df.FilterOp
	opName string
	args   []df.Expr
}
func (m *mockSeriesFilterOp) Name() string { return m.opName }
func (m *mockSeriesFilterOp) Args() []df.Expr { return m.args }
func (m *mockSeriesFilterOp) ApplyFilter(v df.Value, args ...df.Value) bool { panic("not used by kernel path") }
func (m *mockSeriesFilterOp) SetArgs(args ...df.Expr) df.FilterOp {m.args = args; return m}


// --- Existing tests ... (assuming they are present) ---
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
func TestArrowDataFrame_Intersection(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Select_Advanced(t *testing.T) { /* ... */ }

func TestArrowDataFrame_Except_KernelBased(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaL := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable:true},
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
	defer ldf.(df.Releaser).Release()
	rrb := array.NewRecordBuilder(mem, schemaL); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3, 6, 5, 0}, []bool{true, true, true, true, false})
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"B_two", "A_three", "B_six", "", "A_nil_id_diff"}, []bool{true, true, true, false, true})
	rrb.Field(2).(*array.Int64Builder).AppendValues([]int64{2000, 300, 6000, 500, 600}, []bool{true, true, true, true, true})
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_B_except", rRec, dfSchemaL)
	defer rdf.(df.Releaser).Release()
	except1 := ldf.Except(rdf, "id");	defer except1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{ {int64(1), "A_one", int64(100)}, {int64(4), "A_four", int64(100)}, }
	actualData1 := dfToSliceOfInterfaceSlices(except1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, expectedData1, actualData1)
	// ... (rest of Except_KernelBased test as was)
}

func TestDataFrame_Join_Inner(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Join_Left(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Join_Right(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Join_FullOuter(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Join_Cross(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Join_LeftAnti(t *testing.T) { /* ... existing ... */ }

// --- New or Unskipped Semi/Anti Join Tests ---

func TestDataFrame_Join_LeftSemi(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)
	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 0, 5}, []bool{true, true, true, true, false, true})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4", "L_nil", "L5_dup"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_leftsemi", lRec, dfSchemaLeft);	defer ldf.(df.Releaser).Release()

	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 6, 0}, []bool{true, true, true, true, false})
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1_match", "R2_match_a", "R2_match_b", "R6_nomatch", "R_nil_match"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_leftsemi", rRec, dfSchemaRight);	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}
	result1 := ldf.Join(dfSchemaLeft, rdf, df.JoinLeftSemi, joinColsMap, nil)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{ {int64(1), "L1"}, {int64(2), "L2"} } // Default: nulls don't match each other for semi
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, expectedData1, actualData1, "Case 1: Standard Left Semi")
	assert.True(t, result1.Schema().Equals(dfSchemaLeft), "Case 1: Schema should be left table's schema")
	// ... (other test cases for LeftSemi as previously implemented) ...
	// Case 2: Right dataframe empty
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_leftsemi", emptyRecR, dfSchemaRight); defer rdfEmpty.(df.Releaser).Release()
	result2 := ldf.Join(dfSchemaLeft, rdfEmpty, df.JoinLeftSemi, joinColsMap, nil); defer result2.(df.Releaser).Release()
	assert.Equal(t, 0, result2.Len(), "Case 2: Right DF empty, length should be 0")
	// Case 3: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_leftsemi", emptyRecL, dfSchemaLeft); defer ldfEmpty.(df.Releaser).Release()
	result3 := ldfEmpty.Join(dfSchemaLeft, rdf, df.JoinLeftSemi, joinColsMap, nil); defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Left dataframe empty, length should be 0")
}

func TestDataFrame_Join_RightSemi(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaLeft := arrow.NewSchema([]arrow.Field{{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},{Name: "val_l", Type: arrow.BinaryTypes.String}}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)
	schemaRight := arrow.NewSchema([]arrow.Field{{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},{Name: "val_r", Type: arrow.BinaryTypes.String}}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 7, 0}, []bool{true, true, true, false})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1_match", "L2_match", "L7_nomatch", "L_nil_nomatch"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_rightsemi", lRec, dfSchemaLeft);	defer ldf.(df.Releaser).Release()

	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 6, 0, 8}, []bool{true, true, true, true, false, true})
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R2_a", "R2_b", "R6", "R_nil", "R8"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_rightsemi", rRec, dfSchemaRight);	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}
	result1 := ldf.Join(dfSchemaRight, rdf, df.JoinRightSemi, joinColsMap, nil)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{ {int64(1), "R1"}, {int64(2), "R2_a"}, {int64(2), "R2_b"} }
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, expectedData1, actualData1, "Case 1: Standard Right Semi")
	assert.True(t, result1.Schema().Equals(dfSchemaRight), "Case 1: Schema should be right's")
	// ... (other test cases for RightSemi as previously implemented) ...
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_rightsemi", emptyRecL, dfSchemaLeft); defer ldfEmpty.(df.Releaser).Release()
	result2 := ldfEmpty.Join(dfSchemaRight, rdf, df.JoinRightSemi, joinColsMap, nil); defer result2.(df.Releaser).Release()
	assert.Equal(t, 0, result2.Len(), "Case 2: Left DF empty")
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_rightsemi", emptyRecR, dfSchemaRight); defer rdfEmpty.(df.Releaser).Release()
	result3 := ldf.Join(dfSchemaRight, rdfEmpty, df.JoinRightSemi, joinColsMap, nil); defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Right DF empty")
}

func TestDataFrame_Join_RightAnti(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaLeft := arrow.NewSchema([]arrow.Field{{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},{Name: "val_l", Type: arrow.BinaryTypes.String}}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)
	schemaRight := arrow.NewSchema([]arrow.Field{{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},{Name: "val_r", Type: arrow.BinaryTypes.String}}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	lrb1 := array.NewRecordBuilder(mem, schemaLeft); defer lrb1.Release()
	lrb1.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 7, 0}, []bool{true, true, true, false})
	lrb1.Field(1).(*array.StringBuilder).AppendValues([]string{"L1_match", "L2_match", "L7_in_L_not_R", "L_nil_in_L"}, nil)
	lRec1 := lrb1.NewRecord(); defer lRec1.Release()
	ldf1 := arrowimpl.NewArrowDataFrame("ldf1_rightanti", lRec1, dfSchemaLeft);	defer ldf1.(df.Releaser).Release()

	rrb1 := array.NewRecordBuilder(mem, schemaRight); defer rrb1.Release()
	rrb1.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 6, 0, 8}, []bool{true, true, true, true, false, true})
	rrb1.Field(1).(*array.StringBuilder).AppendValues([]string{"R1_match", "R2a_match", "R2b_match", "R6_no_match", "R_nil_in_R_too", "R8_no_match"}, nil)
	rRec1 := rrb1.NewRecord(); defer rRec1.Release()
	rdf1 := arrowimpl.NewArrowDataFrame("rdf1_rightanti", rRec1, dfSchemaRight);	defer rdf1.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}
	result1 := ldf1.Join(dfSchemaRight, rdf1, df.JoinRightAnti, joinColsMap, nil)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(6), "R6_no_match"}, {int64(8), "R8_no_match"}, {nilPlaceholder, "R_nil_in_R_too"},
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, expectedData1, actualData1, "Case 1: Standard Right Anti")
	assert.True(t, result1.Schema().Equals(dfSchemaRight), "Case 1: Schema should be right's")

	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_rightanti", emptyRecL, dfSchemaLeft); defer ldfEmpty.(df.Releaser).Release()
	result2 := ldfEmpty.Join(dfSchemaRight, rdf1, df.JoinRightAnti, joinColsMap, nil); defer result2.(df.Releaser).Release()
	expectedData2 := dfToSliceOfInterfaceSlices(rdf1)
	actualData2 := dfToSliceOfInterfaceSlices(result2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, expectedData2, actualData2, "Case 2: Left DF empty")

	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_rightanti", emptyRecR, dfSchemaRight); defer rdfEmpty.(df.Releaser).Release()
	result3 := ldf1.Join(dfSchemaRight, rdfEmpty, df.JoinRightAnti, joinColsMap, nil); defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Right DF empty")

	rrbAllMatch := array.NewRecordBuilder(mem, schemaRight); defer rrbAllMatch.Release()
	rrbAllMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 0}, []bool{true, true, false})
	rrbAllMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"R_match1", "R_match2", "R_match_nil"}, nil)
	rRecAllMatch := rrbAllMatch.NewRecord(); defer rRecAllMatch.Release()
	rdfAllMatch := arrowimpl.NewArrowDataFrame("rdf_allmatch_rightanti", rRecAllMatch, dfSchemaRight);	defer rdfAllMatch.(df.Releaser).Release()
	result4 := ldf1.Join(dfSchemaRight, rdfAllMatch, df.JoinRightAnti, joinColsMap, nil);	defer result4.(df.Releaser).Release()
	assert.Equal(t, 0, result4.Len(), "Case 4: All right keys match left")
}


// --- Tests for newly implemented methods ---

func TestDataFrame_Rename(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_ForEachRow(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_UpdateSeries(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_AsFormat(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Select(t *testing.T) { /* ... existing ... */ }
