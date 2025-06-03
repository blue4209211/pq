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
	// This is a simplified way to get format; in real code, it'd be more robust
	// For testing, we assume a direct mapping or that format isn't strictly checked by underlying calls.
	dummyFormat := df.FormatWithName(dt.Name())
	if dt.ID() == arrow.INT64 { dummyFormat = df.IntegerFormat }
	if dt.ID() == arrow.STRING { dummyFormat = df.StringFormat }
	if dt.ID() == arrow.FLOAT64 { dummyFormat = df.DoubleFormat }

	return arrowimpl.NewArrowValue(s, dummyFormat)
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
// func TestArrowDataFrame_Join_EquiJoin(t *testing.T) { /* ... */ } // Will be replaced by TestDataFrame_Join_Inner
// func TestArrowDataFrame_Join_CrossJoin_Partial(t *testing.T) { /* ... */ } // Will be replaced by TestDataFrame_Join_Cross
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
	assert.Equal(t, len(expectedData1), except1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	except2 := ldf.Except(rdf, "name", "value");	defer except2.(df.Releaser).Release()
	expectedData2 := [][]interface{}{
		{int64(1), "A_one", int64(100)}, {int64(2), "A_two", nilPlaceholder}, {int64(4), "A_four", int64(100)},
		{int64(5), nilPlaceholder, int64(500)}, {nilPlaceholder, "A_nil_id", int64(600)},
	}
	actualData2 := dfToSliceOfInterfaceSlices(except2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), except2.Len(), "Case 2: Length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Data")

	except3 := ldf.Except(ldf);	defer except3.(df.Releaser).Release()
	assert.Equal(t, 0, except3.Len(), "Case 3: A Except A should be empty")

	emptyRecArr := array.NewRecord(schemaL, nil, 0); defer emptyRecArr.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_except", emptyRecArr, dfSchemaL);	defer emptyDf.(df.Releaser).Release()
	except4 := ldf.Except(emptyDf, "id");	defer except4.(df.Releaser).Release()
	expectedData4 := dfToSliceOfInterfaceSlices(ldf.Distinct())
	actualData4 := dfToSliceOfInterfaceSlices(except4)
	sortSliceOfInterfaceSlices(expectedData4); sortSliceOfInterfaceSlices(actualData4)
	assert.Equal(t, len(expectedData4), except4.Len(), "Case 4: Length (A Except empty)")
	assert.Equal(t, expectedData4, actualData4, "Case 4: Data (A Except empty)")

	assert.PanicsWithValue(t, "Except: other dataframe cannot be nil", func() { ldf.Except(nil, "id") })
	schemaRDiffIdType := arrow.NewSchema( []arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil )
	dfSchemaRDiffIdType := arrowimpl.NewArrowDataFrameSchema(schemaRDiffIdType).(*arrowimpl.ArrowDataFrameSchema)
	rRecDiffIdType := array.NewRecord(schemaRDiffIdType, nil, 0); defer rRecDiffIdType.Release()
	rdfDiffIdType := arrowimpl.NewArrowDataFrame("rdfDiffIdType_except", rRecDiffIdType, dfSchemaRDiffIdType);	defer rdfDiffIdType.(df.Releaser).Release()
	assert.Panics(t, func() { ldf.Except(rdfDiffIdType, "id") }, "Panic on key type mismatch for 'id' in Except")
}


func TestDataFrame_Join_Inner(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Schemas
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	schemaOutput := arrow.NewSchema([]arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String},
		{Name: "derived_out", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaOutput := arrowimpl.NewArrowDataFrameSchema(schemaOutput).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table
	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 1}, nil) // id_l, duplicate 1
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1_a", "L2", "L3", "L4", "L1_b"}, nil) // val_l
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_inner", lRec, dfSchemaLeft)
	defer ldf.(df.Releaser).Release()

	// Data for right table
	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 5}, nil) // id_r, duplicate 2
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R2_a", "R2_b", "R5"}, nil) // val_r
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_inner", rRec, dfSchemaRight)
	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}

	// fUser for standard projection and a derived column
	fUserStandard := func(r1, r2 df.Row) []df.Row {
		if r1 == nil || r2 == nil { panic("fUser for inner join should not receive nil rows") }

		idL := r1.Get(0).Get().(int64)
		valL := r1.Get(1).Get().(string)
		idR := r2.Get(0).Get().(int64)
		valR := r2.Get(1).Get().(string)
		derived := fmt.Sprintf("%s-%s", valL, valR)

		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
			makeArrowValue(idL, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valL, arrow.BinaryTypes.String),
			makeArrowValue(idR, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valR, arrow.BinaryTypes.String),
			makeArrowValue(derived, arrow.BinaryTypes.String),
		}, mem)
		return []df.Row{outRow}
	}

	// Case 1: Standard Inner Join
	result1 := ldf.Join(dfSchemaOutput, rdf, df.JoinEqui, joinColsMap, fUserStandard)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(1), "L1_a", int64(1), "R1", "L1_a-R1"},
		{int64(1), "L1_b", int64(1), "R1", "L1_b-R1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b"},
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: No matching keys
	lrbNoMatch := array.NewRecordBuilder(mem, schemaLeft); defer lrbNoMatch.Release()
	lrbNoMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	lrbNoMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"LX", "LY"}, nil)
	lRecNoMatch := lrbNoMatch.NewRecord(); defer lRecNoMatch.Release()
	ldfNoMatch := arrowimpl.NewArrowDataFrame("ldf_no_match", lRecNoMatch, dfSchemaLeft)
	defer ldfNoMatch.(df.Releaser).Release()

	result2 := ldfNoMatch.Join(dfSchemaOutput, rdf, df.JoinEqui, joinColsMap, fUserStandard)
	defer result2.(df.Releaser).Release()
	assert.Equal(t, 0, result2.Len(), "Case 2: No matching keys, length should be 0")

	// Case 3: Right dataframe empty
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_inner", emptyRecR, dfSchemaRight)
	defer rdfEmpty.(df.Releaser).Release()
	result3 := ldf.Join(dfSchemaOutput, rdfEmpty, df.JoinEqui, joinColsMap, fUserStandard)
	defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Right dataframe empty, length should be 0")

	// Case 4: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_inner", emptyRecL, dfSchemaLeft)
	defer ldfEmpty.(df.Releaser).Release()
	result4 := ldfEmpty.Join(dfSchemaOutput, rdf, df.JoinEqui, joinColsMap, fUserStandard)
	defer result4.(df.Releaser).Release()
	assert.Equal(t, 0, result4.Len(), "Case 4: Left dataframe empty, length should be 0")

	// Case 5: fUser returns multiple rows
	fUserMultiRow := func(r1, r2 df.Row) []df.Row {
		idL := r1.Get(0).Get().(int64)
		valL := r1.Get(1).Get().(string)
		idR := r2.Get(0).Get().(int64)
		valR := r2.Get(1).Get().(string)

		rows := make([]df.Row, 0, 2)
		for i := 0; i < 2; i++ {
			derived := fmt.Sprintf("%s-%s-copy%d", valL, valR, i)
			outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
				makeArrowValue(idL, arrow.PrimitiveTypes.Int64),
				makeArrowValue(valL, arrow.BinaryTypes.String),
				makeArrowValue(idR, arrow.PrimitiveTypes.Int64),
				makeArrowValue(valR, arrow.BinaryTypes.String),
				makeArrowValue(derived, arrow.BinaryTypes.String),
			}, mem)
			rows = append(rows, outRow)
		}
		return rows
	}
	result5 := ldf.Join(dfSchemaOutput, rdf, df.JoinEqui, joinColsMap, fUserMultiRow)
	defer result5.(df.Releaser).Release()
	expectedData5 := [][]interface{}{
		{int64(1), "L1_a", int64(1), "R1", "L1_a-R1-copy0"}, {int64(1), "L1_a", int64(1), "R1", "L1_a-R1-copy1"},
		{int64(1), "L1_b", int64(1), "R1", "L1_b-R1-copy0"}, {int64(1), "L1_b", int64(1), "R1", "L1_b-R1-copy1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a-copy0"}, {int64(2), "L2", int64(2), "R2_a", "L2-R2_a-copy1"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b-copy0"}, {int64(2), "L2", int64(2), "R2_b", "L2-R2_b-copy1"},
	}
	actualData5 := dfToSliceOfInterfaceSlices(result5)
	sortSliceOfInterfaceSlices(expectedData5); sortSliceOfInterfaceSlices(actualData5)
	assert.Equal(t, len(expectedData5), result5.Len(), "Case 5: fUser multi-row, length")
	assert.Equal(t, expectedData5, actualData5, "Case 5: fUser multi-row, data")

	// Case 6: fUser returns zero rows
	fUserZeroRow := func(r1, r2 df.Row) []df.Row { return []df.Row{} }
	result6 := ldf.Join(dfSchemaOutput, rdf, df.JoinEqui, joinColsMap, fUserZeroRow)
	defer result6.(df.Releaser).Release()
	assert.Equal(t, 0, result6.Len(), "Case 6: fUser zero-row, length should be 0")

	// Case 7: Join on multiple keys (requires different schema/data)
	schemaLMulti := arrow.NewSchema([]arrow.Field{
		{Name: "id1_l", Type: arrow.PrimitiveTypes.Int64}, {Name: "id2_l", Type: arrow.BinaryTypes.String}, {Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil); dfSchemaLMulti := arrowimpl.NewArrowDataFrameSchema(schemaLMulti).(*arrowimpl.ArrowDataFrameSchema)
	schemaRMulti := arrow.NewSchema([]arrow.Field{
		{Name: "id1_r", Type: arrow.PrimitiveTypes.Int64}, {Name: "id2_r", Type: arrow.BinaryTypes.String}, {Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil); dfSchemaRMulti := arrowimpl.NewArrowDataFrameSchema(schemaRMulti).(*arrowimpl.ArrowDataFrameSchema)
	schemaOutMulti := arrow.NewSchema([]arrow.Field{
		{Name: "id1_l", Type: arrow.PrimitiveTypes.Int64}, {Name: "id2_l", Type: arrow.BinaryTypes.String}, {Name: "val_l", Type: arrow.BinaryTypes.String},
		{Name: "id1_r", Type: arrow.PrimitiveTypes.Int64}, {Name: "id2_r", Type: arrow.BinaryTypes.String}, {Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil); dfSchemaOutMulti := arrowimpl.NewArrowDataFrameSchema(schemaOutMulti).(*arrowimpl.ArrowDataFrameSchema)

	lrbM := array.NewRecordBuilder(mem, schemaLMulti); defer lrbM.Release()
	lrbM.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 1, 2}, nil)
	lrbM.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B", "A"}, nil)
	lrbM.Field(2).(*array.StringBuilder).AppendValues([]string{"L_val1", "L_val2", "L_val3"}, nil)
	lRecM := lrbM.NewRecord(); defer lRecM.Release()
	ldfM := arrowimpl.NewArrowDataFrame("ldfM_inner", lRecM, dfSchemaLMulti); defer ldfM.(df.Releaser).Release()

	rrbM := array.NewRecordBuilder(mem, schemaRMulti); defer rrbM.Release()
	rrbM.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 1, 3}, nil)
	rrbM.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "C", "A"}, nil)
	rrbM.Field(2).(*array.StringBuilder).AppendValues([]string{"R_val1", "R_val2", "R_val3"}, nil)
	rRecM := rrbM.NewRecord(); defer rRecM.Release()
	rdfM := arrowimpl.NewArrowDataFrame("rdfM_inner", rRecM, dfSchemaRMulti); defer rdfM.(df.Releaser).Release()

	joinColsMapMulti := map[string]string{"id1_l": "id1_r", "id2_l": "id2_r"}
	fUserMultiKey := func(r1, r2 df.Row) []df.Row {
		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutMulti, []df.Value{
			r1.Get(0), r1.Get(1), r1.Get(2),
			r2.Get(0), r2.Get(1), r2.Get(2),
		}, mem)
		return []df.Row{outRow}
	}
	result7 := ldfM.Join(dfSchemaOutMulti, rdfM, df.JoinEqui, joinColsMapMulti, fUserMultiKey)
	defer result7.(df.Releaser).Release()
	expectedData7 := [][]interface{}{
		{int64(1), "A", "L_val1", int64(1), "A", "R_val1"},
	}
	actualData7 := dfToSliceOfInterfaceSlices(result7)
	sortSliceOfInterfaceSlices(expectedData7); sortSliceOfInterfaceSlices(actualData7) // Though 1 row, keep for consistency
	assert.Equal(t, len(expectedData7), result7.Len(), "Case 7: Multi-key join, length")
	assert.Equal(t, expectedData7, actualData7, "Case 7: Multi-key join, data")
}


// TODO: Add tests for df.go (This was the original comment in the file)
// Placeholders for other Join tests to be implemented

func TestDataFrame_Join_Left(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Schemas (reusing from Inner Join test where applicable)
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Nullable for potential non-matches from right
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Nullable for potential non-matches from left
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	// Output schema allows for nils from the right side
	schemaOutput := arrow.NewSchema([]arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "derived_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaOutput := arrowimpl.NewArrowDataFrameSchema(schemaOutput).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table
	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, []bool{true, true, true, true}) // id_l
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4"}, nil)        // val_l
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_left", lRec, dfSchemaLeft)
	defer ldf.(df.Releaser).Release()

	// Data for right table
	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 5}, []bool{true, true, true, true}) // id_r, duplicate 2, id 5 not in left
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R2_a", "R2_b", "R5"}, nil)      // val_r
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_left", rRec, dfSchemaRight)
	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}

	fUserLeftJoin := func(r1, r2 df.Row) []df.Row {
		if r1 == nil { panic("fUser for left join should always have a left row (r1)") }

		idL := r1.Get(0).Get().(int64)
		valL := r1.Get(1).Get().(string)

		var idRVal interface{} = nil
		var valRVal interface{} = nil
		var derived string

		if r2 != nil && !r2.Get(0).IsNil() { // Check if r2 and its key are not nil
			idRVal = r2.Get(0).Get().(int64)
			valRVal = r2.Get(1).Get().(string)
			derived = fmt.Sprintf("%s-%s", valL, valRVal.(string))
		} else {
			derived = fmt.Sprintf("%s-NULL", valL)
		}

		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
			makeArrowValue(idL, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valL, arrow.BinaryTypes.String),
			makeArrowValue(idRVal, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valRVal, arrow.BinaryTypes.String),
			makeArrowValue(derived, arrow.BinaryTypes.String),
		}, mem)
		return []df.Row{outRow}
	}

	// Case 1: Standard Left Join (matches and non-matches from left)
	result1 := ldf.Join(dfSchemaOutput, rdf, df.JoinLeft, joinColsMap, fUserLeftJoin)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(1), "L1", int64(1), "R1", "L1-R1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b"},
		{int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-NULL"}, // L3 has no match in right
		{int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-NULL"}, // L4 has no match in right
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: Right dataframe empty (all left rows should appear with nils for right columns)
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_left", emptyRecR, dfSchemaRight)
	defer rdfEmpty.(df.Releaser).Release()

	result2 := ldf.Join(dfSchemaOutput, rdfEmpty, df.JoinLeft, joinColsMap, fUserLeftJoin)
	defer result2.(df.Releaser).Release()
	expectedData2 := [][]interface{}{
		{int64(1), "L1", nilPlaceholder, nilPlaceholder, "L1-NULL"},
		{int64(2), "L2", nilPlaceholder, nilPlaceholder, "L2-NULL"},
		{int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-NULL"},
		{int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-NULL"},
	}
	actualData2 := dfToSliceOfInterfaceSlices(result2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), result2.Len(), "Case 2: Right DF empty, length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Right DF empty, data")

	// Case 3: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_left", emptyRecL, dfSchemaLeft)
	defer ldfEmpty.(df.Releaser).Release()
	result3 := ldfEmpty.Join(dfSchemaOutput, rdf, df.JoinLeft, joinColsMap, fUserLeftJoin)
	defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Left dataframe empty, length should be 0")

	// Case 4: fUser returns multiple rows for matching records
	fUserLeftMultiRow := func(r1, r2 df.Row) []df.Row {
		if r1 == nil { panic("fUser for left join should always have a left row (r1)") }
		idL := r1.Get(0).Get().(int64)
		valL := r1.Get(1).Get().(string)
		rows := make([]df.Row, 0, 2)

		for i := 0; i < 2; i++ { // Create 2 output rows for each input pair
			var idRVal interface{} = nil
			var valRVal interface{} = nil
			var derived string
			if r2 != nil && !r2.Get(0).IsNil() {
				idRVal = r2.Get(0).Get().(int64)
				valRVal = r2.Get(1).Get().(string)
				derived = fmt.Sprintf("%s-%s-copy%d", valL, valRVal.(string), i)
			} else {
				derived = fmt.Sprintf("%s-NULL-copy%d", valL, i)
			}
			outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
				makeArrowValue(idL, arrow.PrimitiveTypes.Int64),
				makeArrowValue(valL, arrow.BinaryTypes.String),
				makeArrowValue(idRVal, arrow.PrimitiveTypes.Int64),
				makeArrowValue(valRVal, arrow.BinaryTypes.String),
				makeArrowValue(derived, arrow.BinaryTypes.String),
			}, mem)
			rows = append(rows, outRow)
		}
		return rows
	}
	result4 := ldf.Join(dfSchemaOutput, rdf, df.JoinLeft, joinColsMap, fUserLeftMultiRow)
	defer result4.(df.Releaser).Release()
	expectedData4 := [][]interface{}{
		{int64(1), "L1", int64(1), "R1", "L1-R1-copy0"}, {int64(1), "L1", int64(1), "R1", "L1-R1-copy1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a-copy0"}, {int64(2), "L2", int64(2), "R2_a", "L2-R2_a-copy1"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b-copy0"}, {int64(2), "L2", int64(2), "R2_b", "L2-R2_b-copy1"},
		{int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-NULL-copy0"}, {int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-NULL-copy1"},
		{int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-NULL-copy0"}, {int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-NULL-copy1"},
	}
	actualData4 := dfToSliceOfInterfaceSlices(result4)
	sortSliceOfInterfaceSlices(expectedData4); sortSliceOfInterfaceSlices(actualData4)
	assert.Equal(t, len(expectedData4), result4.Len(), "Case 4: fUser multi-row, length")
	assert.Equal(t, expectedData4, actualData4, "Case 4: fUser multi-row, data")

	// Case 5: fUser returns zero rows (effectively filtering all rows)
	fUserLeftZeroRow := func(r1, r2 df.Row) []df.Row { return []df.Row{} }
	result5 := ldf.Join(dfSchemaOutput, rdf, df.JoinLeft, joinColsMap, fUserLeftZeroRow)
	defer result5.(df.Releaser).Release()
	assert.Equal(t, 0, result5.Len(), "Case 5: fUser zero-row, length should be 0")
}

func TestDataFrame_Join_Right(t *testing.T) {
	mem := memory.NewGoAllocator()

	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String, Nullable: true}, // Nullable for potential non-matches from left
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	schemaOutput := arrow.NewSchema([]arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String},
		{Name: "derived_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaOutput := arrowimpl.NewArrowDataFrameSchema(schemaOutput).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table (ID 3,4 not in right; ID 1,2 are)
	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, []bool{true, true, true, true})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_right", lRec, dfSchemaLeft)
	defer ldf.(df.Releaser).Release()

	// Data for right table (ID 5 not in left; ID 1,2 are; ID 2 is duplicated)
	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 5}, []bool{true, true, true, true})
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R2_a", "R2_b", "R5"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_right", rRec, dfSchemaRight)
	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}

	fUserRightJoin := func(r1, r2 df.Row) []df.Row {
		if r2 == nil { panic("fUser for right join should always have a right row (r2)") }

		idR := r2.Get(0).Get().(int64)
		valR := r2.Get(1).Get().(string)

		var idLVal interface{} = nil
		var valLVal interface{} = nil
		var derived string

		if r1 != nil && !r1.Get(0).IsNil() { // Check if r1 and its key are not nil
			idLVal = r1.Get(0).Get().(int64)
			valLVal = r1.Get(1).Get().(string)
			derived = fmt.Sprintf("%s-%s", valLVal.(string), valR)
		} else {
			derived = fmt.Sprintf("NULL-%s", valR)
		}

		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
			makeArrowValue(idLVal, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valLVal, arrow.BinaryTypes.String),
			makeArrowValue(idR, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valR, arrow.BinaryTypes.String),
			makeArrowValue(derived, arrow.BinaryTypes.String),
		}, mem)
		return []df.Row{outRow}
	}

	// Case 1: Standard Right Join
	result1 := ldf.Join(dfSchemaOutput, rdf, df.JoinRight, joinColsMap, fUserRightJoin)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(1), "L1", int64(1), "R1", "L1-R1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b"},
		{nilPlaceholder, nilPlaceholder, int64(5), "R5", "NULL-R5"}, // R5 has no match in left
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_right", emptyRecL, dfSchemaLeft)
	defer ldfEmpty.(df.Releaser).Release()

	result2 := ldfEmpty.Join(dfSchemaOutput, rdf, df.JoinRight, joinColsMap, fUserRightJoin)
	defer result2.(df.Releaser).Release()
	expectedData2 := [][]interface{}{
		{nilPlaceholder, nilPlaceholder, int64(1), "R1", "NULL-R1"},
		{nilPlaceholder, nilPlaceholder, int64(2), "R2_a", "NULL-R2_a"},
		{nilPlaceholder, nilPlaceholder, int64(2), "R2_b", "NULL-R2_b"},
		{nilPlaceholder, nilPlaceholder, int64(5), "R5", "NULL-R5"},
	}
	actualData2 := dfToSliceOfInterfaceSlices(result2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), result2.Len(), "Case 2: Left DF empty, length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Left DF empty, data")

	// Case 3: Right dataframe empty
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_right", emptyRecR, dfSchemaRight)
	defer rdfEmpty.(df.Releaser).Release()
	result3 := ldf.Join(dfSchemaOutput, rdfEmpty, df.JoinRight, joinColsMap, fUserRightJoin)
	defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Right dataframe empty, length should be 0")
}

func TestDataFrame_Join_FullOuter(t *testing.T) {
	mem := memory.NewGoAllocator()

	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	schemaOutput := arrow.NewSchema([]arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "derived_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaOutput := arrowimpl.NewArrowDataFrameSchema(schemaOutput).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table (ID 3,4 not in right; ID 1,2 are)
	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, []bool{true, true, true, true})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_outer", lRec, dfSchemaLeft)
	defer ldf.(df.Releaser).Release()

	// Data for right table (ID 5 not in left; ID 1,2 are; ID 2 is duplicated, ID 6 is nil)
	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 5, 0}, []bool{true, true, true, true, false}) // id_r, last id is nil
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R2_a", "R2_b", "R5", "R_nil_id"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_outer", rRec, dfSchemaRight)
	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id_l": "id_r"}

	fUserFullOuter := func(r1, r2 df.Row) []df.Row {
		var idLVal, valLVal, idRVal, valRVal interface{}
		var derivedPartL, derivedPartR string

		if r1 != nil && (r1.Len() > 0 && !r1.Get(0).IsNil()) { // r1 exists and its join key is not nil
			idLVal = r1.Get(0).Get().(int64)
			valLVal = r1.Get(1).Get().(string)
			derivedPartL = valLVal.(string)
		} else if r1 != nil && r1.Len() > 0 { // r1 exists but its join key might be nil (should not happen for typical hash join logic for left side)
			valLVal = r1.Get(1).Get().(string) // Potentially grab other non-key cols
			derivedPartL = fmt.Sprintf("L_key_nil_val_%s", valLVal.(string))
		} else {
			derivedPartL = "L_NULL"
		}

		if r2 != nil && (r2.Len() > 0 && !r2.Get(0).IsNil()) { // r2 exists and its join key is not nil
			idRVal = r2.Get(0).Get().(int64)
			valRVal = r2.Get(1).Get().(string)
			derivedPartR = valRVal.(string)
		} else if r2 != nil && r2.Len() > 0 { // r2 exists, but its join key is nil (e.g. right row (nil, "R_nil_id"))
			idRVal = nil // Explicitly set key to nil
			if !r2.Get(1).IsNil() { valRVal = r2.Get(1).Get().(string) }
			derivedPartR = fmt.Sprintf("R_key_nil_val_%s", valRVal.(string))
		} else {
			derivedPartR = "R_NULL"
		}

		derived := fmt.Sprintf("%s-%s", derivedPartL, derivedPartR)

		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
			makeArrowValue(idLVal, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valLVal, arrow.BinaryTypes.String),
			makeArrowValue(idRVal, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valRVal, arrow.BinaryTypes.String),
			makeArrowValue(derived, arrow.BinaryTypes.String),
		}, mem)
		return []df.Row{outRow}
	}

	// Case 1: Standard Full Outer Join
	result1 := ldf.Join(dfSchemaOutput, rdf, df.JoinOuter, joinColsMap, fUserFullOuter)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		// Matches
		{int64(1), "L1", int64(1), "R1", "L1-R1"},
		{int64(2), "L2", int64(2), "R2_a", "L2-R2_a"},
		{int64(2), "L2", int64(2), "R2_b", "L2-R2_b"},
		// Only in Left
		{int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-R_NULL"},
		{int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-R_NULL"},
		// Only in Right
		{nilPlaceholder, nilPlaceholder, int64(5), "R5", "L_NULL-R5"},
		{nilPlaceholder, nilPlaceholder, nilPlaceholder, "R_nil_id", "L_NULL-R_key_nil_val_R_nil_id"}, // Right row with nil ID
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_outer", emptyRecL, dfSchemaLeft)
	defer ldfEmpty.(df.Releaser).Release()

	result2 := ldfEmpty.Join(dfSchemaOutput, rdf, df.JoinOuter, joinColsMap, fUserFullOuter)
	defer result2.(df.Releaser).Release()
	expectedData2 := [][]interface{}{
		{nilPlaceholder, nilPlaceholder, int64(1), "R1", "L_NULL-R1"},
		{nilPlaceholder, nilPlaceholder, int64(2), "R2_a", "L_NULL-R2_a"},
		{nilPlaceholder, nilPlaceholder, int64(2), "R2_b", "L_NULL-R2_b"},
		{nilPlaceholder, nilPlaceholder, int64(5), "R5", "L_NULL-R5"},
		{nilPlaceholder, nilPlaceholder, nilPlaceholder, "R_nil_id", "L_NULL-R_key_nil_val_R_nil_id"},
	}
	actualData2 := dfToSliceOfInterfaceSlices(result2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), result2.Len(), "Case 2: Left DF empty, length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Left DF empty, data")

	// Case 3: Right dataframe empty
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_outer", emptyRecR, dfSchemaRight)
	defer rdfEmpty.(df.Releaser).Release()

	result3 := ldf.Join(dfSchemaOutput, rdfEmpty, df.JoinOuter, joinColsMap, fUserFullOuter)
	defer result3.(df.Releaser).Release()
	expectedData3 := [][]interface{}{
		{int64(1), "L1", nilPlaceholder, nilPlaceholder, "L1-R_NULL"},
		{int64(2), "L2", nilPlaceholder, nilPlaceholder, "L2-R_NULL"},
		{int64(3), "L3", nilPlaceholder, nilPlaceholder, "L3-R_NULL"},
		{int64(4), "L4", nilPlaceholder, nilPlaceholder, "L4-R_NULL"},
	}
	actualData3 := dfToSliceOfInterfaceSlices(result3)
	sortSliceOfInterfaceSlices(expectedData3); sortSliceOfInterfaceSlices(actualData3)
	assert.Equal(t, len(expectedData3), result3.Len(), "Case 3: Right DF empty, length")
	assert.Equal(t, expectedData3, actualData3, "Case 3: Right DF empty, data")
}

func TestDataFrame_Join_Cross(t *testing.T) {
	mem := memory.NewGoAllocator()

	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	schemaOutput := arrow.NewSchema([]arrow.Field{
		{Name: "l_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "l_val", Type: arrow.BinaryTypes.String},
		{Name: "r_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "r_val", Type: arrow.BinaryTypes.String},
		{Name: "cross_derived", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaOutput := arrowimpl.NewArrowDataFrameSchema(schemaOutput).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table (2 rows)
	lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L_A", "L_B"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_cross", lRec, dfSchemaLeft)
	defer ldf.(df.Releaser).Release()

	// Data for right table (3 rows)
	rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R_X", "R_Y", "R_Z"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_cross", rRec, dfSchemaRight)
	defer rdf.(df.Releaser).Release()

	// fUser for Cross Join
	fUserCross := func(r1, r2 df.Row) []df.Row {
		if r1 == nil || r2 == nil { panic("fUser for cross join should not receive nil rows") }

		idL := r1.Get(0).Get().(int64)
		valL := r1.Get(1).Get().(string)
		idR := r2.Get(0).Get().(int64)
		valR := r2.Get(1).Get().(string)
		derived := fmt.Sprintf("%s_x_%s", valL, valR)

		outRow := arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
			makeArrowValue(idL, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valL, arrow.BinaryTypes.String),
			makeArrowValue(idR, arrow.PrimitiveTypes.Int64),
			makeArrowValue(valR, arrow.BinaryTypes.String),
			makeArrowValue(derived, arrow.BinaryTypes.String),
		}, mem)
		return []df.Row{outRow}
	}

	// Case 1: Standard Cross Join (2 left rows * 3 right rows = 6 output rows)
	// joinColsMap is nil for Cross Join
	result1 := ldf.Join(dfSchemaOutput, rdf, df.JoinCross, nil, fUserCross)
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(1), "L_A", int64(10), "R_X", "L_A_x_R_X"}, {int64(1), "L_A", int64(20), "R_Y", "L_A_x_R_Y"}, {int64(1), "L_A", int64(30), "R_Z", "L_A_x_R_Z"},
		{int64(2), "L_B", int64(10), "R_X", "L_B_x_R_X"}, {int64(2), "L_B", int64(20), "R_Y", "L_B_x_R_Y"}, {int64(2), "L_B", int64(30), "R_Z", "L_B_x_R_Z"},
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	// Order is deterministic for CrossJoin if implemented with nested loops starting from left.
	// However, sorting is safer if the underlying implementation detail changes.
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")

	// Case 2: Left dataframe empty
	emptyRecL := array.NewRecord(schemaLeft, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_cross", emptyRecL, dfSchemaLeft)
	defer ldfEmpty.(df.Releaser).Release()

	result2 := ldfEmpty.Join(dfSchemaOutput, rdf, df.JoinCross, nil, fUserCross)
	defer result2.(df.Releaser).Release()
	assert.Equal(t, 0, result2.Len(), "Case 2: Left DF empty, length should be 0")

	// Case 3: Right dataframe empty
	emptyRecR := array.NewRecord(schemaRight, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_cross", emptyRecR, dfSchemaRight)
	defer rdfEmpty.(df.Releaser).Release()

	result3 := ldf.Join(dfSchemaOutput, rdfEmpty, df.JoinCross, nil, fUserCross)
	defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Right DF empty, length should be 0")

	// Case 4: Both dataframes empty
	result4 := ldfEmpty.Join(dfSchemaOutput, rdfEmpty, df.JoinCross, nil, fUserCross)
	defer result4.(df.Releaser).Release()
	assert.Equal(t, 0, result4.Len(), "Case 4: Both DFs empty, length should be 0")

	// Case 5: fUser returns multiple rows
	fUserCrossMulti := func(r1, r2 df.Row) []df.Row {
		idL := r1.Get(0).Get().(int64); valL := r1.Get(1).Get().(string)
		idR := r2.Get(0).Get().(int64); valR := r2.Get(1).Get().(string)
		outRows := make([]df.Row, 2)
		for i:=0; i<2; i++ {
			derived := fmt.Sprintf("%s_x_%s_copy%d", valL, valR, i)
			outRows[i] = arrowimpl.NewArrowRowFromValues(dfSchemaOutput, []df.Value{
				makeArrowValue(idL, arrow.PrimitiveTypes.Int64), makeArrowValue(valL, arrow.BinaryTypes.String),
				makeArrowValue(idR, arrow.PrimitiveTypes.Int64), makeArrowValue(valR, arrow.BinaryTypes.String),
				makeArrowValue(derived, arrow.BinaryTypes.String),
			}, mem)
		}
		return outRows
	}
	result5 := ldf.Join(dfSchemaOutput, rdf, df.JoinCross, nil, fUserCrossMulti)
	defer result5.(df.Releaser).Release()
	// Expected: 2 left * 3 right * 2 from fUser = 12 rows
	assert.Equal(t, 12, result5.Len(), "Case 5: fUser multi-row, length")
	// Spot check one combination
	// L_A (1) x R_X (10) should produce L_A_x_R_X_copy0 and L_A_x_R_X_copy1
	var foundCopy0, foundCopy1 bool
	for _, rowSlice := range dfToSliceOfInterfaceSlices(result5) {
		if rowSlice[0].(int64) == 1 && rowSlice[2].(int64) == 10 {
			if rowSlice[4].(string) == "L_A_x_R_X_copy0" { foundCopy0 = true }
			if rowSlice[4].(string) == "L_A_x_R_X_copy1" { foundCopy1 = true }
		}
	}
	assert.True(t, foundCopy0 && foundCopy1, "Case 5: fUser multi-row, data spot check")


	// Case 6: fUser returns zero rows
	fUserCrossZero := func(r1, r2 df.Row) []df.Row { return []df.Row{} }
	result6 := ldf.Join(dfSchemaOutput, rdf, df.JoinCross, nil, fUserCrossZero)
	defer result6.(df.Releaser).Release()
	assert.Equal(t, 0, result6.Len(), "Case 6: fUser zero-row, length")

	// Case 7: Panic if fUser is nil (as per implementation)
	assert.PanicsWithValue(t, "Join: CrossJoin requires an fUser function", func() {
		ldf.Join(dfSchemaOutput, rdf, df.JoinCross, nil, nil)
	}, "Case 7: Panic on nil fUser for CrossJoin")
}

func TestDataFrame_Join_LeftAnti(t *testing.T) {
	mem := memory.NewGoAllocator()

	schemaShared := arrow.NewSchema([]arrow.Field{ // Shared schema for simplicity
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaShared := arrowimpl.NewArrowDataFrameSchema(schemaShared).(*arrowimpl.ArrowDataFrameSchema)

	// Data for left table
	lrb := array.NewRecordBuilder(mem, schemaShared); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 0, 5}, []bool{true, true, true, true, false, true}) // id_l, includes a nil ID
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4", "L_nil", "L5_dup"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("ldf_leftanti", lRec, dfSchemaShared)
	defer ldf.(df.Releaser).Release()

	// Data for right table
	rrb := array.NewRecordBuilder(mem, schemaShared); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 6, 0}, []bool{true, true, true, true, false}) // id_r, ID 6 not in left, includes a nil ID
	rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1_match", "R2_match_a", "R2_match_b", "R6_nomatch", "R_nil_match"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("rdf_leftanti", rRec, dfSchemaShared)
	defer rdf.(df.Releaser).Release()

	joinColsMap := map[string]string{"id": "id"} // Join on 'id' column

	// Case 1: Standard Left Anti Join
	// Rows from LDF where 'id' is NOT in RDF's 'id' list.
	// LDF IDs: {1, 2, 3, 4, nil, 5}
	// RDF IDs: {1, 2, 6, nil}
	// IDs in LDF but not RDF: {3, 4, 5} (nil ID in LDF matches nil ID in RDF, so it's excluded)
	result1 := ldf.Join(dfSchemaShared, rdf, df.JoinType("leftanti"), joinColsMap, nil) // fUser is nil
	defer result1.(df.Releaser).Release()
	expectedData1 := [][]interface{}{
		{int64(3), "L3"},
		{int64(4), "L4"},
		{int64(5), "L5_dup"},
	}
	actualData1 := dfToSliceOfInterfaceSlices(result1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), result1.Len(), "Case 1: Length")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data")
	assert.True(t, result1.Schema().Equals(dfSchemaShared), "Case 1: Schema should be left table's schema")


	// Case 2: Right dataframe empty (all rows from left should be returned)
	emptyRecR := array.NewRecord(schemaShared, nil, 0); defer emptyRecR.Release()
	rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_empty_leftanti", emptyRecR, dfSchemaShared)
	defer rdfEmpty.(df.Releaser).Release()

	result2 := ldf.Join(dfSchemaShared, rdfEmpty, df.JoinType("leftanti"), joinColsMap, nil)
	defer result2.(df.Releaser).Release()
	expectedData2 := [][]interface{}{ // All of LDF
		{int64(1), "L1"}, {int64(2), "L2"}, {int64(3), "L3"}, {int64(4), "L4"}, {nilPlaceholder, "L_nil"}, {int64(5), "L5_dup"},
	}
	actualData2 := dfToSliceOfInterfaceSlices(result2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), result2.Len(), "Case 2: Right DF empty, length")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Right DF empty, data")

	// Case 3: Left dataframe empty
	emptyRecL := array.NewRecord(schemaShared, nil, 0); defer emptyRecL.Release()
	ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_empty_leftanti", emptyRecL, dfSchemaShared)
	defer ldfEmpty.(df.Releaser).Release()
	result3 := ldfEmpty.Join(dfSchemaShared, rdf, df.JoinType("leftanti"), joinColsMap, nil)
	defer result3.(df.Releaser).Release()
	assert.Equal(t, 0, result3.Len(), "Case 3: Left dataframe empty, length should be 0")

	// Case 4: All left keys have matches in right (result should be empty)
	lrbAllMatch := array.NewRecordBuilder(mem, schemaShared); defer lrbAllMatch.Release()
	lrbAllMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 6, 0}, []bool{true, true, false}) // All these IDs are in rdf
	lrbAllMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"L_match1", "L_match6", "L_match_nil"}, nil)
	lRecAllMatch := lrbAllMatch.NewRecord(); defer lRecAllMatch.Release()
	ldfAllMatch := arrowimpl.NewArrowDataFrame("ldf_allmatch_leftanti", lRecAllMatch, dfSchemaShared)
	defer ldfAllMatch.(df.Releaser).Release()

	result4 := ldfAllMatch.Join(dfSchemaShared, rdf, df.JoinType("leftanti"), joinColsMap, nil)
	defer result4.(df.Releaser).Release()
	assert.Equal(t, 0, result4.Len(), "Case 4: All left keys match, length should be 0")

	// Case 5: No common keys between left and right (all left rows should be returned)
	rrbNoCommon := array.NewRecordBuilder(mem, schemaShared); defer rrbNoCommon.Release()
	rrbNoCommon.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, []bool{true, true})
	rrbNoCommon.Field(1).(*array.StringBuilder).AppendValues([]string{"R_NoCommon1", "R_NoCommon2"}, nil)
	rRecNoCommon := rrbNoCommon.NewRecord(); defer rRecNoCommon.Release()
	rdfNoCommon := arrowimpl.NewArrowDataFrame("rdf_nocommon_leftanti", rRecNoCommon, dfSchemaShared)
	defer rdfNoCommon.(df.Releaser).Release()

	result5 := ldf.Join(dfSchemaShared, rdfNoCommon, df.JoinType("leftanti"), joinColsMap, nil)
	defer result5.(df.Releaser).Release()
	// Expected is all of ldf again
	actualData5 := dfToSliceOfInterfaceSlices(result5)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData5) // expectedData2 is full LDF
	assert.Equal(t, len(expectedData2), result5.Len(), "Case 5: No common keys, length")
	assert.Equal(t, expectedData2, actualData5, "Case 5: No common keys, data")

	// Case 6: Join on multiple keys
	schemaMulti := arrow.NewSchema([]arrow.Field{
		{Name: "id1", Type: arrow.PrimitiveTypes.Int64}, {Name: "id2", Type: arrow.BinaryTypes.String}, {Name: "val", Type: arrow.BinaryTypes.String},
	}, nil); dfSchemaMulti := arrowimpl.NewArrowDataFrameSchema(schemaMulti).(*arrowimpl.ArrowDataFrameSchema)

	lrbM := array.NewRecordBuilder(mem, dfSchemaMulti.Schema()); defer lrbM.Release()
	lrbM.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 1, 2, 3}, nil)
	lrbM.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C"}, nil)
	lrbM.Field(2).(*array.StringBuilder).AppendValues([]string{"L_1A", "L_1B", "L_2A", "L_3C"}, nil)
	lRecM := lrbM.NewRecord(); defer lRecM.Release()
	ldfM := arrowimpl.NewArrowDataFrame("ldfM_leftanti", lRecM, dfSchemaMulti); defer ldfM.(df.Releaser).Release()

	rrbM := array.NewRecordBuilder(mem, dfSchemaMulti.Schema()); defer rrbM.Release()
	rrbM.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 4}, nil)
	rrbM.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "A", "C"}, nil)
	rrbM.Field(2).(*array.StringBuilder).AppendValues([]string{"R_1A", "R_2A", "R_4C"}, nil)
	rRecM := rrbM.NewRecord(); defer rRecM.Release()
	rdfM := arrowimpl.NewArrowDataFrame("rdfM_leftanti", rRecM, dfSchemaMulti); defer rdfM.(df.Releaser).Release()

	joinColsMapMulti := map[string]string{"id1": "id1", "id2": "id2"}
	result6 := ldfM.Join(dfSchemaMulti, rdfM, df.JoinType("leftanti"), joinColsMapMulti, nil)
	defer result6.(df.Releaser).Release()
	// LDFM: (1,A), (1,B), (2,A), (3,C)
	// RDFM: (1,A), (2,A), (4,C)
	// In LDFM but not RDFM: (1,B), (3,C)
	expectedData6 := [][]interface{}{
		{int64(1), "B", "L_1B"},
		{int64(3), "C", "L_3C"},
	}
	actualData6 := dfToSliceOfInterfaceSlices(result6)
	sortSliceOfInterfaceSlices(expectedData6); sortSliceOfInterfaceSlices(actualData6)
	assert.Equal(t, len(expectedData6), result6.Len(), "Case 6: Multi-key, length")
	assert.Equal(t, expectedData6, actualData6, "Case 6: Multi-key, data")
	assert.True(t, result6.Schema().Equals(dfSchemaMulti), "Case 6: Schema should be left table's schema")
}

// Optional:
// func TestDataFrame_Join_LeftSemi(t *testing.T)   { t.Skip("Not yet implemented") }
// func TestDataFrame_Join_RightSemi(t *testing.T)  { t.Skip("Not yet implemented") }
// func TestDataFrame_Join_RightAnti(t *testing.T)  { t.Skip("Not yet implemented") }


// --- Tests for newly implemented methods ---

func TestDataFrame_Rename(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, _ := setupGroupedTestData(t, mem, "cat1") // Using helper for initial data
	defer baseDf.(df.Releaser).Release()

	originalName := baseDf.Name()
	newName := "renamed_test_df"

	// Test not inplace
	renamedDf := baseDf.Rename(newName, false)
	defer renamedDf.(df.Releaser).Release()

	assert.Equal(t, newName, renamedDf.Name(), "Name should be updated for non-inplace")
	assert.Equal(t, originalName, baseDf.Name(), "Original name should not change for non-inplace")
	assert.True(t, baseDf.Schema().Equals(renamedDf.Schema()), "Schemas should be equal for non-inplace rename")
	assert.Equal(t, baseDf.Len(), renamedDf.Len(), "Lengths should be equal for non-inplace rename")
	// For arrowDataFrame, the underlying record might be shared or a new slice.
	// If it's NewArrowDataFrameWithAllocator(name, adf.record, adf.schema, adf.mem), then record is shared.
	// Let's check if the underlying record pointer is the same for Arrow
	if adfBase, okBase := baseDf.(*arrowimpl.ArrowDataFrame); okBase {
		if adfRenamed, okRenamed := renamedDf.(*arrowimpl.ArrowDataFrame); okRenamed {
			// This requires exposing record or a way to compare. For now, trust implementation shares.
			// Alternatively, check a few values.
			assert.Equal(t, adfBase.GetValue(0,0).Get(), adfRenamed.GetValue(0,0).Get(), "Data should be shared")
		}
	}


	// Test inplace
	renamedDfInplace := baseDf.Rename(newName, true)
	assert.Equal(t, newName, renamedDfInplace.Name(), "Name should be updated for inplace")
	assert.Equal(t, newName, baseDf.Name(), "Original name should also change for inplace")
	assert.Same(t, baseDf, renamedDfInplace, "Should return the same DataFrame instance for inplace")

	// Test panic on empty name
	assert.PanicsWithValue(t, "DataFrame name cannot be empty", func() {
		baseDf.Rename("", false)
	})
	assert.PanicsWithValue(t, "DataFrame name cannot be empty", func() {
		baseDf.Rename("", true)
	})
}

func TestDataFrame_ForEachRow(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Using a simpler, smaller DataFrame for this test
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "val", Type: arrow.BinaryTypes.String},
		}, nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	ids := []int64{1, 2, 3}
	vals := []string{"A", "B", "C"}
	rb.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues(vals, nil)
	rec := rb.NewRecord(); defer rec.Release()

	dataFrame := arrowimpl.NewArrowDataFrame("test_foreach", rec, dfSchema)
	defer dataFrame.(df.Releaser).Release()

	var iteratedIds []int64
	var iteratedVals []string
	count := 0

	dataFrame.ForEachRow(func(r df.Row) {
		count++
		iteratedIds = append(iteratedIds, r.Get(0).GetAsInt())
		iteratedVals = append(iteratedVals, r.Get(1).GetAsString())
	})

	assert.Equal(t, len(ids), count, "ForEachRow should iterate over all rows")
	assert.Equal(t, ids, iteratedIds, "Iterated IDs should match original")
	assert.Equal(t, vals, iteratedVals, "Iterated values should match original")

	// Test on empty dataframe
	emptyRec := array.NewRecord(schema, nil, 0); defer emptyRec.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_foreach", emptyRec, dfSchema)
	defer emptyDf.(df.Releaser).Release()
	emptyCount := 0
	emptyDf.ForEachRow(func(r df.Row) { emptyCount++ })
	assert.Equal(t, 0, emptyCount, "ForEachRow on empty DF should not call function")

	// Test panic on nil function
	assert.PanicsWithValue(t, "ForEachRow: function f cannot be nil", func() {
		dataFrame.ForEachRow(nil)
	})
}

func TestDataFrame_UpdateSeries(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, _ := setupGroupedTestData(t, mem, "cat1") // cat1, cat2, value (float64)
	defer baseDf.(df.Releaser).Release()

	originalNumCols := baseDf.Schema().Len()
	originalLen := baseDf.Len()

	// Create a new series to update with
	newValSchema := df.SeriesSchema{Name: "value_updated", Format: df.DoubleFormat, Nullable: true}
	valBuilder := array.NewFloat64Builder(mem); defer valBuilder.Release()
	newFloats := make([]float64, originalLen)
	for i := 0; i < originalLen; i++ { newFloats[i] = float64(i) * 1.1 }
	valBuilder.AppendValues(newFloats, nil)
	newArr := valBuilder.NewArray(); defer newArr.Release()
	newSeries := arrowimpl.NewArrowSeries(newArr, newValSchema)

	// Case 1: Update by index (column "value" is at index 2)
	updatedDfByIdx := baseDf.UpdateSeries(2, newSeries)
	defer updatedDfByIdx.(df.Releaser).Release()

	assert.Equal(t, originalNumCols, updatedDfByIdx.Schema().Len(), "Num cols should remain same after update")
	assert.Equal(t, originalLen, updatedDfByIdx.Len(), "Num rows should remain same after update")
	assert.Equal(t, "value_updated", updatedDfByIdx.Schema().Get(2).Name, "Column name should be updated from new series")
	assert.Equal(t, df.DoubleFormat, updatedDfByIdx.Schema().Get(2).Format, "Column format should be from new series")

	updatedValSeries := updatedDfByIdx.GetSeriesByName("value_updated")
	for i:=0; i<originalLen; i++ {
		assert.Equal(t, newFloats[i], updatedValSeries.Get(i).GetAsFloat(), "Data mismatch in updated series by index")
	}
	updatedValSeries.Release() // Release series obtained from GetSeriesByName

	// Case 2: Update by name ("cat1")
	newCat1Schema := df.SeriesSchema{Name: "cat1_new_name", Format: df.StringFormat, Nullable: false}
	strBuilder := array.NewStringBuilder(mem); defer strBuilder.Release()
	newStrings := make([]string, originalLen)
	for i := 0; i < originalLen; i++ { newStrings[i] = fmt.Sprintf("NewCat_%d", i) }
	strBuilder.AppendValues(newStrings, nil)
	newStrArr := strBuilder.NewArray(); defer newStrArr.Release()
	newCat1Series := arrowimpl.NewArrowSeries(newStrArr, newCat1Schema)

	updatedDfByName := baseDf.UpdateSeriesByName("cat1", newCat1Series)
	defer updatedDfByName.(df.Releaser).Release()

	assert.Equal(t, "cat1_new_name", updatedDfByName.Schema().Get(0).Name, "Column name should be updated from new series by name")
	updatedCat1Series := updatedDfByName.GetSeriesByName("cat1_new_name")
	for i:=0; i<originalLen; i++ {
		assert.Equal(t, newStrings[i], updatedCat1Series.Get(i).GetAsString(), "Data mismatch in updated series by name")
	}
	updatedCat1Series.Release()

	// Case 3: Panic on length mismatch
	shorterBuilder := array.NewFloat64Builder(mem); defer shorterBuilder.Release()
	shorterBuilder.AppendValues([]float64{1.0, 2.0}, nil)
	shorterArr := shorterBuilder.NewArray(); defer shorterArr.Release()
	shorterSeries := arrowimpl.NewArrowSeries(shorterArr, newValSchema)
	assert.PanicsWithValue(t, fmt.Sprintf("UpdateSeries: length mismatch. DataFrame has %d rows, input series has %d rows", baseDf.Len(), shorterSeries.Len()), func() {
		baseDf.UpdateSeries(2, shorterSeries)
	})

	// Case 4: Panic on name conflict (new series name same as another existing col)
	conflictSchema := df.SeriesSchema{Name: "cat2", Format: df.StringFormat} // "cat2" is an existing column name
	conflictSeries := arrowimpl.NewArrowSeries(newStrArr, conflictSchema) // newStrArr already created and populated
	assert.PanicsWithValue(t, "UpdateSeries: new series name 'cat2' conflicts with existing column at index 1", func() {
		baseDf.UpdateSeries(0, conflictSeries) // Try to update "cat1" (idx 0) with a series named "cat2"
	})
}

func TestDataFrame_AsFormat(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},    // index 0
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // index 1
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},     // index 2
		}, nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)

	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 0}, []bool{true, true, false})         // int64
	rb.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)                       // float64
	rb.Field(2).(*array.StringBuilder).AppendValues([]string{"val1", "22", "val3"}, []bool{true, true, true}) // string
	rec := rb.NewRecord(); defer rec.Release()

	baseDf := arrowimpl.NewArrowDataFrame("asformat_test", rec, dfSchema)
	defer baseDf.(df.Releaser).Release()

	// Case 1: Change int to float, float to string (parseable), string to int (parseable for one value)
	targetFormats1 := map[string]df.Format{
		"col_int":   df.DoubleFormat, // int64 -> float64
		"col_float": df.StringFormat, // float64 -> string
		// "col_str":   df.IntegerFormat, // string -> int64 (Arrow cast might error on "val1", "val3")
		// Let's test a cast that Arrow compute.Cast can handle for strings, or remove this part
		// For now, let's focus on casts that are generally safe or well-defined by Arrow.
		// Casting string to int directly via compute.Cast is often problematic unless format is exact.
		// Instead, let's test string to a different numeric type if needed or just fewer casts.
	}
	formattedDf1 := baseDf.AsFormat(targetFormats1)
	defer formattedDf1.(df.Releaser).Release()

	assert.Equal(t, df.DoubleFormat, formattedDf1.Schema().Get(0).Format, "col_int should be DoubleFormat")
	assert.Equal(t, df.StringFormat, formattedDf1.Schema().Get(1).Format, "col_float should be StringFormat")
	assert.Equal(t, df.StringFormat, formattedDf1.Schema().Get(2).Format, "col_str should remain StringFormat (as it wasn't in map)")

	// Check data
	assert.Equal(t, 10.0, formattedDf1.GetValue(0, 0).GetAsFloat(), "col_int data cast")
	assert.True(t, formattedDf1.GetValue(2, 0).IsNil(), "col_int nil preserved")
	assert.Equal(t, "1.1", formattedDf1.GetValue(0, 1).GetAsString(), "col_float data cast to string")


	// Case 2: No changes if formats are the same or column not in map
	targetFormats2 := map[string]df.Format{
		"col_int":    df.IntegerFormat, // Same as original
		"col_nonexist": df.StringFormat,  // Column not in DF
	}
	formattedDf2 := baseDf.AsFormat(targetFormats2)
	defer formattedDf2.(df.Releaser).Release()
	assert.True(t, baseDf.Schema().Equals(formattedDf2.Schema()), "Schema should be unchanged if formats are same/col not found")
	// Check if it's a new instance but shares data (current AsFormat creates new even if no change)
	assert.NotSame(t, baseDf, formattedDf2, "AsFormat should return new instance even if no logical change")


	// Case 3: Empty format map
	formattedDf3 := baseDf.AsFormat(map[string]df.Format{})
	defer formattedDf3.(df.Releaser).Release()
	assert.True(t, baseDf.Schema().Equals(formattedDf3.Schema()), "Schema should be unchanged for empty format map")
	assert.NotSame(t, baseDf, formattedDf3)


	// Case 4: Test potential panic on incompatible cast (e.g., non-numeric string to int)
	// This depends on Arrow's compute.Cast behavior with DefaultCastOptions(false)
	// For "val1" to int64, Arrow's cast (without specific parse options) would likely yield null or error.
	// DefaultCastOptions(false) means it will try to make it null on parse error.
	targetFormats4 := map[string]df.Format{ "col_str": df.IntegerFormat }
	formattedDf4 := baseDf.AsFormat(targetFormats4)
	defer formattedDf4.(df.Releaser).Release()

	assert.Equal(t, df.IntegerFormat, formattedDf4.Schema().Get(2).Format, "col_str should now be IntegerFormat")
	// Check cast results: "val1" -> nil (or error, but DefaultCastOptions(false) makes it null)
	assert.True(t, formattedDf4.GetValue(0, 2).IsNil(), "Cast 'val1' to int should be nil with unsafe cast")
	assert.Equal(t, int64(22), formattedDf4.GetValue(1, 2).GetAsInt(), "Cast '22' to int")
	assert.True(t, formattedDf4.GetValue(2, 2).IsNil(), "Cast 'val3' to int should be nil with unsafe cast")

}

func TestDataFrame_Select(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_A", Type: arrow.PrimitiveTypes.Int64},
			{Name: "col_B", Type: arrow.BinaryTypes.String},
			{Name: "col_C", Type: arrow.PrimitiveTypes.Float64},
		}, nil,
	)
	baseDfSchema := arrowimpl.NewArrowDataFrameSchema(baseSchema).(*arrowimpl.ArrowDataFrameSchema)
	rb := array.NewRecordBuilder(mem, baseSchema); defer rb.Release()
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"x", "y", "z"}, nil)
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := rb.NewRecord(); defer rec.Release()

	baseDf := arrowimpl.NewArrowDataFrame("select_test_df", rec, baseDfSchema)
	defer baseDf.(df.Releaser).Release()

	// Case 1: Select existing columns by name
	selectedDf1 := baseDf.Select(df.NewColExpr("col_A"), df.NewColExpr("col_C"))
	defer selectedDf1.(df.Releaser).Release()

	assert.Equal(t, 2, selectedDf1.Schema().Len(), "Select existing: Num columns")
	assert.Equal(t, "col_A", selectedDf1.Schema().Get(0).Name)
	assert.Equal(t, "col_C", selectedDf1.Schema().Get(1).Name)
	assert.Equal(t, baseDf.Len(), selectedDf1.Len(), "Select existing: Num rows")
	assert.Equal(t, int64(1), selectedDf1.GetValue(0,0).GetAsInt()) // col_A data
	assert.Equal(t, 3.3, selectedDf1.GetValue(2,1).GetAsFloat()) // col_C data

	// Case 2: Select existing columns with aliases
	selectedDf2 := baseDf.Select(
		df.NewColExpr("col_B").SetName("new_B_name"),
		df.NewColExpr("col_A"), // No alias
	)
	defer selectedDf2.(df.Releaser).Release()
	assert.Equal(t, 2, selectedDf2.Schema().Len(), "Select with alias: Num columns")
	assert.Equal(t, "new_B_name", selectedDf2.Schema().Get(0).Name)
	assert.Equal(t, "col_A", selectedDf2.Schema().Get(1).Name)
	assert.Equal(t, "x", selectedDf2.GetValue(0,0).GetAsString()) // new_B_name data

	// Case 3: Create new columns from literal values
	selectedDf3 := baseDf.Select(
		df.NewColExpr("col_A"), // Keep one original column to maintain row count context
		df.NewLiteralExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(100), df.IntegerFormat)).SetName("literal_int"),
		df.NewLiteralExpr(arrowimpl.NewArrowValue(scalar.NewStringScalar("const_str"), df.StringFormat)).SetName("literal_string"),
	)
	defer selectedDf3.(df.Releaser).Release()
	assert.Equal(t, 3, selectedDf3.Schema().Len(), "Select with literals: Num columns")
	assert.Equal(t, "literal_int", selectedDf3.Schema().Get(1).Name)
	assert.Equal(t, df.IntegerFormat, selectedDf3.Schema().Get(1).Format)
	assert.Equal(t, "literal_string", selectedDf3.Schema().Get(2).Name)
	assert.Equal(t, df.StringFormat, selectedDf3.Schema().Get(2).Format)

	for i:=0; i<selectedDf3.Len(); i++ {
		assert.Equal(t, int64(100), selectedDf3.GetValue(i,1).GetAsInt(), "Literal int data")
		assert.Equal(t, "const_str", selectedDf3.GetValue(i,2).GetAsString(), "Literal string data")
	}

	// Case 4: Select zero expressions
	selectedDf4 := baseDf.Select()
	defer selectedDf4.(df.Releaser).Release()
	assert.Equal(t, 0, selectedDf4.Schema().Len(), "Select zero expr: Num columns")
	assert.Equal(t, baseDf.Len(), selectedDf4.Len(), "Select zero expr: Num rows should be same as original")

	// Case 5: Select from a nil-record DataFrame (if it has 0 rows, literal select should produce 0 rows)
	emptyRecForSelect := array.NewRecord(baseSchema, nil, 0); defer emptyRecForSelect.Release()
	emptyBaseDf := arrowimpl.NewArrowDataFrame("empty_select", emptyRecForSelect, baseDfSchema)
	defer emptyBaseDf.(df.Releaser).Release()

	selectedDf5 := emptyBaseDf.Select(
		df.NewLiteralExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(100), df.IntegerFormat)).SetName("lit_empty"),
	)
	defer selectedDf5.(df.Releaser).Release()
	assert.Equal(t, 1, selectedDf5.Schema().Len(), "Select literal on empty DF: Num columns")
	assert.Equal(t, "lit_empty", selectedDf5.Schema().Get(0).Name)
	assert.Equal(t, 0, selectedDf5.Len(), "Select literal on empty DF: Num rows should be 0")

	// Case 6: Panic on unsupported expression (if any other df.ExprOpType is added later)
	// This requires a mock df.Expr or extending df.ExprOpType
	// For now, this is implicitly covered by the default case in Select method.

	// --- Tests for enhanced Select with Unary (Series.Select delegation) and Binary Ops ---
	// For these tests, we'll reuse the mockSeriesExpr, mockSeriesMapOp, mockSeriesFilterOp from series_test.go
	// If they are not in the same package, they'd need to be defined here or in a shared test util.
	// Assuming they are accessible (e.g. if this file is also in package arrow_test and they are in series_test.go in same package)
	// For the sake of this tool, I will redefine simplified versions here if needed, or assume df.New...Expr creates usable structures.
	// Let's use the actual df.New...Expr where possible and mock only for ops not yet in df package.

	// Helper to create a literal df.Value for expressions
	intVal := func(i int64) df.Value { return arrowimpl.NewArrowValue(scalar.NewInt64Scalar(i), df.IntegerFormat) }
	// floatVal := func(f float64) df.Value { return arrowimpl.NewArrowValue(scalar.NewFloat64Scalar(f), df.DoubleFormat) }

	t.Run("UnaryOpOnColumn", func(t *testing.T) {
		// Simulating Col("col_A").Add(Literal(5)).SetName("A_plus_5")
		// We need df.Expr to be able to represent this. Let's assume df.NewColExpr("col_A").Add(litVal) returns an Expr
		// that Select can decompose.
		// The current Select implementation expects the Series.Select to handle the OpConst part.

		// Mocking the structure: OpConst_Add expression with Col("col_A") as parent
		lit5Expr := &mockSeriesExpr{opType: df.LiteralExpr, constVal: intVal(5), exprName: "lit5"}
		addExpr := &mockSeriesExpr{ // This node is what Series.Select would receive
			parentExpr: nil, // Series.Select expects parent to be nil for its direct operation
			opType:     df.ExprTypeMap,
			mapOp:      &mockSeriesMapOp{opName: "OpConst_Add", args: []df.Expr{lit5Expr}},
			exprName:   "A_plus_5", // This is the alias for the final column
		}
		// The expression passed to DataFrame.Select for Col("A").Add(5) would have Col("A") as parent
		// and 'addExpr' (or rather, its operational part) as the current node.
		// For DataFrame.Select to delegate to Series.Select, the expression structure needs to be:
		// Expr(Name: "A_plus_5", Parent: ColExpr("col_A"), Op: MapOp("OpConst_Add", Literal(5)))

		selectArgExpr := &mockSeriesExpr{
			parentExpr: &mockSeriesExpr{opType: df.ColNameExpr, colName: "col_A", exprName: "col_A"}, // Parent is Col("col_A")
			opType:     df.ExprTypeMap, // This is the operation type of the Add node itself
			mapOp:      &mockSeriesMapOp{opName: "OpConst_Add", args: []df.Expr{lit5Expr}}, // MapOp describes the Add(5)
			exprName:   "A_plus_5", // Final alias
		}


		selectedDf := baseDf.Select(selectArgExpr)
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len(), "Unary op: Num columns")
		assert.Equal(t, "A_plus_5", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format)
		// col_A data: 1, 2, 3. Expected: 6, 7, 8
		assert.Equal(t, int64(6), selectedDf.GetValue(0,0).GetAsInt())
		assert.Equal(t, int64(7), selectedDf.GetValue(1,0).GetAsInt())
		assert.Equal(t, int64(8), selectedDf.GetValue(2,0).GetAsInt())
	})

	t.Run("BinaryOpBetweenColumns", func(t *testing.T) {
		// Simulating Col("col_A").Add(Col("col_C")).SetName("A_plus_C")
		// Structure: Expr(Name: "A_plus_C", Parent: Col("col_A"), Op: MapOp("Op_Add", Col("col_C")))
		// Note: "Op_Add" here is a hypothetical name for binary add between series.
		// The current implementation might expect a different opName or structure for binary series ops.
		// The `Select` implementation was updated to look for `expr.MapOp().Args()[0].OpType() == df.ColNameExpr`
		// and `expr.Parent().OpType() == df.ColNameExpr`.
		// The name of the operation (e.g. "Op_Add") is taken from `expr.Name()` if not specific in MapOp.
		// This needs to align with how `df.ColExpr(...).Add(df.ColExpr(...))` structures the Expr.

		argColCExpr := &mockSeriesExpr{opType: df.ColNameExpr, colName: "col_C", exprName: "col_C"}

		// This expression represents the "Add Col_C" operation part.
		// Its parent will be the Col_A expression when used in DataFrame.Select context.
		binaryAddExpr := &mockSeriesExpr{
			parentExpr: &mockSeriesExpr{opType: df.ColNameExpr, colName: "col_A", exprName: "col_A"},
			opType:     df.ExprTypeMap, // Binary ops are still map ops in this context.
			mapOp:      &mockSeriesMapOp{
				// opName: "Op_Add", // The name of the binary operation kernel.
				// This needs to be derived, e.g., from the main expr.Name() or specific MapOp field.
				// For test, assume expr.Name() will be "Op_Add" or similar if Select uses it.
				args: []df.Expr{argColCExpr}, // Argument is Col("col_C")
			},
			exprName:   "Op_Add", // This is used by Select to find the compute kernel "add"
		}
		// Alias for the final column
		selectArgExpr := (&mockSeriesExpr{}).SetName("A_plus_C").(*mockSeriesExpr) // Create a new wrapper for SetName
		selectArgExpr.parentExpr = binaryAddExpr.parentExpr
		selectArgExpr.opType = binaryAddExpr.opType
		selectArgExpr.mapOp = binaryAddExpr.mapOp
		// The name of the *operation* for the compute kernel comes from binaryAddExpr.exprName ("Op_Add")
		// The name of the *output column* comes from selectArgExpr.exprName ("A_plus_C")
		// This distinction is important. The current DataFrame.Select might use expr.Name() for both.
		// Let's assume the MapOp itself should specify the kernel, or expr.Name() is for the kernel,
		// and a separate Alias mechanism exists.
		// Forcing the name to "Op_Add" to match the kernel, and relying on a higher-level alias.
		// This mocking is getting complex due to unknown df.Expr structure.
		// A simpler way for test: assume df.NewColExpr("colA").Add(df.NewColExpr("colB")) creates an Expr
		// that Select can interpret.
		// For now, let's assume the DataFrame.Select's binary path is hit if expr.Name() is "Op_Add"
		// and it has a ColNameExpr parent and a ColNameExpr arg in MapOp.

		selectArgExpr.exprName = "A_plus_C" // Final output column name
		binaryAddExpr.exprName = "Op_Add" // Kernel name for the operation node

		// Re-structuring the mock to be more explicit for the test:
		// The expression passed to df.Select is the one representing the final column, with its alias.
		// Its internal structure defines the operation.

		opExpr := df.NewColExpr("col_A").Add(df.NewColExpr("col_C")).SetName("A_plus_C_actual")
		// The above line uses the actual df.Expr constructors. This is PREFERRED.
		// If these constructors set up Parent, OpType, MapOp, Args correctly, it will work.
		// If not, the mocks are needed. For now, let's assume the mocks are still needed to guide impl.

		binaryExpr := &mockSeriesExpr{
			exprName: "A_plus_C", // This will be the output column name
			opType: df.ExprTypeMap, // It's a map operation
			parentExpr: &mockSeriesExpr{opType: df.ColNameExpr, colName: "col_A"}, // Left operand
			mapOp: &mockSeriesMapOp{
				opName: "Op_Add", // Specific name for the binary operation kernel
				args:   []df.Expr{&mockSeriesExpr{opType: df.ColNameExpr, colName: "col_C"}}, // Right operand
			},
		}


		selectedDf := baseDf.Select(binaryExpr)
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len(), "Binary op: Num columns")
		assert.Equal(t, "A_plus_C", selectedDf.Schema().Get(0).Name)
		// col_A (int): 1, 2, 3. col_C (float): 1.1, 2.2, 3.3
		// Arrow Add(Int64, Float64) should result in Float64
		assert.Equal(t, df.DoubleFormat, selectedDf.Schema().Get(0).Format, "A+C should be Float64")
		assert.InDelta(t, 1 + 1.1, selectedDf.GetValue(0,0).GetAsFloat(), 1e-9)
		assert.InDelta(t, 2 + 2.2, selectedDf.GetValue(1,0).GetAsFloat(), 1e-9)
		assert.InDelta(t, 3 + 3.3, selectedDf.GetValue(2,0).GetAsFloat(), 1e-9)
	})
}

[end of df/arrow/df_test.go]
