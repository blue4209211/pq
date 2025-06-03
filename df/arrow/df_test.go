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
func TestDataFrame_Select(t *testing.T) { // To be renamed or merged if TestArrowDataFrame_Select_Advanced exists and is different
	mem := memory.NewGoAllocator()

	// Setup Test Data
	fields := []arrow.Field{
		{Name: "col_int_a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col_int_b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col_str_c", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "col_float_d", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "col_bool_e", Type: arrow.PrimitiveTypes.Boolean, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	rb := array.NewRecordBuilder(mem, arrowSchema); defer rb.Release()
	// Data:
	// col_int_a: {1, 2, nil}
	// col_int_b: {4, nil, 6}
	// col_str_c: {"x", "y", "z"} (no nulls for simplicity in some basic str tests)
	// col_float_d: {1.1, 2.2, nil}
	// col_bool_e: {true, false, true}
	rb.Field(0).(*builder.Int64Builder).AppendValues([]int64{1, 2, 0}, []bool{true, true, false})
	rb.Field(1).(*builder.Int64Builder).AppendValues([]int64{4, 0, 6}, []bool{true, false, true})
	rb.Field(2).(*builder.StringBuilder).AppendValues([]string{"x", "y", "z"}, nil)
	rb.Field(3).(*builder.Float64Builder).AppendValues([]float64{1.1, 2.2, 0}, []bool{true, true, false})
	rb.Field(4).(*builder.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)

	rec := rb.NewRecord(); defer rec.Release()
	baseDf := arrowimpl.NewArrowDataFrame("test_select_df", rec, dfSchema)
	defer baseDf.(df.Releaser).Release()

	t.Run("Select_ColumnOnly", func(t *testing.T) {
		selectedDf := baseDf.Select(expr.NewCol("col_int_a"), expr.NewCol("col_str_c"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, int64(3), selectedDf.Len())
		assert.Equal(t, 2, selectedDf.Schema().Len())
		assert.Equal(t, "col_int_a", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format)
		assert.Equal(t, "col_str_c", selectedDf.Schema().Get(1).Name)
		assert.Equal(t, df.StringFormat, selectedDf.Schema().Get(1).Format)

		expectedData := [][]interface{}{
			{int64(1), "x"},
			{int64(2), "y"},
			{nilPlaceholder, "z"},
		}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_LiteralOnly", func(t *testing.T) {
		selectedDf := baseDf.Select(
			expr.NewLitInt(100).As("lit_int"),
			expr.NewLitString("hello").As("lit_str"),
		)
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, int64(3), selectedDf.Len())
		assert.Equal(t, 2, selectedDf.Schema().Len())
		assert.Equal(t, "lit_int", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format)
		assert.Equal(t, "lit_str", selectedDf.Schema().Get(1).Name)
		assert.Equal(t, df.StringFormat, selectedDf.Schema().Get(1).Format)

		expectedData := [][]interface{}{
			{int64(100), "hello"},
			{int64(100), "hello"},
			{int64(100), "hello"},
		}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_ColumnAndLiteral", func(t *testing.T) {
		selectedDf := baseDf.Select(
			expr.NewCol("col_float_d"),
			expr.NewLitString("const").As("my_const"),
		)
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 2, selectedDf.Schema().Len())
		assert.Equal(t, "col_float_d", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.DoubleFormat, selectedDf.Schema().Get(0).Format)
		assert.Equal(t, "my_const", selectedDf.Schema().Get(1).Name)
		assert.Equal(t, df.StringFormat, selectedDf.Schema().Get(1).Format)

		expectedData := [][]interface{}{
			{1.1, "const"},
			{2.2, "const"},
			{nilPlaceholder, "const"},
		}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_WithAlias", func(t *testing.T) {
		selectedDf := baseDf.Select(
			expr.NewCol("col_int_a").As("aliased_a"),
			expr.NewLitInt(42).As("the_answer"),
		)
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 2, selectedDf.Schema().Len())
		assert.Equal(t, "aliased_a", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format)
		assert.Equal(t, "the_answer", selectedDf.Schema().Get(1).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(1).Format)

		expectedData := [][]interface{}{
			{int64(1), int64(42)},
			{int64(2), int64(42)},
			{nilPlaceholder, int64(42)},
		}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	// BinaryOp ColLit
	t.Run("Select_BinaryOp_ColLit_AddInt", func(t *testing.T) {
		selectedDf := baseDf.Select(expr.NewCol("col_int_a").Add(expr.NewLitInt(5)).As("a_plus_5"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "a_plus_5", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format) // int64 + int64 = int64

		expectedData := [][]interface{}{{int64(6)}, {int64(7)}, {nilPlaceholder}} // 1+5, 2+5, nil+5=nil
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_BinaryOp_ColLit_EqString", func(t *testing.T) {
		selectedDf := baseDf.Select(expr.NewCol("col_str_c").Eq(expr.NewLitString("y")).As("c_equals_y"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "c_equals_y", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.BoolFormat, selectedDf.Schema().Get(0).Format)

		expectedData := [][]interface{}{{false}, {true}, {false}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_BinaryOp_ColLit_GtFloat", func(t *testing.T) {
		// col_float_d: {1.1, 2.2, nil}
		selectedDf := baseDf.Select(expr.NewCol("col_float_d").Gt(expr.NewLitFloat(2.0)).As("d_gt_2"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "d_gt_2", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.BoolFormat, selectedDf.Schema().Get(0).Format)

		// 1.1 > 2.0 = false; 2.2 > 2.0 = true; nil > 2.0 = null (Arrow specific, might be false if not optioned for true on nulls)
		// Assuming standard SQL like null propagation for comparisons.
		expectedData := [][]interface{}{{false}, {true}, {nilPlaceholder}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	// BinaryOp ColCol
	t.Run("Select_BinaryOp_ColCol_AddInt", func(t *testing.T) {
		// col_int_a: {1, 2, nil}
		// col_int_b: {4, nil, 6}
		selectedDf := baseDf.Select(expr.NewCol("col_int_a").Add(expr.NewCol("col_int_b")).As("a_plus_b"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "a_plus_b", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.IntegerFormat, selectedDf.Schema().Get(0).Format)

		// 1+4=5; 2+nil=nil; nil+6=nil
		expectedData := [][]interface{}{{int64(5)}, {nilPlaceholder}, {nilPlaceholder}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_BinaryOp_ColCol_MultiplyFloatInt", func(t *testing.T) {
		// col_float_d: {1.1, 2.2, nil}
		// col_int_a:   {1,   2,   nil}
		selectedDf := baseDf.Select(expr.NewCol("col_float_d").Mul(expr.NewCol("col_int_a")).As("d_times_a"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "d_times_a", selectedDf.Schema().Get(0).Name)
		// Arrow promotes int * float to float
		assert.Equal(t, df.DoubleFormat, selectedDf.Schema().Get(0).Format)

		// 1.1*1=1.1; 2.2*2=4.4; nil*nil=nil
		expectedData := [][]interface{}{{1.1}, {4.4}, {nilPlaceholder}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, len(expectedData), len(actualData))
		for i := range expectedData {
			if expectedData[i][0] == nilPlaceholder {
				assert.True(t, actualData[i][0] == nilPlaceholder || reflect.ValueOf(actualData[i][0]).IsNil())
			} else {
				assert.InDelta(t, expectedData[i][0], actualData[i][0], 0.0001)
			}
		}
	})

	// UnaryOp
	t.Run("Select_UnaryOp_CastIntToString", func(t *testing.T) {
		// col_int_a: {1, 2, nil}
		selectedDf := baseDf.Select(expr.NewCol("col_int_a").Cast(df.StringFormat).As("a_as_str"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "a_as_str", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.StringFormat, selectedDf.Schema().Get(0).Format)

		expectedData := [][]interface{}{{"1"}, {"2"}, {nilPlaceholder}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("Select_UnaryOp_IsNull", func(t *testing.T) {
		// col_int_b: {4, nil, 6}
		selectedDf := baseDf.Select(expr.NewCol("col_int_b").IsNull().As("b_is_null"))
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, 1, selectedDf.Schema().Len())
		assert.Equal(t, "b_is_null", selectedDf.Schema().Get(0).Name)
		assert.Equal(t, df.BoolFormat, selectedDf.Schema().Get(0).Format)

		expectedData := [][]interface{}{{false}, {true}, {false}}
		actualData := dfToSliceOfInterfaceSlices(selectedDf)
		assert.Equal(t, expectedData, actualData)
	})

	// Edge Cases
	t.Run("Select_EmptyExpressions", func(t *testing.T) {
		selectedDf := baseDf.Select() // No expressions
		defer selectedDf.(df.Releaser).Release()

		assert.Equal(t, baseDf.Len(), selectedDf.Len(), "Number of rows should be preserved")
		assert.Equal(t, 0, selectedDf.Schema().Len(), "Schema should have 0 columns")
	})

	t.Run("Select_FromEmptyDataFrame", func(t *testing.T) {
		emptyRec := array.NewRecord(arrowSchema, nil, 0); defer emptyRec.Release()
		emptyDf := arrowimpl.NewArrowDataFrame("empty_select_base", emptyRec, dfSchema)
		defer emptyDf.(df.Releaser).Release()

		selectedCols := emptyDf.Select(expr.NewCol("col_int_a"))
		defer selectedCols.(df.Releaser).Release()
		assert.Equal(t, int64(0), selectedCols.Len())
		assert.Equal(t, 1, selectedCols.Schema().Len())
		assert.Equal(t, "col_int_a", selectedCols.Schema().Get(0).Name)

		selectedLits := emptyDf.Select(expr.NewLitInt(1).As("one"))
		defer selectedLits.(df.Releaser).Release()
		assert.Equal(t, int64(0), selectedLits.Len())
		assert.Equal(t, 1, selectedLits.Schema().Len())
		assert.Equal(t, "one", selectedLits.Schema().Get(0).Name)
	})

	t.Run("Select_Error_ColumnNotFoundInExpr", func(t *testing.T) {
		assert.Panics(t, func() {
			// This panic will occur when Series.Select tries to resolve "non_existent_col"
			// from a record that doesn't have it.
			resDf := baseDf.Select(expr.NewCol("non_existent_col"))
			if resDf != nil { resDf.(df.Releaser).Release() }
		}, "Selecting a non-existent column should panic.")
	})

	t.Run("Select_Error_BinaryOpTypeMismatch", func(t *testing.T) {
		// col_str_c (string) + 5 (int)
		// This depends on how Series.Select and underlying Arrow kernels handle it.
		// It might panic in the expression evaluation part if types are incompatible for the op.
		assert.Panics(t, func() {
			resDf := baseDf.Select(expr.NewCol("col_str_c").Add(expr.NewLitInt(5)).As("str_plus_int"))
			// The panic would typically originate from the Arrow compute function for Add,
			// when it receives a string array and an int scalar/array.
			if resDf != nil { resDf.(df.Releaser).Release() }
		}, "Binary operation with type mismatch should panic.")
	})

}

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

func TestDataFrame_Join_Inner(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Schema for left table
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	// Schema for right table
	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	outputSchemaFields := []arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	outputArrowSchema := arrow.NewSchema(outputSchemaFields, nil)
	outputDfSchema := arrowimpl.NewArrowDataFrameSchema(outputArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	defaultFUser := func(lRow, rRow df.Row) []df.Row {
		if lRow == nil || rRow == nil { return []df.Row{} }

		vals := make([]df.Value, 0, outputDfSchema.Len())
		vals = append(vals, lRow.GetByName("id_l"))
		vals = append(vals, lRow.GetByName("val_l"))
		vals = append(vals, rRow.GetByName("id_r"))
		vals = append(vals, rRow.GetByName("val_r"))
		return []df.Row{arrowimpl.NewArrowRowFromValues(outputDfSchema.(*arrowimpl.ArrowDataFrameSchema), vals)}
	}


	t.Run("BasicInnerJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_inner_basic", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3, 4}, nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R2", "R3", "R4"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_inner_basic", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(2), "L2", int64(2), "R2"},
			{int64(3), "L3", int64(3), "R3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(2), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_LeftEmpty", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_inner_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1","R2"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_inner_lnonempty_r", rRec, dfSchemaRight)
		defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfNonEmpty, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()), "Schema of empty result should match output schema")
	})

	t.Run("EdgeCase_RightEmpty", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfNonEmpty := arrowimpl.NewArrowDataFrame("ldf_inner_rempty_l", lRec, dfSchemaLeft)
		defer ldfNonEmpty.(df.Releaser).Release()

		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_inner_rempty", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfNonEmpty.Join(outputDfSchema, rdfEmpty, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_BothEmpty", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_inner_bothempty_l", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()
		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_inner_bothempty_r", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfEmpty, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_NoMatchingKeys", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_inner_nomatch", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{3,4},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R3","R4"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_inner_nomatch", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
	})

	t.Run("EdgeCase_NullsInJoinKeys", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_null", "L3"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfWithNull := arrowimpl.NewArrowDataFrame("ldf_inner_nullkey", lRec, dfSchemaLeft)
		defer ldfWithNull.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 4}, []bool{false, true, true})
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R_null", "R3", "R4"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfWithNull := arrowimpl.NewArrowDataFrame("rdf_inner_nullkey", rRec, dfSchemaRight)
		defer rdfWithNull.(df.Releaser).Release()

		joinResult := ldfWithNull.Join(outputDfSchema, rdfWithNull, df.JoinInner, map[string]string{"id_l": "id_r"}, defaultFUser)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{{int64(3), "L3", int64(3), "R3"}}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(1), joinResult.Len())
	})
}
func TestDataFrame_Join_Left(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	outputSchemaFields := []arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	outputArrowSchema := arrow.NewSchema(outputSchemaFields, nil)
	outputDfSchema := arrowimpl.NewArrowDataFrameSchema(outputArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	defaultFUserLeft := func(lRow, rRow df.Row) []df.Row {
		vals := make([]df.Value, 0, outputDfSchema.Len())
		if lRow != nil {
			vals = append(vals, lRow.GetByName("id_l"))
			vals = append(vals, lRow.GetByName("val_l"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		if rRow != nil {
			vals = append(vals, rRow.GetByName("id_r"))
			vals = append(vals, rRow.GetByName("val_r"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		return []df.Row{arrowimpl.NewArrowRowFromValues(outputDfSchema.(*arrowimpl.ArrowDataFrameSchema), vals)}
	}

	t.Run("BasicLeftJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_left_basic", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3, 4}, nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R2", "R3", "R4"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_left_basic", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinLeft, map[string]string{"id_l": "id_r"}, defaultFUserLeft)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", int64(2), "R2"},
			{int64(3), "L3", int64(3), "R3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(3), joinResult.Len())
	})

	t.Run("EdgeCase_LeftEmpty_LeftJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_left_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1","R2"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_left_lnonempty_r", rRec, dfSchemaRight)
		defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfNonEmpty, df.JoinLeft, map[string]string{"id_l": "id_r"}, defaultFUserLeft)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_RightEmpty_LeftJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfNonEmpty := arrowimpl.NewArrowDataFrame("ldf_left_rempty_l", lRec, dfSchemaLeft)
		defer ldfNonEmpty.(df.Releaser).Release()

		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_left_rempty", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfNonEmpty.Join(outputDfSchema, rdfEmpty, df.JoinLeft, map[string]string{"id_l": "id_r"}, defaultFUserLeft)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", nilPlaceholder, nilPlaceholder},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, ldfNonEmpty.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_NoMatchingKeys_LeftJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_left_nomatch", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{3,4},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R3","R4"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_left_nomatch", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinLeft, map[string]string{"id_l": "id_r"}, defaultFUserLeft)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", nilPlaceholder, nilPlaceholder},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, ldf.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_NullsInJoinKeys_LeftJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, []bool{true, false, true}) // ID: 1, NULL, 3
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_null", "L3"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfWithNull := arrowimpl.NewArrowDataFrame("ldf_left_nullkey", lRec, dfSchemaLeft)
		defer ldfWithNull.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 4}, []bool{false, true, true}) // ID: NULL, 3, 4
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R_null", "R3", "R4"}, []bool{true, true, true})
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfWithNull := arrowimpl.NewArrowDataFrame("rdf_left_nullkey", rRec, dfSchemaRight)
		defer rdfWithNull.(df.Releaser).Release()

		joinResult := ldfWithNull.Join(outputDfSchema, rdfWithNull, df.JoinLeft, map[string]string{"id_l": "id_r"}, defaultFUserLeft)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{nilPlaceholder, "L_null", nilPlaceholder, nilPlaceholder},
			{int64(3), "L3", int64(3), "R3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, ldfWithNull.Len(), joinResult.Len())
	})
}
func TestDataFrame_Join_Right(t *testing.T) {
	mem := memory.NewGoAllocator()
	schemaLeft := arrow.NewSchema([]arrow.Field{
		{Name: "id_l", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dfSchemaLeft := arrowimpl.NewArrowDataFrameSchema(schemaLeft).(*arrowimpl.ArrowDataFrameSchema)

	schemaRight := arrow.NewSchema([]arrow.Field{
		{Name: "id_r", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r", Type: arrow.BinaryTypes.String},
	}, nil)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)

	outputSchemaFields := []arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	outputArrowSchema := arrow.NewSchema(outputSchemaFields, nil)
	outputDfSchema := arrowimpl.NewArrowDataFrameSchema(outputArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	defaultFUserRight := func(lRow, rRow df.Row) []df.Row {
		vals := make([]df.Value, 0, outputDfSchema.Len())
		if lRow != nil {
			vals = append(vals, lRow.GetByName("id_l"))
			vals = append(vals, lRow.GetByName("val_l"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		if rRow != nil {
			vals = append(vals, rRow.GetByName("id_r"))
			vals = append(vals, rRow.GetByName("val_r"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		return []df.Row{arrowimpl.NewArrowRowFromValues(outputDfSchema.(*arrowimpl.ArrowDataFrameSchema), vals)}
	}

	t.Run("BasicRightJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_right_basic", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3, 4}, nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R2", "R3", "R4"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_right_basic", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinRight, map[string]string{"id_l": "id_r"}, defaultFUserRight)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(2), "L2", int64(2), "R2"},
			{int64(3), "L3", int64(3), "R3"},
			{nilPlaceholder, nilPlaceholder, int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(3), joinResult.Len())
	})

	t.Run("EdgeCase_LeftEmpty_RightJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_right_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1","R2"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_right_lnonempty_r", rRec, dfSchemaRight)
		defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfNonEmpty, df.JoinRight, map[string]string{"id_l": "id_r"}, defaultFUserRight)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{nilPlaceholder, nilPlaceholder, int64(1), "R1"},
			{nilPlaceholder, nilPlaceholder, int64(2), "R2"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, rdfNonEmpty.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_RightEmpty_RightJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfNonEmpty := arrowimpl.NewArrowDataFrame("ldf_right_rempty_l", lRec, dfSchemaLeft)
		defer ldfNonEmpty.(df.Releaser).Release()

		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_right_rempty", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfNonEmpty.Join(outputDfSchema, rdfEmpty, df.JoinRight, map[string]string{"id_l": "id_r"}, defaultFUserRight)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_NoMatchingKeys_RightJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_right_nomatch", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{3,4},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R3","R4"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_right_nomatch", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinRight, map[string]string{"id_l": "id_r"}, defaultFUserRight)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{nilPlaceholder, nilPlaceholder, int64(3), "R3"},
			{nilPlaceholder, nilPlaceholder, int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, rdf.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_NullsInJoinKeys_RightJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_null", "L3"}, []bool{true,true,true})
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfWithNull := arrowimpl.NewArrowDataFrame("ldf_right_nullkey_l", lRec, dfSchemaLeft)
		defer ldfWithNull.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 4}, []bool{false, true, true})
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R_null", "R3", "R4"}, []bool{true,true,true})
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfWithNull := arrowimpl.NewArrowDataFrame("rdf_right_nullkey_r", rRec, dfSchemaRight)
		defer rdfWithNull.(df.Releaser).Release()

		joinResult := ldfWithNull.Join(outputDfSchema, rdfWithNull, df.JoinRight, map[string]string{"id_l": "id_r"}, defaultFUserRight)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{nilPlaceholder, nilPlaceholder, nilPlaceholder, "R_null"},
			{int64(3), "L3", int64(3), "R3"},
			{nilPlaceholder, nilPlaceholder, int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, rdfWithNull.Len(), joinResult.Len())
	})
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

	outputSchemaFields := []arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	outputArrowSchema := arrow.NewSchema(outputSchemaFields, nil)
	outputDfSchema := arrowimpl.NewArrowDataFrameSchema(outputArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	defaultFUserFullOuter := func(lRow, rRow df.Row) []df.Row {
		vals := make([]df.Value, 0, outputDfSchema.Len())
		if lRow != nil {
			vals = append(vals, lRow.GetByName("id_l"))
			vals = append(vals, lRow.GetByName("val_l"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		if rRow != nil {
			vals = append(vals, rRow.GetByName("id_r"))
			vals = append(vals, rRow.GetByName("val_r"))
		} else {
			vals = append(vals, makeArrowValue(nil, arrow.PrimitiveTypes.Int64))
			vals = append(vals, makeArrowValue(nil, arrow.BinaryTypes.String))
		}
		return []df.Row{arrowimpl.NewArrowRowFromValues(outputDfSchema.(*arrowimpl.ArrowDataFrameSchema), vals)}
	}

	t.Run("BasicFullOuterJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_full_basic", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 3}, nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R2", "R3"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_full_basic", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", int64(2), "R2"},
			{nilPlaceholder, nilPlaceholder, int64(3), "R3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(3), joinResult.Len())
	})

	t.Run("EdgeCase_LeftEmpty_FullOuterJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_full_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1","R2"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_full_lnonempty_r", rRec, dfSchemaRight)
		defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfNonEmpty, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{nilPlaceholder, nilPlaceholder, int64(1), "R1"},
			{nilPlaceholder, nilPlaceholder, int64(2), "R2"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, rdfNonEmpty.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_RightEmpty_FullOuterJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfNonEmpty := arrowimpl.NewArrowDataFrame("ldf_full_rempty_l", lRec, dfSchemaLeft)
		defer ldfNonEmpty.(df.Releaser).Release()

		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_full_rempty_r", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfNonEmpty.Join(outputDfSchema, rdfEmpty, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", nilPlaceholder, nilPlaceholder},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, ldfNonEmpty.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_BothEmpty_FullOuterJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_full_bothempty_l", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()
		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_full_bothempty_r", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfEmpty, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})


	t.Run("EdgeCase_NoMatchingKeys_FullOuterJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1","L2"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_full_nomatch", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{3,4},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R3","R4"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_full_nomatch", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{int64(2), "L2", nilPlaceholder, nilPlaceholder},
			{nilPlaceholder, nilPlaceholder, int64(3), "R3"},
			{nilPlaceholder, nilPlaceholder, int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, ldf.Len() + rdf.Len(), joinResult.Len())
	})

	t.Run("EdgeCase_NullsInJoinKeys_FullOuterJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_null", "L3"}, []bool{true,true,true})
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfWithNull := arrowimpl.NewArrowDataFrame("ldf_full_nullkey_l", lRec, dfSchemaLeft)
		defer ldfWithNull.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 4}, []bool{false, true, true})
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R_null", "R3", "R4"}, []bool{true,true,true})
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfWithNull := arrowimpl.NewArrowDataFrame("rdf_full_nullkey_r", rRec, dfSchemaRight)
		defer rdfWithNull.(df.Releaser).Release()

		joinResult := ldfWithNull.Join(outputDfSchema, rdfWithNull, df.JoinFullOuter, map[string]string{"id_l": "id_r"}, defaultFUserFullOuter)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1", nilPlaceholder, nilPlaceholder},
			{nilPlaceholder, "L_null", nilPlaceholder, nilPlaceholder},
			{int64(3), "L3", int64(3), "R3"},
			{nilPlaceholder, nilPlaceholder, nilPlaceholder, "R_null"},
			{nilPlaceholder, nilPlaceholder, int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(5), joinResult.Len())
	})
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

	outputSchemaFields := []arrow.Field{
		{Name: "id_l_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_l_out", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id_r_out", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val_r_out", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	outputArrowSchema := arrow.NewSchema(outputSchemaFields, nil)
	outputDfSchema := arrowimpl.NewArrowDataFrameSchema(outputArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	defaultFUserCross := func(lRow, rRow df.Row) []df.Row {
		vals := make([]df.Value, 0, outputDfSchema.Len())
		vals = append(vals, lRow.GetByName("id_l"))
		vals = append(vals, lRow.GetByName("val_l"))
		vals = append(vals, rRow.GetByName("id_r"))
		vals = append(vals, rRow.GetByName("val_r"))
		return []df.Row{arrowimpl.NewArrowRowFromValues(outputDfSchema.(*arrowimpl.ArrowDataFrameSchema), vals)}
	}

	t.Run("BasicCrossJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2"}, nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldf := arrowimpl.NewArrowDataFrame("ldf_cross_basic", lRec, dfSchemaLeft)
		defer ldf.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R10", "R20", "R30"}, nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdf := arrowimpl.NewArrowDataFrame("rdf_cross_basic", rRec, dfSchemaRight)
		defer rdf.(df.Releaser).Release()

		joinResult := ldf.Join(outputDfSchema, rdf, df.JoinCross, map[string]string{}, defaultFUserCross)
		defer joinResult.(df.Releaser).Release()

		assert.Equal(t, ldf.Len()*rdf.Len(), joinResult.Len(), "Cross join length should be L.Len * R.Len")

		expectedData := [][]interface{}{
			{int64(1), "L1", int64(10), "R10"}, {int64(1), "L1", int64(20), "R20"}, {int64(1), "L1", int64(30), "R30"},
			{int64(2), "L2", int64(10), "R10"}, {int64(2), "L2", int64(20), "R20"}, {int64(2), "L2", int64(30), "R30"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_LeftEmpty_CrossJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_cross_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1},nil)
		rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_cross_lnonempty_r", rRec, dfSchemaRight)
		defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfNonEmpty, df.JoinCross, map[string]string{}, defaultFUserCross)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_RightEmpty_CrossJoin", func(t *testing.T) {
		lrb := array.NewRecordBuilder(mem, schemaLeft); defer lrb.Release()
		lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1},nil)
		lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1"},nil)
		lRec := lrb.NewRecord(); defer lRec.Release()
		ldfNonEmpty := arrowimpl.NewArrowDataFrame("ldf_cross_rempty_l", lRec, dfSchemaLeft)
		defer ldfNonEmpty.(df.Releaser).Release()

		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_cross_rempty", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfNonEmpty.Join(outputDfSchema, rdfEmpty, df.JoinCross, map[string]string{}, defaultFUserCross)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})

	t.Run("EdgeCase_BothEmpty_CrossJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_cross_bothempty_l", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()
		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_cross_bothempty_r", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(outputDfSchema, rdfEmpty, df.JoinCross, map[string]string{}, defaultFUserCross)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, outputDfSchema.Equals(joinResult.Schema()))
	})
}
func TestDataFrame_Join_LeftAnti(t *testing.T) {
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

	// Base Data for ldf (used in multiple subtests)
	lrb_base := array.NewRecordBuilder(mem, schemaLeft); defer lrb_base.Release()
	lrb_base.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 0, 5}, []bool{true, true, true, true, false, true}) // id_l: 1, 2, 3, 4, NULL, 5
	lrb_base.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4", "L_nil", "L5_dup"}, nil)
	lRec_base := lrb_base.NewRecord(); defer lRec_base.Release()
	ldf_base := arrowimpl.NewArrowDataFrame("ldf_base_leftanti", lRec_base, dfSchemaLeft)
	// ldf_base is managed by Retain/Release in subtests that use it.

	joinColsMap := map[string]string{"id_l": "id_r"}

	t.Run("BasicLeftAntiJoin", func(t *testing.T) {
		ldf_base.Retain(); defer ldf_base.Release()
		rrb_basic := array.NewRecordBuilder(mem, schemaRight); defer rrb_basic.Release()
		rrb_basic.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 6, 0}, []bool{true, true, true, true, false}) // id_r: 1, 2, 2, 6, NULL
		rrb_basic.Field(1).(*array.StringBuilder).AppendValues([]string{"R1_match", "R2_match_a", "R2_match_b", "R6_nomatch", "R_nil_match"}, nil)
		rRec_basic := rrb_basic.NewRecord(); defer rRec_basic.Release()
		rdf_basic := arrowimpl.NewArrowDataFrame("rdf_leftanti_basic_r", rRec_basic, dfSchemaRight)
		defer rdf_basic.(df.Releaser).Release()

		joinResult := ldf_base.Join(dfSchemaLeft, rdf_basic, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(3), "L3"},
			{int64(4), "L4"},
			{nilPlaceholder, "L_nil"},
			{int64(5),"L5_dup"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.True(t, joinResult.Schema().Equals(dfSchemaLeft))
	})

	t.Run("EdgeCase_LeftEmpty_LeftAntiJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_leftanti_lempty", emptyLRec, dfSchemaLeft)
		defer ldfEmpty.(df.Releaser).Release()

		rrb := array.NewRecordBuilder(mem, schemaRight); defer rrb.Release()
		rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1},nil); rrb.Field(1).(*array.StringBuilder).AppendValues([]string{"R1"},nil)
		rRec := rrb.NewRecord(); defer rRec.Release()
		rdfNonEmpty := arrowimpl.NewArrowDataFrame("rdf_leftanti_rnonempty", rRec, dfSchemaRight); defer rdfNonEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(dfSchemaLeft, rdfNonEmpty, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
		assert.True(t, joinResult.Schema().Equals(dfSchemaLeft))
	})

	t.Run("EdgeCase_RightEmpty_LeftAntiJoin", func(t *testing.T) {
		ldf_base.Retain(); defer ldf_base.Release()
		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_leftanti_rempty", emptyRRec, dfSchemaRight)
		defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldf_base.Join(dfSchemaLeft, rdfEmpty, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		assert.Equal(t, ldf_base.Len(), joinResult.Len(), "All left rows should be kept if right is empty")
		expectedData := dfToSliceOfInterfaceSlices(ldf_base)
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("EdgeCase_BothEmpty_LeftAntiJoin", func(t *testing.T) {
		emptyLRec := array.NewRecord(schemaLeft, nil, 0); defer emptyLRec.Release()
		ldfEmpty := arrowimpl.NewArrowDataFrame("ldf_leftanti_bothempty_l", emptyLRec, dfSchemaLeft); defer ldfEmpty.(df.Releaser).Release()
		emptyRRec := array.NewRecord(schemaRight, nil, 0); defer emptyRRec.Release()
		rdfEmpty := arrowimpl.NewArrowDataFrame("rdf_leftanti_bothempty_r", emptyRRec, dfSchemaRight); defer rdfEmpty.(df.Releaser).Release()

		joinResult := ldfEmpty.Join(dfSchemaLeft, rdfEmpty, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len())
	})

	t.Run("EdgeCase_NoMatchingKeys_LeftAntiJoin", func(t *testing.T) {
		ldf_base.Retain(); defer ldf_base.Release()
		rrbNoMatch := array.NewRecordBuilder(mem, schemaRight); defer rrbNoMatch.Release()
		rrbNoMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{10,20},nil)
		rrbNoMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"R10","R20"},nil)
		rRecNoMatch := rrbNoMatch.NewRecord(); defer rRecNoMatch.Release()
		rdfNoMatch := arrowimpl.NewArrowDataFrame("rdf_leftanti_nomatch_r", rRecNoMatch, dfSchemaRight)
		defer rdfNoMatch.(df.Releaser).Release()

		joinResult := ldf_base.Join(dfSchemaLeft, rdfNoMatch, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		assert.Equal(t, ldf_base.Len(), joinResult.Len(), "All left rows should be kept if no keys match in right")
		expectedData := dfToSliceOfInterfaceSlices(ldf_base)
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("EdgeCase_NullsInJoinKeys_LeftAntiJoin", func(t *testing.T) {
		lrbNullL := array.NewRecordBuilder(mem, schemaLeft); defer lrbNullL.Release()
		lrbNullL.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3, 0}, []bool{true, false, true, false})
		lrbNullL.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_nil1", "L3", "L_nil2"}, nil)
		lRecNullL := lrbNullL.NewRecord(); defer lRecNullL.Release()
		ldfNull := arrowimpl.NewArrowDataFrame("ldf_leftanti_nullkeys_l", lRecNullL, dfSchemaLeft)
		defer ldfNull.(df.Releaser).Release()

		rrbNullR := array.NewRecordBuilder(mem, schemaRight); defer rrbNullR.Release()
		rrbNullR.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 10}, []bool{false, true, true})
		rrbNullR.Field(1).(*array.StringBuilder).AppendValues([]string{"R_nil", "R3", "R10"}, nil)
		rRecNullR := rrbNullR.NewRecord(); defer rRecNullR.Release()
		rdfNull := arrowimpl.NewArrowDataFrame("rdf_leftanti_nullkeys_r", rRecNullR, dfSchemaRight)
		defer rdfNull.(df.Releaser).Release()

		joinResult := ldfNull.Join(dfSchemaLeft, rdfNull, df.JoinLeftAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1"},
			{nilPlaceholder, "L_nil1"},
			{nilPlaceholder, "L_nil2"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(3), joinResult.Len())
	})
	ldf_base.Release()
}

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

	t.Run("EdgeCase_NoMatchingKeys_LeftSemiJoin", func(t *testing.T) {
		ldf.Retain(); defer ldf.Release() // ldf is from the outer scope of TestDataFrame_Join_LeftSemi

		rrbNoMatch := array.NewRecordBuilder(mem, schemaRight); defer rrbNoMatch.Release()
		rrbNoMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{10,20},nil)
		rrbNoMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"R10","R20"},nil)
		rRecNoMatch := rrbNoMatch.NewRecord(); defer rRecNoMatch.Release()
		rdfNoMatch := arrowimpl.NewArrowDataFrame("rdf_leftsemi_nomatch", rRecNoMatch, dfSchemaRight)
		defer rdfNoMatch.(df.Releaser).Release()

		joinResult := ldf.Join(dfSchemaLeft, rdfNoMatch, df.JoinLeftSemi, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len(), "No rows should be kept if no keys match in right for LeftSemi")
	})

	t.Run("EdgeCase_NullsInJoinKeys_LeftSemiJoin", func(t *testing.T) {
		lrbNull := array.NewRecordBuilder(mem, schemaLeft); defer lrbNull.Release()
		lrbNull.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3, 0}, []bool{true, false, true, false})
		lrbNull.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L_nil_key1", "L3", "L_nil_key2"}, nil)
		lRecNull := lrbNull.NewRecord(); defer lRecNull.Release()
		ldfNull := arrowimpl.NewArrowDataFrame("ldf_leftsemi_null", lRecNull, dfSchemaLeft)
		defer ldfNull.(df.Releaser).Release()

		rrbNull := array.NewRecordBuilder(mem, schemaRight); defer rrbNull.Release()
		rrbNull.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 1}, []bool{false, true, true})
		rrbNull.Field(1).(*array.StringBuilder).AppendValues([]string{"R_nil_key", "R3", "R1_again"}, nil)
		rRecNull_r := rrbNull.NewRecord(); defer rRecNull_r.Release()
		rdfNull := arrowimpl.NewArrowDataFrame("rdf_leftsemi_null_r", rRecNull_r, dfSchemaRight)
		defer rdfNull.(df.Releaser).Release()

		joinResult := ldfNull.Join(dfSchemaLeft, rdfNull, df.JoinLeftSemi, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "L1"},
			{int64(3), "L3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(2), joinResult.Len())
	})
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

	t.Run("EdgeCase_NoMatchingKeys_RightSemiJoin", func(t *testing.T) {
		ldf.Retain(); defer ldf.Release()
		rdf.Retain(); defer rdf.Release()

		lrbNoMatch := array.NewRecordBuilder(mem, schemaLeft); defer lrbNoMatch.Release()
		lrbNoMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{10,20},nil)
		lrbNoMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"L10","L20"},nil)
		lRecNoMatch := lrbNoMatch.NewRecord(); defer lRecNoMatch.Release()
		ldfNoMatch := arrowimpl.NewArrowDataFrame("ldf_rightsemi_nomatch", lRecNoMatch, dfSchemaLeft)
		defer ldfNoMatch.(df.Releaser).Release()

		joinResult := ldfNoMatch.Join(dfSchemaRight, rdf, df.JoinRightSemi, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()
		assert.Equal(t, int64(0), joinResult.Len(), "No right rows should be kept if no keys match in left for RightSemi")
	})

	t.Run("EdgeCase_NullsInJoinKeys_RightSemiJoin", func(t *testing.T) {
		lrbNullL := array.NewRecordBuilder(mem, schemaLeft); defer lrbNullL.Release()
		lrbNullL.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3, 1}, []bool{false, true, true})
		lrbNullL.Field(1).(*array.StringBuilder).AppendValues([]string{"L_nil", "L3", "L1"}, nil)
		lRecNullL := lrbNullL.NewRecord(); defer lRecNullL.Release()
		ldfNull := arrowimpl.NewArrowDataFrame("ldf_rightsemi_null_l", lRecNullL, dfSchemaLeft)
		defer ldfNull.(df.Releaser).Release()

		rrbNullR := array.NewRecordBuilder(mem, schemaRight); defer rrbNullR.Release()
		rrbNullR.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3, 0}, []bool{true, false, true, false})
		rrbNullR.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R_nil_key1", "R3", "R_nil_key2"}, nil)
		rRecNull_r := rrbNullR.NewRecord(); defer rRecNull_r.Release()
		rdfNull_r := arrowimpl.NewArrowDataFrame("rdf_rightsemi_null_r", rRecNull_r, dfSchemaRight)
		defer rdfNull_r.(df.Releaser).Release()

		joinResult := ldfNull.Join(dfSchemaRight, rdfNull_r, df.JoinRightSemi, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "R1"},
			{int64(3), "R3"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(2), joinResult.Len())
	})
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

	t.Run("EdgeCase_NoMatchingKeys_RightAntiJoin", func(t *testing.T) {
		rdf1.Retain(); defer rdf1.Release() // rdf1 is the right table from the base setup of TestDataFrame_Join_RightAnti

		lrbNoMatch := array.NewRecordBuilder(mem, schemaLeft); defer lrbNoMatch.Release()
		lrbNoMatch.Field(0).(*array.Int64Builder).AppendValues([]int64{100,200},nil)
		lrbNoMatch.Field(1).(*array.StringBuilder).AppendValues([]string{"L100","L200"},nil)
		lRecNoMatch := lrbNoMatch.NewRecord(); defer lRecNoMatch.Release()
		ldfNoMatch := arrowimpl.NewArrowDataFrame("ldf_rightanti_nomatch_l", lRecNoMatch, dfSchemaLeft)
		defer ldfNoMatch.(df.Releaser).Release()

		joinResult := ldfNoMatch.Join(dfSchemaRight, rdf1, df.JoinRightAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		assert.Equal(t, rdf1.Len(), joinResult.Len(), "All right rows should be kept if no keys from left match")
		expectedData := dfToSliceOfInterfaceSlices(rdf1)
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
	})

	t.Run("EdgeCase_NullsInJoinKeys_RightAntiJoin", func(t *testing.T) {
		lrbNullL := array.NewRecordBuilder(mem, schemaLeft); defer lrbNullL.Release()
		lrbNullL.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 3}, []bool{false, true})
		lrbNullL.Field(1).(*array.StringBuilder).AppendValues([]string{"L_nil", "L3"}, nil)
		lRecNullL := lrbNullL.NewRecord(); defer lRecNullL.Release()
		ldfNull_l := arrowimpl.NewArrowDataFrame("ldf_rightanti_null_l", lRecNullL, dfSchemaLeft)
		defer ldfNull_l.(df.Releaser).Release()

		rrbNullR := array.NewRecordBuilder(mem, schemaRight); defer rrbNullR.Release()
		rrbNullR.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3, 0, 4}, []bool{true, false, true, false, true})
		rrbNullR.Field(1).(*array.StringBuilder).AppendValues([]string{"R1", "R_nil_key1", "R3", "R_nil_key2", "R4"}, nil)
		rRecNull_r := rrbNullR.NewRecord(); defer rRecNull_r.Release()
		rdfNull_r := arrowimpl.NewArrowDataFrame("rdf_rightanti_null_r", rRecNull_r, dfSchemaRight)
		defer rdfNull_r.(df.Releaser).Release()

		joinResult := ldfNull_l.Join(dfSchemaRight, rdfNull_r, df.JoinRightAnti, joinColsMap, nil)
		defer joinResult.(df.Releaser).Release()

		expectedData := [][]interface{}{
			{int64(1), "R1"},
			{nilPlaceholder, "R_nil_key1"},
			{nilPlaceholder, "R_nil_key2"},
			{int64(4), "R4"},
		}
		actualData := dfToSliceOfInterfaceSlices(joinResult)
		sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
		assert.Equal(t, expectedData, actualData)
		assert.Equal(t, int64(4), joinResult.Len())
	})
}


// --- Tests for newly implemented methods ---

func TestDataFrame_Rename(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_ForEachRow(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_UpdateSeries(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_AsFormat(t *testing.T) { /* ... existing ... */ }
func TestDataFrame_Select(t *testing.T) { /* ... existing ... */ }
