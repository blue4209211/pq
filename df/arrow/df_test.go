//go:build arrow

package arrow_test

import (
	"fmt"
	"sort"
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

// --- Helper functions from previous tests ---
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

const nilPlaceholder = "__NIL_PLACEHOLDER__"

func dfToSliceOfInterfaceSlices(dataFrame df.DataFrame) [][]interface{} {
	var result [][]interface{}
	for r := int64(0); r < dataFrame.Len(); r++ {
		row := dataFrame.GetRow(r)
		var rowData []interface{}
		for c := 0; c < row.Len(); c++ {
			val := row.Get(c)
			if val.IsNil() {
				rowData = append(rowData, nilPlaceholder)
			} else {
				rowData = append(rowData, val.Get())
			}
		}
		result = append(result, rowData)
	}
	return result
}

func sortSliceOfInterfaceSlices(slice [][]interface{}) {
	sort.Slice(slice, func(i, j int) bool {
		return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j])
	})
}

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


func TestArrowDataFrame_Union(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, nil,
	)
	dfSchema1 := arrowimpl.NewArrowDataFrameSchema(schema1).(*arrowimpl.ArrowDataFrameSchema)

	rb1 := array.NewRecordBuilder(mem, schema1); defer rb1.Release()
	rb1.Field(0).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "alpha"}, nil)
	rb1.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 0, 10}, []bool{true, false, true})
	rec1 := rb1.NewRecord(); defer rec1.Release()
	df1 := arrowimpl.NewArrowDataFrame("df1", rec1, dfSchema1)
	defer df1.(*arrowimpl.ArrowDataFrame).Release()

	rb2 := array.NewRecordBuilder(mem, schema1); defer rb2.Release()
	rb2.Field(0).(*array.StringBuilder).AppendValues([]string{"beta", "gamma", "delta"}, nil)
	rb2.Field(1).(*array.Int64Builder).AppendValues([]int64{0, 30, 40}, []bool{false, true, true})
	rec2 := rb2.NewRecord(); defer rec2.Release()
	df2 := arrowimpl.NewArrowDataFrame("df2", rec2, dfSchema1)
	defer df2.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Union of df1 and df2
	union1 := df1.Union(df2); defer union1.(*arrowimpl.ArrowDataFrame).Release()
	expectedData1 := [][]interface{}{ {"alpha", int64(10)}, {"beta", nilPlaceholder}, {"gamma", int64(30)}, {"delta", int64(40)}, }
	actualData1 := dfToSliceOfInterfaceSlices(union1)
	sortSliceOfInterfaceSlices(expectedData1); sortSliceOfInterfaceSlices(actualData1)
	assert.Equal(t, len(expectedData1), int(union1.Len()), "Case 1: Length check")
	assert.True(t, df1.Schema().Equals(union1.Schema()), "Case 1: Schema check")
	assert.Equal(t, expectedData1, actualData1, "Case 1: Data check")

	// Case 2: Union where one DataFrame is a subset
	rb3 := array.NewRecordBuilder(mem, schema1); defer rb3.Release()
	rb3.Field(0).(*array.StringBuilder).AppendValue("alpha")
	rb3.Field(1).(*array.Int64Builder).AppendValue(10)
	rec3 := rb3.NewRecord(); defer rec3.Release()
	df3 := arrowimpl.NewArrowDataFrame("df3", rec3, dfSchema1); defer df3.(*arrowimpl.ArrowDataFrame).Release()
	union2 := df1.Union(df3); defer union2.(*arrowimpl.ArrowDataFrame).Release()
	expectedData2 := [][]interface{}{ {"alpha", int64(10)}, {"beta", nilPlaceholder}, }
	actualData2 := dfToSliceOfInterfaceSlices(union2)
	sortSliceOfInterfaceSlices(expectedData2); sortSliceOfInterfaceSlices(actualData2)
	assert.Equal(t, len(expectedData2), int(union2.Len()), "Case 2: Length check")
	assert.Equal(t, expectedData2, actualData2, "Case 2: Data check")

	// Case 3: Union with an empty DataFrame
	emptyRec := array.NewRecord(schema1, nil, 0); defer emptyRec.Release()
	dfEmpty := arrowimpl.NewArrowDataFrame("empty", emptyRec, dfSchema1); defer dfEmpty.(*arrowimpl.ArrowDataFrame).Release()
	union3a := df1.Union(dfEmpty); defer union3a.(*arrowimpl.ArrowDataFrame).Release()
	actualData3a := dfToSliceOfInterfaceSlices(union3a); sortSliceOfInterfaceSlices(actualData3a)
	assert.Equal(t, len(expectedData2), int(union3a.Len()), "Case 3a: Length (df1 U empty)")
	assert.Equal(t, expectedData2, actualData3a, "Case 3a: Data (df1 U empty)")

	union3b := dfEmpty.Union(df1); defer union3b.(*arrowimpl.ArrowDataFrame).Release()
	actualData3b := dfToSliceOfInterfaceSlices(union3b); sortSliceOfInterfaceSlices(actualData3b)
	assert.Equal(t, len(expectedData2), int(union3b.Len()), "Case 3b: Length (empty U df1)")
	assert.Equal(t, expectedData2, actualData3b, "Case 3b: Data (empty U df1)")

	// Case 4: Union of two empty DataFrames
	union4 := dfEmpty.Union(dfEmpty); defer union4.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), union4.Len(), "Case 4: Length check")
	assert.True(t, dfEmpty.Schema().Equals(union4.Schema()), "Case 4: Schema check")

	// Case 5: Panic conditions
	assert.PanicsWithValue(t, "Union: other dataframe cannot be nil", func() { df1.Union(nil) }, "Case 5a: Panic on nil other DataFrame")

	schemaDiff := arrow.NewSchema([]arrow.Field{{Name: "diff_col", Type: arrow.BinaryTypes.String}}, nil)
	dfSchemaDiff := arrowimpl.NewArrowDataFrameSchema(schemaDiff).(*arrowimpl.ArrowDataFrameSchema)
	recDiff := array.NewRecord(schemaDiff, nil, 0); defer recDiff.Release()
	dfDiffSchema := arrowimpl.NewArrowDataFrame("diffSchema", recDiff, dfSchemaDiff);	defer dfDiffSchema.(*arrowimpl.ArrowDataFrame).Release()
	// The panic message will come from the underlying Append method.
	assert.Panics(t, func() { df1.Union(dfDiffSchema) }, "Case 5b: Panic on schema mismatch")
}

// TODO: Add tests for df.go (This was the original comment in the file)
