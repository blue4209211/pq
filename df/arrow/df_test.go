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
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array { // Used by series_test, added here if df_test needs it too
	b := array.NewInt64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array { // Used by series_test, added here if df_test needs it too
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


func TestArrowDataFrame_GroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "cat1", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "cat2", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)

	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "A", "B", "", "A", ""}, []bool{true, true, true, true, true, false, true, false})
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 2, 1, 2, 1, 1, 0, 0}, []bool{true, true, true, true, true, true, false, false})
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{10.1, 20.2, 10.10, 30.3, 40.4, 50.5, 60.6, 70.7}, nil)
	record := rb.NewRecord(); defer record.Release()
	baseDf := arrowimpl.NewArrowDataFrame("groupby_test_df", record, dfSchema)
	// baseDf is retained by GroupBy calls, so its release is handled by the groupedDf's Release or end of this test.

	// Case 1: GroupBy "cat1"
	grouped1 := baseDf.GroupBy("cat1")
	agdf1, ok1 := grouped1.(*arrowimpl.ArrowGroupedDataFrame)
	assert.True(t, ok1); defer agdf1.Release()
	assert.Equal(t, []string{"cat1"}, agdf1.GetGroupColumns())
	assert.Equal(t, int64(3), agdf1.Len(), "Number of unique groups for cat1")

	// Case 2: GroupBy "cat1", "cat2"
	grouped2 := baseDf.GroupBy("cat1", "cat2")
	agdf2, ok2 := grouped2.(*arrowimpl.ArrowGroupedDataFrame)
	assert.True(t, ok2); defer agdf2.Release()
	assert.Equal(t, []string{"cat1", "cat2"}, agdf2.GetGroupColumns())
	assert.Equal(t, int64(7), agdf2.Len(), "Number of unique groups for (cat1, cat2)")

	// Case 3: GroupBy on empty DataFrame
	emptyRec := array.NewRecord(schema, nil, 0); defer emptyRec.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_groupby", emptyRec, dfSchema) // This df needs release
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	groupedEmpty := emptyDf.GroupBy("cat1")
	agdfEmpty, okEmpty := groupedEmpty.(*arrowimpl.ArrowGroupedDataFrame)
	assert.True(t, okEmpty); defer agdfEmpty.Release()
	assert.Equal(t, int64(0), agdfEmpty.Len(), "GroupBy on empty DF should have 0 groups")
	assert.Empty(t, agdfEmpty.GetKeys(), "GetKeys on empty GroupBy should be empty")

	// Case 4: Panic conditions
	assert.PanicsWithValue(t, "GroupBy requires at least one column name", func() { baseDf.GroupBy() })
	assert.Panics(t, func() { baseDf.GroupBy("cat1", "non_existent_col") }) // Panic message includes col name

	// Release the baseDf as its record was retained by the GroupBy calls and we are done with it here.
	baseDf.(*arrowimpl.ArrowDataFrame).Release()
}


// TODO: Add tests for df.go (This was the original comment in the file)
