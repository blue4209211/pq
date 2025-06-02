//go:build arrow

package arrow_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// getTestDataFrameArrowSchema is defined in previous tests for df_test.go
// For brevity, ensure it's available.
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

// Helper to create a sample record for DataFrame testing
func getTestDataFrameRecord(mem memory.Allocator, schema *arrow.Schema) arrow.Record {
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Row 1: "alpha", 100, 1.1
	// Row 2: "beta", nil, 2.2
	// Row 3: "gamma", 300, nil
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 0, 300}, []bool{true, false, true})    // 0 for beta is nil
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 0}, []bool{true, true, false}) // 0 for gamma is nil

	return b.NewRecord()
}

func TestArrowDataFrame_NewArrowDataFrame(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()

	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	dfInstance := arrowimpl.NewArrowDataFrame("test_df", record, dfSchema)
	assert.NotNil(t, dfInstance)
	assert.Equal(t, "test_df", dfInstance.Name())
	assert.Equal(t, int64(3), dfInstance.Len())
	assert.True(t, dfSchema.Equals(dfInstance.Schema()))

	// Test panic on nil record
	assert.Panics(t, func() {
		arrowimpl.NewArrowDataFrame("test_df_nil_rec", nil, dfSchema)
	})

	// Test panic on nil dfSchema
	assert.Panics(t, func() {
		arrowimpl.NewArrowDataFrame("test_df_nil_schema", record, nil)
	})

	// Test panic on schema mismatch
	 differentArrowSchema := arrow.NewSchema(
		[]arrow.Field{{Name: "another_col", Type: arrow.BinaryTypes.String}}, nil,
	)
	differentDfSchema := arrowimpl.NewArrowDataFrameSchema(differentArrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	assert.Panics(t, func() {
		arrowimpl.NewArrowDataFrame("mismatch_df", record, differentDfSchema)
	})

	// Test Release doesn't panic
	adf, ok := dfInstance.(*arrowimpl.ArrowDataFrame)
	assert.True(t, ok)
	assert.NotPanics(t, func() {
		adf.Release()
	})
	assert.NotPanics(t, func() { // Second release should be safe (idempotent)
		adf.Release()
	})
	assert.Equal(t, int64(0), adf.Len(), "Len should be 0 after release")


}

func TestArrowDataFrame_NewArrowDataFrameFromArrays(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()

	strBuilder := array.NewStringBuilder(mem)
	defer strBuilder.Release()
	strBuilder.AppendValues([]string{"x", "y"}, nil)
	colStr := strBuilder.NewArray()
	defer colStr.Release()

	intBuilder := array.NewInt64Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{10, 20}, nil)
	colInt := intBuilder.NewArray()
	defer colInt.Release()

	floatBuilder := array.NewFloat64Builder(mem)
	defer floatBuilder.Release()
	floatBuilder.AppendValues([]float64{1.5, 2.5}, nil)
	colFloat := floatBuilder.NewArray()
	defer colFloat.Release()

	cols := []arrow.Array{colStr, colInt, colFloat}

	dfInstance, err := arrowimpl.NewArrowDataFrameFromArrays("from_arrays_df", cols, arrowSchema)
	assert.NoError(t, err)
	assert.NotNil(t, dfInstance)
	assert.Equal(t, "from_arrays_df", dfInstance.Name())
	assert.Equal(t, int64(2), dfInstance.Len())
	assert.True(t, arrowSchema.Equal(dfInstance.Schema().(*arrowimpl.ArrowDataFrameSchema).InternalArrowSchema()), "Internal Arrow schemas should match")
	// Release dataframe
	dfInstance.(*arrowimpl.ArrowDataFrame).Release()


	// Test error on column length mismatch
	shortIntBuilder := array.NewInt64Builder(mem)
	defer shortIntBuilder.Release()
	shortIntBuilder.AppendValue(5)
	colIntShort := shortIntBuilder.NewArray()
	defer colIntShort.Release()
	// Re-create colStr and colFloat for this specific test case to avoid double release issues
	strBuilder2 := array.NewStringBuilder(mem); defer strBuilder2.Release(); strBuilder2.AppendValues([]string{"x", "y"}, nil); colStr2 := strBuilder2.NewArray(); defer colStr2.Release()
	floatBuilder2 := array.NewFloat64Builder(mem); defer floatBuilder2.Release(); floatBuilder2.AppendValues([]float64{1.5, 2.5}, nil); colFloat2 := floatBuilder2.NewArray(); defer colFloat2.Release()
	_, err = arrowimpl.NewArrowDataFrameFromArrays("len_mismatch", []arrow.Array{colStr2, colIntShort, colFloat2}, arrowSchema)
	assert.Error(t, err)


	// Test error on schema field count mismatch
	strBuilder3 := array.NewStringBuilder(mem); defer strBuilder3.Release(); strBuilder3.AppendValues([]string{"x", "y"}, nil); colStr3 := strBuilder3.NewArray(); defer colStr3.Release()
	intBuilder3 := array.NewInt64Builder(mem); defer intBuilder3.Release(); intBuilder3.AppendValues([]int64{10,20}, nil); colInt3 := intBuilder3.NewArray(); defer colInt3.Release()
	_, err = arrowimpl.NewArrowDataFrameFromArrays("field_count_mismatch", []arrow.Array{colStr3, colInt3}, arrowSchema)
	assert.Error(t, err)

	// Test error on type mismatch
	strBuilder4 := array.NewStringBuilder(mem); defer strBuilder4.Release(); strBuilder4.AppendValues([]string{"x", "y"}, nil); colStr4_1 := strBuilder4.NewArray(); defer colStr4_1.Release()
	strBuilder5 := array.NewStringBuilder(mem); defer strBuilder5.Release(); strBuilder5.AppendValues([]string{"a", "b"}, nil); colStr4_2 := strBuilder5.NewArray(); defer colStr4_2.Release()
	floatBuilder4 := array.NewFloat64Builder(mem); defer floatBuilder4.Release(); floatBuilder4.AppendValues([]float64{1.5, 2.5}, nil); colFloat4 := floatBuilder4.NewArray(); defer colFloat4.Release()
	_, err = arrowimpl.NewArrowDataFrameFromArrays("type_mismatch", []arrow.Array{colStr4_1, colStr4_2, colFloat4}, arrowSchema)
	assert.Error(t, err)


	// Test with empty columns (but matching schema)
	emptyArrowSchema := arrow.NewSchema([]arrow.Field{{Name: "empty_col", Type: arrow.PrimitiveTypes.Int64}}, nil)
	emptyIntBuilder := array.NewInt64Builder(mem)
	defer emptyIntBuilder.Release()
	colEmptyInt := emptyIntBuilder.NewArray()
	defer colEmptyInt.Release()
	dfEmpty, errEmpty := arrowimpl.NewArrowDataFrameFromArrays("empty_cols_df", []arrow.Array{colEmptyInt}, emptyArrowSchema)
	assert.NoError(t, errEmpty)
	assert.NotNil(t, dfEmpty)
	assert.Equal(t, int64(0), dfEmpty.Len())
	dfEmpty.(*arrowimpl.ArrowDataFrame).Release()
}


func TestArrowDataFrame_Accessors(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	dfInstance := arrowimpl.NewArrowDataFrame("access_df", record, dfSchema)
	defer dfInstance.(*arrowimpl.ArrowDataFrame).Release()

	// Schema()
	assert.True(t, dfSchema.Equals(dfInstance.Schema()))

	// Len()
	assert.Equal(t, int64(3), dfInstance.Len())

	// Name()
	assert.Equal(t, "access_df", dfInstance.Name())

	// GetSeries()
	series0 := dfInstance.GetSeries(0)
	defer series0.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, "col_str", series0.Schema().Name)
	assert.Equal(t, df.StringFormat.Name(), series0.Schema().Format.Name())
	assert.Equal(t, int64(3), series0.Len())
	assert.Equal(t, "beta", series0.Get(1).GetAsString())

	series2 := dfInstance.GetSeries(2)
	defer series2.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, "col_float", series2.Schema().Name)
	assert.Equal(t, df.DoubleFormat.Name(), series2.Schema().Format.Name())
	assert.True(t, series2.Get(2).IsNil())

	assert.Panics(t, func() { dfInstance.GetSeries(-1) })
	assert.Panics(t, func() { dfInstance.GetSeries(3) })

	// GetSeriesByName()
	seriesInt := dfInstance.GetSeriesByName("col_int")
	defer seriesInt.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, "col_int", seriesInt.Schema().Name)
	assert.True(t, seriesInt.Get(1).IsNil())
	assert.Equal(t, int64(300), seriesInt.Get(2).GetAsInt())

	assert.Panics(t, func() { dfInstance.GetSeriesByName("non_existent") })

	// GetRow()
	row0 := dfInstance.GetRow(0)
	assert.Equal(t, 3, row0.Len())
	assert.Equal(t, "alpha", row0.GetAsString(0))
	assert.Equal(t, int64(100), row0.GetAsInt(1))
	assert.False(t, row0.IsAnyNil())

	row1 := dfInstance.GetRow(1)
	assert.True(t, row1.IsNil(1))
	assert.True(t, row1.IsAnyNil())
	assert.Equal(t, 2.2, row1.GetAsDouble(2))

	assert.Panics(t, func() { dfInstance.GetRow(-1) })
	assert.Panics(t, func() { dfInstance.GetRow(3) })

	// GetValue()
	val_0_0 := dfInstance.GetValue(0,0)
	assert.Equal(t, "alpha", val_0_0.GetAsString())

	val_1_1 := dfInstance.GetValue(1,1)
	assert.True(t, val_1_1.IsNil())

	val_2_2 := dfInstance.GetValue(2,2)
	assert.True(t, val_2_2.IsNil())

	val_2_0 := dfInstance.GetValue(2,0)
	assert.Equal(t, "gamma", val_2_0.GetAsString())


	assert.Panics(t, func() { dfInstance.GetValue(-1, 0)})
	assert.Panics(t, func() { dfInstance.GetValue(0, -1)})
	assert.Panics(t, func() { dfInstance.GetValue(3, 0)})
	assert.Panics(t, func() { dfInstance.GetValue(0, 3)})
}

func TestArrowDataFrame_Limit(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()

	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("limit_test_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Basic limit
	limited1 := baseDf.Limit(1, 1)
	defer limited1.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(1), limited1.Len())
	assert.Equal(t, "beta", limited1.GetValue(0,0).GetAsString())
	assert.True(t, limited1.GetValue(0,1).IsNil())
	assert.Equal(t, 2.2, limited1.GetValue(0,2).GetAsDouble())
	assert.True(t, baseDf.Schema().Equals(limited1.Schema()), "Schema should be preserved")

	// Case 2: Offset 0, size > num_rows
	limited2 := baseDf.Limit(0, 5)
	defer limited2.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), limited2.Len())
	assert.Equal(t, "alpha", limited2.GetValue(0,0).GetAsString())

	// Case 3: Offset out of bounds
	limited3 := baseDf.Limit(5, 2)
	defer limited3.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), limited3.Len())
	assert.Equal(t, baseDf.Schema().Len(), limited3.Schema().Len(), "Schema (cols) should be preserved even if empty")


	// Case 4: Size = 0
	limited4 := baseDf.Limit(1, 0)
	defer limited4.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), limited4.Len())
	assert.Equal(t, baseDf.Schema().Len(), limited4.Schema().Len())

	// Case 5: Negative offset (treated as 0)
	limited5 := baseDf.Limit(-2, 2)
	defer limited5.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(2), limited5.Len())
	assert.Equal(t, "alpha", limited5.GetValue(0,0).GetAsString())
	assert.Equal(t, "beta", limited5.GetValue(1,0).GetAsString())

	// Case 6: Limit on an empty DataFrame (0 rows, but schema exists)
	emptyRecord := array.NewRecord(arrowSchema, nil, 0)
	defer emptyRecord.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_df", emptyRecord, dfSchema)
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	limitedEmpty := emptyDf.Limit(0, 5)
	defer limitedEmpty.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), limitedEmpty.Len())
	assert.Equal(t, arrowSchema.NumFields(), limitedEmpty.Schema().Len())
}

func TestArrowDataFrame_SelectBySeriesIndex(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()

	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("selectidx_test_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Select subset of columns (int, str) -> indices 1, 0
	selected1 := baseDf.SelectBySeriesIndex(1, 0)
	defer selected1.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), selected1.Len(), "Number of rows should be preserved")
	assert.Equal(t, 2, selected1.Schema().Len())
	assert.Equal(t, "col_int", selected1.Schema().Get(0).Name)
	assert.Equal(t, "col_str", selected1.Schema().Get(1).Name)
	assert.Equal(t, int64(100), selected1.GetValue(0,0).GetAsInt())
	assert.Equal(t, "alpha", selected1.GetValue(0,1).GetAsString())

	// Case 2: Select single column
	selected2 := baseDf.SelectBySeriesIndex(2)
	defer selected2.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), selected2.Len())
	assert.Equal(t, 1, selected2.Schema().Len())
	assert.Equal(t, "col_float", selected2.Schema().Get(0).Name)
	assert.Equal(t, 1.1, selected2.GetValue(0,0).GetAsDouble())

	// Case 3: Empty list of indices
	selected3 := baseDf.SelectBySeriesIndex()
	defer selected3.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), selected3.Len(), "Num rows preserved for empty selection")
	assert.Equal(t, 0, selected3.Schema().Len(), "Schema should have 0 columns")


	// Case 4: Panic on out-of-bounds index
	assert.Panics(t, func() { baseDf.SelectBySeriesIndex(0, 3) })
	assert.Panics(t, func() { baseDf.SelectBySeriesIndex(-1) })

	// Case 5: Select on an empty DataFrame (0 rows, but schema exists)
	emptyRecord := array.NewRecord(arrowSchema, nil, 0)
	defer emptyRecord.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_df_select", emptyRecord, dfSchema)
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	selectedEmpty := emptyDf.SelectBySeriesIndex(0, 1)
	defer selectedEmpty.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), selectedEmpty.Len())
	assert.Equal(t, 2, selectedEmpty.Schema().Len())
	assert.Equal(t, "col_str", selectedEmpty.Schema().Get(0).Name)
}


func TestArrowDataFrame_SelectBySeriesName(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("selectname_test_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Select subset ("col_float", "col_str")
	selected1 := baseDf.SelectBySeriesName("col_float", "col_str")
	defer selected1.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), selected1.Len())
	assert.Equal(t, 2, selected1.Schema().Len())
	assert.Equal(t, "col_float", selected1.Schema().Get(0).Name)
	assert.Equal(t, "col_str", selected1.Schema().Get(1).Name)
	assert.Equal(t, 1.1, selected1.GetValue(0,0).GetAsDouble())
	assert.Equal(t, "gamma", selected1.GetValue(2,1).GetAsString())

	// Case 2: Empty list of names
	selected2 := baseDf.SelectBySeriesName()
	defer selected2.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(3), selected2.Len())
	assert.Equal(t, 0, selected2.Schema().Len())

	// Case 3: Panic on non-existent name
	assert.Panics(t, func() { baseDf.SelectBySeriesName("col_str", "non_existent_col") })
}


func TestArrowDataFrame_WhereRow(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema)
	defer record.Release()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("where_test_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Filter rows where col_int is not nil
	filterIntNotNil := func(r df.Row) bool {
		return !r.IsNil(1)
	}
	filtered1 := baseDf.WhereRow(filterIntNotNil)
	defer filtered1.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(2), filtered1.Len())
	assert.Equal(t, "alpha", filtered1.GetValue(0,0).GetAsString())
	assert.Equal(t, int64(100), filtered1.GetValue(0,1).GetAsInt())
	assert.Equal(t, "gamma", filtered1.GetValue(1,0).GetAsString())
	assert.Equal(t, int64(300), filtered1.GetValue(1,1).GetAsInt())
	assert.True(t, baseDf.Schema().Equals(filtered1.Schema()), "Schema should be preserved")


	// Case 2: Filter rows where col_str is "beta"
	filterStrIsBeta := func(r df.Row) bool {
		return !r.IsNil(0) && r.GetAsString(0) == "beta"
	}
	filtered2 := baseDf.WhereRow(filterStrIsBeta)
	defer filtered2.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(1), filtered2.Len())
	assert.Equal(t, "beta", filtered2.GetValue(0,0).GetAsString())
	assert.True(t, filtered2.GetValue(0,1).IsNil())
	assert.Equal(t, 2.2, filtered2.GetValue(0,2).GetAsDouble())

	// Case 3: Predicate matches no rows
	filterMatchesNone := func(r df.Row) bool { return false }
	filtered3 := baseDf.WhereRow(filterMatchesNone)
	defer filtered3.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), filtered3.Len())
	assert.Equal(t, baseDf.Schema().Len(), filtered3.Schema().Len())

	// Case 4: Predicate matches all rows
	filterMatchesAll := func(r df.Row) bool { return true }
	filtered4 := baseDf.WhereRow(filterMatchesAll)
	defer filtered4.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, baseDf.Len(), filtered4.Len())
	assert.Equal(t, "gamma", filtered4.GetValue(2,0).GetAsString())


	// Case 5: Filter on an empty DataFrame
	emptyRecord := array.NewRecord(arrowSchema, nil, 0)
	defer emptyRecord.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_where_df", emptyRecord, dfSchema)
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	filteredEmpty := emptyDf.WhereRow(filterMatchesAll)
	defer filteredEmpty.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), filteredEmpty.Len())
	assert.Equal(t, arrowSchema.NumFields(), filteredEmpty.Schema().Len())
}

func TestArrowDataFrame_Sort(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil,
	)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma", "alpha", "beta"}, nil)
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 0, 300, 50, 200}, []bool{true, false, true, true, true})
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 0.5, 3.3, 1.1}, nil)
	record := rb.NewRecord()
	defer record.Release()

	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("sort_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	// Case 1: Sort by "col_int" (idx 1) ASC. Nils first.
	sorted1 := baseDf.Sort(df.SortByIndex{Series: 1, Order: df.SortOrderASC})
	defer sorted1.(*arrowimpl.ArrowDataFrame).Release()

	assert.Equal(t, baseDf.Len(), sorted1.Len())
	assert.True(t, sorted1.GetValue(0, 1).IsNil(), "Row 0, col_int nil")
	assert.Equal(t, "beta", sorted1.GetValue(0, 0).GetAsString())
	assert.Equal(t, int64(50), sorted1.GetValue(1, 1).GetAsInt())
	assert.Equal(t, "alpha", sorted1.GetValue(1, 0).GetAsString())
	assert.Equal(t, int64(100), sorted1.GetValue(2, 1).GetAsInt())
	assert.Equal(t, "alpha", sorted1.GetValue(2, 0).GetAsString())
	assert.Equal(t, int64(200), sorted1.GetValue(3, 1).GetAsInt())
	assert.Equal(t, "beta", sorted1.GetValue(3, 0).GetAsString())
	assert.Equal(t, int64(300), sorted1.GetValue(4, 1).GetAsInt())
	assert.Equal(t, "gamma", sorted1.GetValue(4, 0).GetAsString())

	// Case 2: Sort by "col_str" (idx 0) ASC, then "col_int" (idx 1) DESC. (NullsFirst default)
	sorted2 := baseDf.Sort(
		df.SortByIndex{Series: 0, Order: df.SortOrderASC},
		df.SortByIndex{Series: 1, Order: df.SortOrderDESC},
	)
	defer sorted2.(*arrowimpl.ArrowDataFrame).Release()

	assert.Equal(t, "alpha", sorted2.GetValue(0,0).GetAsString())
	assert.Equal(t, int64(100), sorted2.GetValue(0,1).GetAsInt())
	assert.Equal(t, "alpha", sorted2.GetValue(1,0).GetAsString())
	assert.Equal(t, int64(50), sorted2.GetValue(1,1).GetAsInt())
	assert.Equal(t, "beta", sorted2.GetValue(2,0).GetAsString())
	assert.True(t, sorted2.GetValue(2,1).IsNil())
	assert.Equal(t, "beta", sorted2.GetValue(3,0).GetAsString())
	assert.Equal(t, int64(200), sorted2.GetValue(3,1).GetAsInt())
	assert.Equal(t, "gamma", sorted2.GetValue(4,0).GetAsString())
	assert.Equal(t, int64(300), sorted2.GetValue(4,1).GetAsInt())

	// Case 3: SortByName
	sorted3 := baseDf.SortByName(df.SortByName{Series: "col_float", Order: df.SortOrderASC})
	defer sorted3.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, 0.5, sorted3.GetValue(0,2).GetAsDouble())
	assert.Equal(t, 1.1, sorted3.GetValue(1,2).GetAsDouble())
	assert.Equal(t, 1.1, sorted3.GetValue(2,2).GetAsDouble())
	assert.Equal(t, 2.2, sorted3.GetValue(3,2).GetAsDouble())
	assert.Equal(t, 3.3, sorted3.GetValue(4,2).GetAsDouble())

	// Case 4: Sort empty DataFrame
	emptyRecord := array.NewRecord(schema, nil, 0)
	defer emptyRecord.Release()
	emptyDf := arrowimpl.NewArrowDataFrame("empty_sort_df", emptyRecord, dfSchema)
	defer emptyDf.(*arrowimpl.ArrowDataFrame).Release()

	sortedEmpty := emptyDf.Sort(df.SortByIndex{Series: 0, Order: df.SortOrderASC})
	defer sortedEmpty.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), sortedEmpty.Len())

	// Case 5: Sort with no orders specified
	sortedNoOrders := baseDf.Sort()
	defer sortedNoOrders.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, baseDf.Len(), sortedNoOrders.Len())
	assert.Equal(t, "alpha", sortedNoOrders.GetValue(0,0).GetAsString())
	assert.Equal(t, int64(100), sortedNoOrders.GetValue(0,1).GetAsInt())

	// Case 6: Panic on invalid index for Sort
	assert.Panics(t, func() { baseDf.Sort(df.SortByIndex{Series: 10, Order: df.SortOrderASC}) })

	// Case 7: Panic on invalid name for SortByName
	assert.Panics(t, func() { baseDf.SortByName(df.SortByName{Series: "non_existent_col", Order: df.SortOrderASC}) })
}

// TODO: Add tests for df.go (This was the original comment in the file)
