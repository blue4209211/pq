//go:build arrow

package arrow_test

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow" // Import the implementation package
)

// getTestArrowSchema is already defined in types_test.go, assuming it's accessible
// or redefine/import if necessary. For here, let's assume it's available or we make a local one.
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


	// Test error on column length mismatch
	shortIntBuilder := array.NewInt64Builder(mem)
	defer shortIntBuilder.Release()
	shortIntBuilder.AppendValue(5)
	colIntShort := shortIntBuilder.NewArray()
	defer colIntShort.Release()
	_, err = arrowimpl.NewArrowDataFrameFromArrays("len_mismatch", []arrow.Array{colStr, colIntShort, colFloat}, arrowSchema)
	assert.Error(t, err)

	// Test error on schema field count mismatch
	_, err = arrowimpl.NewArrowDataFrameFromArrays("field_count_mismatch", []arrow.Array{colStr, colInt}, arrowSchema)
	assert.Error(t, err)

	// Test error on type mismatch
	_, err = arrowimpl.NewArrowDataFrameFromArrays("type_mismatch", []arrow.Array{colStr, colStr, colFloat}, arrowSchema) // colInt replaced by colStr
	assert.Error(t, err)


	// Test with empty columns (but matching schema)
	emptyArrowSchema := arrow.NewSchema([]arrow.Field{{Name: "empty_col", Type: arrow.PrimitiveTypes.Int64}}, nil)
	emptyIntBuilder := array.NewInt64Builder(mem)
	defer emptyIntBuilder.Release()
	colEmptyInt := emptyIntBuilder.NewArray() // Zero length
	defer colEmptyInt.Release()
	dfEmpty, errEmpty := arrowimpl.NewArrowDataFrameFromArrays("empty_cols_df", []arrow.Array{colEmptyInt}, emptyArrowSchema)
	assert.NoError(t, errEmpty)
	assert.NotNil(t, dfEmpty)
	assert.Equal(t, int64(0), dfEmpty.Len())
}


func TestArrowDataFrame_Accessors(t *testing.T) {
	mem := memory.NewGoAllocator()
	arrowSchema := getTestDataFrameArrowSchema()
	record := getTestDataFrameRecord(mem, arrowSchema) // 3 rows, 3 cols
	defer record.Release()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)
	dfInstance := arrowimpl.NewArrowDataFrame("access_df", record, dfSchema)

	// Schema()
	assert.True(t, dfSchema.Equals(dfInstance.Schema()))

	// Len()
	assert.Equal(t, int64(3), dfInstance.Len())

	// Name()
	assert.Equal(t, "access_df", dfInstance.Name())

	// GetSeries()
	series0 := dfInstance.GetSeries(0)
	assert.Equal(t, "col_str", series0.Schema().Name)
	assert.Equal(t, df.StringFormat.Name(), series0.Schema().Format.Name())
	assert.Equal(t, int64(3), series0.Len())
	assert.Equal(t, "beta", series0.Get(1).GetAsString())

	series2 := dfInstance.GetSeries(2)
	assert.Equal(t, "col_float", series2.Schema().Name)
	assert.Equal(t, df.DoubleFormat.Name(), series2.Schema().Format.Name())
	assert.True(t, series2.Get(2).IsNil()) // gamma's float is nil

	assert.Panics(t, func() { dfInstance.GetSeries(-1) })
	assert.Panics(t, func() { dfInstance.GetSeries(3) })

	// GetSeriesByName()
	seriesInt := dfInstance.GetSeriesByName("col_int")
	assert.Equal(t, "col_int", seriesInt.Schema().Name)
	assert.True(t, seriesInt.Get(1).IsNil()) // beta's int is nil
	assert.Equal(t, int64(300), seriesInt.Get(2).GetAsInt())

	assert.Panics(t, func() { dfInstance.GetSeriesByName("non_existent") })

	// GetRow()
	row0 := dfInstance.GetRow(0)
	assert.Equal(t, 3, row0.Len())
	assert.Equal(t, "alpha", row0.GetAsString(0))
	assert.Equal(t, int64(100), row0.GetAsInt(1))
	assert.False(t, row0.IsAnyNil())

	row1 := dfInstance.GetRow(1)
	assert.True(t, row1.IsNil(1)) // col_int for beta is nil
	assert.True(t, row1.IsAnyNil())
	assert.Equal(t, 2.2, row1.GetAsDouble(2))

	assert.Panics(t, func() { dfInstance.GetRow(-1) })
	assert.Panics(t, func() { dfInstance.GetRow(3) })

	// GetValue()
	val_0_0 := dfInstance.GetValue(0,0) // alpha
	assert.Equal(t, "alpha", val_0_0.GetAsString())

	val_1_1 := dfInstance.GetValue(1,1) // beta, col_int (nil)
	assert.True(t, val_1_1.IsNil())

	val_2_2 := dfInstance.GetValue(2,2) // gamma, col_float (nil)
	assert.True(t, val_2_2.IsNil())

	val_2_0 := dfInstance.GetValue(2,0) // gamma, col_str
	assert.Equal(t, "gamma", val_2_0.GetAsString())


	assert.Panics(t, func() { dfInstance.GetValue(-1, 0)})
	assert.Panics(t, func() { dfInstance.GetValue(0, -1)})
	assert.Panics(t, func() { dfInstance.GetValue(3, 0)}) // Row out of bounds
	assert.Panics(t, func() { dfInstance.GetValue(0, 3)}) // Col out of bounds
}

// TODO: Add tests for df.go (This was the original comment in the file)
