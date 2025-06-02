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
	"github.com/blue4209211/pq/df/expr" // Assuming expression types are here or in df
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// --- (Existing helpers and tests) ---
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


func TestArrowDataFrame_GetSeriesExprByName(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Using a slightly different schema for this test to ensure all types are covered if needed
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "s", Type: arrow.BinaryTypes.String},
			{Name: "i", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f", Type: arrow.PrimitiveTypes.Float64},
			{Name: "b", Type: arrow.PrimitiveTypes.Boolean},
			{Name: "t", Type: arrow.TimestampTypes.Timestamp_ns},
		}, nil,
	)
	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).Append("a")
	rb.Field(1).(*array.Int64Builder).Append(1)
	rb.Field(2).(*array.Float64Builder).Append(1.0)
	rb.Field(3).(*array.BooleanBuilder).Append(true)
	rb.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixNano()))
	record := rb.NewRecord(); defer record.Release()

	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	baseDf := arrowimpl.NewArrowDataFrame("expr_df", record, dfSchema)
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()

	testCases := []struct {
		colName      string
		expectedType interface{} // Store the expected Go type of the expression struct
		assertFn     func(t *testing.T, e df.Expr)
	}{
		{"s", new(df.StringExpr), func(t *testing.T, e df.Expr) { _, ok := e.(df.StringExpr); assert.True(t, ok, "Expected StringExpr") }},
		{"i", new(df.IntExpr), func(t *testing.T, e df.Expr) { _, ok := e.(df.IntExpr); assert.True(t, ok, "Expected IntExpr") }},
		{"f", new(df.DoubleExpr), func(t *testing.T, e df.Expr) { _, ok := e.(df.DoubleExpr); assert.True(t, ok, "Expected DoubleExpr") }},
		{"b", new(df.BoolExpr), func(t *testing.T, e df.Expr) { _, ok := e.(df.BoolExpr); assert.True(t, ok, "Expected BoolExpr") }},
		{"t", new(df.DatetimeExpr), func(t *testing.T, e df.Expr) { _, ok := e.(df.DatetimeExpr); assert.True(t, ok, "Expected DatetimeExpr") }},
	}

	for _, tc := range testCases {
		t.Run(tc.colName, func(t *testing.T) {
			expr := baseDf.GetSeriesExprByName(tc.colName)
			assert.NotNil(t, expr)
			tc.assertFn(t, expr)
			// Check if the expression returned by GetSeriesExprByName also has Col() method populated
			// This depends on whether NewTYPEColExpr(name) is used vs NewTYPEExpr()
			// Current implementation uses NewTYPEColExpr(name) if available.
			// The mock df.Expr does not have a typed Col field, but the real one might.
			// For now, the type assertion is the main check.
			// If using constructors like df.NewStringColExpr(name), then expr.Col() should return sName.
			// The current code in arrowDataFrame.GetSeriesExprByName was updated to use df.NewTYPEColExpr(sName).
			assert.Equal(t, tc.colName, expr.Col(), "Expression's Col() method should return the column name")
		})
	}

	assert.PanicsWithValue(t, "series with name 'non_existent' not found for GetSeriesExprByName", func() {
		baseDf.GetSeriesExprByName("non_existent")
	})
}


// TODO: Add tests for df.go (This was the original comment in the file)
