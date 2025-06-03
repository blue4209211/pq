//go:build arrow

package arrow_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar" // For creating Arrow values if needed in fUser etc.

	"github.com/blue4209211/pq/df"
	arrowimpl "github.com/blue4209211/pq/df/arrow"
	inmemory "github.com/blue4209211/pq/df/inmemory"
)

const (
	benchmarkNumRows      = 100_000 // Default number of rows for benchmarks
	benchmarkNumGroups    = 100     // Number of distinct groups for cat_string
	benchmarkJoinNumRowsL = 100_000
	benchmarkJoinNumRowsR = 50_000
)

var volatileDF df.DataFrame // To prevent compiler optimizing out benchmarked operations

// generateArrowData creates an arrowDataFrame for benchmarking.
// Schema: id_int (Int64), val_float (Float64), cat_string (String), key_join (Int64)
func generateArrowData(b *testing.B, numRows int, mem memory.Allocator) df.DataFrame {
	b.Helper() // Marks this as a benchmark helper function

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id_int", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "val_float", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "cat_string", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "key_join", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()
	valBuilder := array.NewFloat64Builder(mem)
	defer valBuilder.Release()
	catBuilder := array.NewStringBuilder(mem)
	defer catBuilder.Release()
	keyJoinBuilder := array.NewInt64Builder(mem)
	defer keyJoinBuilder.Release()

	randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numRows; i++ {
		idBuilder.Append(int64(i))
		valBuilder.Append(randSrc.Float64() * 1000)
		catBuilder.Append(fmt.Sprintf("group_%d", i%benchmarkNumGroups))
		keyJoinBuilder.Append(int64(randSrc.Intn(numRows / 2))) // Ensure some key overlap for joins
	}

	cols := []arrow.Array{
		idBuilder.NewArray(),
		valBuilder.NewArray(),
		catBuilder.NewArray(),
		keyJoinBuilder.NewArray(),
	}
	// Release arrays after record takes ownership (or if record creation fails)
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	record := array.NewRecord(schema, cols, int64(numRows))
	// NewArrowDataFrameWithAllocator will retain the record. We created it, so we must release it.
	defer record.Release()

	return arrowimpl.NewArrowDataFrameWithAllocator("benchmark_arrow_df", record, dfSchema, mem)
}

// generateInMemoryData creates an inmemoryDataFrame for benchmarking.
// Schema: id_int (Int64), val_float (Float64), cat_string (String), key_join (Int64)
func generateInMemoryData(b *testing.B, numRows int) df.DataFrame {
	b.Helper()

	schema := df.NewSchemaDetFromMap("benchmark_inmem_df", map[string]df.Format{
		"id_int":     df.IntegerFormat,
		"val_float":  df.DoubleFormat,
		"cat_string": df.StringFormat,
		"key_join":   df.IntegerFormat,
	})

	rows := make([]df.Row, numRows)
	randSrc := rand.New(rand.NewSource(time.Now().UnixNano())) // Use a fixed seed if exact same data is critical across calls

	for i := 0; i < numRows; i++ {
		rowValues := []df.Value{
			inmemory.NewIntValueConst(int64(i)),
			inmemory.NewDoubleValueConst(randSrc.Float64() * 1000),
			inmemory.NewStringValueConst(fmt.Sprintf("group_%d", i%benchmarkNumGroups)),
			inmemory.NewIntValueConst(int64(randSrc.Intn(numRows / 2))),
		}
		rows[i] = inmemory.NewRow(&schema, &rowValues)
	}
	return inmemory.NewDataframeFromRows(schema.Name(), schema, rows)
}

// --- Filter Benchmarks ---

func BenchmarkArrow_Filter(b *testing.B) {
	mem := memory.NewGoAllocator()
	arrowDf := generateArrowData(b, benchmarkNumRows, mem)
	defer arrowDf.(df.Releaser).Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Example filter: id_int > benchmarkNumRows / 2
		// The WhereRow implementation for arrowDataFrame will use compute kernels if possible.
		filtered := arrowDf.WhereRow(func(r df.Row) bool {
			return r.Get(0).GetAsInt() > int64(benchmarkNumRows/2)
		})
		// Ensure the result is used and released to avoid optimizing away and memory leaks
		volatileDF = filtered
		if volatileDF != nil {
			volatileDF.(df.Releaser).Release()
		}
	}
}

func BenchmarkInMemory_Filter(b *testing.B) {
	inMemDf := generateInMemoryData(b, benchmarkNumRows)
	// No explicit release needed for in-memory df normally, GC handles it.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filtered := inMemDf.WhereRow(func(r df.Row) bool {
			return r.Get(0).GetAsInt() > int64(benchmarkNumRows/2)
		})
		volatileDF = filtered
	}
}

// --- GroupBy and Aggregation (Count) Benchmarks ---

func BenchmarkArrow_GroupBy_AggCount(b *testing.B) {
	mem := memory.NewGoAllocator()
	arrowDf := generateArrowData(b, benchmarkNumRows, mem)
	defer arrowDf.(df.Releaser).Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		grouped := arrowDf.GroupBy("cat_string")
		aggConfig := []arrowimpl.AggregationConfig{
			{Func: "count", OutputColName: "count_res"},
		}
		aggregated := grouped.Agg(aggConfig...)

		volatileDF = aggregated
		if volatileDF != nil {
			volatileDF.(df.Releaser).Release()
		}
		if grouped != nil {
			grouped.(df.Releaser).Release()
		}
	}
}

func BenchmarkInMemory_GroupBy_AggCount(b *testing.B) {
	inMemDf := generateInMemoryData(b, benchmarkNumRows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		grouped := inMemDf.GroupBy("cat_string")
		// In-memory AggregationConfig might be different or uses a similar struct
		// Assuming a similar mechanism for defining aggregation.
		// The pq/df/inmemory implementation uses a map for aggregation:
		// map[string]map[string]string{"count": {"col": "out_col_name"}}
		// For simplicity, let's assume a compatible Agg function or adapt.
		// The actual inmemory.Agg takes: aggrFunctions map[string]string, aggrFunctionsOnCol map[string]map[string]string
		// For a simple count on groups, it's usually implicit or a specific call.
		// Let's use the structure from its own tests if available, or simplify.
		// The provided inmemory.GroupedDataFrame has Agg(map[string]map[string]string)
		// Example: Agg(map[string]map[string]string{"value": {"sum": "sum_value"}})
		// For count, it might be: Agg(map[string]map[string]string{"NONE": {"count": "count_res"}})
		// Or, if it follows df.AggregationType:
		aggResult := grouped.Agg(map[string]map[string]string{
			"NONE": {"count": "count_res"}, // This is how inmemory df does group-wise count
		})
		volatileDF = aggResult
	}
}

// --- Join (Inner Join) Benchmarks ---

func BenchmarkArrow_Join_Inner(b *testing.B) {
	mem := memory.NewGoAllocator()

	leftDf := generateArrowData(b, benchmarkJoinNumRowsL, mem)
	defer leftDf.(df.Releaser).Release()
	// For right table, use fewer rows and adjust key generation for realistic join scenarios
	// generateArrowData's key_join is rand.Intn(numRows/2).
	// To ensure matches with leftDf (numRowsL/2), rightDf's numRows should be numRowsL/2 or keys adjusted.
	// Let's make rightDf smaller and its keys target the lower half of leftDf's keys for good match probability.

	// Custom generation for right to control key overlap better for benchmark
	schemaRight := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id_int_r", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "val_float_r", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "cat_string_r", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "key_join_r", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, // This will be named "key_join" in map
		}, nil,
	)
	dfSchemaRight := arrowimpl.NewArrowDataFrameSchema(schemaRight).(*arrowimpl.ArrowDataFrameSchema)
	idBuilderR := array.NewInt64Builder(mem); defer idBuilderR.Release()
	valBuilderR := array.NewFloat64Builder(mem); defer valBuilderR.Release()
	catBuilderR := array.NewStringBuilder(mem); defer catBuilderR.Release()
	keyJoinBuilderR := array.NewInt64Builder(mem); defer keyJoinBuilderR.Release()
	randSrcR := rand.New(rand.NewSource(time.Now().UnixNano() + 1)) // Different seed

	for i := 0; i < benchmarkJoinNumRowsR; i++ {
		idBuilderR.Append(int64(i))
		valBuilderR.Append(randSrcR.Float64() * 100)
		catBuilderR.Append(fmt.Sprintf("group_R%d", i%benchmarkNumGroups))
		keyJoinBuilderR.Append(int64(randSrcR.Intn(benchmarkJoinNumRowsL / 2))) // Keys target 0 to L_rows/2-1
	}
	colsR := []arrow.Array{idBuilderR.NewArray(), valBuilderR.NewArray(), catBuilderR.NewArray(), keyJoinBuilderR.NewArray()}
	defer func() { for _, col := range colsR { col.Release() } }()
	recordR := array.NewRecord(schemaRight, colsR, int64(benchmarkJoinNumRowsR)); defer recordR.Release()
	rightDf := arrowimpl.NewArrowDataFrameWithAllocator("arrow_join_R", recordR, dfSchemaRight, mem)
	defer rightDf.(df.Releaser).Release()

	// Output schema for the join
	outputSchema := arrowimpl.NewArrowDataFrameSchema(arrow.NewSchema([]arrow.Field{
		{Name: "l_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "l_val", Type: arrow.PrimitiveTypes.Float64},
		{Name: "r_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "r_val", Type: arrow.PrimitiveTypes.Float64},
	}, nil)).(*arrowimpl.ArrowDataFrameSchema)

	fUserJoin := func(r1, r2 df.Row) []df.Row {
		// For benchmark, projection should be simple to not dominate join cost itself
		outRow := arrowimpl.NewArrowRowFromValues(outputSchema, []df.Value{
			arrowimpl.NewArrowValue(scalar.NewInt64Scalar(r1.Get(0).GetAsInt()), df.IntegerFormat),         // l_id_int
			arrowimpl.NewArrowValue(scalar.NewFloat64Scalar(r1.Get(1).GetAsFloat()), df.DoubleFormat),   // l_val_float
			arrowimpl.NewArrowValue(scalar.NewInt64Scalar(r2.Get(0).GetAsInt()), df.IntegerFormat),         // r_id_int_r
			arrowimpl.NewArrowValue(scalar.NewFloat64Scalar(r2.Get(1).GetAsFloat()), df.DoubleFormat),   // r_val_float_r
		}, mem) // mem should be accessible or pass DefaultAllocator
		return []df.Row{outRow}
	}
	joinColsMap := map[string]string{"key_join": "key_join_r"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		joinedDf := leftDf.Join(outputSchema, rightDf, df.JoinEqui, joinColsMap, fUserJoin)
		volatileDF = joinedDf
		if volatileDF != nil {
			volatileDF.(df.Releaser).Release()
		}
	}
}

func BenchmarkInMemory_Join_Inner(b *testing.B) {
	leftInMemDf := generateInMemoryData(b, benchmarkJoinNumRowsL)

	// Custom generation for right in-memory table to match key distribution
	schemaRightInMem := df.NewSchemaDetFromMap("inmem_join_R", map[string]df.Format{
		"id_int_r":     df.IntegerFormat, "val_float_r":  df.DoubleFormat,
		"cat_string_r": df.StringFormat,  "key_join_r":   df.IntegerFormat,
	})
	rowsR := make([]df.Row, benchmarkJoinNumRowsR)
	randSrcR := rand.New(rand.NewSource(time.Now().UnixNano() + 2))
	for i := 0; i < benchmarkJoinNumRowsR; i++ {
		rowValsR := []df.Value{
			inmemory.NewIntValueConst(int64(i)),
			inmemory.NewDoubleValueConst(randSrcR.Float64() * 100),
			inmemory.NewStringValueConst(fmt.Sprintf("group_R%d", i%benchmarkNumGroups)),
			inmemory.NewIntValueConst(int64(randSrcR.Intn(benchmarkJoinNumRowsL / 2))),
		}
		rowsR[i] = inmemory.NewRow(&schemaRightInMem, &rowValsR)
	}
	rightInMemDf := inmemory.NewDataframeFromRows(schemaRightInMem.Name(), schemaRightInMem, rowsR)

	// Define output schema for in-memory join projection
	outSchemaInMem := df.NewSchemaDetFromMap("join_out_inmem", map[string]df.Format{
		"l_id":  df.IntegerFormat, "l_val": df.DoubleFormat,
		"r_id":  df.IntegerFormat, "r_val": df.DoubleFormat,
	})

	projectionFunc := func(l, r df.Row) df.Row {
		newVals := &[]df.Value{
			l.GetByName("id_int"), l.GetByName("val_float"),
			r.GetByName("id_int_r"), r.GetByName("val_float_r"),
		}
		return inmemory.NewRow(&outSchemaInMem, newVals)
	}
	joinColsMap := map[string]string{"key_join": "key_join_r"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		joinedDf := leftInMemDf.Join(outSchemaInMem, rightInMemDf, df.JoinEqui, joinColsMap, projectionFunc)
		volatileDF = joinedDf
	}
}
