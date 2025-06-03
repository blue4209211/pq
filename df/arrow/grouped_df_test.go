//go:build arrow

package arrow_test

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
	// "time" // Not directly used in this snippet, but often useful for data setup

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	// "github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// Helpers also needed in this file if not in a shared test utility
// const nilPlaceholder = "__NIL_PLACEHOLDER__" // Assumed from df_test.go via package scope or redefine

// func dfToSliceOfInterfaceSlices(dataFrame df.DataFrame) [][]interface{} { /* ... */ } // Assumed
// func sortSliceOfInterfaceSlices(slice [][]interface{}) { /* ... */ } // Assumed


// setupGroupedTestData creates a base DataFrame and groups it for testing.
// Remember to Release the returned GroupedDataFrame and the original base DataFrame.
func setupGroupedTestData(t *testing.T, mem memory.Allocator, groupByCols ...string) (df.DataFrame, df.GroupedDataFrame) {
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
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{10.1, 20.2, 10.11, 30.3, 40.4, 50.5, 60.6, 70.7}, nil)
	record := rb.NewRecord(); // Do not release here, baseDf takes ownership

	baseDf := arrowimpl.NewArrowDataFrame("grouped_df_test_base", record, dfSchema)
	// NewArrowDataFrame retains record, so we can release our hold on 'record'
	record.Release()

	groupedDf := baseDf.GroupBy(groupByCols...)
	return baseDf, groupedDf
}


func TestArrowGroupedDataFrame_GetGroupColumns_Len_GetKeys(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, groupedDf := setupGroupedTestData(t, mem, "cat1", "cat2")
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()
	defer groupedDf.(*arrowimpl.ArrowGroupedDataFrame).Release()


	assert.Equal(t, []string{"cat1", "cat2"}, groupedDf.GetGroupColumns())
	assert.Equal(t, int64(7), groupedDf.Len())

	keys := groupedDf.GetKeys()
	assert.Equal(t, 7, len(keys), "Number of key rows")

	keyMap := make(map[string]bool)
	for _, keyRow := range keys {
		assert.Equal(t, 2, keyRow.Len(), "Key row should have 2 columns for ('cat1','cat2')")
		k1 := keyRow.Get(0)
		k2 := keyRow.Get(1)
		var k1Str, k2Str string
		if k1.IsNil() { k1Str = "nil" } else { k1Str = k1.GetAsString() }
		if k2.IsNil() { k2Str = "nil" } else { k2Str = strconv.FormatInt(k2.GetAsInt(),10) }
		keyMap[fmt.Sprintf("(%s,%s)", k1Str, k2Str)] = true
	}

	expectedKeyStrings := []string{
		"(A,1)", "(B,2)", "(A,2)", "(B,1)", "(nil,1)", "(A,nil)", "(nil,nil)",
	}
	for _, eks := range expectedKeyStrings {
		assert.True(t, keyMap[eks], "Expected key missing: %s", eks)
	}
}


func TestArrowGroupedDataFrame_Get_ForEach(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, groupedDf := setupGroupedTestData(t, mem, "cat1")
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()
	defer groupedDf.(*arrowimpl.ArrowGroupedDataFrame).Release()

	assert.Equal(t, int64(3), groupedDf.Len()) // Groups for "cat1": "A", "B", nil

	keys := groupedDf.GetKeys()
	var keyA, keyB, keyNil df.Row
	for _, k := range keys {
		// Ensure Get(0) is safe to call
		if k.Len() > 0 {
			val := k.Get(0)
			if val.IsNil() { keyNil = k
			} else if val.GetAsString() == "A" { keyA = k
			} else if val.GetAsString() == "B" { keyB = k }
		}
	}
	assert.NotNil(t, keyA, "Key 'A' not found")
	assert.NotNil(t, keyB, "Key 'B' not found")
	assert.NotNil(t, keyNil, "Key 'nil' not found")

	// Test Get() for group "A"
	groupA_df := groupedDf.Get(keyA);	defer groupA_df.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(4), groupA_df.Len(), "Group 'A' length")
	groupA_data := dfToSliceOfInterfaceSlices(groupA_df)
	for _, row := range groupA_data {
		assert.Equal(t, "A", row[0], "All rows in group 'A' should have cat1='A'")
	}
	foundSpecificA := false
	for _, row := range groupA_data { if row[0]=="A" && row[1]==int64(2) && row[2]==30.3 {foundSpecificA=true; break} }
	assert.True(t, foundSpecificA, "Specific row for group A not found in Get()")

	// Test Get() for group nil
	groupNil_df := groupedDf.Get(keyNil);	defer groupNil_df.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(2), groupNil_df.Len(), "Group 'nil' length")
	groupNil_data := dfToSliceOfInterfaceSlices(groupNil_df)
	for _, row := range groupNil_data {
		assert.Equal(t, nilPlaceholder, row[0], "All rows in group 'nil' should have cat1=nil")
	}

	// Test ForEach()
	numForEachCalls := 0
	totalRowsInGroups := int64(0)
	groupedDf.ForEach(func(k df.Row, groupContentDf df.DataFrame) {
		numForEachCalls++
		totalRowsInGroups += groupContentDf.Len()
		keyCat1Val := k.Get(0)
		for r := int64(0); r < groupContentDf.Len(); r++ {
			rowInGroup := groupContentDf.GetRow(r)
			valInGroup := rowInGroup.Get(0)
			if keyCat1Val.IsNil() {
				assert.True(t, valInGroup.IsNil(), "Mismatch: key is nil, val in group is not for key %v", dfToSliceOfInterfaceSlices(k))
			} else {
				assert.Equal(t, keyCat1Val.GetAsString(), valInGroup.GetAsString(), "Mismatch: key %s, val in group %s", keyCat1Val.GetAsString(), valInGroup.GetAsString())
			}
		}
	})
	assert.Equal(t, int(groupedDf.Len()), numForEachCalls, "ForEach call count")
	assert.Equal(t, baseDf.Len(), totalRowsInGroups, "Sum of rows in ForEach groups should match original DF length")
}

// Mock implementations for df.Expr, df.Value, df.FilterOp, df.MapOp for Series.Select tests
// These need to align with how they are used in arrowSeries.Select()
// These are copied from series_test.go. Consider moving to a shared test util package.
type mockExpr struct {
	exprName     string; exprConstVal df.Value; exprColName  string
	exprOpType   df.ExprOpType; exprFilterOp df.FilterOp
	exprMapOp    df.MapOp; exprParent   df.Expr
}
func (m *mockExpr) Name() string { return m.exprName }
func (m *mockExpr) Const() df.Value { return m.exprConstVal }
func (m *mockExpr) Col() string { return m.exprColName }
func (m *mockExpr) OpType() df.ExprOpType { return m.exprOpType }
func (m *mockExpr) FilterOp() df.FilterOp { return m.exprFilterOp }
func (m *mockExpr) MapOp() df.MapOp { return m.exprMapOp }
func (m *mockExpr) Parent() df.Expr { return m.exprParent }
func (m *mockExpr) SetParent(p df.Expr) df.Expr { m.exprParent = p; return m }
func (m *mockExpr) SetName(n string) df.Expr {m.exprName = n; return m}

type mockFilterOp struct { applyFunc func(v df.Value, args ...df.Value) bool; argExprs  []df.Expr }
func (m *mockFilterOp) Args() []df.Expr { return m.argExprs }
func (m *mockFilterOp) ApplyFilter(v df.Value, args ...df.Value) bool { return m.applyFunc(v, args...) }
func (m *mockFilterOp) SetArgs(args ...df.Expr) df.FilterOp { m.argExprs = args; return m }

type mockMapOp struct { applyFunc func(v df.Value, args ...df.Value) df.Value; argExprs []df.Expr; returnFormat df.Format }
func (m *mockMapOp) Args() []df.Expr { return m.argExprs }
func (m *mockMapOp) ApplyMap(v df.Value, args ...df.Value) df.Value { return m.applyFunc(v, args...) }
func (m *mockMapOp) ReturnFormat() df.Format { return m.returnFormat }
func (m *mockMapOp) SetArgs(args ...df.Expr) df.MapOp { m.argExprs = args; return m }

type mockValue struct { df.Value; data any; mockSchema df.Format; mockIsNil bool }
func (m *mockValue) Get() any { return m.data }
func (m *mockValue) IsNil() bool { return m.mockIsNil }
func (m *mockValue) Schema() df.Format { return m.mockSchema }
// Add other getters if needed by specific tests, e.g.:
// func (m *mockValue) GetAsInt() int64 { if i,ok := m.data.(int64); ok {return i}; panic("not int") }
// func (m *mockValue) GetAsString() string { if s,ok := m.data.(string); ok {return s}; panic("not string") }

// Placeholder for series tests, copied from series_test.go if needed for dfToSliceOfInterfaceSlices or other shared test logic
// For now, these are not directly used by grouped_df_test.go's new tests.
// func TestArrowSeries_NewArrowSeries(t *testing.T) { /* ... */ }
// ... etc. ...

// TestArrowSeries_Expr, TestArrowSeries_Select also belong to series_test.go
// TestArrowSeries_Join, etc. also belong to series_test.go

func TestArrowGroupedDataFrame_Agg(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, groupedDfCat1 := setupGroupedTestData(t, mem, "cat1") // Groups: "A", "B", nil
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()
	defer groupedDfCat1.(*arrowimpl.ArrowGroupedDataFrame).Release()

	// Expected values for cat1 groups (manually calculated from setupGroupedTestData)
	// Group A: cat1="A"
	//  cat2: {1, 1, 2, nil} -> for count(cat2) = 3
	//  value: {10.1, 10.11, 30.3, 60.6}
	//    sum(value) = 10.1 + 10.11 + 30.3 + 60.6 = 111.11
	//    mean(value) = 111.11 / 4 = 27.7775
	//    min(value) = 10.1
	//    max(value) = 60.6
	//    count(value) = 4
	//    count(*) = 4
	// Group B: cat1="B"
	//  cat2: {2, 1}
	//  value: {20.2, 40.4}
	//    sum(value) = 20.2 + 40.4 = 60.6
	//    mean(value) = 60.6 / 2 = 30.3
	//    min(value) = 20.2
	//    max(value) = 40.4
	//    count(value) = 2
	//    count(*) = 2
	// Group nil: cat1=nil
	//  cat2: {1, nil} -> for count(cat2) = 1
	//  value: {50.5, 70.7}
	//    sum(value) = 50.5 + 70.7 = 121.2
	//    mean(value) = 121.2 / 2 = 60.6
	//    min(value) = 50.5
	//    max(value) = 70.7
	//    count(value) = 2
	//    count(*) = 2

	// Case 1: Single aggregations
	t.Run("SingleAggregations", func(t *testing.T) {
		// Count Star
		aggCountStar := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "count", OutputColName: "count_star"})
		defer aggCountStar.(*arrowimpl.ArrowDataFrame).Release()
		expectedCountStar := [][]interface{}{
			{"A", int64(4)}, {"B", int64(2)}, {nilPlaceholder, int64(2)},
		}
		actualCountStar := dfToSliceOfInterfaceSlices(aggCountStar)
		sortSliceOfInterfaceSlices(actualCountStar)
		sortSliceOfInterfaceSlices(expectedCountStar)
		assert.Equal(t, expectedCountStar, actualCountStar, "Count Star")

		// Count on a value column (should ignore nils in value itself, but value col has no nils here)
		aggCountValue := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "count", InputCol: "value", OutputColName: "count_value"})
		defer aggCountValue.(*arrowimpl.ArrowDataFrame).Release()
		expectedCountValue := [][]interface{}{
			{"A", int64(4)}, {"B", int64(2)}, {nilPlaceholder, int64(2)},
		}
		actualCountValue := dfToSliceOfInterfaceSlices(aggCountValue)
		sortSliceOfInterfaceSlices(actualCountValue); sortSliceOfInterfaceSlices(expectedCountValue)
		assert.Equal(t, expectedCountValue, actualCountValue, "Count Value")

		// Count on a category column with nils (cat2)
		aggCountCat2 := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "count", InputCol: "cat2", OutputColName: "count_cat2"})
		defer aggCountCat2.(*arrowimpl.ArrowDataFrame).Release()
		expectedCountCat2 := [][]interface{}{
			{"A", int64(3)}, {"B", int64(2)}, {nilPlaceholder, int64(1)},
		}
		actualCountCat2 := dfToSliceOfInterfaceSlices(aggCountCat2)
		sortSliceOfInterfaceSlices(actualCountCat2); sortSliceOfInterfaceSlices(expectedCountCat2)
		assert.Equal(t, expectedCountCat2, actualCountCat2, "Count Cat2 (has nils)")

		// Sum
		aggSum := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "sum", InputCol: "value", OutputColName: "sum_value"})
		defer aggSum.(*arrowimpl.ArrowDataFrame).Release()
		expectedSum := [][]interface{}{
			{"A", 111.11}, {"B", 60.6}, {nilPlaceholder, 121.2},
		}
		actualSum := dfToSliceOfInterfaceSlices(aggSum)
		sortSliceOfInterfaceSlices(actualSum); sortSliceOfInterfaceSlices(expectedSum)
		assert.Equal(t, expectedSum, actualSum, "Sum Value")

		// Mean
		aggMean := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "mean", InputCol: "value", OutputColName: "mean_value"})
		defer aggMean.(*arrowimpl.ArrowDataFrame).Release()
		expectedMean := [][]interface{}{
			{"A", 27.7775}, {"B", 30.3}, {nilPlaceholder, 60.6},
		}
		actualMean := dfToSliceOfInterfaceSlices(aggMean)
		sortSliceOfInterfaceSlices(actualMean); sortSliceOfInterfaceSlices(expectedMean)
		assert.Equal(t, expectedMean, actualMean, "Mean Value")

		// Min
		aggMin := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "min", InputCol: "value", OutputColName: "min_value"})
		defer aggMin.(*arrowimpl.ArrowDataFrame).Release()
		expectedMin := [][]interface{}{
			{"A", 10.1}, {"B", 20.2}, {nilPlaceholder, 50.5},
		}
		actualMin := dfToSliceOfInterfaceSlices(aggMin)
		sortSliceOfInterfaceSlices(actualMin); sortSliceOfInterfaceSlices(expectedMin)
		assert.Equal(t, expectedMin, actualMin, "Min Value")

		// Max
		aggMax := groupedDfCat1.Agg(arrowimpl.AggregationConfig{Func: "max", InputCol: "value", OutputColName: "max_value"})
		defer aggMax.(*arrowimpl.ArrowDataFrame).Release()
		expectedMax := [][]interface{}{
			{"A", 60.6}, {"B", 40.4}, {nilPlaceholder, 70.7},
		}
		actualMax := dfToSliceOfInterfaceSlices(aggMax)
		sortSliceOfInterfaceSlices(actualMax); sortSliceOfInterfaceSlices(expectedMax)
		assert.Equal(t, expectedMax, actualMax, "Max Value")
	})

	// Case 2: Multiple aggregations
	t.Run("MultipleAggregations", func(t *testing.T) {
		multiAggCfgs := []arrowimpl.AggregationConfig{
			{Func: "sum", InputCol: "value", OutputColName: "total_value"},
			{Func: "count", OutputColName: "num_rows"},
			{Func: "mean", InputCol: "value", OutputColName: "avg_value"},
		}
		aggMulti := groupedDfCat1.Agg(multiAggCfgs...)
		defer aggMulti.(*arrowimpl.ArrowDataFrame).Release()

		expectedMulti := [][]interface{}{
			{"A", 111.11, int64(4), 27.7775},
			{"B", 60.6, int64(2), 30.3},
			{nilPlaceholder, 121.2, int64(2), 60.6},
		}
		actualMulti := dfToSliceOfInterfaceSlices(aggMulti)
		sortSliceOfInterfaceSlices(actualMulti); sortSliceOfInterfaceSlices(expectedMulti)
		assert.Equal(t, expectedMulti, actualMulti, "Multiple Aggregations")

		// Check column names
		assert.Equal(t, "cat1", aggMulti.Schema().Get(0).Name)
		assert.Equal(t, "total_value", aggMulti.Schema().Get(1).Name)
		assert.Equal(t, "num_rows", aggMulti.Schema().Get(2).Name)
		assert.Equal(t, "avg_value", aggMulti.Schema().Get(3).Name)
	})

	// Case 3: Group by multiple columns
	t.Run("GroupByMultipleColumns", func(t *testing.T) {
		_, groupedDfCat1Cat2 := setupGroupedTestData(t, mem, "cat1", "cat2")
		defer groupedDfCat1Cat2.(*arrowimpl.ArrowGroupedDataFrame).Release()

		aggCfgs := []arrowimpl.AggregationConfig{
			{Func: "sum", InputCol: "value", OutputColName: "sum_val"},
			{Func: "count", OutputColName: "count_rows"},
		}
		aggMultiKey := groupedDfCat1Cat2.Agg(aggCfgs...)
		defer aggMultiKey.(*arrowimpl.ArrowDataFrame).Release()

		// Expected: cat1, cat2, sum_val, count_rows
		// A,1: (10.1, 10.11) -> sum 20.21, count 2
		// B,2: (20.2) -> sum 20.2, count 1
		// A,2: (30.3) -> sum 30.3, count 1
		// B,1: (40.4) -> sum 40.4, count 1
		// nil,1: (50.5) -> sum 50.5, count 1
		// A,nil: (60.6) -> sum 60.6, count 1
		// nil,nil: (70.7) -> sum 70.7, count 1
		expectedAggMultiKey := [][]interface{}{
			{"A", int64(1), 20.21, int64(2)},
			{"B", int64(2), 20.2, int64(1)},
			{"A", int64(2), 30.3, int64(1)},
			{"B", int64(1), 40.4, int64(1)},
			{nilPlaceholder, int64(1), 50.5, int64(1)},
			{"A", nilPlaceholder, 60.6, int64(1)},
			{nilPlaceholder, nilPlaceholder, 70.7, int64(1)},
		}
		actualAggMultiKey := dfToSliceOfInterfaceSlices(aggMultiKey)
		sortSliceOfInterfaceSlices(actualAggMultiKey); sortSliceOfInterfaceSlices(expectedAggMultiKey)
		assert.Equal(t, expectedAggMultiKey, actualAggMultiKey, "Agg group by cat1, cat2")
	})

	// Case 4: Empty DataFrame
	t.Run("EmptyDataFrame", func(t *testing.T) {
		emptySchema := arrow.NewSchema(
			[]arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}, nil,
		)
		emptyDfSchema := arrowimpl.NewArrowDataFrameSchema(emptySchema).(*arrowimpl.ArrowDataFrameSchema)
		emptyRec := array.NewRecord(emptySchema, nil, 0); defer emptyRec.Release()
		emptyBase := arrowimpl.NewArrowDataFrame("empty_base", emptyRec, emptyDfSchema)
		defer emptyBase.(*arrowimpl.ArrowDataFrame).Release()

		groupedEmpty := emptyBase.GroupBy("key")
		defer groupedEmpty.(*arrowimpl.ArrowGroupedDataFrame).Release()

		aggEmpty := groupedEmpty.Agg(arrowimpl.AggregationConfig{Func: "count", OutputColName: "count_all"})
		defer aggEmpty.(*arrowimpl.ArrowDataFrame).Release()
		assert.Equal(t, 0, aggEmpty.Len(), "Agg on empty grouped DF should be empty")
		assert.Equal(t, 2, aggEmpty.Schema().Len(), "Agg on empty grouped DF should have key + agg col in schema") // key, count_all
	})

	// Case 5: Aggregation with no configs (should return distinct keys)
	t.Run("NoAggregationConfigs", func(t *testing.T) {
		keysOnlyDf := groupedDfCat1.Agg() // No AggregationConfig
		defer keysOnlyDf.(*arrowimpl.ArrowDataFrame).Release()

		expectedKeys := [][]interface{}{
			{"A"}, {"B"}, {nilPlaceholder},
		}
		actualKeys := dfToSliceOfInterfaceSlices(keysOnlyDf)
		sortSliceOfInterfaceSlices(actualKeys); sortSliceOfInterfaceSlices(expectedKeys)
		assert.Equal(t, expectedKeys, actualKeys, "Agg with no configs")
		assert.Equal(t, 1, keysOnlyDf.Schema().Len(), "Schema for no-config agg should have only key col")
		assert.Equal(t, "cat1", keysOnlyDf.Schema().Get(0).Name)
	})

	// Case 6: stddev and variance (Arrow kernels might return nil if count is too low, e.g. 1)
	// Group A (4 items): stddev/variance should be calculable
	// Group B (2 items): stddev/variance should be calculable
	// Group nil (2 items): stddev/variance should be calculable
	t.Run("StddevVariance", func(t *testing.T) {
		stdDevVarCfgs := []arrowimpl.AggregationConfig{
			{Func: "stddev", InputCol: "value", OutputColName: "stddev_val"},
			{Func: "variance", InputCol: "value", OutputColName: "var_val"},
		}
		aggStdVar := groupedDfCat1.Agg(stdDevVarCfgs...)
		defer aggStdVar.(*arrowimpl.ArrowDataFrame).Release()

		// Expected values need to be calculated carefully or taken from a trusted source.
		// Arrow's variance is sample variance (ddof=1). Stddev is sqrt of that.
		// Group A: {10.1, 10.11, 30.3, 60.6} -> mean 27.7775
		//   var: ((10.1-m)^2 + (10.11-m)^2 + (30.3-m)^2 + (60.6-m)^2) / (4-1)
		//        (312.495 + 312.150 + 6.365 + 1077.300) / 3 = 1708.31 / 3 = 569.4366...
		//   stddev: sqrt(569.4366) = 23.8628...
		// Group B: {20.2, 40.4} -> mean 30.3
		//   var: ((20.2-m)^2 + (40.4-m)^2) / (2-1) = ((-10.1)^2 + (10.1)^2)/1 = (102.01 + 102.01)/1 = 204.02
		//   stddev: sqrt(204.02) = 14.2835...
		// Group nil: {50.5, 70.7} -> mean 60.6
		//   var: ((50.5-m)^2 + (70.7-m)^2) / (2-1) = ((-10.1)^2 + (10.1)^2)/1 = 204.02
		//   stddev: sqrt(204.02) = 14.2835...
		expectedStdVar := [][]interface{}{
			{"A", 23.862870655501 Asturias, 569.4366666666666}, // Approx
			{"B", 14.283556953193877, 204.02},
			{nilPlaceholder, 14.283556953193877, 204.02},
		}
		actualStdVar := dfToSliceOfInterfaceSlices(aggStdVar)

		// Sort for comparison
		sort.Slice(actualStdVar, func(i, j int) bool {
			valI, _ := actualStdVar[i][0].(string) // Assuming key is first and string or nil
            valJ, _ := actualStdVar[j][0].(string)
            if actualStdVar[i][0] == nilPlaceholder { valI = "zzz_nil" } // Ensure nils sort consistently
            if actualStdVar[j][0] == nilPlaceholder { valJ = "zzz_nil" }
			return valI < valJ
		})
		sort.Slice(expectedStdVar, func(i, j int) bool {
			valI, _ := expectedStdVar[i][0].(string)
            valJ, _ := expectedStdVar[j][0].(string)
            if expectedStdVar[i][0] == nilPlaceholder { valI = "zzz_nil" }
            if expectedStdVar[j][0] == nilPlaceholder { valJ = "zzz_nil" }
			return valI < valJ
		})

		assert.Equal(t, len(expectedStdVar), len(actualStdVar))
		for i := range expectedStdVar {
			assert.Equal(t, expectedStdVar[i][0], actualStdVar[i][0], "Key mismatch for stddev/var") // Key
			// Using assert.InDelta for float comparisons
			assert.InDelta(t, expectedStdVar[i][1].(float64), actualStdVar[i][1].(float64), 1e-5, "Stddev mismatch for key %v", expectedStdVar[i][0])
			assert.InDelta(t, expectedStdVar[i][2].(float64), actualStdVar[i][2].(float64), 1e-5, "Variance mismatch for key %v", expectedStdVar[i][0])
		}
	})
}

func TestArrowGroupedDataFrame_Where(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, groupedDfCat1 := setupGroupedTestData(t, mem, "cat1") // Groups: "A", "B", nil
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()
	defer groupedDfCat1.(*arrowimpl.ArrowGroupedDataFrame).Release()

	// Case 1: Filter groups based on key value (keep only group "A")
	t.Run("FilterByKey", func(t *testing.T) {
		filteredByKey_gdf := groupedDfCat1.Where(func(key df.Row, groupContent df.DataFrame) bool {
			return !key.Get(0).IsNil() && key.Get(0).GetAsString() == "A"
		})
		defer filteredByKey_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()

		assert.Equal(t, int64(1), filteredByKey_gdf.Len(), "Filtered by key should have 1 group ('A')")
		keys := filteredByKey_gdf.GetKeys()
		assert.Equal(t, "A", keys[0].Get(0).GetAsString(), "The only key should be 'A'")

		// Check if the 'A' group content is correct
		groupA_df := filteredByKey_gdf.Get(keys[0])
		defer groupA_df.(*arrowimpl.ArrowDataFrame).Release()
		assert.Equal(t, int64(4), groupA_df.Len(), "Group 'A' length after key filter")
	})

	// Case 2: Filter groups based on group size (keep groups with > 2 rows)
	// Group A: 4 rows, Group B: 2 rows, Group nil: 2 rows. Should keep only Group A.
	t.Run("FilterByGroupSize", func(t *testing.T) {
		filteredBySize_gdf := groupedDfCat1.Where(func(key df.Row, groupContent df.DataFrame) bool {
			return groupContent.Len() > 2
		})
		defer filteredBySize_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()
		assert.Equal(t, int64(1), filteredBySize_gdf.Len(), "Filtered by size should have 1 group ('A')")
		keys := filteredBySize_gdf.GetKeys()
		assert.Equal(t, "A", keys[0].Get(0).GetAsString(), "The only key for size filter should be 'A'")
	})

	// Case 3: Filter groups based on an aggregate property (sum of 'value' in group > 100)
	// Group A sum(value) = 111.11
	// Group B sum(value) = 60.6
	// Group nil sum(value) = 121.2
	// Should keep Group A and Group nil.
	t.Run("FilterByGroupAggregate", func(t *testing.T) {
		filteredByAgg_gdf := groupedDfCat1.Where(func(key df.Row, groupContent df.DataFrame) bool {
			// Perform an ad-hoc aggregation on the groupContent
			// NOTE: This is less efficient as it re-aggregates for each group.
			// A more optimized version might pre-calculate aggregates if this is common.
			if groupContent.Len() == 0 { return false }

			sumValSeries := groupContent.GetSeriesByName("value").Select(df.NewExpr(df.SumOp))
			defer sumValSeries.Release()
			if sumValSeries.Len() == 0 || sumValSeries.IsNil(0) { return false }

			sumVal := sumValSeries.Get(0).GetAsFloat()
			return sumVal > 100.0
		})
		defer filteredByAgg_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()

		assert.Equal(t, int64(2), filteredByAgg_gdf.Len(), "Filtered by aggregate should have 2 groups")
		keys := filteredByAgg_gdf.GetKeys()
		keyMap := make(map[string]bool)
		for _, k := range keys {
			if k.Get(0).IsNil() { keyMap["nil"] = true
			} else { keyMap[k.Get(0).GetAsString()] = true }
		}
		assert.True(t, keyMap["A"], "Group A should be present after aggregate filter")
		assert.True(t, keyMap["nil"], "Group nil should be present after aggregate filter")
	})

	// Case 4: Predicate returns false for all groups
	t.Run("FilterAllOut", func(t *testing.T) {
		filteredAllOut_gdf := groupedDfCat1.Where(func(key df.Row, groupContent df.DataFrame) bool {
			return false
		})
		defer filteredAllOut_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()
		assert.Equal(t, int64(0), filteredAllOut_gdf.Len(), "Filtered all out should have 0 groups")
	})

	// Case 5: Predicate returns true for all groups
	t.Run("FilterNoneOut", func(t *testing.T) {
		filteredNoneOut_gdf := groupedDfCat1.Where(func(key df.Row, groupContent df.DataFrame) bool {
			return true
		})
		defer filteredNoneOut_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()
		assert.Equal(t, groupedDfCat1.Len(), filteredNoneOut_gdf.Len(), "Filtered none out should have all original groups")
		// Check if one of the groups is still accessible and correct
		keys := filteredNoneOut_gdf.GetKeys()
		var keyB df.Row
		for _, k := range keys { if !k.Get(0).IsNil() && k.Get(0).GetAsString() == "B" { keyB = k; break } }
		assert.NotNil(t, keyB, "Key 'B' not found in 'none out' filter result")
		groupB_df := filteredNoneOut_gdf.Get(keyB)
		defer groupB_df.(*arrowimpl.ArrowDataFrame).Release()
		assert.Equal(t, int64(2), groupB_df.Len(), "Group 'B' length in 'none out' filter result")
	})

	// Case 6: Grouped by multiple columns
	t.Run("FilterWithMultiColumnKeys", func(t *testing.T) {
		_, groupedDfCat1Cat2 := setupGroupedTestData(t, mem, "cat1", "cat2")
		defer groupedDfCat1Cat2.(*arrowimpl.ArrowGroupedDataFrame).Release()

		// Keep groups where cat1 is "A" AND cat2 is 1
		// Original keys: (A,1), (B,2), (A,2), (B,1), (nil,1), (A,nil), (nil,nil)
		// Should keep only (A,1)
		filteredMultiKey_gdf := groupedDfCat1Cat2.Where(func(key df.Row, groupContent df.DataFrame) bool {
			c1Nil := key.Get(0).IsNil()
			c2Nil := key.Get(1).IsNil()
			if !c1Nil && !c2Nil {
				return key.Get(0).GetAsString() == "A" && key.Get(1).GetAsInt() == 1
			}
			return false
		})
		defer filteredMultiKey_gdf.(*arrowimpl.ArrowGroupedDataFrame).Release()
		assert.Equal(t, int64(1), filteredMultiKey_gdf.Len(), "Filtered multi-key gdf len")
		keys := filteredMultiKey_gdf.GetKeys()
		assert.Equal(t, "A", keys[0].Get(0).GetAsString())
		assert.Equal(t, int64(1), keys[0].Get(1).GetAsInt())
	})
}

func TestArrowGroupedDataFrame_Map(t *testing.T) {
	mem := memory.NewGoAllocator()
	baseDf, groupedDf := setupGroupedTestData(t, mem, "cat1")
	defer baseDf.(*arrowimpl.ArrowDataFrame).Release()
	defer groupedDf.(*arrowimpl.ArrowGroupedDataFrame).Release()

	// As Map is not fully implemented and prints a warning, this test just checks it doesn't panic
	// and returns the original grouped dataframe.
	t.Run("BasicMapCallNoPanic", func(t *testing.T) {
		mappedGdf := groupedDf.Map(func(key df.Row, groupDf df.DataFrame) df.DataFrame {
			// This function might not even be called if Map returns early.
			// If it were called, it should return a df.DataFrame.
			// For this test, returning the original groupDf is fine.
			groupDf.(df.Releaser).Retain() // If we were to return it.
			return groupDf
		})
		// Since current Map returns original, it doesn't need its own release.
		// If Map started returning a new GDF, mappedGdf would need release.
		assert.Same(t, groupedDf, mappedGdf, "Map should return the original GDF for now")
	})
}
