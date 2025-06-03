//go:build arrow

package arrow_test

import (
	"fmt"
	"sort"
	// "strconv" // Not immediately needed, can add if specific tests require it
	"testing"
	// "time" // Not immediately needed
	// "reflect" // Not immediately needed

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/builder"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	arrowimpl "github.com/blue4209211/pq/df/arrow"
	"github.com/stretchr/testify/assert"
)

// --- Helper functions (copied from df/arrow/df_test.go) ---

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
	// Determine appropriate df.Format based on arrow.DataType
	var dfFmt df.Format
	switch dt.ID() {
	case arrow.INT64:
		dfFmt = df.IntegerFormat
	case arrow.STRING:
		dfFmt = df.StringFormat
	case arrow.FLOAT64:
		dfFmt = df.DoubleFormat
	case arrow.BOOL:
		dfFmt = df.BoolFormat
	default:
		// Fallback or panic for unhandled types
		panic(fmt.Sprintf("unsupported arrow.DataType in makeArrowValue for df.Format: %s", dt.Name()))
	}
	return arrowimpl.NewArrowValue(s, dfFmt)
}

// Array builder helpers (copied from df/arrow/df_test.go)
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := builder.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := builder.NewStringBuilder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}
func getTestFloat64Array(mem memory.Allocator, values []float64, valids []bool) arrow.Array {
	b := builder.NewFloat64Builder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}
func getTestBoolArray(mem memory.Allocator, values []bool, valids []bool) arrow.Array {
	b := builder.NewBooleanBuilder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}

// getBaseTestDfForGrouping is a more generic helper than the one in df_test,
// allowing direct construction with arrays for varied test cases.
func getBaseTestDfForGrouping(t *testing.T, mem memory.Allocator, name string, fields []arrow.Field, columnData ...arrow.Array) (df.DataFrame, []arrow.Array) {
	if len(fields) != len(columnData) {
		panic("mismatch between number of fields and number of column data arrays")
	}
	schema := arrow.NewSchema(fields, nil)
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)

	// Retain arrays as they will be part of the record
	for _, arr := range columnData {
		arr.Retain()
	}

	rec := array.NewRecord(schema, columnData, -1)
	// NewRecord also retains, so release the ones given if they were retained before calling NewRecord
	// However, the getTest*Array helpers create new arrays that are not yet part of any record.
	// The record takes ownership. So, explicit release of columnData after record creation is needed
	// if they are not used elsewhere.
	// For safety, the caller of getBaseTestDfForGrouping should manage the release of initially created arrays
	// if they are not immediately consumed and released by NewRecord logic if it copies.
	// NewRecord does not copy, it retains. So the input arrays must be released by caller eventually.
	// Let's return the arrays so the caller can manage their lifecycle.

	return arrowimpl.NewArrowDataFrame(name, rec, dfSchema), columnData
}

// Helper Function to Create a Base GroupedDataFrame for Aggregation Tests
func getTestGroupedDataFrameForAgg(t *testing.T, mem memory.Allocator) (df.DataFrame, df.GroupedDataFrame) {
	fields := []arrow.Field{
		{Name: "key1_str", Type: arrow.BinaryTypes.String, Nullable: true},          // Grouping key 1
		{Name: "key2_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},          // Grouping key 2
		{Name: "val_sum_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},       // For sum, mean, min, max
		{Name: "val_mean_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},   // For sum, mean, min, max
		{Name: "val_count_str_nullable", Type: arrow.BinaryTypes.String, Nullable: true}, // For count (non-null)
		{Name: "val_all_nulls_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // For testing agg on all nulls
	}

	// Data:
	// Group G1: {"groupA", 10}
	//   {"groupA", 10, 100, 10.1, "apple", nil}
	//   {"groupA", 10, 200, 20.2, "banana", nil}
	//   {"groupA", 10, nil, 30.3, nil, nil} // null for val_sum_int and val_count_str_nullable
	// Group G2: {"groupB", 20}
	//   {"groupB", 20, 50, 5.5, "cat", nil}
	//   {"groupB", 20, 60, 6.6, "dog", nil}
	// Group G3: {"groupA", 30} // Different int key
	//   {"groupA", 30, 70, 7.7, "eel", nil}
	// Group G4: {nil, 10} // Null string key
	//   {nil, 10, 80, 8.8, "frog", nil}

	key1Data := getTestStringArray(mem, []string{"groupA", "groupA", "groupA", "groupB", "groupB", "groupA", "", "groupC"}, []bool{true, true, true, true, true, true, false, true}) // Last "" is actually nil
	key2Data := getTestInt64Array(mem, []int64{10, 10, 10, 20, 20, 30, 10, 40}, nil) // Last 40 for groupC
	valSumIntData := getTestInt64Array(mem, []int64{100, 200, 0, 50, 60, 70, 80, 90}, []bool{true, true, false, true, true, true, true, true})
	valMeanFloatData := getTestFloat64Array(mem, []float64{10.1, 20.2, 30.3, 5.5, 6.6, 7.7, 8.8, 9.9}, nil)
	valCountStrData := getTestStringArray(mem, []string{"apple", "banana", "", "cat", "dog", "eel", "frog", ""}, []bool{true, true, false, true, true, true, true, false})
	valAllNullsData := getTestFloat64Array(mem, []float64{0,0,0,0,0,0,0,0}, []bool{false,false,false,false,false,false,false,false})


	arrays := []arrow.Array{key1Data, key2Data, valSumIntData, valMeanFloatData, valCountStrData, valAllNullsData}
	// Don't release arrays here, getBaseTestDfForGrouping will give them to the record, which retains them.
	// The caller of getTestGroupedDataFrameForAgg will be responsible for releasing the original dataframe, which releases the record and thus the arrays.

	originalDf, _ := getBaseTestDfForGrouping(t, mem, "test_agg_df", fields, arrays...)
	// Arrays are now owned by originalDf's record.

	groupedDf := originalDf.GroupBy("key1_str", "key2_int")
	return originalDf, groupedDf
}

func TestGroupedDataFrame_GetGroupColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{
		{Name: "col_a", Type: arrow.BinaryTypes.String},
		{Name: "col_b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "col_c", Type: arrow.PrimitiveTypes.Float64},
	}
	colAData := getTestStringArray(mem, []string{"x", "y"}, nil); defer colAData.Release()
	colBData := getTestInt64Array(mem, []int64{1, 2}, nil); defer colBData.Release()
	colCData := getTestFloat64Array(mem, []float64{1.1, 2.2}, nil); defer colCData.Release()

	baseDf, arrs := getBaseTestDfForGrouping(t, mem, "get_group_cols_test", fields, colAData, colBData, colCData)
	defer baseDf.(df.Releaser).Release()
	for _, arr := range arrs { defer arr.Release()}


	groupingCols := []string{"col_a", "col_b"}
	groupedDf := baseDf.GroupBy(groupingCols...)
	defer groupedDf.(arrowimpl.Releaser).Release() // Assuming GroupedDataFrame implements Releaser

	retrievedCols := groupedDf.GetGroupColumns()
	assert.Equal(t, groupingCols, retrievedCols, "GetGroupColumns should return the correct column names.")

	// Test that the returned slice is a copy
	retrievedCols[0] = "changed_value_local_only"
	assert.Equal(t, "col_a", groupedDf.GetGroupColumns()[0], "Modifying returned slice should not affect original")
}

func TestGroupedDataFrame_Len(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("LenWithMultipleGroups", func(t *testing.T) {
		// Use the agg helper which creates 4 distinct groups:
		// {"groupA", 10}, {"groupB", 20}, {"groupA", 30}, {nil, 10}, {"groupC", 40} -> 5 groups
		originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
		defer originalDf.(df.Releaser).Release()
		defer groupedDf.(arrowimpl.Releaser).Release()

		// Expected groups:
		// 1. key1_str="groupA", key2_int=10
		// 2. key1_str="groupB", key2_int=20
		// 3. key1_str="groupA", key2_int=30
		// 4. key1_str=NULL, key2_int=10
		// 5. key1_str="groupC", key2_int=40
		assert.Equal(t, int64(5), groupedDf.Len(), "Len() should return the correct number of unique groups.")
	})

	t.Run("LenWithSingleGroup", func(t *testing.T) {
		fields := []arrow.Field{
			{Name: "key", Type: arrow.BinaryTypes.String},
			{Name: "val", Type: arrow.PrimitiveTypes.Int64},
		}
		keyData := getTestStringArray(mem, []string{"a", "a", "a"}, nil); defer keyData.Release()
		valData := getTestInt64Array(mem, []int64{1,2,3}, nil); defer valData.Release()

		dfSingleGroup, arrs := getBaseTestDfForGrouping(t, mem, "single_group_len", fields, keyData, valData)
		defer dfSingleGroup.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }

		groupedSingle := dfSingleGroup.GroupBy("key")
		defer groupedSingle.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(1), groupedSingle.Len())
	})

	t.Run("LenOnEmptyDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()

		emptyDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_len", fields, keyDataEmpty)
		defer emptyDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }

		groupedEmpty := emptyDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(0), groupedEmpty.Len(), "Len() on a grouped empty DataFrame should be 0.")
	})

	t.Run("LenAfterGroupingAllRowsIntoOneGroup", func(t *testing.T) {
		fields := []arrow.Field{ {Name: "val", Type: arrow.PrimitiveTypes.Int64} }
		valData := getTestInt64Array(mem, []int64{1,2,3,4,5}, nil); defer valData.Release()

		// No explicit grouping columns means the entire DataFrame is one group if an aggregation is applied.
		// However, GroupBy without columns is not standard. Let's assume it's not allowed or results in 0 groups.
		// The current arrow impl might panic or handle it differently.
		// If GroupBy() is called with no arguments, it typically means no grouping.
		// The GroupedDataFrame concept implies grouping keys.
		// Let's test grouping by a column that has the same value for all rows.
		constKeyData := getTestStringArray(mem, []string{"all_same", "all_same", "all_same"}, nil); defer constKeyData.Release()
		valConstData := getTestInt64Array(mem, []int64{1,2,3}, nil); defer valConstData.Release()
		dfAllSameGroup, arrs := getBaseTestDfForGrouping(t, mem, "all_same_group",
			[]arrow.Field{{Name: "const_key", Type: arrow.BinaryTypes.String}, {Name:"val", Type:arrow.PrimitiveTypes.Int64}},
			constKeyData, valConstData)
		defer dfAllSameGroup.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }

		groupedAllSame := dfAllSameGroup.GroupBy("const_key")
		defer groupedAllSame.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(1), groupedAllSame.Len(), "Len() should be 1 if all rows fall into the same group.")
	})
}

func TestGroupedDataFrame_Where(t *testing.T) {
	mem := memory.NewGoAllocator()
	originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
	defer originalDf.(df.Releaser).Release()
	defer groupedDf.(arrowimpl.Releaser).Release() // Original groupedDf

	t.Run("Where_FilterByKey", func(t *testing.T) {
		// Filter for groups where key1_str == "groupA"
		// Expected matching keys: {"groupA", 10} and {"groupA", 30}
		filteredGroupedDf := groupedDf.Where(func(keyRow df.Row, groupDf df.DataFrame) bool {
			// IMPORTANT: Release the groupDf passed to the filter function, as Where will re-Get it if needed.
			defer groupDf.(df.Releaser).Release()
			key1Val := keyRow.GetByName("key1_str")
			return !key1Val.IsNil() && key1Val.GetAsString() == "groupA"
		})
		defer filteredGroupedDf.(arrowimpl.Releaser).Release()

		assert.Equal(t, int64(2), filteredGroupedDf.Len(), "Should be 2 groups with key1_str='groupA'")

		foundKeyA10 := false
		foundKeyA30 := false
		filteredGroupedDf.ForEach(func(keyRow df.Row, groupDf df.DataFrame){
			defer groupDf.(df.Releaser).Release()
			key1 := keyRow.GetByName("key1_str").GetAsString()
			key2 := keyRow.GetByName("key2_int").GetAsInt()
			assert.Equal(t, "groupA", key1)
			if key2 == 10 { foundKeyA10 = true }
			if key2 == 30 { foundKeyA30 = true }
		})
		assert.True(t, foundKeyA10, "Group key (groupA, 10) missing after filter")
		assert.True(t, foundKeyA30, "Group key (groupA, 30) missing after filter")
	})

	t.Run("Where_FilterByGroupSize", func(t *testing.T) {
		// Filter for groups having more than 1 row.
		// G1: {"groupA", 10} -> 3 rows
		// G2: {"groupB", 20} -> 2 rows
		// Others have 1 row. So, 2 groups should remain.
		filteredGroupedDf := groupedDf.Where(func(keyRow df.Row, groupDf df.DataFrame) bool {
			defer groupDf.(df.Releaser).Release()
			return groupDf.Len() > 1
		})
		defer filteredGroupedDf.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(2), filteredGroupedDf.Len(), "Should be 2 groups with more than 1 row")
	})

	t.Run("Where_FilterReturnsNoGroups", func(t *testing.T) {
		filteredGroupedDf := groupedDf.Where(func(keyRow df.Row, groupDf df.DataFrame) bool {
			defer groupDf.(df.Releaser).Release()
			return false // No group satisfies this
		})
		defer filteredGroupedDf.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(0), filteredGroupedDf.Len(), "No groups should remain if filter is always false")
	})

	t.Run("Where_OnEmptyGroupedDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()
		emptyBaseDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_where", fields, keyDataEmpty)
		defer emptyBaseDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }
		groupedEmpty := emptyBaseDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()

		filtered := groupedEmpty.Where(func(kr df.Row, gdf df.DataFrame) bool {
			if gdf != nil { gdf.(df.Releaser).Release() }
			return true
		})
		defer filtered.(arrowimpl.Releaser).Release()
		assert.Equal(t, int64(0), filtered.Len())
	})
}

func TestGroupedDataFrame_Map(t *testing.T) {
	mem := memory.NewGoAllocator()
	originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
	defer originalDf.(df.Releaser).Release()
	defer groupedDf.(arrowimpl.Releaser).Release()

	t.Run("Map_SelectFirstRowOfEachGroup", func(t *testing.T) {
		// The map function will take each group (as a DataFrame) and return a new DataFrame
		// containing only the first row of that group.
		// The resulting GroupedDataFrame should then effectively contain these first rows,
		// still grouped by the original keys.
		mapFunc := func(keyRow df.Row, groupSubDf df.DataFrame) df.DataFrame {
			if groupSubDf.Len() == 0 {
				// Return an empty DF with the same schema if the group is empty
				return arrowimpl.NewArrowDataFrame(groupSubDf.Name()+"_map_empty", nil, groupSubDf.Schema().(*arrowimpl.ArrowDataFrameSchema))
			}
			// Select the first row. Limit(0,1)
			firstRowDf := groupSubDf.Limit(0, 1)
			// Important: The returned DataFrame from mapFunc must be an *arrowDataFrame
			// and it must be explicitly managed (released) if not returned to something that takes ownership.
			// In this case, the caller of Map (the test) will own the final result.
			// The intermediate DFs created here inside mapFunc and returned to the Map method
			// will be handled by the Map method's implementation (e.g. it concatenates records).
			return firstRowDf
		}

		mappedGroupedDf := groupedDf.Map(mapFunc)
		defer mappedGroupedDf.(arrowimpl.Releaser).Release()

		assert.Equal(t, groupedDf.Len(), mappedGroupedDf.Len(), "Number of groups should remain the same after Map")

		// Verify content: each group in mappedGroupedDf should contain exactly one row,
		// which is the first row of the corresponding group in the original groupedDf.
		mappedGroupedDf.ForEach(func(keyRow df.Row, mappedSubGroupDf df.DataFrame) {
			defer mappedSubGroupDf.(df.Releaser).Release()
			assert.Equal(t, int64(1), mappedSubGroupDf.Len(), fmt.Sprintf("Group for key %v should have 1 row after map", keyRow))

			originalSubGroupDf := groupedDf.Get(keyRow) // Get original group
			defer originalSubGroupDf.(df.Releaser).Release()

			if originalSubGroupDf.Len() > 0 {
				expectedFirstRow := originalSubGroupDf.GetRow(0)
				actualFirstRowInMapped := mappedSubGroupDf.GetRow(0)

				// Compare row contents (example for first column)
				// This requires a deep comparison of row values.
				// For simplicity, we'll just check one value as a proxy.
				// A full test would iterate all columns.
				assert.Equal(t, expectedFirstRow.Get(2).Get(), actualFirstRowInMapped.Get(2).Get(), // Compare val_sum_int
					fmt.Sprintf("Content mismatch for key %v", keyRow))
			}
		})
	})

	t.Run("Map_PanicIfFuncReturnsNil", func(t *testing.T) {
		mapFuncNil := func(keyRow df.Row, groupSubDf df.DataFrame) df.DataFrame {
			// Release groupSubDf as it's an input to this lambda and won't be used if we return nil
			if r, ok := groupSubDf.(df.Releaser); ok { r.Release() }
			return nil
		}
		assert.Panics(t, func() {
			res := groupedDf.Map(mapFuncNil)
			if res != nil { res.(arrowimpl.Releaser).Release() }
		})
	})

	t.Run("Map_PanicIfFuncReturnsNonArrowDf", func(t *testing.T) {
		// Mock a non-arrow DataFrame
		type dummyDataFrame struct { df.DataFrame } // Minimal struct to satisfy interface
		// Add methods to dummyDataFrame to satisfy df.DataFrame if needed, or ensure type assertion is the primary check.
		// For this test, the type assertion in arrowGroupedDataFrame.Map is key.

		mapFuncNonArrow := func(keyRow df.Row, groupSubDf df.DataFrame) df.DataFrame {
			if r, ok := groupSubDf.(df.Releaser); ok { r.Release() } // Release the input group
			return &dummyDataFrame{} // Return a non-arrow DataFrame
		}
		assert.Panics(t, func() {
			res := groupedDf.Map(mapFuncNonArrow)
			if res != nil { res.(arrowimpl.Releaser).Release() }
		})
	})
}

func TestGroupedDataFrame_Agg(t *testing.T) {
	mem := memory.NewGoAllocator()
	originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
	defer originalDf.(df.Releaser).Release()
	defer groupedDf.(arrowimpl.Releaser).Release()

	// Expected groups and their characteristics for manual verification:
	// G1: {"groupA", 10} -> 3 rows. val_sum_int: {100, 200, nil} -> sum 300, mean 150. val_mean_float: {10.1, 20.2, 30.3} -> sum 60.6, mean 20.2. val_count_str_nullable: {"apple", "banana", nil} -> count 2
	// G2: {"groupB", 20} -> 2 rows. val_sum_int: {50, 60} -> sum 110, mean 55. val_mean_float: {5.5, 6.6} -> sum 12.1, mean 6.05. val_count_str_nullable: {"cat", "dog"} -> count 2
	// G3: {"groupA", 30} -> 1 row.  val_sum_int: {70} -> sum 70, mean 70. val_mean_float: {7.7} -> sum 7.7, mean 7.7. val_count_str_nullable: {"eel"} -> count 1
	// G4: {nil, 10}      -> 1 row.  val_sum_int: {80} -> sum 80, mean 80. val_mean_float: {8.8} -> sum 8.8, mean 8.8. val_count_str_nullable: {"frog"} -> count 1
	// G5: {"groupC", 40} -> 1 row.  val_sum_int: {90} -> sum 90, mean 90. val_mean_float: {9.9} -> sum 9.9, mean 9.9. val_count_str_nullable: {nil} -> count 0

	t.Run("Agg_SingleCountSpecificColumn", func(t *testing.T) {
		aggConfigs := []df.AggregationConfig{
			{Func: "count", InputCol: "val_count_str_nullable", OutputColName: "count_val_str"},
		}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()

		assert.Equal(t, groupedDf.Len(), aggDf.Len(), "Number of rows in aggregated DF should equal number of groups")
		assert.Equal(t, 3, aggDf.Schema().Len(), "Schema should have key cols + 1 agg col") // key1, key2, count_val_str
		assert.Equal(t, "count_val_str", aggDf.Schema().Get(2).Name)
		assert.Equal(t, df.IntegerFormat, aggDf.Schema().Get(2).Format) // Count is Int

		// Verify data - requires finding the correct row as order is not guaranteed.
		// For simplicity, we'll check a known group, e.g., {"groupA", 10}
		// This is brittle if GetRow order changes. A map-based check would be better for full validation.
		// Let's find the row for {"groupA", 10}
		var foundG1 bool
		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			if !row.GetByName("key1_str").IsNil() && row.GetByName("key1_str").GetAsString() == "groupA" && row.GetByName("key2_int").GetAsInt() == 10 {
				assert.Equal(t, int64(2), row.GetByName("count_val_str").GetAsInt(), "Count for groupA,10")
				foundG1 = true
				break
			}
		}
		assert.True(t, foundG1, "Group G1 (groupA, 10) not found in agg results")
	})

	t.Run("Agg_SingleCountAll", func(t *testing.T) {
		aggConfigs := []df.AggregationConfig{
			{Func: "count", OutputColName: "group_row_count"}, // No InputCol
		}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()
		assert.Equal(t, 3, aggDf.Schema().Len())
		assert.Equal(t, "group_row_count", aggDf.Schema().Get(2).Name)

		// Group {"groupA", 10} had 3 rows
		var foundG1 bool
		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			if !row.GetByName("key1_str").IsNil() && row.GetByName("key1_str").GetAsString() == "groupA" && row.GetByName("key2_int").GetAsInt() == 10 {
				assert.Equal(t, int64(3), row.GetByName("group_row_count").GetAsInt(), "Row count for groupA,10")
				foundG1 = true; break
			}
		}
		assert.True(t, foundG1)
	})

	t.Run("Agg_SingleSumInt", func(t *testing.T) {
		aggConfigs := []df.AggregationConfig{{Func: "sum", InputCol: "val_sum_int", OutputColName: "sum_val_int"}}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()
		// val_sum_int for {"groupA", 10} is {100, 200, nil}, sum should be 300
		var foundG1 bool
		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			if !row.GetByName("key1_str").IsNil() && row.GetByName("key1_str").GetAsString() == "groupA" && row.GetByName("key2_int").GetAsInt() == 10 {
				// Arrow's sum on int with nulls might produce int or float. Assuming int if all inputs are int.
				// The actual type depends on compute kernel. Let's check the value.
				// If original column was int64, sum is often int64.
				assert.Equal(t, df.IntegerFormat, row.GetByName("sum_val_int").Schema().Format)
				assert.Equal(t, int64(300), row.GetByName("sum_val_int").GetAsInt())
				foundG1 = true; break
			}
		}
		assert.True(t, foundG1)
	})

	t.Run("Agg_SingleMeanFloat", func(t *testing.T) {
		aggConfigs := []df.AggregationConfig{{Func: "mean", InputCol: "val_mean_float", OutputColName: "mean_val_float"}}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()
		// val_mean_float for {"groupA", 10} is {10.1, 20.2, 30.3}, mean should be 20.2
		var foundG1 bool
		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			if !row.GetByName("key1_str").IsNil() && row.GetByName("key1_str").GetAsString() == "groupA" && row.GetByName("key2_int").GetAsInt() == 10 {
				assert.Equal(t, df.DoubleFormat, row.GetByName("mean_val_float").Schema().Format) // Mean is usually float
				assert.InDelta(t, 20.2, row.GetByName("mean_val_float").GetAsDouble(), 0.00001)
				foundG1 = true; break
			}
		}
		assert.True(t, foundG1)
	})

	t.Run("Agg_MultipleAggregations", func(t *testing.T) {
		aggConfigs := []df.AggregationConfig{
			{Func: "sum", InputCol: "val_sum_int", OutputColName: "total_sum_int"},
			{Func: "mean", InputCol: "val_mean_float", OutputColName: "avg_mean_float"},
			{Func: "count", InputCol: "val_count_str_nullable", OutputColName: "non_null_count_str"},
			{Func: "min", InputCol: "val_sum_int", OutputColName: "min_sum_int"},
			{Func: "max", InputCol: "val_mean_float", OutputColName: "max_mean_float"},
		}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()

		assert.Equal(t, 2 + len(aggConfigs), aggDf.Schema().Len(), "Schema length for multiple aggs")

		// Spot check for group {"groupA", 10}
		// SumInt: 300, MeanFloat: 20.2, CountStr: 2, MinSumInt: 100, MaxMeanFloat: 30.3
		var foundG1 bool
		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			if !row.GetByName("key1_str").IsNil() && row.GetByName("key1_str").GetAsString() == "groupA" && row.GetByName("key2_int").GetAsInt() == 10 {
				assert.Equal(t, int64(300), row.GetByName("total_sum_int").GetAsInt())
				assert.InDelta(t, 20.2, row.GetByName("avg_mean_float").GetAsDouble(), 0.00001)
				assert.Equal(t, int64(2), row.GetByName("non_null_count_str").GetAsInt())
				assert.Equal(t, int64(100), row.GetByName("min_sum_int").GetAsInt())
				assert.InDelta(t, 30.3, row.GetByName("max_mean_float").GetAsDouble(), 0.00001)
				foundG1 = true; break
			}
		}
		assert.True(t, foundG1)
	})

	t.Run("Agg_AllNullsColumn", func(t *testing.T){
		aggConfigs := []df.AggregationConfig{
			{Func: "sum", InputCol: "val_all_nulls_float", OutputColName: "sum_all_null"},
			{Func: "count", InputCol: "val_all_nulls_float", OutputColName: "count_all_null"},
			{Func: "mean", InputCol: "val_all_nulls_float", OutputColName: "mean_all_null"},
		}
		aggDf := groupedDf.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()

		for i := int64(0); i < aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			// Sum of all nulls is often null or 0 depending on kernel. Arrow sum kernel returns null if all inputs are null.
			assert.True(t, row.GetByName("sum_all_null").IsNil(), "Sum of all nulls should be null")
			assert.Equal(t, int64(0), row.GetByName("count_all_null").GetAsInt(), "Count of all nulls should be 0")
			assert.True(t, row.GetByName("mean_all_null").IsNil(), "Mean of all nulls should be null")
		}
	})

	t.Run("Agg_NoConfigsReturnsDistinctKeys", func(t *testing.T){
		aggDf := groupedDf.Agg() // No aggregation configs
		defer aggDf.(df.Releaser).Release()

		assert.Equal(t, groupedDf.Len(), aggDf.Len(), "Agg with no configs should return same number of rows as groups")
		assert.Equal(t, len(groupedDf.GetGroupColumns()), aggDf.Schema().Len(), "Schema should only contain group key columns")

		// Verify keys are the same
		keysFromGrouped := groupedDf.GetKeys()
		keysFromAgg := make([][]interface{}, aggDf.Len())
		keySchema := keysFromGrouped[0].Schema() // Assume at least one key

		for i:=int64(0); i<aggDf.Len(); i++ {
			row := aggDf.GetRow(i)
			rowData := make([]interface{}, keySchema.Len())
			for j:=0; j<keySchema.Len(); j++ {
				val := row.Get(j) // Use index as names might slightly differ if not careful with schema from uniqueKeysTable
				if val.IsNil() { rowData[j] = nilPlaceholder } else { rowData[j] = val.Get() }
			}
			keysFromAgg[i] = rowData
		}
		sortSliceOfInterfaceSlices(keysFromAgg)

		expectedKeysFromGrouped := make([][]interface{}, len(keysFromGrouped))
		for i, keyRow := range keysFromGrouped {
			rowData := make([]interface{}, keyRow.Len())
			for j:=0; j<keyRow.Len(); j++ {
				val := keyRow.Get(j)
				if val.IsNil() { rowData[j] = nilPlaceholder } else { rowData[j] = val.Get() }
			}
			expectedKeysFromGrouped[i] = rowData
		}
		sortSliceOfInterfaceSlices(expectedKeysFromGrouped)
		assert.Equal(t, expectedKeysFromGrouped, keysFromAgg)
	})

	t.Run("AggOnEmptyGroupedDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}, {Name: "val", Type: arrow.PrimitiveTypes.Int64}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()
		valDataEmpty := getTestInt64Array(mem, []int64{}, nil); defer valDataEmpty.Release()

		emptyBaseDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_agg", fields, keyDataEmpty, valDataEmpty)
		defer emptyBaseDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }
		groupedEmpty := emptyBaseDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()

		aggConfigs := []df.AggregationConfig{{Func: "count", InputCol: "val", OutputColName: "count_val"}}
		aggDf := groupedEmpty.Agg(aggConfigs...)
		defer aggDf.(df.Releaser).Release()

		assert.Equal(t, int64(0), aggDf.Len())
		assert.Equal(t, 2, aggDf.Schema().Len()) // key + count_val
		assert.Equal(t, "key", aggDf.Schema().Get(0).Name)
		assert.Equal(t, "count_val", aggDf.Schema().Get(1).Name)
	})
}

func TestGroupedDataFrame_ForEach(t *testing.T) {
	mem := memory.NewGoAllocator()
	originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
	defer originalDf.(df.Releaser).Release()
	defer groupedDf.(arrowimpl.Releaser).Release()

	t.Run("ForEachIterationAndContent", func(t *testing.T) {
		numGroups := groupedDf.Len()
		executionCount := int64(0)

		allKeysFromGetKeys := groupedDf.GetKeys() // For later comparison

		groupedDf.ForEach(func(keyRow df.Row, groupSubDf df.DataFrame) {
			executionCount++
			assert.NotNil(t, keyRow, "KeyRow in ForEach should not be nil")
			assert.NotNil(t, groupSubDf, "Group DataFrame in ForEach should not be nil")
			defer groupSubDf.(df.Releaser).Release()

			// Check if keyRow is one of the keys from GetKeys
			foundKey := false
			for _, k := range allKeysFromGetKeys {
				// Simple equality check for key rows (assuming Get/GetByName access is consistent)
				// This is a basic check. A more robust check would compare values field by field.
				// For this test, we rely on the fact that keyRow's internal structure should match one from GetKeys.
				// Direct comparison of df.Row objects is not feasible unless they are identical instances or deeply comparable.
				// Let's compare their string representations for simplicity in a test, or compare values.

				// Compare values (more robust)
				match := true
				if k.Len() == keyRow.Len() {
					for i := 0; i < k.Len(); i++ {
						if k.Get(i).IsNil() != keyRow.Get(i).IsNil() ||
						   (!k.Get(i).IsNil() && k.Get(i).Get() != keyRow.Get(i).Get()) {
							match = false
							break
						}
					}
				} else {
					match = false
				}
				if match {
					foundKey = true
					break
				}
			}
			assert.True(t, foundKey, fmt.Sprintf("KeyRow provided to ForEach (%v) was not found in GetKeys result.", keyRow))


			// Verify all rows in groupSubDf match the keyRow for grouping columns
			for r := int64(0); r < groupSubDf.Len(); r++ {
				subDfRow := groupSubDf.GetRow(r)
				for i, keyColName := range groupedDf.GetGroupColumns() {
					expectedKeyVal := keyRow.Get(i)
					actualValInSubDf := subDfRow.GetByName(keyColName)
					if expectedKeyVal.IsNil() {
						assert.True(t, actualValInSubDf.IsNil())
					} else {
						assert.Equal(t, expectedKeyVal.Get(), actualValInSubDf.Get())
					}
				}
			}
		})
		assert.Equal(t, numGroups, executionCount, "ForEach should execute once for each group")
	})

	t.Run("ForEachOnEmptyGroupedDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()
		emptyBaseDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_foreach", fields, keyDataEmpty)
		defer emptyBaseDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }

		groupedEmpty := emptyBaseDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()

		callbackCalled := false
		groupedEmpty.ForEach(func(keyRow df.Row, groupSubDf df.DataFrame) {
			callbackCalled = true
			if groupSubDf != nil { groupSubDf.(df.Releaser).Release() }
		})
		assert.False(t, callbackCalled, "ForEach callback should not be called for an empty grouped DataFrame.")
	})
}

func TestGroupedDataFrame_Get(t *testing.T) {
	mem := memory.NewGoAllocator()
	originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem) // Provides a complex DF
	defer originalDf.(df.Releaser).Release()
	defer groupedDf.(arrowimpl.Releaser).Release()

	keys := groupedDf.GetKeys()
	assert.Greater(t, len(keys), 0, "Should have some keys to test Get")

	t.Run("GetExistingGroups", func(t *testing.T) {
		for _, keyRow := range keys {
			groupSubDf := groupedDf.Get(keyRow)
			defer groupSubDf.(df.Releaser).Release()

			assert.NotNil(t, groupSubDf, "Get(key) should return a DataFrame")
			assert.True(t, originalDf.Schema().Equals(groupSubDf.Schema()), "Schema of subgroup should match original DataFrame schema")

			// Verify all rows in groupSubDf match the keyRow for grouping columns
			for r := int64(0); r < groupSubDf.Len(); r++ {
				subDfRow := groupSubDf.GetRow(r)
				for i, keyColName := range groupedDf.GetGroupColumns() {
					expectedKeyVal := keyRow.Get(i)
					actualValInSubDf := subDfRow.GetByName(keyColName)
					if expectedKeyVal.IsNil() {
						assert.True(t, actualValInSubDf.IsNil(), fmt.Sprintf("Row %d, Key col %s: Expected nil, got %v", r, keyColName, actualValInSubDf.Get()))
					} else {
						assert.Equal(t, expectedKeyVal.Get(), actualValInSubDf.Get(), fmt.Sprintf("Row %d, Key col %s: Mismatch. Expected %v, got %v", r, keyColName, expectedKeyVal.Get(), actualValInSubDf.Get()))
					}
				}
			}
		}
	})

	t.Run("GetWithManuallyConstructedKey", func(t *testing.T) {
		// Assuming {"groupA", 10} is a valid key from getTestGroupedDataFrameForAgg
		// Manually construct a key df.Row.
		// The schema for the key row must match the schema of the keys returned by GetKeys().
		// This means it should be based on the grouping columns' types from the original DF.
		keySchemaFields := []arrow.Field{
			{Name: "key1_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "key2_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		keyArrowSchema := arrow.NewSchema(keySchemaFields, nil)
		keyDfSchema := arrowimpl.NewArrowDataFrameSchema(keyArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

		manualKeyValues := []df.Value{
			makeArrowValue("groupA", arrow.BinaryTypes.String),
			makeArrowValue(int64(10), arrow.PrimitiveTypes.Int64),
		}
		manualKeyRow := arrowimpl.NewArrowRowFromValues(keyDfSchema, manualKeyValues)

		groupDf := groupedDf.Get(manualKeyRow)
		defer groupDf.(df.Releaser).Release()
		assert.NotNil(t, groupDf)
		assert.Greater(t, groupDf.Len(), int64(0), "Expected non-empty DataFrame for key {'groupA', 10}")
		// Further checks as above: all rows in groupDf must match this key.
		groupDf.ForEachRow(func(r df.Row){
			assert.Equal(t, "groupA", r.GetByName("key1_str").Get())
			assert.Equal(t, int64(10), r.GetByName("key2_int").Get())
		})
	})

	t.Run("GetWithNonExistentKey", func(t *testing.T) {
		keySchemaFields := []arrow.Field{
			{Name: "key1_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "key2_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		keyArrowSchema := arrow.NewSchema(keySchemaFields, nil)
		keyDfSchema := arrowimpl.NewArrowDataFrameSchema(keyArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

		nonExistentKeyValues := []df.Value{
			makeArrowValue("non_existent_group", arrow.BinaryTypes.String),
			makeArrowValue(int64(-999), arrow.PrimitiveTypes.Int64),
		}
		nonExistentKeyRow := arrowimpl.NewArrowRowFromValues(keyDfSchema, nonExistentKeyValues)

		emptyGroupDf := groupedDf.Get(nonExistentKeyRow)
		defer emptyGroupDf.(df.Releaser).Release()
		assert.NotNil(t, emptyGroupDf)
		assert.Equal(t, int64(0), emptyGroupDf.Len(), "DataFrame for non-existent key should be empty")
		assert.True(t, originalDf.Schema().Equals(emptyGroupDf.Schema()), "Schema for non-existent key DF should match original")
	})

	t.Run("GetOnEmptyGroupedDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()
		emptyBaseDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_get", fields, keyDataEmpty)
		defer emptyBaseDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }
		groupedEmpty := emptyBaseDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()

		keySchemaFields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true}}
		keyArrowSchema := arrow.NewSchema(keySchemaFields, nil)
		keyDfSchema := arrowimpl.NewArrowDataFrameSchema(keyArrowSchema).(*arrowimpl.ArrowDataFrameSchema)
		someKeyRow := arrowimpl.NewArrowRowFromValues(keyDfSchema, []df.Value{makeArrowValue("any", arrow.BinaryTypes.String)})

		dfFromEmptyGrouped := groupedEmpty.Get(someKeyRow)
		defer dfFromEmptyGrouped.(df.Releaser).Release()
		assert.NotNil(t, dfFromEmptyGrouped)
		assert.Equal(t, int64(0), dfFromEmptyGrouped.Len())
		assert.True(t, emptyBaseDf.Schema().Equals(dfFromEmptyGrouped.Schema()))
	})
}

func TestGroupedDataFrame_GetKeys(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("GetKeysWithMultipleGroupsAndNullsInKeys", func(t *testing.T) {
		// Data includes: {"groupA", 10}, {"groupB", 20}, {"groupA", 30}, {nil, 10}, {"groupC", 40}
		originalDf, groupedDf := getTestGroupedDataFrameForAgg(t, mem)
		defer originalDf.(df.Releaser).Release()
		defer groupedDf.(arrowimpl.Releaser).Release()

		keys := groupedDf.GetKeys()
		assert.Equal(t, 5, len(keys), "Should be 5 unique groups")

		// Expected key schema: key1_str (string), key2_int (int64)
		expectedKeySchemaFields := []arrow.Field{
			{Name: "key1_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "key2_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Nullable because original might be, or if a key itself is null.
		}
		// The actual key schema might also include nullable flags based on input.
		// For simplicity, check names and basic types.
		// The NewArrowDataFrameSchemaFromRecord in GetKeys will reflect the uniqueKeysTable schema.

		if len(keys) > 0 {
			keyRowSchema := keys[0].Schema()
			assert.Equal(t, len(expectedKeySchemaFields), keyRowSchema.Len(), "Key row schema length mismatch")
			assert.Equal(t, "key1_str", keyRowSchema.Get(0).Name)
			assert.Equal(t, df.StringFormat.Name(), keyRowSchema.Get(0).Format.Name()) // Check format name
			assert.Equal(t, "key2_int", keyRowSchema.Get(1).Name)
			assert.Equal(t, df.IntegerFormat.Name(), keyRowSchema.Get(1).Format.Name())
		}

		// Convert keys to a slice of slices for easier comparison after sorting
		// Note: order of keys from GetKeys is not guaranteed.
		actualKeyData := make([][]interface{}, len(keys))
		for i, keyRow := range keys {
			actualKeyData[i] = []interface{}{keyRow.Get(0).Get(), keyRow.Get(1).Get()}
			// Handle nil for string key for consistent sorting/comparison
			if keyRow.Get(0).IsNil() {
				actualKeyData[i][0] = nilPlaceholder // Use placeholder for sorting if actual nil is problematic
			}
		}
		sortSliceOfInterfaceSlices(actualKeyData)

		expectedKeyData := [][]interface{}{
			{"groupA", int64(10)},
			{"groupA", int64(30)},
			{"groupB", int64(20)},
			{"groupC", int64(40)},
			{nilPlaceholder, int64(10)}, // Group with NULL key1_str
		}
		sortSliceOfInterfaceSlices(expectedKeyData)
		assert.Equal(t, expectedKeyData, actualKeyData, "Key data mismatch")
	})

	t.Run("GetKeysOnEmptyGroupedDataFrame", func(t *testing.T) {
		fields := []arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}}
		keyDataEmpty := getTestStringArray(mem, []string{}, nil); defer keyDataEmpty.Release()

		emptyDf, arrs := getBaseTestDfForGrouping(t, mem, "empty_df_keys", fields, keyDataEmpty)
		defer emptyDf.(df.Releaser).Release()
		for _, arr := range arrs { defer arr.Release() }

		groupedEmpty := emptyDf.GroupBy("key")
		defer groupedEmpty.(arrowimpl.Releaser).Release()

		keys := groupedEmpty.GetKeys()
		assert.Empty(t, keys, "GetKeys on an empty grouped DataFrame should return empty slice.")
	})
}
