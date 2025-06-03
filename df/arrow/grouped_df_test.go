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
