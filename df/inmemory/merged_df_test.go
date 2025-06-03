//go:build inmemory

package inmemory_test // Test files should be in _test package

import (
	"fmt"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

// Helper to create an inmemory.DataFrame for testing
func createTestInMemoryDF(t *testing.T, name string, colDefs []df.SeriesSchema, rowsData [][]interface{}) df.DataFrame {
	t.Helper()
	schema := df.NewSchema(name, colDefs)

	rows := make([]df.Row, len(rowsData))
	for i, rowData := range rowsData {
		values := make([]df.Value, len(rowData))
		if len(rowData) != len(colDefs) {
			t.Fatalf("Row data length %d does not match colDefs length %d for row %d", len(rowData), len(colDefs), i)
		}
		for j, cellData := range rowData {
			colFormat := colDefs[j].Format
			// Use concrete types from inmemory package
			switch colFormat.Name() {
			case df.IntegerFormat.Name():
				if cellData == nil {
					values[j] = inmemory.NewIntValue(nil)
				} else {
					values[j] = inmemory.NewIntValueConst(cellData.(int64))
				}
			case df.StringFormat.Name():
				if cellData == nil {
					values[j] = inmemory.NewStringValue(nil)
				} else {
					values[j] = inmemory.NewStringValueConst(cellData.(string))
				}
			case df.DoubleFormat.Name():
				if cellData == nil {
					values[j] = inmemory.NewDoubleValue(nil)
				} else {
					values[j] = inmemory.NewDoubleValueConst(cellData.(float64))
				}
			case df.BoolFormat.Name():
				if cellData == nil {
					values[j] = inmemory.NewBoolValue(nil)
				} else {
					values[j] = inmemory.NewBoolValueConst(cellData.(bool))
				}
			default:
				t.Fatalf("Unsupported format in test helper: %s", colFormat.Name())
			}
		}
		rows[i] = inmemory.NewRow(&schema, &values)
	}
	return inmemory.NewDataframeFromRowAndName(name, schema, &rows)
}

// Helper to convert DataFrame to slice of slices for easier comparison
func dfToSlice(t *testing.T, dataFrame df.DataFrame) [][]interface{} {
	t.Helper()
	var result [][]interface{}
	if dataFrame == nil {
		return result
	}
	for r := int64(0); r < dataFrame.Len(); r++ {
		row := dataFrame.GetRow(r)
		var rowData []interface{}
		for c := 0; c < row.Len(); c++ {
			val := row.Get(c)
			if val.IsNil() {
				rowData = append(rowData, nil) // Use actual nil for easier comparison with expected
			} else {
				rowData = append(rowData, val.Get())
			}
		}
		result = append(result, rowData)
	}
	return result
}


func TestNewMergeDataframe_EmptyInput(t *testing.T) {
	mergedDf, err := inmemory.NewMergeDataframe("test_empty")
	assert.Error(t, err, "Expected error for empty input")
	if err != nil { // Check error message only if error is not nil
		assert.Equal(t, "empty data", err.Error())
	}
	assert.Nil(t, mergedDf, "DataFrame should be nil on error")
}

func TestNewMergeDataframe_SingleDataFrame(t *testing.T) {
	cols := []df.SeriesSchema{
		{Name: "id", Format: df.IntegerFormat},
		{Name: "name", Format: df.StringFormat},
	}
	data1 := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}
	df1 := createTestInMemoryDF(t, "df1", cols, data1)

	mergedDf, err := inmemory.NewMergeDataframe("renamed_df", df1)
	assert.NoError(t, err)
	assert.NotNil(t, mergedDf)

	assert.Equal(t, "renamed_df", mergedDf.Name(), "Merged DF name should be updated")
	assert.Equal(t, "df1", df1.Name(), "Original DF name should be unchanged")

	assert.True(t, df1.Schema().Equals(mergedDf.Schema()), "Schemas should be equal")
	assert.Equal(t, df1.Len(), mergedDf.Len(), "Lengths should be equal")
	assert.Equal(t, dfToSlice(t, df1), dfToSlice(t, mergedDf), "Row data should be identical")
}

func TestNewMergeDataframe_Multiple_IdenticalSchemas(t *testing.T) {
	cols := []df.SeriesSchema{
		{Name: "id", Format: df.IntegerFormat},
		{Name: "value", Format: df.StringFormat},
	}
	data1 := [][]interface{}{{int64(1), "A"}, {int64(2), "B"}}
	df1 := createTestInMemoryDF(t, "df1", cols, data1)

	data2 := [][]interface{}{{int64(3), "C"}, {int64(4), "D"}}
	df2 := createTestInMemoryDF(t, "df2", cols, data2)

	data3 := [][]interface{}{{int64(5), "E"}}
	df3 := createTestInMemoryDF(t, "df3", cols, data3)

	mergedDf, err := inmemory.NewMergeDataframe("merged_identical", df1, df2, df3)
	assert.NoError(t, err)
	assert.NotNil(t, mergedDf)

	assert.Equal(t, "merged_identical", mergedDf.Name())
	assert.True(t, df1.Schema().Equals(mergedDf.Schema()), "Schema should be from the first DataFrame")
	expectedLen := df1.Len() + df2.Len() + df3.Len()
	assert.Equal(t, expectedLen, mergedDf.Len(), "Length should be sum of input lengths")

	expectedData := [][]interface{}{
		{int64(1), "A"}, {int64(2), "B"},
		{int64(3), "C"}, {int64(4), "D"},
		{int64(5), "E"},
	}
	assert.Equal(t, expectedData, dfToSlice(t, mergedDf), "Row data should be concatenated")
}

func TestNewMergeDataframe_WithEmptyDataFrames(t *testing.T) {
	cols := []df.SeriesSchema{
		{Name: "val", Format: df.DoubleFormat},
	}
	data1 := [][]interface{}{{1.1}, {2.2}}
	df1 := createTestInMemoryDF(t, "df1", cols, data1)

	dfEmpty := createTestInMemoryDF(t, "dfEmpty", cols, [][]interface{}{})

	data2 := [][]interface{}{{3.3}}
	df2 := createTestInMemoryDF(t, "df2", cols, data2)

	mergedDf1, err1 := inmemory.NewMergeDataframe("merged_with_empty1", df1, dfEmpty, df2)
	assert.NoError(t, err1)
	assert.NotNil(t, mergedDf1)
	assert.Equal(t, df1.Len()+df2.Len(), mergedDf1.Len())
	assert.True(t, df1.Schema().Equals(mergedDf1.Schema()))
	expectedData1 := [][]interface{}{{1.1}, {2.2}, {3.3}}
	assert.Equal(t, expectedData1, dfToSlice(t, mergedDf1))

	mergedDf2, err2 := inmemory.NewMergeDataframe("merged_with_empty2", dfEmpty, df1, df2)
	assert.NoError(t, err2)
	assert.NotNil(t, mergedDf2)
	assert.Equal(t, df1.Len()+df2.Len(), mergedDf2.Len())
	assert.True(t, dfEmpty.Schema().Equals(mergedDf2.Schema()))
	expectedData2 := [][]interface{}{{1.1}, {2.2}, {3.3}}
	assert.Equal(t, expectedData2, dfToSlice(t, mergedDf2))

	dfEmpty2 := createTestInMemoryDF(t, "dfEmpty2", cols, [][]interface{}{})
	mergedDf3, err3 := inmemory.NewMergeDataframe("merged_all_empty", dfEmpty, dfEmpty2)
	assert.NoError(t, err3)
	assert.NotNil(t, mergedDf3)
	assert.Equal(t, int64(0), mergedDf3.Len())
	assert.True(t, dfEmpty.Schema().Equals(mergedDf3.Schema()))
}


func TestNewMergeDataframe_SchemaCompatibility_Implicit(t *testing.T) {
	t.Run("CompatibleStructureDifferentNames", func(t *testing.T) {
		cols1 := []df.SeriesSchema{
			{Name: "colA", Format: df.IntegerFormat},
			{Name: "colB", Format: df.StringFormat},
		}
		data1 := [][]interface{}{{int64(1), "hello"}}
		df1 := createTestInMemoryDF(t, "df1_names", cols1, data1)

		cols2 := []df.SeriesSchema{
			{Name: "fieldA", Format: df.IntegerFormat},
			{Name: "fieldB", Format: df.StringFormat},
		}
		data2 := [][]interface{}{{int64(100), "world"}}
		df2 := createTestInMemoryDF(t, "df2_names", cols2, data2)

		mergedDf, err := inmemory.NewMergeDataframe("merged_compat_names", df1, df2)
		assert.NoError(t, err)
		assert.NotNil(t, mergedDf)

		assert.Equal(t, "colA", mergedDf.Schema().Get(0).Name)
		assert.Equal(t, "colB", mergedDf.Schema().Get(1).Name)

		expectedData := [][]interface{}{
			{int64(1), "hello"},
			{int64(100), "world"},
		}
		assert.Equal(t, expectedData, dfToSlice(t, mergedDf))
	})

	t.Run("IncompatibleTypes", func(t *testing.T) {
		cols1 := []df.SeriesSchema{
			{Name: "colA", Format: df.IntegerFormat},
			{Name: "colB", Format: df.StringFormat},
		}
		data1 := [][]interface{}{{int64(1), "hello"}}
		df1 := createTestInMemoryDF(t, "df1_types", cols1, data1)

		cols2 := []df.SeriesSchema{
			{Name: "colA", Format: df.StringFormat},
			{Name: "colB", Format: df.IntegerFormat},
		}
		data2 := [][]interface{}{{"test", int64(99)}}
		df2 := createTestInMemoryDF(t, "df2_types", cols2, data2)

		mergedDf, err := inmemory.NewMergeDataframe("merged_incompat_types", df1, df2)
		assert.NoError(t, err, "NewMergeDataframe itself does not error on schema type incompatibility")
		assert.NotNil(t, mergedDf)

		row0 := mergedDf.GetRow(0)
		assert.Equal(t, int64(1), row0.Get(0).GetAsInt())
		assert.Equal(t, "hello", row0.Get(1).GetAsString())

		row1 := mergedDf.GetRow(1)
		// df1 schema: colA (idx 0) is int, colB (idx 1) is string
		// df2 data was: "test" (for colA's position), int64(99) (for colB's position)

		// The inmemory.Value concrete types will be StringValue and IntValue.
		// Accessing them with the wrong GetAs<Type>() will panic.
		assert.Panics(t, func() { _ = row1.Get(0).GetAsInt() }, "Accessing string as int should panic")
		assert.Equal(t, "test", row1.Get(0).GetAsString(), "Accessing string as string should work")

		assert.Panics(t, func() { _ = row1.Get(1).GetAsString() }, "Accessing int as string should panic")
		assert.Equal(t, int64(99), row1.Get(1).GetAsInt(), "Accessing int as int should work")
	})
}
