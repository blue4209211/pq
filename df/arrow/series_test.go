//go:build arrow

package arrow_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar" // For timestamp test
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// Helper to create a simple Int64 array for testing series
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}

// Helper to create a simple String array for testing series
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}

// Helper to create a simple Boolean array
func getTestBoolArray(mem memory.Allocator, values []bool, valids []bool) arrow.Array {
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}

// Helper to create a Float64 array
func getTestFloat64Array(mem memory.Allocator, values []float64, valids []bool) arrow.Array {
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.AppendValues(values, valids)
	return b.NewArray()
}

// Helper to create a Timestamp array (nanosecond)
func getTestTimestampArrayNano(mem memory.Allocator, values []time.Time, valids []bool) arrow.Array {
	b := array.NewTimestampBuilder(mem, arrow.TimestampTypes.Timestamp_ns)
	defer b.Release()
	tsValues := make([]arrow.Timestamp, len(values))
	for i, v := range values {
		if valids == nil || (len(valids) > i && valids[i]) {
			tsValues[i] = arrow.Timestamp(v.UnixNano())
		}
	}
	b.AppendValues(tsValues, valids)
	return b.NewArray()
}


func TestArrowSeries_NewArrowSeries(t *testing.T) {
	mem := memory.NewGoAllocator()
	arr := getTestInt64Array(mem, []int64{1, 2, 3}, nil)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "col_int", Format: df.IntegerFormat}
	series := arrowimpl.NewArrowSeries(arr, sSchema)
	defer series.(*arrowimpl.ArrowSeries).Release()

	assert.NotNil(t, series)
	assert.Equal(t, sSchema, series.Schema())
	assert.Equal(t, int64(3), series.Len())

	assert.Panics(t, func() {
		arrowimpl.NewArrowSeries(nil, sSchema)
	}, "NewArrowSeries with nil array should panic")
}

func TestArrowSeries_Schema_Len_Get(t *testing.T) {
	mem := memory.NewGoAllocator()
	values := []int64{10, 20, 0, 40}
	valids := []bool{true, true, false, true}
	arr := getTestInt64Array(mem, values, valids)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "test_int_series", Format: df.IntegerFormat}
	series := arrowimpl.NewArrowSeries(arr, sSchema)
	defer series.(*arrowimpl.ArrowSeries).Release()

	assert.Equal(t, sSchema, series.Schema())
	assert.Equal(t, int64(len(values)), series.Len())

	val0 := series.Get(0)
	assert.False(t, val0.IsNil())
	assert.Equal(t, values[0], val0.GetAsInt())
	val2 := series.Get(2)
	assert.True(t, val2.IsNil())
	assert.Panics(t, func() { val2.GetAsInt() })

	assert.Panics(t, func() { series.Get(-1) })
	assert.Panics(t, func() { series.Get(series.Len()) })

	emptyArr := getTestInt64Array(mem, []int64{}, nil)
	defer emptyArr.Release()
	emptySeries := arrowimpl.NewArrowSeries(emptyArr, sSchema)
	defer emptySeries.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), emptySeries.Len())
	assert.Panics(t, func() { emptySeries.Get(0) })
}


func TestArrowSeries_Copy(t *testing.T) {
	mem := memory.NewGoAllocator()
	arr := getTestStringArray(mem, []string{"a", "b", "c"}, nil)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "col_str", Format: df.StringFormat}
	originalSeries := arrowimpl.NewArrowSeries(arr, sSchema)
	defer originalSeries.(*arrowimpl.ArrowSeries).Release()

	copiedSeries := originalSeries.Copy()
	defer copiedSeries.(*arrowimpl.ArrowSeries).Release()

	assert.NotSame(t, originalSeries, copiedSeries)
	assert.True(t, originalSeries.Schema().Equals(copiedSeries.Schema()))
	assert.Equal(t, originalSeries.Len(), copiedSeries.Len())
	for i := int64(0); i < originalSeries.Len(); i++ {
		assert.True(t, originalSeries.Get(i).Equals(copiedSeries.Get(i)))
	}
}

func TestArrowSeries_ForEach(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchema := df.SeriesSchema{Name: "foreach_int", Format: df.IntegerFormat}

	emptyArr := getTestInt64Array(mem, []int64{}, nil)
	defer emptyArr.Release()
	emptySeries := arrowimpl.NewArrowSeries(emptyArr, sSchema)
	defer emptySeries.(*arrowimpl.ArrowSeries).Release()
	countEmpty := 0
	emptySeries.ForEach(func(v df.Value) { countEmpty++ })
	assert.Equal(t, 0, countEmpty)

	values := []int64{5, 10, 0, 15}
	valids := []bool{true, true, false, true}
	arr := getTestInt64Array(mem, values, valids)
	defer arr.Release()
	series := arrowimpl.NewArrowSeries(arr, sSchema)
	defer series.(*arrowimpl.ArrowSeries).Release()

	var results []int64
	var nilEncountered bool
	series.ForEach(func(v df.Value) {
		if v.IsNil() { nilEncountered = true } else { results = append(results, v.GetAsInt()) }
	})
	assert.Equal(t, []int64{5, 10, 15}, results)
	assert.True(t, nilEncountered)
}

func TestArrowSeries_Limit(t *testing.T) {
	mem := memory.NewGoAllocator()
	values := []int64{0, 1, 2, 3, 4, 5}
	sSchema := df.SeriesSchema{Name: "limit_int", Format: df.IntegerFormat}
	arr := getTestInt64Array(mem, values, nil)
	defer arr.Release()
	series := arrowimpl.NewArrowSeries(arr, sSchema)
	defer series.(*arrowimpl.ArrowSeries).Release()

	limited1 := series.Limit(1, 3)
	defer limited1.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(3), limited1.Len())
	assert.Equal(t, int64(1), limited1.Get(0).GetAsInt())

	limited4 := series.Limit(10, 2)
	defer limited4.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), limited4.Len())
}

func TestArrowSeries_Where(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "where_int", Format: df.IntegerFormat}

	intVals := []int64{1, 2, 0, 3, 4, 0, 5}
	intValids := []bool{true, true, false, true, true, false, true}
	intArr := getTestInt64Array(mem, intVals, intValids)
	defer intArr.Release()
	intSeries := arrowimpl.NewArrowSeries(intArr, sSchemaInt)
	defer intSeries.(*arrowimpl.ArrowSeries).Release()

	evenFilter := func(v df.Value) bool { return !v.IsNil() && v.GetAsInt()%2 == 0 }
	filteredEvens := intSeries.Where(evenFilter)
	defer filteredEvens.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(2), filteredEvens.Len())
	assert.Equal(t, int64(2), filteredEvens.Get(0).GetAsInt())
	assert.Equal(t, int64(4), filteredEvens.Get(1).GetAsInt())
}

func TestArrowSeries_Sort(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test 1: Int64 sort ascending
	sSchemaInt := df.SeriesSchema{Name: "sort_int", Format: df.IntegerFormat}
	intVals := []int64{30, 0, 10, 0, 20}
	intValids := []bool{true, false, true, false, true} // Two nils (0s)
	intArr := getTestInt64Array(mem, intVals, intValids)
	defer intArr.Release()
	intSeries := arrowimpl.NewArrowSeries(intArr, sSchemaInt)
	defer intSeries.(*arrowimpl.ArrowSeries).Release()

	sortedIntAsc := intSeries.Sort(df.SortOrderASC)
	defer sortedIntAsc.(*arrowimpl.ArrowSeries).Release()
	// Expect nils first: nil, nil, 10, 20, 30
	assert.Equal(t, intSeries.Len(), sortedIntAsc.Len())
	assert.True(t, sortedIntAsc.Get(0).IsNil(), "ASC Sort: Nil 1")
	assert.True(t, sortedIntAsc.Get(1).IsNil(), "ASC Sort: Nil 2")
	assert.Equal(t, int64(10), sortedIntAsc.Get(2).GetAsInt(), "ASC Sort: 10")
	assert.Equal(t, int64(20), sortedIntAsc.Get(3).GetAsInt(), "ASC Sort: 20")
	assert.Equal(t, int64(30), sortedIntAsc.Get(4).GetAsInt(), "ASC Sort: 30")

	// Test 2: Int64 sort descending
	sortedIntDesc := intSeries.Sort(df.SortOrderDESC)
	defer sortedIntDesc.(*arrowimpl.ArrowSeries).Release()
	// Expect (nil first): nil, nil, 30, 20, 10
	assert.True(t, sortedIntDesc.Get(0).IsNil(), "DESC Sort: Nil 1")
	assert.True(t, sortedIntDesc.Get(1).IsNil(), "DESC Sort: Nil 2")
	assert.Equal(t, int64(30), sortedIntDesc.Get(2).GetAsInt(), "DESC Sort: 30")
	assert.Equal(t, int64(20), sortedIntDesc.Get(3).GetAsInt(), "DESC Sort: 20")
	assert.Equal(t, int64(10), sortedIntDesc.Get(4).GetAsInt(), "DESC Sort: 10")


	// Test 3: String sort ascending
	sSchemaStr := df.SeriesSchema{Name: "sort_str", Format: df.StringFormat}
	strVals := []string{"banana", "apple", "", "cherry", "date"} // "" is nil
	strValids := []bool{true, true, false, true, true}
	strArr := getTestStringArray(mem, strVals, strValids)
	defer strArr.Release()
	strSeries := arrowimpl.NewArrowSeries(strArr, sSchemaStr)
	defer strSeries.(*arrowimpl.ArrowSeries).Release()

	sortedStrAsc := strSeries.Sort(df.SortOrderASC)
	defer sortedStrAsc.(*arrowimpl.ArrowSeries).Release()
	// Expect nil first: nil (""), "apple", "banana", "cherry", "date"
	assert.True(t, sortedStrAsc.Get(0).IsNil())
	assert.Equal(t, "apple", sortedStrAsc.Get(1).GetAsString())
	assert.Equal(t, "banana", sortedStrAsc.Get(2).GetAsString())
	assert.Equal(t, "cherry", sortedStrAsc.Get(3).GetAsString())
	assert.Equal(t, "date", sortedStrAsc.Get(4).GetAsString())

	// Test 4: Float64 sort descending
	sSchemaFloat := df.SeriesSchema{Name: "sort_float", Format: df.DoubleFormat}
	floatVals := []float64{3.3, 0.0, 1.1, 0.0, 2.2} // two nils
	floatValids := []bool{true, false, true, false, true}
	floatArr := getTestFloat64Array(mem, floatVals, floatValids)
	defer floatArr.Release()
	floatSeries := arrowimpl.NewArrowSeries(floatArr, sSchemaFloat)
	defer floatSeries.(*arrowimpl.ArrowSeries).Release()

	sortedFloatDesc := floatSeries.Sort(df.SortOrderDESC)
	defer sortedFloatDesc.(*arrowimpl.ArrowSeries).Release()
	// Expect nils first: nil, nil, 3.3, 2.2, 1.1
	assert.True(t, sortedFloatDesc.Get(0).IsNil())
	assert.True(t, sortedFloatDesc.Get(1).IsNil())
	assert.Equal(t, 3.3, sortedFloatDesc.Get(2).GetAsDouble())
	assert.Equal(t, 2.2, sortedFloatDesc.Get(3).GetAsDouble())
	assert.Equal(t, 1.1, sortedFloatDesc.Get(4).GetAsDouble())

	// Test 5: Empty series sort
	emptyArr := getTestInt64Array(mem, []int64{}, nil)
	defer emptyArr.Release()
	emptySeries := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt)
	defer emptySeries.(*arrowimpl.ArrowSeries).Release()
	sortedEmpty := emptySeries.Sort(df.SortOrderASC)
	defer sortedEmpty.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sortedEmpty.Len())

	// Test 6: Timestamp sort ascending
	sSchemaTime := df.SeriesSchema{Name: "sort_time", Format: df.DateTimeFormat}
	timeVals := []time.Time{
		time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC), // t2
		{}, // nil placeholder
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),  // t1
		time.Date(2023, 1, 20, 0, 0, 0, 0, time.UTC), // t3
	}
	timeValids := []bool{true, false, true, true}
	timeArr := getTestTimestampArrayNano(mem, timeVals, timeValids)
	defer timeArr.Release()
	timeSeries := arrowimpl.NewArrowSeries(timeArr,sSchemaTime)
	defer timeSeries.(*arrowimpl.ArrowSeries).Release()

	sortedTimeAsc := timeSeries.Sort(df.SortOrderASC)
	defer sortedTimeAsc.(*arrowimpl.ArrowSeries).Release()

	assert.True(t, sortedTimeAsc.Get(0).IsNil(), "Timestamp ASC: Nil 1")
	assert.Equal(t, timeVals[2], sortedTimeAsc.Get(1).GetAsDatetime(), "Timestamp ASC: t1")
	assert.Equal(t, timeVals[0], sortedTimeAsc.Get(2).GetAsDatetime(), "Timestamp ASC: t2")
	assert.Equal(t, timeVals[3], sortedTimeAsc.Get(3).GetAsDatetime(), "Timestamp ASC: t3")
}

// TODO: Add more tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
