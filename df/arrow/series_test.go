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


func TestArrowSeries_NewArrowSeries(t *testing.T) {
	mem := memory.NewGoAllocator()
	arr := getTestInt64Array(mem, []int64{1, 2, 3}, nil)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "col_int", Format: df.IntegerFormat}
	series := arrowimpl.NewArrowSeries(arr, sSchema)

	assert.NotNil(t, series)
	assert.Equal(t, sSchema, series.Schema())
	assert.Equal(t, int64(3), series.Len())

	// Test with nil array (should panic as per current NewArrowSeries implementation)
	assert.Panics(t, func() {
		arrowimpl.NewArrowSeries(nil, sSchema)
	}, "NewArrowSeries with nil array should panic")
}

func TestArrowSeries_Schema_Len_Get(t *testing.T) {
	mem := memory.NewGoAllocator()
	values := []int64{10, 20, 0, 40}
	valids := []bool{true, true, false, true} // 0 is nil
	arr := getTestInt64Array(mem, values, valids)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "test_int_series", Format: df.IntegerFormat}
	series := arrowimpl.NewArrowSeries(arr, sSchema)

	// Schema()
	assert.Equal(t, sSchema, series.Schema())

	// Len()
	assert.Equal(t, int64(len(values)), series.Len())

	// Get()
	val0 := series.Get(0)
	assert.False(t, val0.IsNil())
	assert.Equal(t, values[0], val0.GetAsInt())

	val1 := series.Get(1)
	assert.False(t, val1.IsNil())
	assert.Equal(t, values[1], val1.GetAsInt())

	val2 := series.Get(2) // This one is nil
	assert.True(t, val2.IsNil())
	assert.Panics(t, func() { val2.GetAsInt() }, "GetAsInt on a nil pq/df.Value should panic")

	val3 := series.Get(3)
	assert.False(t, val3.IsNil())
	assert.Equal(t, values[3], val3.GetAsInt())

	// Get() out of bounds
	assert.Panics(t, func() { series.Get(-1) })
	assert.Panics(t, func() { series.Get(series.Len()) })


	// Test with empty array
	emptyArr := getTestInt64Array(mem, []int64{}, nil)
	defer emptyArr.Release()
	emptySeries := arrowimpl.NewArrowSeries(emptyArr, sSchema)
	assert.Equal(t, int64(0), emptySeries.Len())
	assert.Panics(t, func() { emptySeries.Get(0) })
}


func TestArrowSeries_Copy(t *testing.T) {
	mem := memory.NewGoAllocator()
	arr := getTestStringArray(mem, []string{"a", "b", "c"}, nil)
	defer arr.Release()

	sSchema := df.SeriesSchema{Name: "col_str", Format: df.StringFormat}
	originalSeries := arrowimpl.NewArrowSeries(arr, sSchema)

	copiedSeries := originalSeries.Copy()
	// Release original series's array to ensure copy is independent for data validity
    // This is a bit tricky because NewArrowSeries doesn't explicitly say it retains the array internally for the series struct,
    // but Copy() does a Retain on the slice. Best practice would be for NewArrowSeries to also Retain.
    // For this test, let's assume NewArrowSeries (and thus originalSeries) "owns" its reference to arr.
    // When originalSeries goes out of scope or is GC'd, its arr would be released if not retained elsewhere.
    // The `copiedSeries` should have its own valid reference.
    // To simulate this, we can manually release the original `arr` after copy, if the original series
    // did not explicitly Retain its own reference beyond the lifetime of the passed `arr`.
    // However, the current `NewArrowSeries` just assigns `arr`. `Copy` does `Retain`.
    // So, releasing `arr` here tests if `copiedSeries` correctly retained.
    // arr.Release() // This might be too aggressive if originalSeries is used after this.
                   // Instead, let's focus on the content and independent instance.

	// Ensure they are different instances but have same content and schema
	assert.NotSame(t, originalSeries, copiedSeries)
	assert.True(t, originalSeries.Schema().Equals(copiedSeries.Schema())) // Compare schema content
	assert.Equal(t, originalSeries.Len(), copiedSeries.Len())

	for i := int64(0); i < originalSeries.Len(); i++ {
		assert.True(t, originalSeries.Get(i).Equals(copiedSeries.Get(i)), "Values at index %d should be equal", i)
	}


	// Test copying a series created with an empty array (not nil array, as constructor panics)
	emptyArr := getTestStringArray(mem, []string{}, nil)
	defer emptyArr.Release()
	emptyOriginalSeries := arrowimpl.NewArrowSeries(emptyArr, sSchema)
	emptyCopiedSeries := emptyOriginalSeries.Copy()

	assert.NotSame(t, emptyOriginalSeries, emptyCopiedSeries)
	assert.Equal(int64(0), emptyCopiedSeries.Len())
	assert.True(t, emptyOriginalSeries.Schema().Equals(emptyCopiedSeries.Schema()))

}

// TODO: Add tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
