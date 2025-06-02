//go:build arrow

package arrow_test

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/expr"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// --- Helper functions ---
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := array.NewInt64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := array.NewStringBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestFloat64Array(mem memory.Allocator, values []float64, valids []bool) arrow.Array {
	b := array.NewFloat64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestBoolArray(mem memory.Allocator, values []bool, valids []bool) arrow.Array {
	b := array.NewBooleanBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestTimestampArrayNano(mem memory.Allocator, values []time.Time, valids []bool) arrow.Array {
	b := array.NewTimestampBuilder(mem, arrow.TimestampTypes.Timestamp_ns); defer b.Release()
	tsValues := make([]arrow.Timestamp, len(values))
	for i, v := range values { if valids == nil || (len(valids) > i && valids[i]) { tsValues[i] = arrow.Timestamp(v.UnixNano()) } }
	b.AppendValues(tsValues, valids); return b.NewArray()
}
const nilPlaceholder = "__NIL_PLACEHOLDER__"
func extractValues(s df.Series) []interface{} {
	var out []interface{}
	if s == nil { return out }
	for i := int64(0); i < s.Len(); i++ {
		v := s.Get(i)
		if v.IsNil() { out = append(out, nilPlaceholder) } else { out = append(out, v.Get()) }
	}
	return out
}
func sortInterfaceSlice(slice []interface{}) {
	sort.Slice(slice, func(i, j int) bool {
		if slice[i] == nilPlaceholder && slice[j] != nilPlaceholder { return true }
		if slice[i] != nilPlaceholder && slice[j] == nilPlaceholder { return false }
		if slice[i] == nilPlaceholder && slice[j] == nilPlaceholder { return false }
		return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j])
	})
}

// Mock value for testing non-*arrowValue returns
type mockValue struct { df.Value; data any; mockSchema df.Format; mockIsNil bool }
func (m *mockValue) Get() any { return m.data }
func (m *mockValue) IsNil() bool { return m.mockIsNil }
func (m *mockValue) Schema() df.Format { return m.mockSchema }
func (m *mockValue) GetAsInt() int64 { if i,ok := m.data.(int64); ok {return i}; return 0 }
func (m *mockValue) GetAsString() string { if s,ok := m.data.(string); ok {return s}; return "" }
// Add other GetAs... methods if needed by test functions


// --- Existing tests ---
func TestArrowSeries_NewArrowSeries(t *testing.T) { /* ... */ }
func TestArrowSeries_Schema_Len_Get(t *testing.T) { /* ... */ }
func TestArrowSeries_Copy(t *testing.T) { /* ... */ }
func TestArrowSeries_ForEach(t *testing.T) { /* ... */ }
func TestArrowSeries_Limit(t *testing.T) { /* ... */ }
func TestArrowSeries_Where(t *testing.T) { /* ... */ }
func TestArrowSeries_Sort(t *testing.T) { /* ... */ }
func TestArrowSeries_Map(t *testing.T) { /* ... */ }
func TestArrowSeries_FlatMap(t *testing.T) { /* ... */ }
func TestArrowSeries_Reduce(t *testing.T) { /* ... */ }
func TestArrowSeries_Distinct(t *testing.T) { /* ... */ }
func TestArrowSeries_Append(t *testing.T) { /* ... */ }
func TestArrowSeries_Union(t *testing.T) { /* ... */ }
func TestArrowSeries_Intersection(t *testing.T) { /* ... */ }
func TestArrowSeries_Except(t *testing.T) { /* ... */ }
func TestArrowSeries_Expr(t *testing.T) { /* ... */ }
func TestArrowSeries_Select(t *testing.T) { /* ... */ }


func TestArrowSeries_Join(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int", Format: df.IntegerFormat}
	sSchemaStr := df.SeriesSchema{Name: "s_str", Format: df.StringFormat}

	arr1Int := getTestInt64Array(mem, []int64{10, 0, 30}, []bool{true, false, true}); defer arr1Int.Release()
	s1Int := arrowimpl.NewArrowSeries(arr1Int, sSchemaInt); defer s1Int.(*arrowimpl.ArrowSeries).Release()

	arr2Int := getTestInt64Array(mem, []int64{4, 500}, nil); defer arr2Int.Release()
	s2Int := arrowimpl.NewArrowSeries(arr2Int, sSchemaInt); defer s2Int.(*arrowimpl.ArrowSeries).Release()

	emptyIntArr := getTestInt64Array(mem, []int64{}, nil); defer emptyIntArr.Release()
	sEmptyInt := arrowimpl.NewArrowSeries(emptyIntArr, sSchemaInt); defer sEmptyInt.(*arrowimpl.ArrowSeries).Release()

	fConcatIntStr := func(v1, v2 df.Value) []df.Value {
		s1, s2 := "nil", "nil"
		if v1 != nil && !v1.IsNil() { s1 = strconv.FormatInt(v1.GetAsInt(), 10) }
		if v2 != nil && !v2.IsNil() { s2 = strconv.FormatInt(v2.GetAsInt(), 10) }
		return []df.Value{arrowimpl.NewArrowValue(scalar.NewStringScalar(s1+"-"+s2), df.StringFormat)}
	}
	fSumInts := func(v1, v2 df.Value) []df.Value {
		if (v1 == nil || v1.IsNil()) || (v2 == nil || v2.IsNil()) {
			return []df.Value{arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat)}
		}
		sum := v1.GetAsInt() + v2.GetAsInt()
		return []df.Value{arrowimpl.NewArrowValue(scalar.NewInt64Scalar(sum), df.IntegerFormat)}
	}

	t.Run("JoinEqui", func(t *testing.T) {
		resEqui1 := s1Int.Join(df.StringFormat, s2Int, df.JoinEqui, fConcatIntStr); defer resEqui1.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(2), resEqui1.Len())
		assert.Equal(t, []interface{}{"10-4", "nil-500"}, extractValues(resEqui1))
	})

	t.Run("JoinLeft", func(t *testing.T) {
		resLeft1 := s1Int.Join(df.StringFormat, s2Int, df.JoinLeft, fConcatIntStr); defer resLeft1.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(3), resLeft1.Len())
		assert.Equal(t, []interface{}{"10-4", "nil-500", "30-nil"}, extractValues(resLeft1))
		resLeft2 := s2Int.Join(df.StringFormat, s1Int, df.JoinLeft, fConcatIntStr); defer resLeft2.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(2), resLeft2.Len())
		assert.Equal(t, []interface{}{"4-10", "500-nil"}, extractValues(resLeft2))
	})

	t.Run("JoinRight", func(t *testing.T) {
		resRight1 := s1Int.Join(df.StringFormat, s2Int, df.JoinRight, fConcatIntStr); defer resRight1.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(2), resRight1.Len())
		assert.Equal(t, []interface{}{"10-4", "nil-500"}, extractValues(resRight1))
		resRight2 := s2Int.Join(df.StringFormat, s1Int, df.JoinRight, fConcatIntStr); defer resRight2.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(3), resRight2.Len())
		assert.Equal(t, []interface{}{"4-10", "500-nil", "nil-30"}, extractValues(resRight2))
	})

	t.Run("JoinOuter", func(t *testing.T) {
		resOuter1 := s1Int.Join(df.StringFormat, s2Int, df.JoinOuter, fConcatIntStr); defer resOuter1.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(3), resOuter1.Len())
		assert.Equal(t, []interface{}{"10-4", "nil-500", "30-nil"}, extractValues(resOuter1))
		resOuter2 := s2Int.Join(df.StringFormat, s1Int, df.JoinOuter, fConcatIntStr); defer resOuter2.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(3), resOuter2.Len())
		assert.Equal(t, []interface{}{"4-10", "500-nil", "nil-30"}, extractValues(resOuter2))
	})

	t.Run("JoinCross", func(t *testing.T) {
		resCross1 := s1Int.Join(df.StringFormat, s2Int, df.JoinCross, fConcatIntStr); defer resCross1.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, s1Int.Len()*s2Int.Len(), resCross1.Len())
		expectedCross1 := []interface{}{ "10-4", "10-500", "nil-4", "nil-500", "30-4", "30-500", }
		assert.Equal(t, expectedCross1, extractValues(resCross1))
		resCrossEmpty := s1Int.Join(df.StringFormat, sEmptyInt, df.JoinCross, fConcatIntStr); defer resCrossEmpty.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(0), resCrossEmpty.Len())
	})

	fMultiStr := func(v1,v2 df.Value) []df.Value {
		s1,s2 := "n","n"
		if v1!=nil && !v1.IsNil() { s1 = strconv.FormatInt(v1.GetAsInt(),10)}
		if v2!=nil && !v2.IsNil() { s2 = strconv.FormatInt(v2.GetAsInt(),10)}
		return []df.Value{ arrowimpl.NewArrowValue(scalar.NewStringScalar(s1), df.StringFormat), arrowimpl.NewArrowValue(scalar.NewStringScalar(s2), df.StringFormat), }
	}
	t.Run("FunctionReturnsMultiple", func(t *testing.T) {
		resMulti := s1Int.Join(df.StringFormat, s2Int, df.JoinEqui, fMultiStr); defer resMulti.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(2*2), resMulti.Len())
		expectedMulti := []interface{}{"10", "4", "n", "500"}
		assert.Equal(t, expectedMulti, extractValues(resMulti))
	})

	fReturnsIntForString := func(v1,v2 df.Value) []df.Value {
		if (v1 == nil || v1.IsNil()) { return []df.Value{arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat)} }
		return []df.Value{arrowimpl.NewArrowValue(scalar.NewInt64Scalar(v1.GetAsInt()), df.IntegerFormat)}
	}
	t.Run("OutputCasting", func(t *testing.T) {
		resCast := s1Int.Join(df.StringFormat, s2Int, df.JoinEqui, fReturnsIntForString); defer resCast.(*arrowimpl.ArrowSeries).Release()
		assert.Equal(t, int64(2), resCast.Len())
		assert.Equal(t, "10", resCast.Get(0).GetAsString())
		assert.True(t, resCast.Get(1).IsNil())
		assert.Equal(t, df.StringFormat.Name(), resCast.Schema().Format.Name())
	})

	t.Run("Panics", func(t *testing.T) {
		assert.PanicsWithValue(t, "Join: outputFormat cannot be nil", func() { s1Int.Join(nil, s2Int, df.JoinEqui, fSumInts) })
		assert.PanicsWithValue(t, "Join: function f cannot be nil", func() { s1Int.Join(df.IntegerFormat, s2Int, df.JoinEqui, nil) })

		// Panics if otherSeriesRaw is nil for join types that require it (most of them, unless left series is also empty)
		// The JoinEqui will try to access otherSeries.Get(i) which will panic if otherSeriesRaw was nil.
		// For a more specific message from Join itself, it depends on how nil otherSeriesRaw is handled.
		// The current implementation of Join panics if otherSeriesRaw is nil and otherSeries is needed.
		// Let's test a case where s1Int is not empty, but other is nil.
		var nilSeries df.Series = nil
		assert.Panics(t, func() { s1Int.Join(df.IntegerFormat, nilSeries, df.JoinEqui, fSumInts) })


		fBadReturn := func(v1,v2 df.Value) []df.Value { return []df.Value{&mockValue{mockSchema: df.StringFormat}} }
		assert.PanicsWithValue(t, fmt.Sprintf("Join: func f returned non-*arrowValue: %T", &mockValue{}), func() {
			s1Int.Join(df.StringFormat, s2Int, df.JoinEqui, fBadReturn)
		})

		fTypeClash := func(v1, v2 df.Value) []df.Value {
			return []df.Value{arrowimpl.NewArrowValue(scalar.NewInt64Scalar(1), df.IntegerFormat)}
		}
		assert.Panics(t, func() { s1Int.Join(df.DateTimeFormat, s2Int, df.JoinEqui, fTypeClash)})
	})
}

// TODO: Add more tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
```
