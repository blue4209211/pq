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
type mockValue struct { df.Value; data any; mockSchema df.Format; mockIsNil bool }
func (m *mockValue) Get() any { return m.data }
func (m *mockValue) IsNil() bool { return m.mockIsNil }
func (m *mockValue) Schema() df.Format { return m.mockSchema }
func (m *mockValue) GetAsInt() int64 { if i,ok := m.data.(int64); ok {return i}; panic("mockValue not int") }
func (m *mockValue) GetAsString() string { if s,ok := m.data.(string); ok {return s}; panic("mockValue not string") }


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

func TestArrowSeries_Expr(t *testing.T) {
	mem := memory.NewGoAllocator()
	seriesName := "my_series"
	sSchema := df.SeriesSchema{Name: seriesName, Format: df.IntegerFormat}
	arr := getTestInt64Array(mem, []int64{1, 2, 3}, nil); defer arr.Release()
	s := arrowimpl.NewArrowSeries(arr, sSchema); defer s.(*arrowimpl.ArrowSeries).Release()

	expr := s.Expr()
	assert.NotNil(t, expr, "Expr() should not return nil")

	// Assuming df.Expr has methods to inspect its properties,
	// consistent with how df.ColNameExpr and df.LiteralExpr were used in DataFrame.Select
	assert.Equal(t, df.ColNameExpr, expr.OpType(), "Expression type should be ColNameExpr")
	assert.Equal(t, seriesName, expr.Col(), "Expression column name should match series name")
	assert.Equal(t, seriesName, expr.Name(), "Expression name should match series name by default for ColExpr")

	// Test unnamed series panic
	unnamedSchema := df.SeriesSchema{Name: "", Format: df.IntegerFormat}
	unnamedSeries := arrowimpl.NewArrowSeries(arr, unnamedSchema) // arr is already created
	// No defer release for unnamedSeries explicitly if it's just for this panic test,
	// or if arr is the main owner and already deferred. arr is from getTestInt64Array, used by `s`.
	// For safety, if NewArrowSeries always retains, then a release would be needed if not panicking.
	// But since it panics, it's okay.
	assert.PanicsWithValue(t, "Expr: cannot create a column expression for an unnamed series", func() {
		unnamedSeries.Expr()
	}, "Expr() on unnamed series should panic")
}

func TestArrowSeries_Select(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchema := df.SeriesSchema{Name: "test_series_for_select", Format: df.IntegerFormat}
	arr := getTestInt64Array(mem, []int64{1, 2, 3}, nil); defer arr.Release()
	s := arrowimpl.NewArrowSeries(arr, sSchema); defer s.(*arrowimpl.ArrowSeries).Release()

	// Create a dummy expression to pass to Select
	// This would typically be a more complex expression in real use.
	// For this test, we only care that Select panics correctly.
	// We use a literal expression as a simple valid df.Expr.
	dummyExpr := df.NewLiteralExpr(arrowimpl.NewArrowValue(scalar.NewInt64Scalar(5), df.IntegerFormat)).SetName("dummy_expr_for_series_select")

	expectedPanicMsg := fmt.Sprintf("Select on arrowSeries is partially implemented. Full expression (%s) evaluation TBD.", dummyExpr.Name())

	assert.PanicsWithValue(t, expectedPanicMsg, func() {
		s.Select(dummyExpr)
	}, "Series.Select should panic with the specified message")
}

func TestArrowSeries_Join(t *testing.T) { /* ... */ }


func TestArrowSeries_AsFormat(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int", Format: df.IntegerFormat}
	sSchemaStr := df.SeriesSchema{Name: "s_str", Format: df.StringFormat}
	sSchemaFloat := df.SeriesSchema{Name: "s_float", Format: df.DoubleFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 30}, []bool{true, false, true}); defer arr1.Release()
	s1Int := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1Int.(*arrowimpl.ArrowSeries).Release()

	s1AsStr := s1Int.AsFormat(df.StringFormat);	defer s1AsStr.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, s1Int.Len(), s1AsStr.Len())
	assert.Equal(t, df.StringFormat.Name(), s1AsStr.Schema().Format.Name())
	assert.Equal(t, "10", s1AsStr.Get(0).GetAsString())
	assert.True(t, s1AsStr.Get(1).IsNil())
	assert.Equal(t, "30", s1AsStr.Get(2).GetAsString())

	arr2 := getTestStringArray(mem, []string{"100", "0", "300"}, []bool{true, false, true}); defer arr2.Release() // "0" is nil string
	s2Str := arrowimpl.NewArrowSeries(arr2, sSchemaStr); defer s2Str.(*arrowimpl.ArrowSeries).Release()
	s2AsInt := s2Str.AsFormat(df.IntegerFormat); defer s2AsInt.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(100), s2AsInt.Get(0).GetAsInt())
	assert.True(t, s2AsInt.Get(1).IsNil()) // nil string to nil int
	assert.Equal(t, int64(300), s2AsInt.Get(2).GetAsInt())

	arr3 := getTestStringArray(mem, []string{"abc"}, nil); defer arr3.Release()
	s3Str := arrowimpl.NewArrowSeries(arr3, sSchemaStr); defer s3Str.(*arrowimpl.ArrowSeries).Release()
	assert.Panics(t, func() { s3Str.AsFormat(df.IntegerFormat) }, "Cast non-numeric string to int should panic")

	arr4 := getTestFloat64Array(mem, []float64{10.1, 0.0, 10.9, 30.5}, []bool{true, false, true, true}); defer arr4.Release()
	s4Float := arrowimpl.NewArrowSeries(arr4, sSchemaFloat); defer s4Float.(*arrowimpl.ArrowSeries).Release()
	s4AsInt := s4Float.AsFormat(df.IntegerFormat);	defer s4AsInt.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(10), s4AsInt.Get(0).GetAsInt())
	assert.True(t, s4AsInt.Get(1).IsNil())
	assert.Equal(t, int64(10), s4AsInt.Get(2).GetAsInt())
	assert.Equal(t, int64(30), s4AsInt.Get(3).GetAsInt())

	s1AsIntCopy := s1Int.AsFormat(df.IntegerFormat);	defer s1AsIntCopy.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, s1Int.Len(), s1AsIntCopy.Len())
	assert.True(t, df.IntegerFormat.Equals(s1AsIntCopy.Schema().Format))
	assert.Equal(t, extractValues(s1Int), extractValues(s1AsIntCopy))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyAsStr := sEmpty.AsFormat(df.StringFormat);	defer sEmptyAsStr.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyAsStr.Len())

	assert.PanicsWithValue(t, "AsFormat: targetFormat cannot be nil", func() { s1Int.AsFormat(nil) })
}


func TestArrowSeries_WhenNil_Series(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int_wn", Format: df.IntegerFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 30, 0}, []bool{true, false, true, false}); defer arr1.Release()
	s1 := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1.(*arrowimpl.ArrowSeries).Release()

	fillVal1 := arrowimpl.NewArrowValue(scalar.NewInt64Scalar(99), df.IntegerFormat)
	s1Filled1 := s1.WhenNil(fillVal1);	defer s1Filled1.(*arrowimpl.ArrowSeries).Release()
	expected1 := []interface{}{int64(10), int64(99), int64(30), int64(99)}
	assert.Equal(t, expected1, extractValues(s1Filled1))

	fillValNil := arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat)
	s1FilledNil := s1.WhenNil(fillValNil);	defer s1FilledNil.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1FilledNil))

	arrNoNil := getTestInt64Array(mem, []int64{1,2,3}, nil); defer arrNoNil.Release()
	sNoNil := arrowimpl.NewArrowSeries(arrNoNil, sSchemaInt); defer sNoNil.(*arrowimpl.ArrowSeries).Release()
	sNoNilFilled := sNoNil.WhenNil(fillVal1);	defer sNoNilFilled.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(sNoNil), extractValues(sNoNilFilled))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyFilled := sEmpty.WhenNil(fillVal1);	defer sEmptyFilled.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyFilled.Len())

	assert.PanicsWithValue(t, "WhenNil: fillValue cannot be nil (can be a nil df.Value though)", func() { s1.WhenNil(nil) })

	fillValStr := arrowimpl.NewArrowValue(scalar.NewStringScalar("abc"), df.StringFormat)
	assert.Panics(t, func() { s1.WhenNil(fillValStr) })
}

func TestArrowSeries_When_Series(t *testing.T) {
	mem := memory.NewGoAllocator()
	sSchemaInt := df.SeriesSchema{Name: "s_int_when", Format: df.IntegerFormat}

	arr1 := getTestInt64Array(mem, []int64{10, 0, 20, 10, 30, 0}, []bool{true, false, true, true, true, false})
	defer arr1.Release()
	s1 := arrowimpl.NewArrowSeries(arr1, sSchemaInt); defer s1.(*arrowimpl.ArrowSeries).Release()

	replaceMap1 := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewInt64Scalar(100), df.IntegerFormat),
		nil:       arrowimpl.NewArrowValue(scalar.NewInt64Scalar(99), df.IntegerFormat),
	}
	s1Replaced1 := s1.When(replaceMap1);	defer s1Replaced1.(*arrowimpl.ArrowSeries).Release()
	expected1 := []interface{}{int64(100), int64(99), int64(20), int64(100), int64(30), int64(99)}
	assert.Equal(t, expected1, extractValues(s1Replaced1))

	replaceMap2 := map[any]df.Value{
		int64(20): arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.PrimitiveTypes.Int64), df.IntegerFormat),
	}
	s1Replaced2 := s1.When(replaceMap2);	defer s1Replaced2.(*arrowimpl.ArrowSeries).Release()
	expected2 := []interface{}{int64(10), nilPlaceholder, nilPlaceholder, int64(10), int64(30), nilPlaceholder}
	assert.Equal(t, expected2, extractValues(s1Replaced2))

	replaceMapNoMatch := map[any]df.Value{ int64(999): arrowimpl.NewArrowValue(scalar.NewInt64Scalar(1000), df.IntegerFormat) }
	s1NoMatch := s1.When(replaceMapNoMatch);	defer s1NoMatch.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1NoMatch))

	s1EmptyMap := s1.When(map[any]df.Value{});	defer s1EmptyMap.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, extractValues(s1), extractValues(s1EmptyMap))

	emptyArr := getTestInt64Array(mem, []int64{}, nil); defer emptyArr.Release()
	sEmpty := arrowimpl.NewArrowSeries(emptyArr, sSchemaInt); defer sEmpty.(*arrowimpl.ArrowSeries).Release()
	sEmptyReplaced := sEmpty.When(replaceMap1);	defer sEmptyReplaced.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(0), sEmptyReplaced.Len())

	replaceMapCast := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewStringScalar("1000"), df.StringFormat),
	}
	s1ReplacedCast := s1.When(replaceMapCast);	defer s1ReplacedCast.(*arrowimpl.ArrowSeries).Release()
	assert.Equal(t, int64(1000), s1ReplacedCast.Get(0).GetAsInt())
	assert.Equal(t, int64(1000), s1ReplacedCast.Get(3).GetAsInt())
	assert.True(t, s1ReplacedCast.Get(1).IsNil())

	replaceMapBadCast := map[any]df.Value{
		int64(10): arrowimpl.NewArrowValue(scalar.NewStringScalar("not-an-int"), df.StringFormat),
	}
	assert.Panics(t, func() { s1.When(replaceMapBadCast) })
}

// TODO: Add more tests for other Series methods (Map, Filter, Sort, etc.) once implemented.
// This TODO was part of the original structure, some of these are now tested.
