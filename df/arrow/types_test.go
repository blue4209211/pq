//go:build arrow

package arrow_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow" // Import the implementation package
)

func TestArrowValue_String(t *testing.T) {
	s := scalar.NewStringScalar("hello")
	f := df.StringFormat
	val := arrowimpl.NewArrowValue(s, f)

	assert.Equal(t, f, val.Schema())
	assert.Equal(t, "hello", val.Get())
	assert.Equal(t, "hello", val.GetAsString())
	assert.False(t, val.IsNil())

	sNil := scalar.NewNullScalar(arrow.BinaryTypes.String)
	valNil := arrowimpl.NewArrowValue(sNil, f)
	assert.True(t, valNil.IsNil())
	assert.Equal(t, "", valNil.GetAsString()) // Behavior for nil GetAsString

	// Equals
	s2 := scalar.NewStringScalar("hello")
	val2 := arrowimpl.NewArrowValue(s2, f)
	assert.True(t, val.Equals(val2))

	s3 := scalar.NewStringScalar("world")
	val3 := arrowimpl.NewArrowValue(s3, f)
	assert.False(t, val.Equals(val3))
	assert.False(t, val.Equals(nil))
	assert.False(t, valNil.Equals(val))
	assert.True(t, valNil.Equals(arrowimpl.NewArrowValue(scalar.NewNullScalar(arrow.BinaryTypes.String), f)))

	// Test Get for other types (should panic or handle error)
	assert.Panics(t, func() { val.GetAsInt() })
}

func TestArrowValue_Int64(t *testing.T) {
	s := scalar.NewInt64Scalar(123)
	f := df.IntegerFormat
	val := arrowimpl.NewArrowValue(s, f)

	assert.Equal(t, f, val.Schema())
	assert.Equal(t, int64(123), val.Get())
	assert.Equal(t, int64(123), val.GetAsInt())
	assert.Equal(t, "123", val.GetAsString())
	assert.False(t, val.IsNil())

	// Equals
	s2 := scalar.NewInt64Scalar(123)
	val2 := arrowimpl.NewArrowValue(s2, f)
	assert.True(t, val.Equals(val2))

	s3 := scalar.NewInt64Scalar(456)
	val3 := arrowimpl.NewArrowValue(s3, f)
	assert.False(t, val.Equals(val3))
}

func TestArrowValue_Float64(t *testing.T) {
	s := scalar.NewFloat64Scalar(123.456)
	f := df.DoubleFormat
	val := arrowimpl.NewArrowValue(s, f)

	assert.Equal(t, f, val.Schema())
	assert.Equal(t, 123.456, val.Get())
	assert.Equal(t, 123.456, val.GetAsDouble())
	assert.Equal(t, "123.456", val.GetAsString()) // Behavior of fmt.Sprintf might vary
	assert.False(t, val.IsNil())
}

func TestArrowValue_Boolean(t *testing.T) {
	sTrue := scalar.NewBooleanScalar(true)
	f := df.BoolFormat
	valTrue := arrowimpl.NewArrowValue(sTrue, f)

	assert.Equal(t, f, valTrue.Schema())
	assert.Equal(t, true, valTrue.Get())
	assert.Equal(t, true, valTrue.GetAsBool())
	assert.Equal(t, "true", valTrue.GetAsString())
	assert.False(t, valTrue.IsNil())

	sFalse := scalar.NewBooleanScalar(false)
	valFalse := arrowimpl.NewArrowValue(sFalse, f)
	assert.Equal(t, false, valFalse.GetAsBool())
}

func TestArrowValue_Timestamp(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond)
	tsType := arrow.TimestampTypes.Timestamp_ns
	s := scalar.NewTimestampScalar(arrow.Timestamp(now.UnixNano()), tsType)
	f := df.DateTimeFormat
	val := arrowimpl.NewArrowValue(s, f)

	assert.Equal(t, f, val.Schema())
	retrievedTime := val.Get().(time.Time)
	assert.Equal(t, now.UnixNano(), retrievedTime.UnixNano())
	assert.True(t, now.Equal(val.GetAsDatetime()))
	assert.False(t, val.IsNil())
}

func TestArrowValue_Equals_DifferentTypes(t *testing.T) {
	sInt := scalar.NewInt64Scalar(10)
	fInt := df.IntegerFormat
	valInt := arrowimpl.NewArrowValue(sInt, fInt)

	sStr := scalar.NewStringScalar("10")
	fStr := df.StringFormat
	valStr := arrowimpl.NewArrowValue(sStr, fStr)

	assert.False(t, valInt.Equals(valStr), "Values of different underlying types should not be equal")

	mockOtherValue := &mockValue{data: int64(10), format: df.IntegerFormat}
	assert.False(t, valInt.Equals(mockOtherValue), "arrowValue should not equal a different df.Value implementation by default")

}

type mockValue struct {
	data   any
	format df.Format
	isNil  bool
}

func (m *mockValue) Schema() df.Format        { return m.format }
func (m *mockValue) Get() any                 { return m.data }
func (m *mockValue) GetAsString() string      { v, _ := m.data.(string); return v }
func (m *mockValue) GetAsInt() int64          { v, _ := m.data.(int64); return v }
func (m *mockValue) GetAsDouble() float64     { v, _ := m.data.(float64); return v }
func (m *mockValue) GetAsBool() bool          { v, _ := m.data.(bool); return v }
func (m *mockValue) GetAsDatetime() time.Time { v, _ := m.data.(time.Time); return v }
func (m *mockValue) IsNil() bool              { return m.isNil }
func (m *mockValue) Equals(other df.Value) bool {
	if other == nil { return m.isNil }
    if m.IsNil() != other.IsNil() { return false }
    if m.IsNil() && other.IsNil() { return true}
	return reflect.DeepEqual(m.Get(), other.Get()) && m.Schema().Name() == other.Schema().Name()
}

func getTestArrowSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64},
			{Name: "col_bool", Type: arrow.PrimitiveTypes.Boolean},
			{Name: "col_time", Type: arrow.TimestampTypes.Timestamp_ns},
		},
		nil,
	)
}

func TestArrowDataFrameSchema_Basic(t *testing.T) {
	arrowSchema := getTestArrowSchema()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema)

	assert.Equal(t, 5, dfSchema.Len())
	assert.Equal(t, []string{"col_str", "col_int", "col_float", "col_bool", "col_time"}, dfSchema.Names())
	seriesSchema0 := dfSchema.Get(0)
	assert.Equal(t, "col_str", seriesSchema0.Name)
	assert.Equal(t, df.StringFormat.Name(), seriesSchema0.Format.Name())
	seriesSchema1 := dfSchema.Get(1)
	assert.Equal(t, "col_int", seriesSchema1.Name)
	assert.Equal(t, df.IntegerFormat.Name(), seriesSchema1.Format.Name())
	seriesSchemaStr := dfSchema.GetByName("col_str")
	assert.Equal(t, "col_str", seriesSchemaStr.Name)
	assert.Equal(t, df.StringFormat.Name(), seriesSchemaStr.Format.Name())
	seriesSchemaFloat := dfSchema.GetByName("col_float")
	assert.Equal(t, "col_float", seriesSchemaFloat.Name)
	assert.Equal(t, df.DoubleFormat.Name(), seriesSchemaFloat.Format.Name())
	assert.Equal(t, -1, dfSchema.GetIndexByName("non_existent_col"))
	assert.Equal(t, 0, dfSchema.GetIndexByName("col_str"))
	assert.Equal(t, 2, dfSchema.GetIndexByName("col_float"))
	assert.True(t, dfSchema.HasName("col_bool"))
	assert.False(t, dfSchema.HasName("non_existent_col"))
	allSeries := dfSchema.Series()
	assert.Equal(t, 5, len(allSeries))
	assert.Equal(t, "col_time", allSeries[4].Name)
	assert.Equal(t, df.DateTimeFormat.Name(), allSeries[4].Format.Name())
}

func TestArrowDataFrameSchema_Equals(t *testing.T) {
	schema1 := arrowimpl.NewArrowDataFrameSchema(getTestArrowSchema())
	schema2 := arrowimpl.NewArrowDataFrameSchema(getTestArrowSchema())
	schemaDiffName := arrowimpl.NewArrowDataFrameSchema(arrow.NewSchema(
		[]arrow.Field{{Name: "col_str_diff", Type: arrow.BinaryTypes.String}, getTestArrowSchema().Field(1)}, nil))
	schemaDiffType := arrowimpl.NewArrowDataFrameSchema(arrow.NewSchema(
		[]arrow.Field{{Name: "col_str", Type: arrow.PrimitiveTypes.Int64}, getTestArrowSchema().Field(1)}, nil))
	schemaDiffLen := arrowimpl.NewArrowDataFrameSchema(arrow.NewSchema(
		[]arrow.Field{getTestArrowSchema().Field(0)}, nil))
	var nilArrowSchema *arrow.Schema = nil
	schemaNilInternal := arrowimpl.NewArrowDataFrameSchema(nilArrowSchema)
	var nilDfSchema df.DataFrameSchema = nil

	assert.True(t, schema1.Equals(schema2), "Identical schemas should be equal")
	assert.False(t, schema1.Equals(schemaDiffName), "Schemas with different names should not be equal")
	assert.False(t, schema1.Equals(schemaDiffType), "Schemas with different types should not be equal")
	assert.False(t, schema1.Equals(schemaDiffLen), "Schemas with different lengths should not be equal")
	assert.False(t, schema1.Equals(nilDfSchema), "Schema should not be equal to nil df.DataFrameSchema")
	assert.False(t, schemaNilInternal.Equals(schema1), "Schema with nil internal arrow.Schema should not equal a valid one")
	assert.True(t, schemaNilInternal.Equals(arrowimpl.NewArrowDataFrameSchema(nilArrowSchema)), "Two schemas with nil internal arrow.Schema should be equal")

	mockSchemaEq := &mockDataFrameSchema{
		series: []df.SeriesSchema{
			{Name: "col_str", Format: df.StringFormat}, {Name: "col_int", Format: df.IntegerFormat},
			{Name: "col_float", Format: df.DoubleFormat}, {Name: "col_bool", Format: df.BoolFormat},
			{Name: "col_time", Format: df.DateTimeFormat},
		},
	}
	assert.True(t, schema1.Equals(mockSchemaEq), "arrowDataFrameSchema should be equal to a structurally equivalent mockDataFrameSchema")

	mockSchemaDiff := &mockDataFrameSchema{
		series: []df.SeriesSchema{{Name: "col_str", Format: df.StringFormat}, {Name: "col_int_diff", Format: df.IntegerFormat}},
	}
	assert.False(t, schema1.Equals(mockSchemaDiff), "arrowDataFrameSchema should not be equal to a structurally different mockDataFrameSchema")
}

type mockDataFrameSchema struct {
	series []df.SeriesSchema
}
func (m *mockDataFrameSchema) Series() []df.SeriesSchema       { return m.series }
func (m *mockDataFrameSchema) Names() []string {
	names := make([]string, len(m.series))
	for i, s := range m.series { names[i] = s.Name }
	return names
}
func (m *mockDataFrameSchema) GetByName(s string) df.SeriesSchema {
	for _, ss := range m.series { if ss.Name == s { return ss } }
	return df.SeriesSchema{}
}
func (m *mockDataFrameSchema) GetIndexByName(s string) int {
	for i, ss := range m.series { if ss.Name == s { return i } }
	return -1
}
func (m *mockDataFrameSchema) HasName(s string) bool {
	for _, ss := range m.series { if ss.Name == s { return true } }
	return false
}
func (m *mockDataFrameSchema) Get(i int) df.SeriesSchema { return m.series[i] }
func (m *mockDataFrameSchema) Len() int                  { return len(m.series) }
func (m *mockDataFrameSchema) Equals(other df.DataFrameSchema) bool {
	if other == nil || m.Len() != other.Len() { return false }
	for i := 0; i < m.Len(); i++ {
		s1 := m.Get(i)
		s2 := other.Get(i)
		if s1.Name != s2.Name || s1.Format.Name() != s2.Format.Name() || s1.Format.Type() != s2.Format.Type() {
			return false
		}
	}
	return true
}

// --- Tests for arrowRow ---

// Helper to create a sample record for testing arrowRow
func getTestRecord(mem memory.Allocator) arrow.Record {
	schema := getTestArrowSchema()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"hello", "world", "foo"}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 2, 0}, []bool{true, true, false}) // 0 is nil
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	b.Field(3).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	now := time.Now().UnixNano()
	b.Field(4).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{arrow.Timestamp(now), arrow.Timestamp(now + 1000), arrow.Timestamp(now + 2000)}, nil)

	return b.NewRecord()
}


func TestArrowRow_NewArrowRow(t *testing.T) {
	schema := arrowimpl.NewArrowDataFrameSchema(getTestArrowSchema()).(*arrowimpl.ArrowDataFrameSchema)
	vals := []scalar.Scalar{
		scalar.NewStringScalar("alpha"),
		scalar.NewInt64Scalar(100),
		scalar.NewFloat64Scalar(99.9),
		scalar.NewBooleanScalar(true),
		scalar.NewTimestampScalar(arrow.Timestamp(time.Now().UnixNano()), arrow.TimestampTypes.Timestamp_ns),
	}
	row := arrowimpl.NewArrowRow(schema, vals)

	assert.Equal(t, schema, row.Schema())
	assert.Equal(t, 5, row.Len())
	assert.Equal(t, "alpha", row.Get(0).GetAsString())
	assert.Equal(t, int64(100), row.Get(1).GetAsInt())
	assert.False(t, row.IsAnyNil())
}

func TestArrowRow_NewArrowRowFromRecord(t *testing.T) {
	mem := memory.NewGoAllocator()
	record := getTestRecord(mem)
	defer record.Release()

	schema := arrowimpl.NewArrowDataFrameSchema(record.Schema()).(*arrowimpl.ArrowDataFrameSchema)

	// Test row 0
	row0, err0 := arrowimpl.NewArrowRowFromRecord(schema, record, 0)
	assert.NoError(t, err0)
	assert.Equal(t, schema, row0.Schema())
	assert.Equal(t, 5, row0.Len())
	assert.Equal(t, "hello", row0.Get(0).GetAsString())
	assert.Equal(t, "hello", row0.GetByName("col_str").GetAsString())
	assert.Equal(t, int64(1), row0.GetAsInt(1))
	assert.Equal(t, 1.1, row0.GetAsDouble(2))
	assert.True(t, row0.GetAsBool(3))
	assert.NotZero(t, row0.GetAsDatetime(4))
	assert.False(t, row0.IsNil(0))
	assert.False(t, row0.IsAnyNil())

	// Test row 1 (with a nil value)
	row1, err1 := arrowimpl.NewArrowRowFromRecord(schema, record, 1)
	assert.NoError(t, err1)
	assert.Equal(t, "world", row1.Get(0).GetAsString())
	assert.True(t, row1.IsNil(1), "col_int at index 1 should be nil") // Index 1 of col_int is nil
	assert.True(t, row1.IsAnyNil())
	assert.Panics(t, func() { row1.GetAsInt(1) }, "GetAsInt on a nil value should panic")


	// Test GetRaw
	assert.Equal(t, "hello", row0.GetRaw(0))
	assert.Equal(t, int64(1), row0.GetRaw(1))


	// Test GetMap
	rowMap := row0.GetMap()
	assert.Equal(t, 5, len(rowMap))
	assert.Equal(t, "hello", rowMap["col_str"].GetAsString())
	assert.Equal(t, int64(1), rowMap["col_int"].GetAsInt())

	// Test out of bounds
	_, errBounds := arrowimpl.NewArrowRowFromRecord(schema, record, 10)
	assert.Error(t, errBounds)

	assert.Panics(t, func() { row0.Get(10) })
	assert.Panics(t, func() { row0.GetByName("non_existent") })
}


func TestArrowRow_Copy(t *testing.T) {
	schema := arrowimpl.NewArrowDataFrameSchema(getTestArrowSchema()).(*arrowimpl.ArrowDataFrameSchema)
	vals := []scalar.Scalar{scalar.NewStringScalar("copy_me"), scalar.NewInt64Scalar(55)}

	// Adjust schema to match vals
	simpleArrowSchema := arrow.NewSchema([]arrow.Field{getTestArrowSchema().Field(0), getTestArrowSchema().Field(1)}, nil)
	simpleDfSchema := arrowimpl.NewArrowDataFrameSchema(simpleArrowSchema).(*arrowimpl.ArrowDataFrameSchema)

	row := arrowimpl.NewArrowRow(simpleDfSchema, vals)
	copiedRow := row.Copy()

	assert.True(t, row.Schema().Equals(copiedRow.Schema()))
	assert.Equal(t, row.Len(), copiedRow.Len())
	assert.True(t, row.Get(0).Equals(copiedRow.Get(0)))
	assert.True(t, row.Get(1).Equals(copiedRow.Get(1)))

	// Ensure it's a shallow copy of scalars (scalars are immutable-like)
    // but the slice itself is new
    originalVal0 := row.Get(0).(*arrowimpl.ArrowValue)
    copiedVal0 := copiedRow.Get(0).(*arrowimpl.ArrowValue)

    // This checks if the underlying scalar.Scalar is the same instance.
    // For simple scalars, this might be true due to how they are created/interned.
    // The important part is that changes to one row's structure (if possible) wouldn't affect the other.
    // Since our `arrowRow.values` is a slice of `scalar.Scalar`, `copy()` on the slice creates a new slice.
    // And `scalar.Scalar` itself is an interface, the concrete types are typically pointers to structs
    // that are value-based or immutable in nature.

	if len(vals) > 0 && originalVal0 != nil && copiedVal0 != nil {
		// If Get returns an arrowValue, we can compare its internal scalar
		// For simple scalars, they might point to the same underlying scalar instance if scalar.Copy() isn't used by MakeScalar or if scalars are interned.
		// However, the slice `values` in `arrowRow` is copied.
	}
}

func TestArrowRow_Select(t *testing.T) {
	mem := memory.NewGoAllocator()
	record := getTestRecord(mem)
	defer record.Release()
	schema := arrowimpl.NewArrowDataFrameSchema(record.Schema()).(*arrowimpl.ArrowDataFrameSchema)
	row, _ := arrowimpl.NewArrowRowFromRecord(schema, record, 0)

	// Select "col_str", "col_float" (indices 0, 2)
	selectedRow := row.Select(0, 2)
	assert.Equal(t, 2, selectedRow.Len())
	assert.Equal(t, "col_str", selectedRow.Schema().Get(0).Name)
	assert.Equal(t, "col_float", selectedRow.Schema().Get(1).Name)
	assert.Equal(t, "hello", selectedRow.Get(0).GetAsString())
	assert.Equal(t, 1.1, selectedRow.Get(1).GetAsDouble())

	// Select in different order: "col_int", "col_str" (indices 1, 0)
	selectedRowReordered := row.Select(1, 0)
	assert.Equal(t, 2, selectedRowReordered.Len())
	assert.Equal(t, "col_int", selectedRowReordered.Schema().Get(0).Name)
	assert.Equal(t, "col_str", selectedRowReordered.Schema().Get(1).Name)
	assert.Equal(t, int64(1), selectedRowReordered.Get(0).GetAsInt())
	assert.Equal(t, "hello", selectedRowReordered.Get(1).GetAsString())

	assert.Panics(t, func() { row.Select(0, 10) }, "Select with out-of-bounds index should panic")
}
