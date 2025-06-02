//go:build arrow

package arrow

import (
	"context" // Added for dfValueToArrowScalar potential use of compute.WithAllocator
	"fmt"
	"reflect"
	"time"

	"git.querycap.com/practice/df" // Corrected import path
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/builder" // For appendScalarToBuilder
	"github.com/apache/arrow/go/v14/arrow/compute" // For dfValueToArrowScalar potential use of compute.WithAllocator
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
)

// --- arrowValue ---
type arrowValue struct {
	val    scalar.Scalar // Underlying Arrow scalar value
	format df.Format     // The df.Format associated with this value
}

func NewArrowValue(s scalar.Scalar, f df.Format) df.Value {
	if s == nil { 
		panic("NewArrowValue: input scalar.Scalar cannot be nil; use scalar.NewNullScalar for typed nulls")
	}
	if f == nil { 
		panic("NewArrowValue: input df.Format cannot be nil")
	}
	return &arrowValue{val: s, format: f}
}
func (v *arrowValue) Schema() df.Format { return v.format } 
func (v *arrowValue) IsNil() bool       { return v.val == nil || !v.val.IsValid() }

func (v *arrowValue) Get() any {
	if v.IsNil() { return nil }
	switch s := v.val.(type) {
	case *scalar.String: return s.String() 
	case *scalar.LargeString: return s.String()
	case *scalar.Int64: return s.Value
	case *scalar.Float64: return s.Value
	case *scalar.Boolean: return s.Value
	case *scalar.Timestamp: return s.ToTime(arrow.Nanosecond) 
	case *scalar.Date32: return s.ToTime()
	case *scalar.Date64: return s.ToTime()
	default:
		panic(fmt.Sprintf("arrowValue.Get(): unhandled scalar type %T (value: %s)", v.val, v.val.String()))
	}
}
func (v *arrowValue) GetAsString() string { 
	if v.IsNil() { return "" } 
	return fmt.Sprintf("%v", v.Get()) 
}
func (v *arrowValue) GetAsInt() int64 {
	if s, ok := v.val.(*scalar.Int64); ok && s.IsValid() { return s.Value }
	panic(fmt.Sprintf("cannot convert arrowValue of format %s (scalar type %T, value %s) to int64", v.format.Name(), v.val, v.val.String()))
}
func (v *arrowValue) GetAsDouble() float64 {
	if s, ok := v.val.(*scalar.Float64); ok && s.IsValid() { return s.Value }
	panic(fmt.Sprintf("cannot convert arrowValue of format %s (scalar type %T, value %s) to float64", v.format.Name(), v.val, v.val.String()))
}
func (v *arrowValue) GetAsBool() bool {
	if s, ok := v.val.(*scalar.Boolean); ok && s.IsValid() { return s.Value }
	panic(fmt.Sprintf("cannot convert arrowValue of format %s (scalar type %T, value %s) to bool", v.format.Name(), v.val, v.val.String()))
}
func (v *arrowValue) GetAsDatetime() time.Time {
	if s, ok := v.val.(*scalar.Timestamp); ok && s.IsValid() { return s.ToTime(arrow.Nanosecond) } 
	if s, ok := v.val.(*scalar.Date32); ok && s.IsValid() { return s.ToTime()}
	if s, ok := v.val.(*scalar.Date64); ok && s.IsValid() { return s.ToTime()}
	panic(fmt.Sprintf("cannot convert arrowValue of format %s (scalar type %T, value %s) to time.Time", v.format.Name(), v.val, v.val.String()))
}
func (v *arrowValue) Equals(other df.Value) bool {
	if other == nil || other.IsNil() { return v.IsNil() }
	if v.IsNil() { return false } 
	otherArrowVal, ok := other.(*arrowValue)
	if !ok { 
		if v.format.Name() == other.Schema().Name() && v.format.Type() == other.Schema().Type() {
			return reflect.DeepEqual(v.Get(), other.Get())
		}
		return false 
	}
	return scalar.Equals(v.val, otherArrowVal.val)
}
// Exported for testing from arrow_test package
func (v *arrowValue) IsNilInternalScalar() bool { return v.val == nil || !v.val.IsValid() }
func (v *arrowValue) InternalScalarType() arrow.DataType { if v.val == nil {return nil}; return v.val.DataType() }

var _ df.Value = (*arrowValue)(nil)


// --- arrowRow ---
type arrowRow struct {
	schema *arrowDataFrameSchema 
	values []scalar.Scalar     
}

// Internal constructor for arrowRow
func newArrowRow(schema *arrowDataFrameSchema, values []scalar.Scalar) df.Row {
	if schema == nil || schema.schema == nil { panic("newArrowRow: schema or its internal arrow.Schema is nil") }
	if schema.schema.NumFields() != len(values) {
		panic(fmt.Sprintf("newArrowRow: schema field count %d and values length %d mismatch", schema.schema.NumFields(), len(values)))
	}
	return &arrowRow{schema: schema, values: values}
}

// NewArrowRowFromRecord creates a df.Row from a specific row index in an arrow.Record.
// The dfSchema must be an *arrowDataFrameSchema.
func NewArrowRowFromRecord(dfSchema df.DataFrameSchema, rec arrow.Record, rowIndex int) (df.Row, error) {
	arrowSchema, ok := dfSchema.(*arrowDataFrameSchema)
	if !ok { return nil, fmt.Errorf("NewArrowRowFromRecord: dfSchema must be *arrowDataFrameSchema, got %T", dfSchema)}
	if arrowSchema.schema == nil { return nil, fmt.Errorf("NewArrowRowFromRecord: schema's internal arrow.Schema is nil") }
	if rec == nil { return nil, fmt.Errorf("NewArrowRowFromRecord: input record is nil") }
	if int(rec.NumCols()) != arrowSchema.schema.NumFields() {
		return nil, fmt.Errorf("NewArrowRowFromRecord: record column count %d mismatches schema field count %d", rec.NumCols(), arrowSchema.schema.NumFields())
	}
	if rowIndex < 0 || rowIndex >= int(rec.NumRows()) {
		return nil, fmt.Errorf("NewArrowRowFromRecord: rowIndex %d out of bounds for record with %d rows", rowIndex, rec.NumRows())
	}
	values := make([]scalar.Scalar, rec.NumCols())
	for i, col := range rec.Columns() {
		values[i] = scalar.MakeScalar(col, rowIndex)
	}
	return newArrowRow(arrowSchema, values), nil
}
func (r *arrowRow) Schema() df.DataFrameSchema { return r.schema }
func (r *arrowRow) Len() int                   { return len(r.values) }
func (r *arrowRow) Get(i int) df.Value {
	if i < 0 || i >= len(r.values) { panic("Get: index out of bounds") }
	// This relies on arrowDataFrameSchema.Get returning a valid df.SeriesSchema
	// which includes the correct df.Format for the column.
	seriesSchema := r.schema.Get(i) 
	return NewArrowValue(r.values[i], seriesSchema.Format)
}
func (r *arrowRow) GetByName(s string) df.Value {
	idx := r.schema.GetIndexByName(s)
	if idx == -1 { panic(fmt.Sprintf("GetByName: column '%s' not found", s)) }
	return r.Get(idx)
}
func (r *arrowRow) GetRaw(i int) any {
	if i < 0 || i >= len(r.values) { panic("GetRaw: index out of bounds") }
	s := r.values[i]
	if s == nil || !s.IsValid() { return nil }
	seriesSchema := r.schema.Get(i)
	v := NewArrowValue(s, seriesSchema.Format) 
	return v.Get() 
}
func (r *arrowRow) GetAsString(i int) string      { return r.Get(i).GetAsString() }
func (r *arrowRow) GetAsInt(i int) int64          { return r.Get(i).GetAsInt() }
func (r *arrowRow) GetAsDouble(i int) float64     { return r.Get(i).GetAsDouble() }
func (r *arrowRow) GetAsBool(i int) bool          { return r.Get(i).GetAsBool() }
func (r *arrowRow) GetAsDatetime(i int) time.Time { return r.Get(i).GetAsDatetime() }
func (r *arrowRow) GetMap() map[string]df.Value {
	res := make(map[string]df.Value, len(r.values))
	for i, name := range r.schema.Names() { res[name] = r.Get(i) }
	return res
}
func (r *arrowRow) IsNil(i int) bool { // Modified to match df.Row interface (no error)
	if i < 0 || i >= len(r.values) { panic("IsNil: index out of bounds") }
	s := r.values[i]
	return s == nil || !s.IsValid()
}
func (r *arrowRow) IsAnyNil() bool {
	for _, s := range r.values { if s == nil || !s.IsValid() { return true } }
	return false
}
func (r *arrowRow) Copy() df.Row {
	newValues := make([]scalar.Scalar, len(r.values))
	copy(newValues, r.values) 
	return newArrowRow(r.schema, newValues)
}
func (r *arrowRow) Select(indices ...int) df.Row {
	newSchemaFields := make([]arrow.Field, len(indices))
	newValues := make([]scalar.Scalar, len(indices))
	currentFields := r.schema.schema.Fields()
	for i, idx := range indices {
		if idx < 0 || idx >= len(currentFields) { panic(fmt.Sprintf("select index %d out of bounds", idx)) }
		newSchemaFields[i] = currentFields[idx]
		newValues[i] = r.values[idx] 
	}
	selectedArrowSchema := arrow.NewSchema(newSchemaFields, r.schema.schema.Metadata())
	selectedDfSchema := NewArrowDataFrameSchema(selectedArrowSchema).(*arrowDataFrameSchema)
	return newArrowRow(selectedDfSchema, newValues)
}
func (r *arrowRow) Append(name string, val df.Value) df.Row {
	panic("arrowRow.Append is not supported; rows are typically fixed by DataFrame schema context")
}
var _ df.Row = (*arrowRow)(nil)


// --- arrowDataFrameSchema ---
type arrowDataFrameSchema struct {
	schema *arrow.Schema 
}
func NewArrowDataFrameSchema(schema *arrow.Schema) df.DataFrameSchema {
	if schema == nil { 
		schema = arrow.NewSchema([]arrow.Field{}, nil)
	}
	return &arrowDataFrameSchema{schema: schema}
}
// Exported for testing from arrow_test package
func (s *arrowDataFrameSchema) InternalArrowSchema() *arrow.Schema { return s.schema } 

func (s *arrowDataFrameSchema) Series() []df.SeriesSchema {
	seriesSchemas := make([]df.SeriesSchema, s.schema.NumFields())
	for i, field := range s.schema.Fields() {
		seriesSchemas[i] = df.SeriesSchema{Name: field.Name, Format: ArrowToDfFormat(field.Type), Nullable: field.Nullable}
	}
	return seriesSchemas
}
func (s *arrowDataFrameSchema) Names() []string {
	names := make([]string, s.schema.NumFields())
	for i, field := range s.schema.Fields() { names[i] = field.Name }
	return names
}
// GetByName from existing df.go (blue4209211/pq/df) returns df.SeriesSchema directly, not (int, df.SeriesSchema, bool)
// Adhering to that for now.
func (s *arrowDataFrameSchema) GetByName(name string) df.SeriesSchema { 
	idx := s.schema.FieldIndices(name)
	if len(idx) == 0 { return df.SeriesSchema{Name:name, Format:df.UnknownFormat} } // Return an empty/unknown schema if not found
	field := s.schema.Field(idx[0])
	return df.SeriesSchema{Name: field.Name, Format: ArrowToDfFormat(field.Type), Nullable: field.Nullable}
}
func (s *arrowDataFrameSchema) GetIndexByName(name string) int {
	idx := s.schema.FieldIndices(name)
	if len(idx) == 0 { return -1 }
	return idx[0]
}
func (s *arrowDataFrameSchema) HasName(name string) bool { return len(s.schema.FieldIndices(name)) > 0 }
// Get from existing df.go (blue4209211/pq/df) returns df.SeriesSchema directly
func (s *arrowDataFrameSchema) Get(i int) df.SeriesSchema { 
	if i < 0 || i >= s.schema.NumFields() { panic("Get: index out of bounds for schema fields") }
	field := s.schema.Field(i)
	return df.SeriesSchema{Name: field.Name, Format: ArrowToDfFormat(field.Type), Nullable: field.Nullable}
}
func (s *arrowDataFrameSchema) Len() int { return s.schema.NumFields() }
func (s *arrowDataFrameSchema) Equals(other df.DataFrameSchema) bool {
	if other == nil { return false }
	otherArrowSchema, ok := other.(*arrowDataFrameSchema)
	if !ok { 
		if s.Len() != other.Len() { return false }
		for i := 0; i < s.Len(); i++ {
			s1, s2 := s.Get(i), other.Get(i)
			// Assuming df.Format has an Equals method or is comparable.
			formatEquals := false
			if s1.Format != nil && other.Get(i).Format != nil {
				// This check is problematic if df.Format is an interface.
				// Using reflect.DeepEqual for formats as a general approach if Equals method not on interface.
				formatEquals = reflect.DeepEqual(s1.Format, s2.Format)
			} else if s1.Format == nil && other.Get(i).Format == nil {
				formatEquals = true
			}
			if s1.Name != s2.Name || !formatEquals || s1.Nullable != s2.Nullable { return false } 
		}
		return true
	}
	return s.schema.Equal(otherArrowSchema.schema) 
}
var _ df.DataFrameSchema = (*arrowDataFrameSchema)(nil)


// --- Helper Functions (package level) ---
// Exported ArrowToDfFormat for use in tests
func ArrowToDfFormat(dt arrow.DataType) df.Format {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING: return df.StringFormat
	case arrow.BINARY, arrow.LARGE_BINARY: return df.StringFormat // Consider a distinct df.BinaryFormat if needed
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64: return df.IntegerFormat 
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64: return df.IntegerFormat 
	case arrow.FLOAT32, arrow.FLOAT64: return df.DoubleFormat 
	case arrow.BOOL: return df.BoolFormat
	case arrow.TIMESTAMP: return df.DateTimeFormat 
	case arrow.DATE32, arrow.DATE64: return df.DateTimeFormat 
	case arrow.NULL: return df.UnknownFormat 
	default:
		panic(fmt.Sprintf("ArrowToDfFormat: unhandled Arrow data type %s", dt.Name()))
	}
}
func dfFormatToArrowType(f df.Format) (arrow.DataType, error) { // Made error return consistent
	if f == nil { return nil, fmt.Errorf("dfFormatToArrowType: df.Format cannot be nil") }
	switch f { // Assuming f is comparable (not an interface with different underlying types for same logical format)
	case df.StringFormat: return arrow.BinaryTypes.String, nil 
	case df.IntegerFormat: return arrow.PrimitiveTypes.Int64, nil
	case df.DoubleFormat: return arrow.PrimitiveTypes.Float64, nil
	case df.BoolFormat: return arrow.PrimitiveTypes.Boolean, nil
	case df.DateTimeFormat: return arrow.TimestampTypes.Timestamp_ns, nil 
	case df.UnknownFormat: return nil, fmt.Errorf("cannot convert df.UnknownFormat to Arrow type")
	default:
		// This path means f is a df.Format instance not matching known singletons.
		// It might be a custom format or an issue with how formats are defined/compared.
		return nil, fmt.Errorf("dfFormatToArrowType: unhandled df.Format name %s, type %v", f.Name(), f.Type())
	}
}

func dfValueToArrowScalar(val df.Value, targetType arrow.DataType, mem memory.Allocator) (scalar.Scalar, error) {
	if val == nil { return nil, fmt.Errorf("input df.Value is nil interface") } 
	
	var effectiveTargetType arrow.DataType = targetType
	if effectiveTargetType == nil {
		var err error
		effectiveTargetType, err = dfFormatToArrowType(val.Schema())
		if err != nil {
			return nil, fmt.Errorf("dfValueToArrowScalar: cannot determine target Arrow type for df.Value schema %v: %w", val.Schema(), err)
		}
	}

	if val.IsNil() { 
		return scalar.NewNullScalar(effectiveTargetType), nil 
	}

	if av, ok := val.(*arrowValue); ok { 
		if arrow.TypeEqual(av.val.DataType(), effectiveTargetType) { return av.val, nil }
		ctx := context.Background(); if mem != nil { ctx = compute.WithAllocator(ctx, mem) }
		castedScalar, err := scalar.Cast(ctx, av.val, effectiveTargetType)
		if err != nil { return nil, fmt.Errorf("failed to cast scalar from %s to %s: %w", av.val.DataType(), effectiveTargetType, err) }
		return castedScalar, nil 
	}
	
	// Fallback for generic df.Value
	// This is less type-safe and relies on Get() returning types that match the df.Format's promise.
	switch val.Schema() { // Use val.Schema() to guide conversion from generic df.Value
	case df.IntegerFormat:
		return scalar.NewInt64Scalar(val.GetAsInt()), nil
	case df.DoubleFormat:
		return scalar.NewFloat64Scalar(val.GetAsDouble()), nil
	case df.StringFormat:
		return scalar.NewStringScalar(val.GetAsString()), nil
	case df.BoolFormat:
		return scalar.NewBooleanScalar(val.GetAsBool()), nil
	case df.DateTimeFormat:
		if tsType, ok := effectiveTargetType.(*arrow.TimestampType); ok {
			return scalar.NewTimestampFromTime(val.GetAsDatetime(), tsType), nil
		}
		return nil, fmt.Errorf("targetType for DateTimeFormat must be TimestampType, got %s", effectiveTargetType.Name())
	default:
		return nil, fmt.Errorf("unsupported conversion from df.Value (format %s, Go type %T) to Arrow type %s", val.Schema().Name(), val.Get(), effectiveTargetType.Name())
	}
}

func appendScalarToBuilder(b array.Builder, s scalar.Scalar, targetType arrow.DataType) error {
	if s == nil { return fmt.Errorf("appendScalarToBuilder: input scalar is nil pointer")}
	if !s.IsValid() { b.AppendNull(); return nil } 
	
	var scalarToAppend scalar.Scalar = s
	var castedScalarReleaser memory.Releasable 

	if !arrow.TypeEqual(s.DataType(), targetType) {
		ctx := context.Background() 
		casted, err := scalar.Cast(ctx, s, targetType)
		if err != nil {
			return fmt.Errorf("casting scalar from %s to %s failed: %w", s.DataType(), targetType, err)
		}
		scalarToAppend = casted
		if releasable, ok := casted.(memory.Releasable); ok { 
			castedScalarReleaser = releasable 
		}
	}
	if castedScalarReleaser != nil { 
		defer castedScalarReleaser.Release()
	}

	switch tb := b.(type) {
	case *builder.Int64Builder:
		v, ok := scalarToAppend.(*scalar.Int64); if !ok { return fmt.Errorf("expected Int64 scalar for Int64Builder, got %T (value: %s)", scalarToAppend, scalarToAppend) }; tb.Append(v.Value)
	case *builder.Float64Builder:
		v, ok := scalarToAppend.(*scalar.Float64); if !ok { return fmt.Errorf("expected Float64 scalar, got %T", scalarToAppend) }; tb.Append(v.Value)
	case *builder.StringBuilder:
		v, ok := scalarToAppend.(scalar.StringScalar); if !ok { return fmt.Errorf("expected StringScalar, got %T", scalarToAppend) }; tb.Append(v.String())
	case *builder.BooleanBuilder:
		v, ok := scalarToAppend.(*scalar.Boolean); if !ok { return fmt.Errorf("expected Boolean scalar, got %T", scalarToAppend) }; tb.Append(v.Value)
	case *builder.TimestampBuilder:
		v, ok := scalarToAppend.(*scalar.Timestamp); if !ok { return fmt.Errorf("expected Timestamp scalar, got %T", scalarToAppend) }; tb.Append(v.Value)
	case *builder.Date32Builder:
		v, ok := scalarToAppend.(*scalar.Date32); if !ok { return fmt.Errorf("expected Date32 scalar, got %T", scalarToAppend) }; tb.Append(v.Value)
	case *builder.Date64Builder:
		v, ok := scalarToAppend.(*scalar.Date64); if !ok { return fmt.Errorf("expected Date64 scalar, got %T", scalarToAppend) }; tb.Append(v.Value)
	default:
		return fmt.Errorf("unsupported builder type in appendScalarToBuilder: %T for scalar type %s", b, scalarToAppend.DataType().Name())
	}
	return nil
}

// Helper functions to check df.Format type.
func isConcreteFormatType(format df.Format, targetKnownType df.Format) bool {
	if format == nil { return false }
	return format == targetKnownType
}
// These assume df package defines these as comparable values (e.g. var StringFormat = &formatImpl{...})
func isIntegerFormat(f df.Format) bool { return isConcreteFormatType(f, df.IntegerFormat) } 
func isFloatFormat(f df.Format) bool   { return isConcreteFormatType(f, df.DoubleFormat) }  
func isStringFormat(f df.Format) bool  { return isConcreteFormatType(f, df.StringFormat) }
func isBoolFormat(f df.Format) bool    { return isConcreteFormatType(f, df.BoolFormat) }
func isTimeFormat(f df.Format) bool    { return isConcreteFormatType(f, df.DateTimeFormat) } 
func isDateFormat(f df.Format) bool    { 
	// Assuming DateTimeFormat might be used for dates if no specific DateFormat exists in df,
	// or if df.DateFormat is distinct. This depends on df package definitions.
	// If df.DateFormat exists and is distinct from df.DateTimeFormat:
	// return isConcreteFormatType(f, df.DateFormat) || isConcreteFormatType(f, df.DateTimeFormat) // If DateTime can hold dates
	return isConcreteFormatType(f, df.DateTimeFormat) // Or a specific df.DateFormat
}


// NewNilArrowRow creates a df.Row where all values are nil, matching the given schema.
// Exported for direct use in tests or other packages if necessary.
func NewNilArrowRow(schema *arrowDataFrameSchema, mem memory.Allocator) df.Row {
	if schema == nil || schema.schema == nil {
		panic("NewNilArrowRow: input schema or its internal arrow.Schema cannot be nil")
	}
	// mem is not directly used by NewNullScalar or newArrowRow currently.
	if schema.schema.NumFields() == 0 {
		return newArrowRow(schema, []scalar.Scalar{})
	}

	nilScalars := make([]scalar.Scalar, schema.schema.NumFields())
	for i, field := range schema.schema.Fields() {
		nilScalars[i] = scalar.NewNullScalar(field.Type)
	}
	return newArrowRow(schema, nilScalars)
}
```
And `df/arrow/types_test.go` with the new test:
```go
//go:build arrow

package arrow_test

import (
	"fmt"
	"reflect"
	"sort" 
	"strconv" 
	"testing"
	"time"

	"git.querycap.com/practice/df" 
	arrowimpl "git.querycap.com/practice/df/arrow"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/assert"
)

// Helper to get a common test schema (assumed to exist from prior tests)
func getTestArrowSchemaForNilRowTest() *arrow.Schema { 
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "col_bool", Type: arrow.PrimitiveTypes.Boolean, Nullable: true},
			{Name: "col_time_ns", Type: arrow.TimestampTypes.Timestamp_ns, Nullable: true},
			{Name: "col_date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		},
		nil, 
	)
}


func TestNewNilArrowRow(t *testing.T) {
	mem := memory.NewGoAllocator() 
	
	t.Run("SchemaWithMultipleFields", func(t *testing.T) {
		arrowSchema := getTestArrowSchemaForNilRowTest()
		// Cast to internal type *arrowimpl.ArrowDataFrameSchema for NewNilArrowRow
		// NewArrowDataFrameSchema is the public constructor.
		dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)

		// Call the (now exported for test) NewNilArrowRow
		nilRow := arrowimpl.NewNilArrowRow(dfSchema, mem) 
		
		assert.NotNil(t, nilRow)
		assert.Equal(t, arrowSchema.NumFields(), nilRow.Len(), "Row length should match field count")
		
		assert.True(t, nilRow.Schema().Equals(dfSchema), "Row schema should match input dfSchema")

		for i := 0; i < nilRow.Len(); i++ {
			originalField := arrowSchema.Field(i)
			// df.Row.IsNil does not return error
			assert.True(t, nilRow.IsNil(i), fmt.Sprintf("Value at index %d (%s) should be nil", i, originalField.Name))
			
			// df.Row.Get does not return error
			val := nilRow.Get(i) 
			assert.NotNil(t, val, "df.Value should not be nil interface")
			assert.True(t, val.IsNil(), fmt.Sprintf("df.Value at index %d (%s) should be nil", i, originalField.Name))
			
			expectedDfFormat := arrowimpl.ArrowToDfFormat(originalField.Type) 
			valSchemaFormat := val.Schema() // This is df.Format as per problem's df.Value interface

			// Check Name and Type of df.Format
			// Need to ensure df.Format has Name() and Type() methods as per definition
			// For this test, we assume df.Format is comparable or has an Equals method.
			// If df.Format's Name() or Type() methods are not part of the actual df.Format interface,
			// then this part of the test needs adjustment.
			// For now, comparing directly assuming they are comparable or singletons from df package.
			assert.Equal(t, expectedDfFormat, valSchemaFormat, fmt.Sprintf("Schema format for col %d (%s)", i, originalField.Name))


			// Also check the underlying scalar type in the arrowValue
			if av, ok := val.(*arrowimpl.ArrowValue); ok { 
				assert.True(t, av.IsNilInternalScalar(), fmt.Sprintf("Internal scalar for col %d (%s) should be nil/invalid", i, originalField.Name))
				assert.True(t, arrow.TypeEqual(originalField.Type, av.InternalScalarType()), fmt.Sprintf("Internal scalar type for col %d (%s) should match field type", i, originalField.Name))
			} else {
				t.Errorf("nilRow.Get(%d) did not return an *arrowimpl.ArrowValue as expected by this test's internals", i)
			}
		}
	})

	t.Run("SchemaWithZeroFields", func(t *testing.T) {
		arrowSchema0 := arrow.NewSchema([]arrow.Field{}, nil)
		dfSchema0 := arrowimpl.NewArrowDataFrameSchema(arrowSchema0).(*arrowimpl.ArrowDataFrameSchema)
		nilRow0 := arrowimpl.NewNilArrowRow(dfSchema0, mem)
		assert.NotNil(t, nilRow0)
		assert.Equal(t, 0, nilRow0.Len(), "Row length for 0-field schema should be 0")
		assert.True(t, nilRow0.Schema().Equals(dfSchema0))
	})

	t.Run("PanicOnNilSchema", func(t *testing.T) {
		var nilDfSchema *arrowimpl.ArrowDataFrameSchema = nil
		assert.PanicsWithValue(t, "NewNilArrowRow: input schema or its internal arrow.Schema cannot be nil", func() {
			arrowimpl.NewNilArrowRow(nilDfSchema, mem)
		})
	})
}

// Placeholder for other tests that might exist in this file
func TestArrowValue(t *testing.T) {
	// Example of how other tests might look
	t.Run("StringValue", func(t *testing.T) {
		s := scalar.NewStringScalar("hello")
		v := arrowimpl.NewArrowValue(s, df.StringFormat) // Assuming df.StringFormat is defined
		assert.False(t, v.IsNil())
		assert.Equal(t, "hello", v.Get())
		assert.Equal(t, "hello", v.GetAsString())
		assert.Equal(t, df.StringFormat, v.Schema())
	})
}

func TestArrowDataFrameSchema(t *testing.T) {
	// Example
	t.Run("EmptySchema", func(t *testing.T) {
		as := arrow.NewSchema(nil,nil)
		dfs := arrowimpl.NewArrowDataFrameSchema(as)
		assert.Equal(t, 0, dfs.Len())
	})
}
```
df/arrow/types_test.go
<<<<<<< SEARCH
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
	"git.querycap.com/practice/df" // Ensure this is the correct import path
	"github.com/stretchr/testify/assert"

	arrowimpl "git.querycap.com/practice/df/arrow" // Package to test (internal call)
)

// --- (Existing tests for arrowValue, arrowDataFrameSchema, arrowRow must be here) ---
// --- (Helper getTestArrowSchema() from previous tests is useful here) ---
/*
func getTestArrowSchema() *arrow.Schema { // From previous tests
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "col_bool", Type: arrow.PrimitiveTypes.Boolean, Nullable: true},
			{Name: "col_time", Type: arrow.TimestampTypes.Timestamp_ns, Nullable: true},
		},
		nil, 
	)
}
*/


func TestNewNilArrowRow(t *testing.T) {
	mem := memory.NewGoAllocator() // Allocator might not be strictly needed by NewNullScalar but good practice if helpers use it.
	
	// Case 1: Schema with multiple fields of different types
	arrowSchema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "c1_str", Type: arrow.BinaryTypes.String},
			{Name: "c2_int", Type: arrow.PrimitiveTypes.Int64},
			{Name: "c3_bool", Type: arrow.PrimitiveTypes.Boolean},
		}, nil,
	)
	dfSchema1 := arrowimpl.NewArrowDataFrameSchema(arrowSchema1).(*arrowimpl.ArrowDataFrameSchema)

	nilRow1 := arrowimpl.CallNewNilArrowRow(dfSchema1, mem) // Using exported wrapper if newNilArrowRow is not exported
	
	assert.NotNil(t, nilRow1)
	assert.Equal(t, 3, nilRow1.Len(), "Row length should match field count")
	assert.True(t, nilRow1.Schema().Equals(dfSchema1), "Row schema should match input")

	for i := 0; i < nilRow1.Len(); i++ {
		assert.True(t, nilRow1.IsNil(i), fmt.Sprintf("Value at index %d should be nil", i))
		val := nilRow1.Get(i)
		assert.True(t, val.IsNil(), fmt.Sprintf("df.Value at index %d should be nil", i))
		// Check that the df.Value schema format matches the original field type
		expectedDfFormat := arrowimpl.ArrowToDfFormat(arrowSchema1.Field(i).Type)
		assert.Equal(t, expectedDfFormat.Name(), val.Schema().Name(), fmt.Sprintf("Schema name for col %d", i))
		assert.Equal(t, expectedDfFormat.Type(), val.Schema().Type(), fmt.Sprintf("Schema type for col %d", i))
	}

	// Case 2: Schema with zero fields
	arrowSchema0 := arrow.NewSchema([]arrow.Field{}, nil)
	dfSchema0 := arrowimpl.NewArrowDataFrameSchema(arrowSchema0).(*arrowimpl.ArrowDataFrameSchema)
	nilRow0 := arrowimpl.CallNewNilArrowRow(dfSchema0, mem)
	assert.NotNil(t, nilRow0)
	assert.Equal(t, 0, nilRow0.Len(), "Row length for 0-field schema should be 0")
	assert.True(t, nilRow0.Schema().Equals(dfSchema0))


	// Case 3: Panic if input schema is nil or its internal arrow.Schema is nil
	var nilDfSchema *arrowimpl.ArrowDataFrameSchema = nil
	assert.PanicsWithValue(t, "newNilArrowRow: input schema or its internal arrow.Schema cannot be nil", func() {
		arrowimpl.CallNewNilArrowRow(nilDfSchema, mem)
	})
	
	// Create a dfSchema with a nil internal arrow.Schema
	// This state should ideally not occur if constructors are used correctly.
	// Forcing it for test:
	dfSchemaWithNilInternal := arrowimpl.NewArrowDataFrameSchema(nil).(*arrowimpl.ArrowDataFrameSchema) 
	// The above NewArrowDataFrameSchema(nil) actually creates a valid empty schema.
	// So the panic "internal arrow.Schema cannot be nil" inside newNilArrowRow might not be reachable
	// if dfSchema itself is not nil. The first check `schema == nil` covers nilDfSchema.
	// The check `schema.schema == nil` inside newNilArrowRow:
	// If NewArrowDataFrameSchema(nil) creates schema.schema as non-nil empty schema, then this is fine.
}

// --- (Rest of types_test.go content) ---
=======
//go:build arrow

package arrow_test

import (
	"fmt"
	"reflect"
	"sort" 
	"strconv" 
	"testing"
	"time"

	"git.querycap.com/practice/df" 
	arrowimpl "git.querycap.com/practice/df/arrow"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/assert"
)

// Helper to get a common test schema (assumed to exist from prior tests)
func getTestArrowSchemaForNilRowTest() *arrow.Schema { 
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "col_bool", Type: arrow.PrimitiveTypes.Boolean, Nullable: true},
			{Name: "col_time_ns", Type: arrow.TimestampTypes.Timestamp_ns, Nullable: true},
			{Name: "col_date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		},
		nil, 
	)
}


func TestNewNilArrowRow(t *testing.T) {
	mem := memory.NewGoAllocator() 
	
	t.Run("SchemaWithMultipleFields", func(t *testing.T) {
		arrowSchema := getTestArrowSchemaForNilRowTest()
		// Cast to internal type *arrowimpl.ArrowDataFrameSchema for NewNilArrowRow
		// NewArrowDataFrameSchema is the public constructor.
		dfSchema := arrowimpl.NewArrowDataFrameSchema(arrowSchema).(*arrowimpl.ArrowDataFrameSchema)

		// Call the (now exported for test) NewNilArrowRow
		nilRow := arrowimpl.NewNilArrowRow(dfSchema, mem) 
		
		assert.NotNil(t, nilRow)
		assert.Equal(t, arrowSchema.NumFields(), nilRow.Len(), "Row length should match field count")
		
		assert.True(t, nilRow.Schema().Equals(dfSchema), "Row schema should match input dfSchema")

		for i := 0; i < nilRow.Len(); i++ {
			originalField := arrowSchema.Field(i)
			// df.Row.IsNil does not return error
			assert.True(t, nilRow.IsNil(i), fmt.Sprintf("Value at index %d (%s) should be nil", i, originalField.Name))
			
			// df.Row.Get does not return error
			val := nilRow.Get(i) 
			assert.NotNil(t, val, "df.Value should not be nil interface")
			assert.True(t, val.IsNil(), fmt.Sprintf("df.Value at index %d (%s) should be nil", i, originalField.Name))
			
			expectedDfFormat := arrowimpl.ArrowToDfFormat(originalField.Type) 
			valSchemaFormat := val.Schema() // This is df.Format as per problem's df.Value interface

			assert.Equal(t, expectedDfFormat, valSchemaFormat, fmt.Sprintf("Schema format for col %d (%s)", i, originalField.Name))


			// Also check the underlying scalar type in the arrowValue
			if av, ok := val.(*arrowimpl.ArrowValue); ok { 
				assert.True(t, av.IsNilInternalScalar(), fmt.Sprintf("Internal scalar for col %d (%s) should be nil/invalid", i, originalField.Name))
				assert.True(t, arrow.TypeEqual(originalField.Type, av.InternalScalarType()), fmt.Sprintf("Internal scalar type for col %d (%s) should match field type", i, originalField.Name))
			} else {
				t.Errorf("nilRow.Get(%d) did not return an *arrowimpl.ArrowValue as expected by this test's internals", i)
			}
		}
	})

	t.Run("SchemaWithZeroFields", func(t *testing.T) {
		arrowSchema0 := arrow.NewSchema([]arrow.Field{}, nil)
		dfSchema0 := arrowimpl.NewArrowDataFrameSchema(arrowSchema0).(*arrowimpl.ArrowDataFrameSchema)
		nilRow0 := arrowimpl.NewNilArrowRow(dfSchema0, mem)
		assert.NotNil(t, nilRow0)
		assert.Equal(t, 0, nilRow0.Len(), "Row length for 0-field schema should be 0")
		assert.True(t, nilRow0.Schema().Equals(dfSchema0))
	})

	t.Run("PanicOnNilSchema", func(t *testing.T) {
		var nilDfSchema *arrowimpl.ArrowDataFrameSchema = nil
		assert.PanicsWithValue(t, "NewNilArrowRow: input schema or its internal arrow.Schema cannot be nil", func() {
			arrowimpl.NewNilArrowRow(nilDfSchema, mem)
		})
	})
}

// Placeholder for other tests that might exist in this file
func TestArrowValue(t *testing.T) {
	// Example of how other tests might look
	t.Run("StringValue", func(t *testing.T) {
		s := scalar.NewStringScalar("hello")
		// Assuming df.StringFormat is defined and accessible
		// For testing, ensure df.StringFormat is part of the df package or defined for tests.
		// If df.StringFormat is an interface, this comparison might not be direct.
		// Let's use a format that's known to be df.StringFormat for this test.
		
		// Get the df.Format by converting from known Arrow type
		strFormat := arrowimpl.ArrowToDfFormat(arrow.BinaryTypes.String)

		v := arrowimpl.NewArrowValue(s, strFormat) 
		assert.False(t, v.IsNil())
		assert.Equal(t, "hello", v.Get())
		assert.Equal(t, "hello", v.GetAsString())
		assert.Equal(t, strFormat, v.Schema())
	})
}

func TestArrowDataFrameSchema(t *testing.T) {
	// Example
	t.Run("EmptySchema", func(t *testing.T) {
		as := arrow.NewSchema(nil,nil) // Creates an empty schema
		dfs := arrowimpl.NewArrowDataFrameSchema(as)
		assert.Equal(t, 0, dfs.Len())
	})
}
>>>>>>> REPLACE
