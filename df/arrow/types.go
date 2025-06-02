//go:build arrow

package arrow

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
)

// arrowValue is the Arrow-based implementation of the df.Value interface.
type arrowValue struct {
	val    scalar.Scalar
	format df.Format
}

// NewArrowValue creates a new arrowValue.
func NewArrowValue(s scalar.Scalar, f df.Format) df.Value {
	return &arrowValue{val: s, format: f}
}

func (v *arrowValue) Schema() df.Format {
	return v.format
}

func (v *arrowValue) Get() any {
	if v.val == nil || !v.val.IsValid() {
		return nil
	}
	switch s := v.val.(type) {
	case *scalar.String:
		return s.String()
	case *scalar.Int64:
		return s.Value
	case *scalar.Float64:
		return s.Value
	case *scalar.Boolean:
		return s.Value
	case *scalar.Timestamp:
		return s.ToTime(arrow.Nanosecond)
	default:
		panic(fmt.Sprintf("unsupported arrow scalar type for Get: %T", v.val))
	}
}

func (v *arrowValue) GetAsString() string {
	if v.val == nil || !v.val.IsValid() {
		return ""
	}
	return fmt.Sprintf("%v", v.Get())
}

func (v *arrowValue) GetAsInt() int64 {
	if s, ok := v.val.(*scalar.Int64); ok && s.IsValid() {
		return s.Value
	}
	panic(fmt.Sprintf("cannot convert %T to int64", v.val))
}

func (v *arrowValue) GetAsDouble() float64 {
	if s, ok := v.val.(*scalar.Float64); ok && s.IsValid() {
		return s.Value
	}
	panic(fmt.Sprintf("cannot convert %T to float64", v.val))
}

func (v *arrowValue) GetAsBool() bool {
	if s, ok := v.val.(*scalar.Boolean); ok && s.IsValid() {
		return s.Value
	}
	panic(fmt.Sprintf("cannot convert %T to bool", v.val))
}

func (v *arrowValue) GetAsDatetime() time.Time {
	if s, ok := v.val.(*scalar.Timestamp); ok && s.IsValid() {
		return s.ToTime(arrow.Nanosecond)
	}
	panic(fmt.Sprintf("cannot convert %T to time.Time", v.val))
}

func (v *arrowValue) IsNil() bool {
	return v.val == nil || !v.val.IsValid()
}

func (v *arrowValue) Equals(other df.Value) bool {
	if other == nil || other.IsNil() {
		return v.IsNil()
	}
	if v.IsNil() {
		return false
	}
	otherArrowVal, ok := other.(*arrowValue)
	if !ok {
		return false
	}
	return scalar.Equals(v.val, otherArrowVal.val)
}

var _ df.Value = (*arrowValue)(nil)

// arrowRow is the Arrow-based implementation of the df.Row interface.
type arrowRow struct {
	schema *arrowDataFrameSchema // Reference to the DataFrame schema
	values []scalar.Scalar     // Data for this row
	// We might not need rowIndex if values are self-contained for the row.
	// If values are extracted from a record batch, then rowIndex is relevant.
	// For now, assuming values are for a single row.
}

// NewArrowRow creates a new arrowRow.
// This constructor assumes that the []scalar.Scalar directly corresponds to the schema.
func NewArrowRow(schema *arrowDataFrameSchema, values []scalar.Scalar) df.Row {
	if schema.Len() != len(values) {
		panic("schema length and values length mismatch")
	}
	return &arrowRow{schema: schema, values: values}
}

// NewArrowRowFromRecord creates a row from a specific index in an arrow.Record
func NewArrowRowFromRecord(schema *arrowDataFrameSchema, rec arrow.Record, rowIndex int) (df.Row, error) {
	if rowIndex < 0 || rowIndex >= int(rec.NumRows()) {
		return nil, fmt.Errorf("rowIndex %d out of bounds for record with %d rows", rowIndex, rec.NumRows())
	}
	if int(rec.NumCols()) != schema.Len() {
		return nil, fmt.Errorf("record column count %d does not match schema length %d", rec.NumCols(), schema.Len())
	}

	values := make([]scalar.Scalar, rec.NumCols())
	for i, col := range rec.Columns() {
		values[i] = scalar.MakeScalar(col.Data(), rowIndex)
	}
	return &arrowRow{schema: schema, values: values}, nil
}


func (r *arrowRow) Schema() df.DataFrameSchema {
	return r.schema
}

func (r *arrowRow) GetRaw(i int) any {
	if i < 0 || i >= len(r.values) {
		panic("index out of bounds")
	}
	s := r.values[i]
	if s == nil || !s.IsValid() {
		return nil
	}
	// This is a simplified Get() from arrowValue.
	// It might be better to return the scalar.Scalar itself or use a more robust conversion.
	switch sc := s.(type) {
	case *scalar.String:
		return sc.String()
	case *scalar.Int64:
		return sc.Value
	case *scalar.Float64:
		return sc.Value
	case *scalar.Boolean:
		return sc.Value
	case *scalar.Timestamp:
		return sc.ToTime(arrow.Nanosecond)
	default:
		panic(fmt.Sprintf("unsupported arrow scalar type for GetRaw: %T", s))
	}
}

func (r *arrowRow) Get(i int) df.Value {
	if i < 0 || i >= len(r.values) {
		panic("index out of bounds")
	}
	// The df.Format should be derived from the schema for this column index
	colSchema := r.schema.Get(i)
	return NewArrowValue(r.values[i], colSchema.Format)
}

func (r *arrowRow) GetByName(s string) df.Value {
	idx := r.schema.GetIndexByName(s)
	if idx == -1 {
		panic(fmt.Sprintf("column %s not found", s))
	}
	return r.Get(idx)
}

func (r *arrowRow) Len() int {
	return len(r.values)
}

func (r *arrowRow) GetAsString(i int) string {
	return r.Get(i).GetAsString()
}

func (r *arrowRow) GetAsInt(i int) int64 {
	return r.Get(i).GetAsInt()
}

func (r *arrowRow) GetAsDouble(i int) float64 {
	return r.Get(i).GetAsDouble()
}

func (r *arrowRow) GetAsBool(i int) bool {
	return r.Get(i).GetAsBool()
}

func (r *arrowRow) GetAsDatetime(i int) time.Time {
	return r.Get(i).GetAsDatetime()
}

func (r *arrowRow) GetMap() map[string]df.Value {
	res := make(map[string]df.Value, len(r.values))
	for i, name := range r.schema.Names() {
		res[name] = r.Get(i)
	}
	return res
}

func (r *arrowRow) IsAnyNil() bool {
	for _, v := range r.values {
		if v == nil || !v.IsValid() {
			return true
		}
	}
	return false
}

func (r *arrowRow) IsNil(i int) bool {
	if i < 0 || i >= len(r.values) {
		panic("index out of bounds")
	}
	return r.values[i] == nil || !r.values[i].IsValid()
}

func (r *arrowRow) Copy() df.Row {
	newValues := make([]scalar.Scalar, len(r.values))
	// For scalar.Scalar, direct assignment should be fine as they are typically immutable
	// or represent single values. If they were mutable and shared, a deep copy would be needed.
	copy(newValues, r.values)
	return NewArrowRow(r.schema, newValues)
}

func (r *arrowRow) Select(indices ...int) df.Row {
	newSchemaFields := make([]arrow.Field, len(indices))
	newValues := make([]scalar.Scalar, len(indices))
	newDfSeriesSchema := make([]df.SeriesSchema, len(indices))

	for i, idx := range indices {
		if idx < 0 || idx >= r.schema.Len() {
			panic(fmt.Sprintf("select index %d out of bounds for row with length %d", idx, r.schema.Len()))
		}
		originalField := r.schema.schema.Field(idx) // Accessing underlying arrow.Schema
		newSchemaFields[i] = originalField
		newValues[i] = r.values[idx]
		newDfSeriesSchema[i] = r.schema.Get(idx)
	}

	// Create a new arrow.Schema for the selected columns
	selectedArrowSchema := arrow.NewSchema(newSchemaFields, nil)
	// Wrap it in our arrowDataFrameSchema
	selectedDfSchema := NewArrowDataFrameSchema(selectedArrowSchema).(*arrowDataFrameSchema)

	return NewArrowRow(selectedDfSchema, newValues)
}

func (r *arrowRow) Append(name string, val df.Value) df.Row {
    // Appending to a row implies changing its schema, which is complex.
    // The df.Row interface's Append is more about creating a *new* row with an additional field,
    // rather than mutating the existing row in place, especially if these rows are part of a DataFrame.
    // This operation is more logical at the DataFrame level or when constructing new rows.
    // For now, let's panic as this is not straightforward for an Arrow-backed row without context.
	panic("Append operation on arrowRow is not directly supported in this manner; schema would need to change.")
}

var _ df.Row = (*arrowRow)(nil)

// arrowDataFrameSchema is the Arrow-based implementation of the df.DataFrameSchema interface.
type arrowDataFrameSchema struct {
	schema *arrow.Schema
}

func NewArrowDataFrameSchema(schema *arrow.Schema) df.DataFrameSchema {
	return &arrowDataFrameSchema{schema: schema}
}

func arrowToDfFormat(dt arrow.DataType) df.Format {
	switch dt.ID() {
	case arrow.STRING:
		return df.StringFormat
	case arrow.INT64:
		return df.IntegerFormat
	case arrow.FLOAT64:
		return df.DoubleFormat
	case arrow.BOOL:
		return df.BoolFormat
	case arrow.TIMESTAMP:
		return df.DateTimeFormat
	default:
		return df.NewGenericFormat(dt.Name(), reflect.Interface)
	}
}

func (s *arrowDataFrameSchema) Series() []df.SeriesSchema {
	if s.schema == nil {
		return nil
	}
	seriesSchemas := make([]df.SeriesSchema, s.schema.NumFields())
	for i, field := range s.schema.Fields() {
		seriesSchemas[i] = df.SeriesSchema{
			Name:   field.Name,
			Format: arrowToDfFormat(field.Type),
		}
	}
	return seriesSchemas
}

func (s *arrowDataFrameSchema) Names() []string {
	if s.schema == nil {
		return nil
	}
	names := make([]string, s.schema.NumFields())
	for i, field := range s.schema.Fields() {
		names[i] = field.Name
	}
	return names
}

func (s *arrowDataFrameSchema) GetByName(name string) df.SeriesSchema {
	if s.schema == nil {
		panic("schema is nil")
	}
	idx := s.schema.FieldIndices(name)
	if len(idx) == 0 {
		return df.SeriesSchema{}
	}
	field := s.schema.Field(idx[0])
	return df.SeriesSchema{
		Name:   field.Name,
		Format: arrowToDfFormat(field.Type),
	}
}

func (s *arrowDataFrameSchema) GetIndexByName(name string) int {
	if s.schema == nil {
		panic("schema is nil")
	}
	idx := s.schema.FieldIndices(name)
	if len(idx) == 0 {
		return -1
	}
	return idx[0]
}

func (s *arrowDataFrameSchema) HasName(name string) bool {
	if s.schema == nil {
		return false
	}
	return len(s.schema.FieldIndices(name)) > 0
}

func (s *arrowDataFrameSchema) Get(i int) df.SeriesSchema {
	if s.schema == nil || i < 0 || i >= s.schema.NumFields() {
		panic("index out of bounds or schema is nil")
	}
	field := s.schema.Field(i)
	return df.SeriesSchema{
		Name:   field.Name,
		Format: arrowToDfFormat(field.Type),
	}
}

func (s *arrowDataFrameSchema) Len() int {
	if s.schema == nil {
		return 0
	}
	return s.schema.NumFields()
}

func (s *arrowDataFrameSchema) Equals(other df.DataFrameSchema) bool {
	if other == nil {
		return false
	}
	otherArrowSchema, ok := other.(*arrowDataFrameSchema)
	if !ok {
        if s.Len() != other.Len() {
            return false
        }
        for i := 0; i < s.Len(); i++ {
            s1 := s.Get(i)
            s2 := other.Get(i)
            if s1.Name != s2.Name || s1.Format.Name() != s2.Format.Name() || s1.Format.Type() != s2.Format.Type() {
                return false
            }
        }
        return true
	}
    if s.schema == nil && otherArrowSchema.schema == nil {
        return true
    }
    if s.schema == nil || otherArrowSchema.schema == nil {
        return false
    }
	return s.schema.Equal(otherArrowSchema.schema)
}

var _ df.DataFrameSchema = (*arrowDataFrameSchema)(nil)
