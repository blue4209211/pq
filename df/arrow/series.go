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

// arrowSeries is the Arrow-based implementation of the df.Series interface.
type arrowSeries struct {
	schema df.SeriesSchema
	arr    arrow.Array
}

// NewArrowSeries creates a new Arrow-based Series.
// It's important that the arr.DataType() is compatible with schema.Format.
func NewArrowSeries(arr arrow.Array, schema df.SeriesSchema) df.Series {
	// Basic validation, can be expanded
	if arr == nil {
		panic("arrow.Array cannot be nil")
	}
	// It might be good to also check if arr.DataType() matches schema.Format
	// For example, using a helper like dfFormatToArrowType or arrowTypeToDfFormat

	return &arrowSeries{
		schema: schema,
		arr:    arr,
	}
}

func (as *arrowSeries) Schema() df.SeriesSchema {
	return as.schema
}

func (as *arrowSeries) Len() int64 {
	if as.arr == nil {
		return 0
	}
	return int64(as.arr.Len())
}

func (as *arrowSeries) Get(index int64) df.Value {
	if as.arr == nil || index < 0 || index >= int64(as.arr.Len()) {
		panic(fmt.Sprintf("index %d out of bounds for series of length %d", index, as.Len()))
	}

	// Create a scalar from the array element at the given index
	// scalar.MakeScalar takes (data *Data, i int)
	// We need to ensure that we are passing the correct Data object.
	// For primitive types, arr.Data() should be okay.
	// For nested types, this might need more careful handling.
	valScalar := scalar.MakeScalar(as.arr.Data(), int(index))

	// The df.Format for the arrowValue should be the series' own format.
	return NewArrowValue(valScalar, as.schema.Format)
}

func (as *arrowSeries) ForEach(f func(df.Value)) {
	panic("not implemented")
}

func (as *arrowSeries) Sort(order df.SortOrder) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Map(schema df.Format, f func(df.Value) df.Value) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) FlatMap(schema df.Format, f func(df.Value) []df.Value) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Reduce(f func(df.Value, df.Value) df.Value, startValue df.Value) df.Value {
	panic("not implemented")
}

func (as *arrowSeries) Where(f func(df.Value) bool) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Limit(offset int, size int) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Distinct() df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Copy() df.Series {
    if as.arr == nil {
        // If the original array is nil, creating a new series with a nil array seems consistent.
        // However, the NewArrowSeries constructor panics on nil array.
        // For now, let's return a series with a schema but no data if arr is nil.
        // This behavior might need refinement based on how nil arrays are handled upstream or in constructors.
        emptyArr, _ := array.NewBuilder(arrow.FixedWidthTypes.Boolean).NewBooleanArray([]bool{})
        return NewArrowSeries(emptyArr, as.schema)
    }
    // array.NewSlice creates a new array that is a zero-copy view of the original.
    // Retain increases the reference count of the underlying data buffers.
    newArrSlice := array.NewSlice(as.arr, 0, as.arr.Len())
    newArrSlice.Retain()
    // It's crucial to manage the lifecycle of newArrSlice. If it's returned
    // and the original as.arr is released, newArrSlice will still be valid.
    // However, the caller of Copy() or the arrowSeries struct itself would
    // be responsible for eventually calling Release() on the array it holds.
    // For now, we create it, retain it, and the new series will own this reference.
    // The original `defer newArr.Release()` was incorrect as it would release immediately.
	return NewArrowSeries(newArrSlice, as.schema)
}

func (as *arrowSeries) Group() df.GroupedSeries {
	panic("not implemented")
}

func (as *arrowSeries) Select(e df.Expr) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) WhenNil(t df.Value) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) When(t map[any]df.Value) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) AsFormat(t df.Format) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Expr() df.Expr {
	panic("not implemented")
}

func (as *arrowSeries) Append(series df.Series) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Intersection(series df.Series) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Except(series df.Series) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Union(series df.Series) df.Series {
	panic("not implemented")
}

func (as *arrowSeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) df.Series {
	panic("not implemented")
}

// Ensure arrowSeries implements the df.Series interface.
var _ df.Series = (*arrowSeries)(nil)
