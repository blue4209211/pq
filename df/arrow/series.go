//go:build arrow

package arrow

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/builder"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
)

// arrowSeries struct definition
type arrowSeries struct {
	schema df.SeriesSchema
	arr    arrow.Array
	mem    memory.Allocator
}

// Helper to get arrow.DataType from df.Format
func dfFormatToArrowType(f df.Format) arrow.DataType {
	switch f.Name() {
	case df.StringFormat.Name(), "string":
		return arrow.BinaryTypes.String
	case df.IntegerFormat.Name(), "integer", "int64":
		return arrow.PrimitiveTypes.Int64
	case df.DoubleFormat.Name(), "double", "float64":
		return arrow.PrimitiveTypes.Float64
	case df.BoolFormat.Name(), "boolean", "bool":
		return arrow.PrimitiveTypes.Boolean
	case df.DateTimeFormat.Name(), "datetime":
		// Ensure this matches the TimestampType used by your scalars/arrays, e.g., Nanosecond.
		return arrow.TimestampTypes.Timestamp_ns
	default:
		// Attempt to use reflect.Type if available in df.Format for generic cases,
		// but this is hard to map directly to specific Arrow types without more info.
		// For now, panic for unhandled specific known types.
		panic(fmt.Sprintf("unsupported df.Format ('%s', type: %v) to Arrow DataType conversion", f.Name(), f.Type()))
	}
}

// Helper function to append a scalar.Scalar to an array.Builder
func appendScalarToBuilder(b array.Builder, s scalar.Scalar) error {
	if s == nil || !s.IsValid() {
		b.AppendNull()
		return nil
	}
	switch typedBuilder := b.(type) {
	case *builder.Int64Builder:
		if v, ok := s.(*scalar.Int64); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Int64 scalar for Int64Builder, got %T (value: %v)", s, s)}
	case *builder.Float64Builder:
		if v, ok := s.(*scalar.Float64); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Float64 scalar for Float64Builder, got %T (value: %v)", s, s)}
	case *builder.StringBuilder:
		if v, ok := s.(scalar.StringScalar); ok { typedBuilder.Append(v.String()) } else { return fmt.Errorf("type mismatch: expected StringScalar for StringBuilder, got %T (value: %v)", s, s)}
	case *builder.BooleanBuilder:
		if v, ok := s.(*scalar.Boolean); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Boolean scalar for BooleanBuilder, got %T (value: %v)", s, s)}
	case *builder.TimestampBuilder:
		if v, ok := s.(*scalar.Timestamp); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Timestamp scalar for TimestampBuilder, got %T (value: %v)", s, s)}
	// TODO: Add other supported types (Date32, Date64, Decimal, etc.)
	default:
		// This generic append might work for some types if the builder supports it, but it's risky.
		// For example, trying to append a scalar.String to a builder.Date32Builder would fail.
		// A more robust solution would involve ensuring type compatibility or using compute functions.
		// b.AppendValueFromString(s.String()) // Example of a risky generic approach
		return fmt.Errorf("unsupported builder type in appendScalarToBuilder: %T for scalar %T (value: %v)", b, s,s)
	}
	return nil
}


func (as *arrowSeries) Map(outputSchema df.Format, f func(df.Value) df.Value) df.Series {
	if as.arr == nil {
		panic("cannot map over a nil series")
	}

	outputArrowType := dfFormatToArrowType(outputSchema)
	b := builder.NewBuilder(as.mem, outputArrowType)
	defer b.Release()

	for i := int64(0); i < as.Len(); i++ {
		originalVal := as.Get(i)
		mappedVal := f(originalVal)

		if mappedVal == nil || mappedVal.IsNil() {
			b.AppendNull()
			continue
		}

		av, ok := mappedVal.(*arrowValue)
		if !ok {
			// If not an arrowValue, try to convert to a scalar of the target type.
			// This path is complex and error-prone. Best if f returns arrowValue.
			// For now, we require arrowValue for simplicity and type safety.
			panic(fmt.Sprintf("Map function returned a df.Value of type %T, expected *arrowValue. Consider wrapping result in NewArrowValue.", mappedVal))
		}
		if av.val == nil || !av.val.IsValid() {
			b.AppendNull()
			continue
		}

		// Check if the scalar type from the function matches the builder's type.
		// This is a stricter check. If a conversion is intended, f should handle it.
		if !arrow.TypeEqual(av.val.DataType(), outputArrowType) {
			// Attempt to cast the scalar if types don't match. This is experimental.
			// A better approach might be for `f` to ensure it returns the correct type,
			// or for `Map` to have a more sophisticated type conversion mechanism.
			castedScalar, err := scalar.Cast(compute.DefaultCastOptions(false), av.val, outputArrowType)
			if err != nil {
				panic(fmt.Sprintf("Map: error casting scalar from %s to %s: %v", av.val.DataType().Name(), outputArrowType.Name(), err))
			}
			defer castedScalar.Release() // Release the new scalar after appending
			err = appendScalarToBuilder(b, castedScalar)
			if err != nil {
				panic(fmt.Sprintf("Map: error appending casted scalar to builder: %v. Output Arrow Type: %s, Scalar Type: %s", err, outputArrowType.Name(), castedScalar.DataType().Name()))
			}
		} else {
			err := appendScalarToBuilder(b, av.val)
			if err != nil {
				panic(fmt.Sprintf("Map: error appending scalar to builder: %v. Output Arrow Type: %s, Scalar Type: %s", err, outputArrowType.Name(), av.val.DataType().Name()))
			}
		}
	}

	newArr := b.NewArray()
	// defer newArr.Release() // NewArrowSeriesWithAllocator will retain
	return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: as.schema.Name, Format: outputSchema}, as.mem)
}


func (as *arrowSeries) FlatMap(outputSchema df.Format, f func(df.Value) []df.Value) df.Series {
	if as.arr == nil {
		panic("cannot flatMap over a nil series")
	}

	outputArrowType := dfFormatToArrowType(outputSchema)
	b := builder.NewBuilder(as.mem, outputArrowType)
	defer b.Release()

	for i := int64(0); i < as.Len(); i++ {
		originalVal := as.Get(i)
		mappedResultSlice := f(originalVal)

		for _, mappedVal := range mappedResultSlice {
			if mappedVal == nil || mappedVal.IsNil() {
				b.AppendNull()
				continue
			}
			av, ok := mappedVal.(*arrowValue)
			if !ok {
				panic(fmt.Sprintf("FlatMap function returned a df.Value of type %T, expected *arrowValue. Consider wrapping result in NewArrowValue.", mappedVal))
			}
			if av.val == nil || !av.val.IsValid() {
				b.AppendNull()
				continue
			}

			if !arrow.TypeEqual(av.val.DataType(), outputArrowType) {
				castedScalar, err := scalar.Cast(compute.DefaultCastOptions(false), av.val, outputArrowType)
				if err != nil {
					panic(fmt.Sprintf("FlatMap: error casting scalar from %s to %s: %v", av.val.DataType().Name(), outputArrowType.Name(), err))
				}
				defer castedScalar.Release()
				err = appendScalarToBuilder(b, castedScalar)
				if err != nil {
					panic(fmt.Sprintf("FlatMap: error appending casted scalar to builder: %v. Output Arrow Type: %s, Scalar Type: %s", err, outputArrowType.Name(), castedScalar.DataType().Name()))
				}
			} else {
				err := appendScalarToBuilder(b, av.val)
				if err != nil {
					panic(fmt.Sprintf("FlatMap: error appending scalar to builder: %v. Output Arrow Type: %s, Scalar Type: %s", err, outputArrowType.Name(), av.val.DataType().Name()))
				}
			}
		}
	}
	newArr := b.NewArray()
	// defer newArr.Release() // NewArrowSeriesWithAllocator will retain
	return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: as.schema.Name, Format: outputSchema}, as.mem)
}

func (as *arrowSeries) Reduce(f func(currentAccumulator df.Value, currentValue df.Value) df.Value, startValue df.Value) df.Value {
	if startValue == nil {
		panic("Reduce startValue cannot be nil")
	}

	accumulator := startValue
    if as.arr == nil || as.Len() == 0 {
		return accumulator
	}

	for i := int64(0); i < as.Len(); i++ {
		currentVal := as.Get(i)
		accumulator = f(accumulator, currentVal)
	}
	return accumulator
}

func (as *arrowSeries) Distinct() df.Series {
	if as.arr == nil || as.arr.Len() == 0 {
		return as.Copy()
	}

	ctx := compute.WithAllocator(context.Background(), as.mem)
	datum := arrow.NewArrayDatum(as.arr) // Wrap array in Datum
	defer datum.Release()

	uniqueDatum, err := compute.Unique(ctx, datum)
	if err != nil {
		panic(fmt.Sprintf("failed to compute unique values: %v", err))
	}
	defer uniqueDatum.Release()

	uniqueArr, ok := uniqueDatum.(*arrow.ArrayDatum).Value.(arrow.Array)
	if !ok {
		panic(fmt.Sprintf("compute.Unique did not return an ArrayDatum as expected, got %T", uniqueDatum))
	}
    // NewArrowSeriesWithAllocator will Retain uniqueArr.
	return NewArrowSeriesWithAllocator(uniqueArr, as.schema, as.mem)
}


// --- ALL other methods of arrowSeries from previous steps must be present below ---
func NewArrowSeries(arr arrow.Array, schema df.SeriesSchema) df.Series {
	return NewArrowSeriesWithAllocator(arr, schema, memory.DefaultAllocator)
}
func NewArrowSeriesWithAllocator(arr arrow.Array, schema df.SeriesSchema, mem memory.Allocator) df.Series {
	if arr == nil { panic("arrow.Array cannot be nil") }
	if mem == nil { panic("memory.Allocator cannot be nil") }
	arr.Retain(); return &arrowSeries{schema: schema, arr: arr, mem: mem}
}
func (as *arrowSeries) Schema() df.SeriesSchema { return as.schema }
func (as *arrowSeries) Len() int64 { if as.arr == nil { return 0 }; return int64(as.arr.Len()) }
func (as *arrowSeries) Get(index int64) df.Value {
	if as.arr == nil || index < 0 || index >= int64(as.arr.Len()) { panic(fmt.Sprintf("index %d out of bounds for series of length %d", index, as.Len()))}
	// scalar.MakeScalar does not retain the array data, it just provides a view.
	return NewArrowValue(scalar.MakeScalar(as.arr, int(index)), as.schema.Format)
}
func (as *arrowSeries) ForEach(f func(df.Value)) { if as.arr == nil { return }; for i := int64(0); i < as.Len(); i++ { f(as.Get(i)) } }
func (as *arrowSeries) Limit(offset int, size int) df.Series {
	if as.arr == nil { panic("cannot limit a nil series") }
	currentLen := as.arr.Len(); if offset < 0 { offset = 0 }
	if offset >= currentLen {
		b := builder.NewBuilder(as.mem, as.arr.DataType()); defer b.Release()
		newArr := b.NewArray(); /*defer newArr.Release()*/
		return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
	}
	if offset+size > currentLen { size = currentLen - offset }
	if size < 0 { size = 0 }
	newSlice := array.NewSlice(as.arr, int64(offset), int64(offset+size))
	// defer newSlice.Release() // NewArrowSeriesWithAllocator will retain.
	return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}
func (as *arrowSeries) Where(f func(df.Value) bool) df.Series {
    if as.arr == nil { panic("cannot filter a nil series") }
    b := builder.NewBuilder(as.mem, as.arr.DataType()); defer b.Release()
    for i := int64(0); i < as.Len(); i++ {
        val := as.Get(i)
        if f(val) {
            arrowVal, ok := val.(*arrowValue)
            if !ok && !val.IsNil() {
                panic(fmt.Sprintf("Where: filter function processed a value of unexpected type %T", val))
            }

            if val.IsNil() || (ok && (arrowVal.val == nil || !arrowVal.val.IsValid())) {
                b.AppendNull()
            } else {
                if err := appendScalarToBuilder(b, arrowVal.val); err != nil {
                    panic(fmt.Sprintf("Where: error appending scalar: %v. Scalar type: %s, Builder type: %s",
                        err, arrowVal.val.DataType().Name(), b.Type().Name()))
                }
            }
        }
    }
    newArr := b.NewArray(); /*defer newArr.Release()*/
    return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
}
func (as *arrowSeries) Sort(order df.SortOrder) df.Series {
    if as.arr == nil || as.arr.Len() == 0 { return as.Copy() }
    ctx := compute.WithAllocator(context.Background(), as.mem)
    arrowSortOrder := arrow.Ascending; if order == df.SortOrderDESC { arrowSortOrder = arrow.Descending }
    // Wrap as.arr in a Datum for compute functions
    arrDatum := arrow.NewArrayDatum(as.arr)
    defer arrDatum.Release()
    indicesDatum, err := compute.SortIndices(ctx, arrDatum, compute.SortOptions{Order: arrowSortOrder, NullPlacement: arrow.NullsFirst})
    if err != nil { panic(fmt.Sprintf("failed to get sort indices: %v", err)) }
    defer indicesDatum.Release()
    indicesArr, ok := indicesDatum.(*arrow.ArrayDatum).Value.(arrow.Array)
    if !ok { panic("SortIndices did not return an array datum as expected") }
    // indicesArr is owned by indicesDatum, no need to retain/release separately unless taken out of context.

    sortedArrDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrDatum, arrow.NewArrayDatum(indicesArr))
    if err != nil { panic(fmt.Sprintf("failed to take sorted elements: %v", err)) }
    defer sortedArrDatum.Release()
    sortedArr, ok := sortedArrDatum.(*arrow.ArrayDatum).Value.(arrow.Array)
    if !ok { panic("Take did not return an array datum as expected") }
    // NewArrowSeriesWithAllocator will Retain the sortedArr.
    return NewArrowSeriesWithAllocator(sortedArr, as.schema, as.mem)
}
func (as *arrowSeries) Copy() df.Series {
    if as.arr == nil {
         if as.schema.Format != nil && as.mem != nil {
            dt := dfFormatToArrowType(as.schema.Format)
            bld := builder.NewBuilder(as.mem, dt)
            defer bld.Release()
            emptyArr := bld.NewArray(); /*defer emptyArr.Release()*/
            return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
         }
         panic("cannot copy a series with a nil internal array and no way to determine type/allocator")
    }
    newSlice := array.NewSlice(as.arr, 0, as.arr.Len())
    // defer newSlice.Release() // NewArrowSeriesWithAllocator will retain.
	return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}
func (as *arrowSeries) Release() { if as.arr != nil { as.arr.Release(); as.arr = nil } }

// Stubs for remaining methods
func (as *arrowSeries) Group() df.GroupedSeries { panic("not implemented") }
func (as *arrowSeries) Select(e df.Expr) df.Series { panic("not implemented") }
func (as *arrowSeries) WhenNil(t df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) When(t map[any]df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) AsFormat(t df.Format) df.Series { panic("not implemented") }
func (as *arrowSeries) Expr() df.Expr { panic("not implemented") }
func (as *arrowSeries) Append(series df.Series) df.Series { panic("not implemented") }
func (as *arrowSeries) Intersection(series df.Series) df.Series { panic("not implemented") }
func (as *arrowSeries) Except(series df.Series) df.Series { panic("not implemented") }
func (as *arrowSeries) Union(series df.Series) df.Series { panic("not implemented") }
func (as *arrowSeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) df.Series { panic("not implemented") }

var _ df.Series = (*arrowSeries)(nil)
