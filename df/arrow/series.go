//go:build arrow

package arrow

import (
	"context"
	"fmt"
	// "reflect" // Not used in the original, check if needed by my changes
	// "time" // Not used in the original, check if needed by my changes

	"git.querycap.com/practice/df" // MODIFIED: Import path
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/builder" // Using generic builder
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
)

type arrowSeries struct {
	schema df.SeriesSchema
	arr    arrow.Array
	mem    memory.Allocator
}

// REMOVED local dfFormatToArrowType, will use the one from types.go
// REMOVED local appendScalarToBuilder, will use the one from types.go
// The dfValueToArrowScalar used by AsFormat, WhenNil, When will come from types.go

func NewArrowSeries(arr arrow.Array, schema df.SeriesSchema) df.Series { return NewArrowSeriesWithAllocator(arr, schema, memory.DefaultAllocator) }
func NewArrowSeriesWithAllocator(rawArr arrow.Array, schema df.SeriesSchema, mem memory.Allocator) df.Series {
	if rawArr == nil { panic("NewArrowSeriesWithAllocator: arrow.Array cannot be nil") };
	if mem == nil { panic("NewArrowSeriesWithAllocator: memory.Allocator cannot be nil") }

	// If schema.Format is UnknownFormat (or nil, if Format is an interface and nil is possible for "unknown"),
	// and inference from array also yields UnknownFormat, then it's an issue, unless it's a NullType array.
	if (schema.Format == df.UnknownFormat || schema.Format == nil) &&
	   arrowToDfFormat(rawArr.DataType()) == df.UnknownFormat &&
	   rawArr.DataType().ID() != arrow.NULL {
         panic(fmt.Sprintf("NewArrowSeriesWithAllocator: df.SeriesSchema.Format is %v and cannot be inferred from array type %s", schema.Format, rawArr.DataType().Name()))
    }

	// 1. Determine effective df.Format
	effectiveFormat := schema.Format
	if effectiveFormat == df.UnknownFormat || effectiveFormat == nil {
		inferredFormat := arrowToDfFormat(rawArr.DataType())
		// If still unknown after inference (and not a Null array type), then panic.
		if inferredFormat == df.UnknownFormat && rawArr.DataType().ID() != arrow.NULL {
			panic(fmt.Sprintf("NewArrowSeriesWithAllocator: cannot infer df.Format from arrow array type %s and input schema.Format was %v", rawArr.DataType().Name(), schema.Format))
		}
		effectiveFormat = inferredFormat
	}
	// After inference, effectiveFormat must be valid (not Unknown or nil if Format is an interface)
	// unless it's a NullType array which can pair with UnknownFormat.
	if (effectiveFormat == df.UnknownFormat || effectiveFormat == nil) && rawArr.DataType().ID() != arrow.NULL {
		panic(fmt.Sprintf("NewArrowSeriesWithAllocator: effective df.Format is still %v after inference for non-Null array type %s", effectiveFormat, rawArr.DataType().Name()))
	}

	// 2. Validate df.Format compatibility with rawArr.DataType()
	// expectedArrowType is what the effectiveFormat maps to in Arrow terms.
	expectedArrowType, err := dfFormatToArrowType(effectiveFormat)
	if err != nil {
		// This panic should ideally not be reached if effectiveFormat is not UnknownFormat
		// and dfFormatToArrowType covers all known df.Formats.
		panic(fmt.Sprintf("NewArrowSeriesWithAllocator: cannot map effectiveFormat %v to arrow.DataType: %v", effectiveFormat, err))
	}

	// If the actual array type doesn't match what's expected by effectiveFormat,
	// check for known compatible groups (e.g. any Arrow int type vs df.IntFormat).
	if rawArr.DataType().ID() != arrow.NULL && expectedArrowType.ID() != rawArr.DataType().ID() {
		actualArrFormat := arrowToDfFormat(rawArr.DataType()) // What df.Format the actual array maps to
		compatible := false
		if isIntegerFormat(actualArrFormat) && isIntegerFormat(effectiveFormat) {
			compatible = true
		} else if isFloatFormat(actualArrFormat) && isFloatFormat(effectiveFormat) {
			compatible = true
		} else if isStringFormat(actualArrFormat) && isStringFormat(effectiveFormat) {
			compatible = true
		} else if isBoolFormat(actualArrFormat) && isBoolFormat(effectiveFormat) {
			compatible = true
		} else if isTimeFormat(actualArrFormat) && isTimeFormat(effectiveFormat) {
            compatible = true
        } else if isDateFormat(actualArrFormat) && isDateFormat(effectiveFormat) {
            compatible = true
        }
        // Add other compatibility rules if necessary

		if !compatible && actualArrFormat != effectiveFormat {
			panic(fmt.Sprintf("NewArrowSeriesWithAllocator: effectiveFormat %v (expects Arrow %s) is not compatible with actual array type %s (which maps to df.Format %v)",
				effectiveFormat, expectedArrowType.Name(), rawArr.DataType().Name(), actualArrFormat))
		}
	}

	// 3. Determine effective Nullability
	effectiveNullable := schema.Nullable // Start with user's preference from input schema
	if rawArr.NullN() > 0 {
		// Data has nulls. If schema was explicitly set by user to NOT nullable, this is a contradiction.
		// Check if schema.Format was originally set (not inferred) to make this decision.
		if (schema.Format != nil && schema.Format != df.UnknownFormat) && !schema.Nullable {
			panic(fmt.Sprintf("NewArrowSeriesWithAllocator: schema for series '%s' (Format: %v) is marked non-nullable, but data contains %d nulls",
                schema.Name, effectiveFormat, rawArr.NullN()))
		}
		effectiveNullable = true // Data has nulls, so series must be nullable.
	} else {
		// Data has no nulls.
		// If schema.Format was NOT specified by user (i.e., it was inferred), then nullability also gets inferred as false (non-nullable).
		// Otherwise (schema.Format was specified by user), respect schema.Nullable specified by the user.
		if schema.Format == nil || schema.Format == df.UnknownFormat {
			effectiveNullable = false
		}
		// if schema.Format was specified by user, effectiveNullable is already schema.Nullable from above, which is correct.
	}

	finalSchema := df.SeriesSchema{
		Name:     schema.Name,
		Format:   effectiveFormat,
		Nullable: effectiveNullable,
		Metadata: schema.Metadata, // Preserve metadata
	}

	rawArr.Retain()
	// Use finalSchema here
	return &arrowSeries{schema: finalSchema, arr: rawArr, mem: mem}
}

func (as *arrowSeries) Schema() df.SeriesSchema { return as.schema }
func (as *arrowSeries) Len() int { if as.arr == nil { return 0 }; return as.arr.Len() } // MODIFIED: int64 to int

func (as *arrowSeries) Get(index int) df.Value { // MODIFIED: int64 to int
	if as.arr == nil || index < 0 || index >= as.arr.Len() {
		// Return a typed null value consistent with Series format
		arrowDt, err := dfFormatToArrowType(as.schema.Format)
		if err != nil {
			// This case is tricky: Get interface doesn't return error.
			// Panic if we can't even determine the type for a null value.
			panic(fmt.Errorf("Get: cannot determine arrow type for schema format %v for out-of-bounds access: %w", as.schema.Format, err))
		}
		nullScalar := scalar.NewNullScalar(arrowDt)
		return NewArrowValue(nullScalar, as.schema.Format)
	}
	return NewArrowValue(scalar.MakeScalar(as.arr, index), as.schema.Format)
}

// REMOVED dfFormatToArrowTypeUnsafe helper, direct error handling or panic inline

func (as *arrowSeries) ForEach(f func(df.Value)) { if as.arr == nil { return }; for i := 0; i < as.Len(); i++ { f(as.Get(i)) } }

func (as *arrowSeries) Limit(offset int, size int) df.Series {
	if as.arr == nil { panic("cannot limit a nil series") }; currentLen := as.arr.Len(); if offset < 0 { offset = 0 }
	if offset >= currentLen {
		// Return empty series of the same type
		b := array.NewBuilder(as.mem, as.arr.DataType()); defer b.Release() // MODIFIED: builder.NewBuilder to array.NewBuilder
		newArr := b.NewArray()
		// newArr is already retained by NewArray, but NewArrowSeriesWithAllocator will retain again.
		// This is fine as long as releases match.
		return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
	}
	if offset+size > currentLen { size = currentLen - offset }; if size < 0 { size = 0 }
	newSlice := array.NewSlice(as.arr, int64(offset), int64(offset+size))
	// newSlice is retained. NewArrowSeriesWithAllocator will retain again.
	return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}

func (as *arrowSeries) Where(f func(df.Value) bool) df.Series {
    if as.arr == nil { panic("cannot filter a nil series") };
    b := array.NewBuilder(as.mem, as.arr.DataType()); defer b.Release() // MODIFIED: builder.NewBuilder to array.NewBuilder
    for i := 0; i < as.Len(); i++ { // MODIFIED: int64 to int
        val := as.Get(i)
        if f(val) {
            arrowVal, ok := val.(*arrowValue)
            if !ok && !val.IsNil() { // If val is not nil, it must be arrowValue
                 panic(fmt.Errorf("Where: df.Value is not an arrowValue and not nil, type: %T", val))
            }

            var scalarToAppend scalar.Scalar
            if val.IsNil() { // If df.Value itself is nil
                scalarToAppend = scalar.NewNullScalar(b.Type())
            } else { // val is a valid arrowValue (which might wrap a nil scalar)
                scalarToAppend = arrowVal.s
            }

            err := appendScalarToBuilder(b, scalarToAppend, b.Type()) // Using types.go helper
            if err != nil {
                panic(fmt.Errorf("Where: error appending scalar to builder: %w. Scalar: %v, BuilderType: %v", err, scalarToAppend, b.Type()))
            }
        }
    }
    newArr := b.NewArray()
    return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
}

func (as *arrowSeries) Sort(order df.SortOrder) df.Series {
    if as.arr == nil || as.arr.Len() == 0 { return as.Copy() };
    ctx := compute.WithAllocator(context.Background(), as.mem) // compute.DefaultContext might be enough if no specific allocator needed for compute ops

    arrowSortOrder := arrow.Ascending
    if order == df.Descending { // Assuming df.Descending is the correct enum value
        arrowSortOrder = arrow.Descending
    }

    // compute.SortIndices takes an Datum input
    arrDatum := arrow.NewArrayDatum(as.arr)
    // arrDatum does not need Release() if it's just a wrapper and doesn't retain as.arr
    // However, Arrow examples often show it. Let's assume it's safer with Release if created.
    // According to docs, NewArrayDatum does not acquire ownership (no Retain). So no Release needed.

    indicesDatum, err := compute.SortIndices(ctx, arrDatum, compute.SortOptions{Order: arrowSortOrder, NullPlacement: arrow.NullsFirst})
    if err != nil { panic(fmt.Errorf("SortIndices failed: %w", err)) };
    defer indicesDatum.Release() // indicesDatum is a new Datum, needs release.

    indicesArr, ok := indicesDatum.Value().(arrow.Array) // Use Value() then type assert
    if !ok { panic("SortIndices did not return a valid ArrayDatum containing an Array") }
    // indicesArr is owned by indicesDatum.

    // Create a new Datum for indicesArr for the Take operation.
    // This might be redundant if Take can accept arrow.Array directly, but API expects Datum.
    indicesArrDatum := arrow.NewArrayDatum(indicesArr)
    // Not releasing indicesArrDatum as it's just a wrapper for indicesArr which is owned by indicesDatum.

    sortedArrDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrDatum, indicesArrDatum)
    if err != nil { panic(fmt.Errorf("Take failed: %w", err)) };
    defer sortedArrDatum.Release() // sortedArrDatum is new, needs release.

    sortedArr, ok := sortedArrDatum.Value().(arrow.Array)
    if !ok { panic("Take did not return a valid ArrayDatum containing an Array") }
    // sortedArr is owned by sortedArrDatum. NewArrowSeriesWithAllocator will Retain.
    return NewArrowSeriesWithAllocator(sortedArr, as.schema, as.mem)
}

func (as *arrowSeries) Map(outputFormat df.Format, f func(df.Value) df.Value) df.Series {
	if as.arr == nil { panic("map on nil series") };
	outputArrowType, err := dfFormatToArrowType(outputFormat)
	if err != nil {
		panic(fmt.Errorf("Map: could not get arrow type for output format %v: %w", outputFormat, err))
	}

	b := array.NewBuilder(as.mem, outputArrowType); defer b.Release()
	for i := 0; i < as.Len(); i++ {
		originalVal := as.Get(i);
		mappedVal := f(originalVal) // mappedVal is df.Value

		scalarToAppend, errConv := dfValueToArrowScalar(mappedVal, outputArrowType)
		if errConv != nil {
			panic(fmt.Errorf("Map: failed to convert mapped df.Value (from input %v) to Arrow scalar for type %s: %w", originalVal, outputArrowType.Name(), errConv))
		}
		// dfValueToArrowScalar from types.go does not retain the scalar it returns, so no release needed for scalarToAppend here.

		errAppend := appendScalarToBuilder(b, scalarToAppend, outputArrowType)
		if errAppend != nil {
			panic(fmt.Errorf("Map: error appending scalar to builder: %w. Scalar: %v", errAppend, scalarToAppend))
		}
	}
	newArr := b.NewArray()
	finalNullable := true // Safest default: operations might introduce nulls.
	if newArr.NullN() == 0 {
		// If no nulls were produced, can we infer it's non-nullable?
		// This is only true if the mapping function `f` guarantees non-null output
		// AND the outputFormat itself isn't something like a "nullable string" type.
		// This level of inference is hard. A simpler rule:
		// If the original series was non-nullable AND f guarantees non-null, then non-nullable.
		// For now, if newArr.NullN() == 0, we *could* set it to false, but it's an assumption.
		// Let's assume `f` can produce nils, so `true` is safer unless `newArr.NullN() == 0`.
		// If `f` *cannot* produce nils (e.g. `v.GetAsInt() * 2`), and input has no nils for type errors,
		// then nullability might be preserved or become false.
		// Sticking to `newArr.NullN() == 0` is a data-driven way.
		finalNullable = false
	}
	newSchema := df.SeriesSchema{Name: as.schema.Name, Format: outputFormat, Nullable: finalNullable, Metadata: as.schema.Metadata}
	return NewArrowSeriesWithAllocator(newArr, newSchema, as.mem)
}

func (as *arrowSeries) FlatMap(outputFormat df.Format, f func(df.Value) []df.Value) df.Series {
	if as.arr == nil { panic("flatMap on nil series") };
	outputArrowType, err := dfFormatToArrowType(outputFormat)
	if err != nil {
		panic(fmt.Errorf("FlatMap: could not get arrow type for output format %v: %w", outputFormat, err))
	}

	b := array.NewBuilder(as.mem, outputArrowType); defer b.Release()
	for i := 0; i < as.Len(); i++ {
		originalVal := as.Get(i)
		for _, mappedVal := range f(originalVal) { // mappedVal is df.Value
			scalarToAppend, errConv := dfValueToArrowScalar(mappedVal, outputArrowType)
			if errConv != nil {
				panic(fmt.Errorf("FlatMap: failed to convert mapped df.Value (from input %v) to Arrow scalar for type %s: %w", originalVal, outputArrowType.Name(), errConv))
			}
			// No release for scalarToAppend from dfValueToArrowScalar (from types.go)

			errAppend := appendScalarToBuilder(b, scalarToAppend, outputArrowType)
			if errAppend != nil {
				panic(fmt.Errorf("FlatMap: error appending scalar to builder: %w. Scalar: %v", errAppend, scalarToAppend))
			}
		}
	}
	newArr := b.NewArray()
	finalNullable := true // FlatMap can easily produce more or fewer items; nulls can appear.
	if newArr.NullN() == 0 {
		// Similar to Map, if f guarantees non-null, and output type isn't inherently nullable,
		// then Nullable could be false.
		finalNullable = false
	}
	newSchema := df.SeriesSchema{Name: as.schema.Name, Format: outputFormat, Nullable: finalNullable, Metadata: as.schema.Metadata}
	return NewArrowSeriesWithAllocator(newArr, newSchema, as.mem)
}

func (as *arrowSeries) Reduce(f func(df.Value, df.Value) df.Value, startValue df.Value) df.Value {
	if startValue == nil { panic("Reduce startValue cannot be nil interface") };
	acc := startValue
    if as.arr == nil || as.Len() == 0 { return acc }
	for i := 0; i < as.Len(); i++ { acc = f(acc, as.Get(i)) } // MODIFIED: int64 to int
	return acc
}

func (as *arrowSeries) Distinct() df.Series {
	if as.arr == nil || as.arr.Len() == 0 { return as.Copy() }
	ctx := compute.WithAllocator(context.Background(), as.mem) // Or compute.DefaultContext()
	arrDatum := arrow.NewArrayDatum(as.arr); defer arrDatum.Release()
	uniqueDatum, err := compute.Unique(ctx, arrDatum)
	if err != nil { panic(fmt.Sprintf("Unique failed: %v", err)) };
	defer uniqueDatum.Release()

	uniqueArr, ok := uniqueDatum.(*arrow.ArrayDatum).Value().(arrow.Array)
	if !ok { panic("Unique did not return a valid ArrayDatum containing an Array") }
    return NewArrowSeriesWithAllocator(uniqueArr, as.schema, as.mem)
}

func (as *arrowSeries) Copy() df.Series {
    if as.arr == nil {
        if as.schema.Format != nil && as.mem != nil {
            dt, err := dfFormatToArrowType(as.schema.Format)
            if err != nil {
                panic(fmt.Errorf("Copy: cannot determine arrow type for nil series' schema format %v: %w", as.schema.Format, err))
            }
            bld := array.NewBuilder(as.mem, dt); defer bld.Release()
            emptyArr := bld.NewArray() // Retained
            return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
        }
        panic("cannot copy nil series with no type/allocator info")
    }
	// NewSlice creates a new array struct that shares data buffers but has its own ref count.
	// This is a shallow copy of data, but a new array instance.
	newSlice := array.NewSlice(as.arr, 0, int64(as.arr.Len()))
    return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}

func (as *arrowSeries) Release() { if as.arr != nil { as.arr.Release(); as.arr = nil } }

// prepareOtherForSetOp (from my template) is better than direct type assertion and panic.
// I'll replace the direct assertions in Append, Intersection, etc., with this helper or similar logic.
func (as *arrowSeries) prepareOtherForSetOp(otherRaw df.Series, operationName string) (arrow.Array, error) {
	if otherRaw == nil {
		return nil, fmt.Errorf("%s: other series cannot be nil", operationName)
	}
	other, ok := otherRaw.(*arrowSeries)
	if !ok {
		return nil, fmt.Errorf("%s: expected *arrowSeries, got %T. Cross-implementation operations not yet supported.", operationName, otherRaw)
	}
	if other.arr == nil {
		return nil, fmt.Errorf("%s: other series has nil internal array", operationName)
	}

	if arrow.TypeEqual(as.arr.DataType(), other.arr.DataType()) {
		other.arr.Retain() // Caller must release
		return other.arr, nil
	}

	// Try to cast 'other' to 'as' type if df.Formats are the same (suggesting conceptual compatibility)
	if as.schema.Format == other.schema.Format {
		ctx := compute.DefaultContext() // Or compute.WithAllocator(context.Background(), as.mem)
		castedOtherArray, castErr := compute.Cast(ctx, other.arr, as.arr.DataType(), compute.DefaultCastOptions(false))
		if castErr != nil {
			return nil, fmt.Errorf("%s: type mismatch (self=%s, other=%s) and cast failed: %w",
				operationName, as.arr.DataType().Name(), other.arr.DataType().Name(), castErr)
		}
		// castedOtherArray is new and needs to be managed by caller (usually released after use)
		return castedOtherArray, nil
	}

	return nil, fmt.Errorf("%s: type mismatch (self=%s, other=%s) and df.Format mismatch (self=%v, other=%v)",
		operationName, as.arr.DataType().Name(), other.arr.DataType().Name(), as.schema.Format, other.schema.Format)
}


func (as *arrowSeries) Append(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil { // If current series is nil (e.g. uninitialized)
		if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 { // Appending nothing to nil series
			// Return a valid empty series or panic, current Copy handles creating empty from schema
			return as.Copy()
		}
		// If as.arr is nil but other is not, effectively this becomes otherSeriesRaw.Copy()
		// This case should ideally be handled by ensuring as.arr is initialized (e.g. to empty array)
		// For now, let's assume if as.arr is nil, it's an empty series of its schema type.
		// This means it should behave like Len() == 0.
	}

	if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 {
		return as.Copy() // Appending empty series is a no-op
	}
	if as.Len() == 0 { // If current series is empty but other is not
		// Check type compatibility before just copying otherSeriesRaw
		// to ensure the result is of type as.schema.Format
		_, ok := otherSeriesRaw.(*arrowSeries)
		if !ok { panic(fmt.Sprintf("Append: expected *arrowSeries, got %T", otherSeriesRaw))}

		// If otherSeriesRaw's format matches as.schema.Format, then copy is fine.
		// Otherwise, it might need conversion.
		if otherSeriesRaw.Schema().Format != as.schema.Format {
			// This implies a conversion is needed for the "empty" part of 'as'
			// which is complex. Simplest is to require format match or panic.
			// Or, treat as otherSeriesRaw.AsFormat(as.schema.Format).
			panic(fmt.Sprintf("Append: format mismatch when appending to empty series. Self: %v, Other: %v", as.schema.Format, otherSeriesRaw.Schema().Format))
		}
		return otherSeriesRaw.Copy()
	}

	otherArr, err := as.prepareOtherForSetOp(otherSeriesRaw, "Append")
	if err != nil { panic(err) }
	defer otherArr.Release()

	concatenatedArr, err := array.Concatenate([]arrow.Array{as.arr, otherArr}, as.mem)
	if err != nil { panic(fmt.Sprintf("Append: failed to concatenate arrays: %v", err)) }
	// concatenatedArr is retained by Concatenate. NewArrowSeriesWithAllocator will retain again.
	return NewArrowSeriesWithAllocator(concatenatedArr, as.schema, as.mem)
}

func (as *arrowSeries) Union(otherSeries df.Series) df.Series {
	if as.arr == nil { panic("Union on nil series") }
	// Appending will handle type checks and potential casting if formats match.
	appended := as.Append(otherSeries) // This returns a new series
	// Make sure to release the intermediate 'appended' series' array
	defer appended.Release()

	// Distinct will operate on the result of Append.
	return appended.Distinct()
}

func (as *arrowSeries) Intersection(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil { panic("Intersection on nil series") }
	if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 || as.Len() == 0 {
		dt, err := dfFormatToArrowType(as.schema.Format)
		if err != nil { panic(fmt.Errorf("Intersection: cannot get arrow type for empty result: %w", err)) }
		bld := array.NewBuilder(as.mem, dt); defer bld.Release()
        emptyArr := bld.NewArray(); // Retained
        return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
	}

	otherArr, err := as.prepareOtherForSetOp(otherSeriesRaw, "Intersection")
	if err != nil { panic(fmt.Errorf("Intersection prepare error: %w", err)) }
	defer otherArr.Release() // otherArr was retained by prepareOtherForSetOp or is new (casted)

	ctx := compute.WithAllocator(context.Background(), as.mem)
	// NewArrayData does not retain input array, so no release needed for leftDatum/rightDatum wrappers
	leftDatum := arrow.NewArrayDatum(as.arr)
	rightDatum := arrow.NewArrayDatum(otherArr)

	resultSetDatum, err := compute.SetIntersection(ctx, leftDatum, rightDatum, compute.SetLookupOptions{NullMatchingBehavior: compute.MatchNulls})
	if err != nil { panic(fmt.Sprintf("Intersection: compute.SetIntersection failed: %w", err)) };
	defer resultSetDatum.Release() // This is a new datum, release it

	resultArr, ok := resultSetDatum.Value().(arrow.Array)
	if !ok { panic("Intersection: compute.SetIntersection did not return ArrayDatum containing an Array") }
	// resultArr is owned by resultSetDatum. NewArrowSeries retains it.
    return NewArrowSeriesWithAllocator(resultArr, as.schema, as.mem)
}

func (as *arrowSeries) Except(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil { panic("Except on nil series") }
	if as.Len() == 0 {
		dt, err := dfFormatToArrowType(as.schema.Format)
		if err != nil { panic(fmt.Errorf("Except: cannot get arrow type for empty result: %w", err)) }
		bld := array.NewBuilder(as.mem, dt); defer bld.Release()
        emptyArr := bld.NewArray(); // Retained
        return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
	}
	if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 { return as.Copy() }

	otherArr, err := as.prepareOtherForSetOp(otherSeriesRaw, "Except")
	if err != nil { panic(fmt.Errorf("Except prepare error: %w", err)) }
	defer otherArr.Release() // otherArr was retained by prepareOtherForSetOp or is new (casted)

	ctx := compute.WithAllocator(context.Background(), as.mem)
	leftDatum := arrow.NewArrayDatum(as.arr)
	rightDatum := arrow.NewArrayDatum(otherArr)

	resultSetDatum, err := compute.SetDifference(ctx, leftDatum, rightDatum, compute.SetLookupOptions{NullMatchingBehavior: compute.MatchNulls})
	if err != nil { panic(fmt.Errorf("Except: compute.SetDifference failed: %w", err)) };
	defer resultSetDatum.Release() // This is a new datum, release it

	resultArr, ok := resultSetDatum.Value().(arrow.Array)
	if !ok { panic("Except: compute.SetDifference did not return ArrayDatum containing an Array") }
	// resultArr is owned by resultSetDatum. NewArrowSeries retains it.
    return NewArrowSeriesWithAllocator(resultArr, as.schema, as.mem)
}

func (as *arrowSeries) AsFormat(targetFormat df.Format) df.Series {
	if as.arr == nil { panic("AsFormat called on nil series array") }
	if targetFormat == nil { panic("AsFormat: targetFormat cannot be nil") } // Assuming df.Format is non-nil interface

	if as.schema.Format == targetFormat { // TODO: Ensure df.Format has proper Equals method if it's not a basic comparable type.
		return as.Copy()
	}

	targetArrowType, err := dfFormatToArrowType(targetFormat)
	if err != nil {
		panic(fmt.Errorf("AsFormat: cannot map target df.Format %v to Arrow type: %w", targetFormat, err))
	}

	if arrow.TypeEqual(as.arr.DataType(), targetArrowType) {
		// Data type is already correct, just update schema.Format
		// NewArrowSeriesWithAllocator will retain as.arr
		newSchema := df.SeriesSchema{Name: as.schema.Name, Format: targetFormat, Nullable: as.arr.NullN() > 0}
		return NewArrowSeriesWithAllocator(as.arr, newSchema, as.mem)
	}

	ctx := compute.WithAllocator(context.Background(), as.mem)
	castOptions := compute.DefaultCastOptions(false) // false = allow unsafe casts like float to int truncation.
	                                               // Set to true for strict (error on overflow/truncation).
	castedArray, err := compute.Cast(ctx, as.arr, targetArrowType, castOptions)
	if err != nil {
		panic(fmt.Errorf("AsFormat: failed to cast array from %s (df.Format %v) to %s (df.Format %v): %w",
			as.arr.DataType().Name(), as.schema.Format, targetArrowType.Name(), targetFormat, err))
	}
	// castedArray is new and retained by compute.Cast. NewArrowSeriesWithAllocator will retain again.

	newSchema := df.SeriesSchema{Name: as.schema.Name, Format: targetFormat, Nullable: castedArray.NullN() > 0, Metadata: as.schema.Metadata}
	return NewArrowSeriesWithAllocator(castedArray, newSchema, as.mem)
}


func (as *arrowSeries) WhenNil(fillValue df.Value) df.Series {
	if as.arr == nil { panic("WhenNil called on nil series array") }
	if fillValue == nil { panic("WhenNil: fillValue interface cannot be nil (can be a nil df.Value though)")} // df.Value can be nil if interface itself is nil

	if as.arr.NullN() == 0 { return as.Copy() } // No nulls to fill

	ctx := compute.WithAllocator(context.Background(), as.mem)
	targetArrowType := as.arr.DataType()

	fillScalar, err := dfValueToArrowScalar(fillValue, targetArrowType)
	if err != nil {
		panic(fmt.Errorf("WhenNil: error converting fillValue (value: %v, format: %v) to Arrow scalar for type %s: %w",
			fillValue.Get(), fillValue.Schema().Format, targetArrowType.Name(), err))
	}

	// If fillValue itself was a nil df.Value, then fillScalar will be a !IsValid scalar (a typed Null).
	// compute.FillNull correctly handles filling with a typed Null scalar, effectively making it a no-op
	// for those nulls being filled with another null.
	// The original check `if fillValue.IsNil() && targetArrowType.ID() != arrow.NULL` and then
	// `if !fillScalar.IsValid() { return as.Copy() }` was trying to optimize for this.
	// compute.FillNull is idempotent if fillScalar is a typed null of the array's type.
	// So, no need for an early exit if fillValue.IsNil().

	fillScalarDatum := arrow.NewScalarDatum(fillScalar)
	seriesDatum := arrow.NewArrayDatum(as.arr)

	resultDatum, err := compute.FillNull(ctx, seriesDatum, fillScalarDatum)
	if err != nil { panic(fmt.Errorf("WhenNil: FillNull compute failed: %w", err)) }
	defer resultDatum.Release()

	// The resultDatum contains the new array.
	newArr := resultDatum.Value().(arrow.Array)

	currentSchema := as.schema
	finalNullable := true
	if !fillValue.IsNil() && newArr.NullN() == 0 { // If filled with non-null and result has no nulls
		finalNullable = false
	} else if fillValue.IsNil() && as.arr.NullN() > 0 { // If filled with null, nullability depends on original
		finalNullable = true // Still nullable
	} else if newArr.NullN() > 0 { // If there are still nulls for any other reason
		finalNullable = true
	} else { // No nulls in result, and fillValue was not nil (already covered), or original had no nulls
		finalNullable = false
	}

	newSchema := df.SeriesSchema{
		Name:     currentSchema.Name,
		Format:   currentSchema.Format,
		Nullable: finalNullable,
		Metadata: currentSchema.Metadata,
	}
	return NewArrowSeriesWithAllocator(newArr, newSchema, as.mem)
}

func (as *arrowSeries) When(replacementMap map[any]df.Value) df.Series {
	if as.arr == nil { panic("When called on nil series array") }
	if len(replacementMap) == 0 { return as.Copy() }

	colType := as.arr.DataType()
	b := array.NewBuilder(as.mem, colType); defer b.Release() // MODIFIED: builder.NewBuilder to array.NewBuilder

	for r := 0; r < as.Len(); r++ {
		currentDfVal := as.Get(r) // This is *arrowValue from our Get method

		// Determine the key for map lookup. If df.Value is nil, use nil as key.
		// Otherwise, use its Go representation.
		var goKeyForLookup any
		if currentDfVal.IsNil() {
			goKeyForLookup = nil // Standard way to represent nil in a map key if desired
		} else {
			// arrowValue.Get() returns any. This should be fine for map keys if types are simple.
			goKeyForLookup = currentDfVal.Get()
		}

		replacementDfVal, shouldReplace := replacementMap[goKeyForLookup]

		var scalarToAppend scalar.Scalar
		var errConv error

		if shouldReplace {
			scalarToAppend, errConv = dfValueToArrowScalar(replacementDfVal, colType)
			if errConv != nil {
				panic(fmt.Errorf("When: error converting replacement df.Value (for key %v, value %v) to Arrow scalar type %s: %w",
					goKeyForLookup, replacementDfVal, colType.Name(), errConv))
			}
		} else {
			// No replacement, use original scalar.
			// currentDfVal must be *arrowValue to get .s
			arrowVal, ok := currentDfVal.(*arrowValue)
			if !ok && !currentDfVal.IsNil() { // If not nil, it must be arrowValue
                panic(fmt.Errorf("When: original df.Value is not *arrowValue and not nil, type: %T", currentDfVal))
            }
            if currentDfVal.IsNil() { // If original was nil, create a nil scalar of target type
                 scalarToAppend = scalar.NewNullScalar(colType)
            } else {
                 scalarToAppend = arrowVal.s
            }
		}
		// dfValueToArrowScalar from types.go does not retain, so no release for scalarToAppend here.

		errAppend := appendScalarToBuilder(b, scalarToAppend, colType)
		if errAppend != nil {
			panic(fmt.Errorf("When: error appending scalar (for key %v, scalar %v) to builder: %w",
				goKeyForLookup, scalarToAppend, errAppend))
		}
	}
	newArr := b.NewArray()

	currentSchema := as.schema
	finalNullable := true // Default to true because replacements can introduce nulls.
	if newArr.NullN() == 0 {
		// If no nulls in the new array, it *could* be non-nullable.
		// This is true if the original array had no nulls that were *not* replaced,
		// AND all replacement values used were non-nil.
		allReplacementsWereNonNull := true
		for _, valToReplaceWith := range replacementMap {
			if valToReplaceWith.IsNil() {
				allReplacementsWereNonNull = false
				break
			}
		}
		if allReplacementsWereNonNull { // If all potential replacements are non-null
			// We still need to consider if an original non-null value that wasn't in the map
			// could have been preserved. This is guaranteed.
			// So, if newArr.NullN() == 0 AND all replacementMap values are non-Null,
			// it implies the resulting series is non-nullable.
			finalNullable = false
		}
	}

	newSchema := df.SeriesSchema{
		Name:     currentSchema.Name,
		Format:   currentSchema.Format, // Type is preserved
		Nullable: finalNullable,
		Metadata: currentSchema.Metadata,
	}
	return NewArrowSeriesWithAllocator(newArr, newSchema, as.mem)
}

// --- Stubs for remaining methods ---
func (as *arrowSeries) Expr() df.Expr { panic("Expr not implemented for arrowSeries") }
func (as *arrowSeries) Select(e df.Expr) df.Series { panic("Select not implemented for arrowSeries") }

// Group and Join are more complex and often belong to DataFrame or a specific GroupedSeries type.
// func (as *arrowSeries) Group() df.GroupedSeries { panic("not implemented") }
// func (as *arrowSeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) df.Series { panic("not implemented") }

var _ df.Series = (*arrowSeries)(nil)

[end of df/arrow/series.go]
