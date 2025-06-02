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

type arrowSeries struct {
	schema df.SeriesSchema
	arr    arrow.Array
	mem    memory.Allocator
}

func dfFormatToArrowType(f df.Format) arrow.DataType {
	switch f.Name() {
	case df.StringFormat.Name(), "string": return arrow.BinaryTypes.String
	case df.IntegerFormat.Name(), "integer", "int64": return arrow.PrimitiveTypes.Int64
	case df.DoubleFormat.Name(), "double", "float64": return arrow.PrimitiveTypes.Float64
	case df.BoolFormat.Name(), "boolean", "bool": return arrow.PrimitiveTypes.Boolean
	case df.DateTimeFormat.Name(), "datetime": return arrow.TimestampTypes.Timestamp_ns
	default: panic(fmt.Sprintf("unsupported df.Format ('%s', type: %v) to Arrow DataType conversion", f.Name(), f.Type()))
	}
}

func appendScalarToBuilder(b array.Builder, s scalar.Scalar) error {
	if s == nil || !s.IsValid() { b.AppendNull(); return nil }
	switch typedBuilder := b.(type) {
	case *builder.Int64Builder: if v, ok := s.(*scalar.Int64); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Int64 for Int64Builder, got %T (value: %v)", s, s)}
	case *builder.Float64Builder: if v, ok := s.(*scalar.Float64); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Float64 for Float64Builder, got %T (value: %v)", s, s)}
	case *builder.StringBuilder: if v, ok := s.(scalar.StringScalar); ok { typedBuilder.Append(v.String()) } else { return fmt.Errorf("type mismatch: expected StringScalar for StringBuilder, got %T (value: %v)", s, s)}
	case *builder.BooleanBuilder: if v, ok := s.(*scalar.Boolean); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Boolean for BooleanBuilder, got %T (value: %v)", s, s)}
	case *builder.TimestampBuilder: if v, ok := s.(*scalar.Timestamp); ok { typedBuilder.Append(v.Value) } else { return fmt.Errorf("type mismatch: expected Timestamp for TimestampBuilder, got %T (value: %v)", s, s)}
	default: return fmt.Errorf("unsupported builder type in appendScalarToBuilder: %T for scalar %T (value: %v)", b, s,s)
	}
	return nil
}

func NewArrowSeries(arr arrow.Array, schema df.SeriesSchema) df.Series { return NewArrowSeriesWithAllocator(arr, schema, memory.DefaultAllocator) }
func NewArrowSeriesWithAllocator(arr arrow.Array, schema df.SeriesSchema, mem memory.Allocator) df.Series {
	if arr == nil { panic("arrow.Array cannot be nil") }; if mem == nil { panic("memory.Allocator cannot be nil") }
	arr.Retain(); return &arrowSeries{schema: schema, arr: arr, mem: mem}
}

func (as *arrowSeries) Schema() df.SeriesSchema { return as.schema }
func (as *arrowSeries) Len() int64 { if as.arr == nil { return 0 }; return int64(as.arr.Len()) }

func (as *arrowSeries) Get(index int64) df.Value {
	if as.arr == nil || index < 0 || index >= int64(as.arr.Len()) { panic(fmt.Sprintf("index %d out of bounds", index))}
	return NewArrowValue(scalar.MakeScalar(as.arr, int(index)), as.schema.Format)
}

func (as *arrowSeries) ForEach(f func(df.Value)) { if as.arr == nil { return }; for i := int64(0); i < as.Len(); i++ { f(as.Get(i)) } }

func (as *arrowSeries) Limit(offset int, size int) df.Series {
	if as.arr == nil { panic("cannot limit a nil series") }; currentLen := as.arr.Len(); if offset < 0 { offset = 0 }
	if offset >= currentLen {
		b := builder.NewBuilder(as.mem, as.arr.DataType()); defer b.Release()
		newArr := b.NewArray()
		return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
	}
	if offset+size > currentLen { size = currentLen - offset }; if size < 0 { size = 0 }
	newSlice := array.NewSlice(as.arr, int64(offset), int64(offset+size))
	return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}

func (as *arrowSeries) Where(f func(df.Value) bool) df.Series {
    if as.arr == nil { panic("cannot filter a nil series") }; b := builder.NewBuilder(as.mem, as.arr.DataType()); defer b.Release()
    for i := int64(0); i < as.Len(); i++ {
        val := as.Get(i)
        if f(val) {
            arrowVal, ok := val.(*arrowValue)
            if !ok && !val.IsNil() { panic(fmt.Sprintf("Where: unexpected type %T", val)) }
            if val.IsNil() || (ok && (arrowVal.val == nil || !arrowVal.val.IsValid())) { b.AppendNull()
            } else { if err := appendScalarToBuilder(b, arrowVal.val); err != nil { panic(fmt.Sprintf("Where: append error: %v. Scalar type: %s, Builder type: %s", err, arrowVal.val.DataType().Name(), b.Type().Name()))}}}
    }
    newArr := b.NewArray()
    return NewArrowSeriesWithAllocator(newArr, as.schema, as.mem)
}

func (as *arrowSeries) Sort(order df.SortOrder) df.Series {
    if as.arr == nil || as.arr.Len() == 0 { return as.Copy() }; ctx := compute.WithAllocator(context.Background(), as.mem)
    arrowSortOrder := arrow.Ascending; if order == df.SortOrderDESC { arrowSortOrder = arrow.Descending }
    arrDatum := arrow.NewArrayDatum(as.arr); defer arrDatum.Release()
    indicesDatum, err := compute.SortIndices(ctx, arrDatum, compute.SortOptions{Order: arrowSortOrder, NullPlacement: arrow.NullsFirst})
    if err != nil { panic(fmt.Sprintf("SortIndices failed: %v", err)) }; defer indicesDatum.Release()
    indicesArr, ok := indicesDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("SortIndices bad return") }
    sortedArrDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrDatum, arrow.NewArrayDatum(indicesArr))
    if err != nil { panic(fmt.Sprintf("Take failed: %v", err)) }; defer sortedArrDatum.Release()
    sortedArr, ok := sortedArrDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("Take bad return") }
    return NewArrowSeriesWithAllocator(sortedArr, as.schema, as.mem)
}

func (as *arrowSeries) Map(outputSchema df.Format, f func(df.Value) df.Value) df.Series {
	if as.arr == nil { panic("map on nil series") }; outputArrowType := dfFormatToArrowType(outputSchema)
	b := builder.NewBuilder(as.mem, outputArrowType); defer b.Release()
	for i := int64(0); i < as.Len(); i++ {
		originalVal := as.Get(i); mappedVal := f(originalVal)
		if mappedVal == nil || mappedVal.IsNil() { b.AppendNull(); continue }
		av, ok := mappedVal.(*arrowValue); if !ok { panic(fmt.Sprintf("Map function returned a df.Value of type %T, expected *arrowValue. Consider wrapping result in NewArrowValue.", mappedVal)) }
		if av.val == nil || !av.val.IsValid() { b.AppendNull(); continue }

		if !arrow.TypeEqual(av.val.DataType(), outputArrowType) {
			castedScalar, err := scalar.Cast(compute.DefaultCastOptions(false), av.val, outputArrowType)
			if err != nil { panic(fmt.Sprintf("Map: cast scalar from %s to %s failed: %v", av.val.DataType().Name(), outputArrowType.Name(), err)) }
			err = appendScalarToBuilder(b, castedScalar)
			castedScalar.Release() // Release explicitly after use
			if err != nil { panic(fmt.Sprintf("Map append casted scalar error: %v", err)) }
		} else {
			if err := appendScalarToBuilder(b, av.val); err != nil { panic(fmt.Sprintf("Map append error: %v", err)) }
		}
	}
	newArr := b.NewArray()
	return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: as.schema.Name, Format: outputSchema}, as.mem)
}

func (as *arrowSeries) FlatMap(outputSchema df.Format, f func(df.Value) []df.Value) df.Series {
	if as.arr == nil { panic("flatMap on nil series") }; outputArrowType := dfFormatToArrowType(outputSchema)
	b := builder.NewBuilder(as.mem, outputArrowType); defer b.Release()
	for i := int64(0); i < as.Len(); i++ {
		for _, mappedVal := range f(as.Get(i)) {
			if mappedVal == nil || mappedVal.IsNil() { b.AppendNull(); continue }
			av, ok := mappedVal.(*arrowValue); if !ok { panic(fmt.Sprintf("FlatMap function returned a df.Value of type %T, expected *arrowValue. Consider wrapping result in NewArrowValue.", mappedVal)) }
			if av.val == nil || !av.val.IsValid() { b.AppendNull(); continue }

			if !arrow.TypeEqual(av.val.DataType(), outputArrowType) {
				castedScalar, err := scalar.Cast(compute.DefaultCastOptions(false), av.val, outputArrowType)
				if err != nil {panic(fmt.Sprintf("FlatMap: cast scalar from %s to %s failed: %v", av.val.DataType().Name(), outputArrowType.Name(), err))}
				err = appendScalarToBuilder(b, castedScalar)
				castedScalar.Release() // Release explicitly after use
				if err != nil {panic(fmt.Sprintf("FlatMap append casted scalar error: %v", err))}
			} else {
				if err := appendScalarToBuilder(b, av.val); err != nil { panic(fmt.Sprintf("FlatMap append error: %v", err)) }
			}
		}
	}
	newArr := b.NewArray()
	return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: as.schema.Name, Format: outputSchema}, as.mem)
}

func (as *arrowSeries) Reduce(f func(df.Value, df.Value) df.Value, startValue df.Value) df.Value {
	if startValue == nil { panic("Reduce startValue cannot be nil") }; acc := startValue
    if as.arr == nil || as.Len() == 0 { return acc }
	for i := int64(0); i < as.Len(); i++ { acc = f(acc, as.Get(i)) }
	return acc
}

func (as *arrowSeries) Distinct() df.Series {
	if as.arr == nil || as.arr.Len() == 0 { return as.Copy() }
	ctx := compute.WithAllocator(context.Background(), as.mem)
	arrDatum := arrow.NewArrayDatum(as.arr); defer arrDatum.Release()
	uniqueDatum, err := compute.Unique(ctx, arrDatum)
	if err != nil { panic(fmt.Sprintf("Unique failed: %v", err)) }; defer uniqueDatum.Release()
	uniqueArr, ok := uniqueDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("Unique bad return") }
    return NewArrowSeriesWithAllocator(uniqueArr, as.schema, as.mem)
}

func (as *arrowSeries) Copy() df.Series {
    if as.arr == nil {
         if as.schema.Format != nil && as.mem != nil { dt := dfFormatToArrowType(as.schema.Format); bld := builder.NewBuilder(as.mem, dt); defer bld.Release(); emptyArr := bld.NewArray(); return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem) }
         panic("cannot copy nil series with no type/allocator info")
    }
	newSlice := array.NewSlice(as.arr, 0, as.arr.Len())
    return NewArrowSeriesWithAllocator(newSlice, as.schema, as.mem)
}

func (as *arrowSeries) Release() { if as.arr != nil { as.arr.Release(); as.arr = nil } }

func (as *arrowSeries) Append(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil { if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 { return as.Copy() }}
	if otherSeriesRaw == nil { return as.Copy() }
	otherSeries, ok := otherSeriesRaw.(*arrowSeries); if !ok { panic(fmt.Sprintf("Append: expected *arrowSeries, got %T", otherSeriesRaw)) }
	if (as.arr == nil || as.Len() == 0) && (otherSeries.arr == nil || otherSeries.Len() == 0) { return as.Copy() }
	if otherSeries.arr == nil || otherSeries.Len() == 0 { return as.Copy() }
	if as.arr == nil || as.Len() == 0 { return otherSeries.Copy() }
	if !arrow.TypeEqual(as.arr.DataType(), otherSeries.arr.DataType()) { panic(fmt.Sprintf("Append: type mismatch, current type %s, other type %s", as.arr.DataType(), otherSeries.arr.DataType())) }
	if !as.schema.Format.Equals(otherSeries.schema.Format) { panic(fmt.Sprintf("Append: df.Format mismatch, current '%s', other '%s'", as.schema.Format.Name(), otherSeries.schema.Format.Name())) }
	concatenatedArr, err := array.Concatenate([]arrow.Array{as.arr, otherSeries.arr}, as.mem)
	if err != nil { panic(fmt.Sprintf("Append: failed to concatenate arrays: %v", err)) }
	return NewArrowSeriesWithAllocator(concatenatedArr, as.schema, as.mem)
}

func (as *arrowSeries) Union(otherSeries df.Series) df.Series {
	appended := as.Append(otherSeries)
	// The appended series is temporary, so its resources should be managed.
	// Distinct creates a new series.
	distinctSeries := appended.Distinct()
	if appSer, ok := appended.(*arrowSeries); ok {
		appSer.Release()
	}
	return distinctSeries
}

func (as *arrowSeries) Intersection(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil || otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 || as.Len() == 0 {
		dt := dfFormatToArrowType(as.schema.Format)
        bld := builder.NewBuilder(as.mem, dt); defer bld.Release()
        emptyArr := bld.NewArray(); return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
	}
	otherSeries, ok := otherSeriesRaw.(*arrowSeries); if !ok { panic(fmt.Sprintf("Intersection: expected *arrowSeries, got %T", otherSeriesRaw)) }
	if !arrow.TypeEqual(as.arr.DataType(), otherSeries.arr.DataType()) { panic(fmt.Sprintf("Intersection: type mismatch, current type %s, other type %s", as.arr.DataType(), otherSeries.arr.DataType())) }
	ctx := compute.WithAllocator(context.Background(), as.mem)
	leftDatum := arrow.NewArrayDatum(as.arr); defer leftDatum.Release()
	rightDatum := arrow.NewArrayDatum(otherSeries.arr); defer rightDatum.Release()
	resultSetDatum, err := compute.SetIntersection(ctx, leftDatum, rightDatum, compute.SetLookupOptions{NullMatchingBehavior: compute.MatchNulls})
	if err != nil { panic(fmt.Sprintf("Intersection: compute.SetIntersection failed: %v", err)) }; defer resultSetDatum.Release()
	resultArr, ok := resultSetDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("Intersection: compute.SetIntersection did not return ArrayDatum") }
    return NewArrowSeriesWithAllocator(resultArr, as.schema, as.mem)
}

func (as *arrowSeries) Except(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil || as.Len() == 0 {
		dt := dfFormatToArrowType(as.schema.Format)
        bld := builder.NewBuilder(as.mem, dt); defer bld.Release()
        emptyArr := bld.NewArray(); return NewArrowSeriesWithAllocator(emptyArr, as.schema, as.mem)
	}
	if otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 { return as.Copy() }
	otherSeries, ok := otherSeriesRaw.(*arrowSeries); if !ok { panic(fmt.Sprintf("Except: expected *arrowSeries, got %T", otherSeriesRaw)) }
	if !arrow.TypeEqual(as.arr.DataType(), otherSeries.arr.DataType()) { panic(fmt.Sprintf("Except: type mismatch, current type %s, other type %s", as.arr.DataType(), otherSeries.arr.DataType())) }
	ctx := compute.WithAllocator(context.Background(), as.mem)
	leftDatum := arrow.NewArrayDatum(as.arr); defer leftDatum.Release()
	rightDatum := arrow.NewArrayDatum(otherSeries.arr); defer rightDatum.Release()
	resultSetDatum, err := compute.SetDifference(ctx, leftDatum, rightDatum, compute.SetLookupOptions{NullMatchingBehavior: compute.MatchNulls})
	if err != nil { panic(fmt.Sprintf("Except: compute.SetDifference failed: %v", err)) }; defer resultSetDatum.Release()
	resultArr, ok := resultSetDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("Except: compute.SetDifference did not return ArrayDatum") }
    return NewArrowSeriesWithAllocator(resultArr, as.schema, as.mem)
}

func (as *arrowSeries) Join(outputFormat df.Format, otherSeriesRaw df.Series, jointype df.JoinType, f func(v1 df.Value, v2 df.Value) []df.Value) df.Series {
	if outputFormat == nil { panic("Join: outputFormat cannot be nil") }
	if f == nil { panic("Join: function f cannot be nil") }

	var otherSeries *arrowSeries
	var ok bool
	isOtherSeriesValid := false
	if otherSeriesRaw != nil {
		otherSeries, ok = otherSeriesRaw.(*arrowSeries)
		if !ok { panic(fmt.Sprintf("Join: expected otherSeries to be *arrowSeries, got %T", otherSeriesRaw)) }
		if otherSeries.arr != nil { isOtherSeriesValid = true }
	}

	outputArrowType := dfFormatToArrowType(outputFormat)
	b := builder.NewBuilder(as.mem, outputArrowType); defer b.Release()

	createNilDfValue := func(seriesFormat df.Format) df.Value {
		if seriesFormat == nil { panic("cannot create nil df.Value from nil series format for join padding") }
		return NewArrowValue(scalar.NewNullScalar(dfFormatToArrowType(seriesFormat)), seriesFormat)
	}

	var nilVal1, nilVal2 df.Value // Typed nil placeholders
	if as.arr != nil { nilVal1 = createNilDfValue(as.schema.Format) }
	if isOtherSeriesValid { nilVal2 = createNilDfValue(otherSeries.schema.Format) }

	processOutput := func(outputVals []df.Value) {
		for _, outVal := range outputVals {
			if outVal == nil || outVal.IsNil() { b.AppendNull(); continue }
			av, castOk := outVal.(*arrowValue)
			if !castOk { panic(fmt.Sprintf("Join: func f returned non-*arrowValue: %T", outVal)) }

			if !arrow.TypeEqual(av.val.DataType(), outputArrowType) {
				castedScalar, err := scalar.Cast(compute.DefaultCastOptions(false), av.val, outputArrowType)
				if err != nil { panic(fmt.Sprintf("Join: cast scalar from %s to %s failed: %v", av.val.DataType(), outputArrowType, err)) }
				err = appendScalarToBuilder(b, castedScalar)
				castedScalar.Release() // Release explicitly after use
				if err != nil { panic(fmt.Sprintf("Join: append casted scalar error: %v", err)) }
			} else {
				if err := appendScalarToBuilder(b, av.val); err != nil { panic(fmt.Sprintf("Join: append scalar error: %v", err)) }
			}
		}
	}

	len1 := as.Len(); var len2 int64; if isOtherSeriesValid { len2 = otherSeries.Len() }

	switch jointype {
	case df.JoinEqui:
		limit := len1; if len2 < limit { limit = len2 }
		for i := int64(0); i < limit; i++ { processOutput(f(as.Get(i), otherSeries.Get(i))) }
	case df.JoinLeft:
		if as.arr == nil { break }
		for i := int64(0); i < len1; i++ {
			v1 := as.Get(i)
			var v2 df.Value = nilVal2
			if isOtherSeriesValid && i < len2 { v2 = otherSeries.Get(i) } else if !isOtherSeriesValid { v2 = nil /* raw nil if otherSeries was completely nil */ }
			processOutput(f(v1, v2))
		}
	case df.JoinRight:
		if !isOtherSeriesValid { break }
		for i := int64(0); i < len2; i++ {
			v2 := otherSeries.Get(i)
			var v1 df.Value = nilVal1
			if as.arr != nil && i < len1 { v1 = as.Get(i) } else if as.arr == nil { v1 = nil /* raw nil if as.arr was completely nil */ }
			processOutput(f(v1, v2))
		}
	case df.JoinOuter:
		maxLen := len1; if len2 > maxLen { maxLen = len2 }
		if as.arr == nil && !isOtherSeriesValid { break } // Both effectively nil/empty
		for i := int64(0); i < maxLen; i++ {
			var v1, v2 df.Value
			if as.arr != nil && i < len1 { v1 = as.Get(i) } else if as.arr != nil { v1 = nilVal1 } else { v1 = nil }
			if isOtherSeriesValid && i < len2 { v2 = otherSeries.Get(i) } else if isOtherSeriesValid { v2 = nilVal2 } else { v2 = nil }
			processOutput(f(v1, v2))
		}
	case df.JoinCross:
		if as.arr == nil || !isOtherSeriesValid || len1 == 0 || len2 == 0 { break }
		for i := int64(0); i < len1; i++ {
			for j := int64(0); j < len2; j++ { processOutput(f(as.Get(i), otherSeries.Get(j))) }
		}
	default: panic(fmt.Sprintf("Join: unsupported join type: %s", jointype))
	}
	newArr := b.NewArray()
	return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: as.schema.Name, Format: outputFormat}, as.mem)
}


// Stubs for remaining methods
func (as *arrowSeries) Expr() df.Expr { /* ... */ } // Assumed implemented from previous step
func (as *arrowSeries) Select(e df.Expr) df.Series { /* ... */ } // Assumed implemented
func (as *arrowSeries) Group() df.GroupedSeries { panic("not implemented") }
func (as *arrowSeries) WhenNil(t df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) When(t map[any]df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) AsFormat(t df.Format) df.Series { panic("not implemented") }

var _ df.Series = (*arrowSeries)(nil)
