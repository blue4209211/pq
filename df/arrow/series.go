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
	case df.StringFormat.Name(), "string": return arrow.BinaryTypes.String
	case df.IntegerFormat.Name(), "integer", "int64": return arrow.PrimitiveTypes.Int64
	case df.DoubleFormat.Name(), "double", "float64": return arrow.PrimitiveTypes.Float64
	case df.BoolFormat.Name(), "boolean", "bool": return arrow.PrimitiveTypes.Boolean
	case df.DateTimeFormat.Name(), "datetime": return arrow.TimestampTypes.Timestamp_ns
	default: panic(fmt.Sprintf("unsupported df.Format ('%s', type: %v) to Arrow DataType conversion", f.Name(), f.Type()))
	}
}

// Helper function to append a scalar.Scalar to an array.Builder
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
			defer castedScalar.Release()
			if err := appendScalarToBuilder(b, castedScalar); err != nil { panic(fmt.Sprintf("Map append casted scalar error: %v", err)) }
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
				defer castedScalar.Release()
				if err := appendScalarToBuilder(b, castedScalar); err != nil {panic(fmt.Sprintf("FlatMap append casted scalar error: %v", err))}
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
func (as *arrowSeries) Union(otherSeries df.Series) df.Series { appended := as.Append(otherSeries); return appended.Distinct() }
func (as *arrowSeries) Intersection(otherSeriesRaw df.Series) df.Series {
	if as.arr == nil || otherSeriesRaw == nil || otherSeriesRaw.Len() == 0 || as.Len() == 0 {
		dt := dfFormatToArrowType(as.schema.Format); bld := builder.NewBuilder(as.mem, dt); defer bld.Release()
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
		dt := dfFormatToArrowType(as.schema.Format); bld := builder.NewBuilder(as.mem, dt); defer bld.Release()
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

func (as *arrowSeries) Expr() df.Expr {
	switch as.schema.Format.Name() {
	case df.BoolFormat.Name(): return df.NewBoolExpr()
	case df.IntegerFormat.Name(): return df.NewIntExpr()
	case df.DoubleFormat.Name(): return df.NewDoubleExpr()
	case df.StringFormat.Name(): return df.NewStringExpr()
	case df.DateTimeFormat.Name(): return df.NewDatetimeExpr()
	default: panic(fmt.Sprintf("Expr() not supported for series format: %s", as.schema.Format.Name()))
	}
}

func (as *arrowSeries) Select(e df.Expr) df.Series {
	if e == nil { panic("expression cannot be nil for Select") }

	if e.Const() != nil {
		constVal := e.Const(); outputArrowType := dfFormatToArrowType(constVal.Schema())
		b := builder.NewBuilder(as.mem, outputArrowType); defer b.Release()
		var constScalar scalar.Scalar
		if cv, ok := constVal.(*arrowValue); ok { constScalar = cv.val
		} else {
			switch outputArrowType.ID() {
			case arrow.INT64: constScalar = scalar.NewInt64Scalar(constVal.GetAsInt())
			case arrow.FLOAT64: constScalar = scalar.NewFloat64Scalar(constVal.GetAsDouble())
			case arrow.STRING: constScalar = scalar.NewStringScalar(constVal.GetAsString())
			case arrow.BOOL: constScalar = scalar.NewBooleanScalar(constVal.GetAsBool())
			case arrow.TIMESTAMP: constScalar = scalar.NewTimestampScalar(arrow.Timestamp(constVal.GetAsDatetime().UnixNano()), arrow.TimestampTypes.Timestamp_ns)
			default: panic(fmt.Sprintf("unsupported constant type for series select: %s", constVal.Schema().Name()))
			}
		}
		if constScalar == nil { panic("expression constant df.Value converted to nil scalar.Scalar") }
		for i := int64(0); i < as.Len(); i++ { if err := appendScalarToBuilder(b, constScalar); err != nil { panic(fmt.Sprintf("Select (const): error appending scalar: %v", err))}}
		newArr := b.NewArray()
		return NewArrowSeriesWithAllocator(newArr, df.SeriesSchema{Name: e.Name(), Format: constVal.Schema()}, as.mem)
	}

	if e.Col() == as.schema.Name || (e.Col() == "" && e.OpType() == "" && e.Parent() == nil) { // Simple column selection
		return as.Copy()
	}

	if e.OpType() == df.ExprTypeFilter && e.FilterOp() != nil {
		filterOp := e.FilterOp(); var filterArgs []df.Value
		for _, argExpr := range filterOp.Args() { if argExpr.Const() == nil { panic("filter arguments must be constants") }; filterArgs = append(filterArgs, argExpr.Const())}
		return as.Where(func(v df.Value) bool { return filterOp.ApplyFilter(v, filterArgs...) })
	}

	if e.OpType() == df.ExprTypeMap && e.MapOp() != nil {
		mapOp := e.MapOp(); var mapArgs []df.Value
		for _, argExpr := range mapOp.Args() { if argExpr.Const() == nil { panic("map arguments must be constants") }; mapArgs = append(mapArgs, argExpr.Const())}
		return as.Map(mapOp.ReturnFormat(), func(v df.Value) df.Value { return mapOp.ApplyMap(v, mapArgs...) })
	}

	if e.Parent() != nil {
	    parentSeries := as.Select(e.Parent()); defer parentSeries.(*arrowSeries).Release()
	    // This is still a simplification; a proper engine would transform 'e' to remove the parent part.
	    // For now, we try to re-evaluate the operation part of 'e' on the result of the parent.
	    // This requires 'e' to be re-evaluated without its parent context, which is not directly supported by this basic structure.
	    // The following is a placeholder for this complex logic.
	    // We'd need to construct a new expression that is only the operation part of 'e'
	    // and then call parentSeries.Select(operationOnlyExpr).
	    panic(fmt.Sprintf("recursive expression evaluation in Series.Select via Parent() is not fully supported for OpType: %s", e.OpType()))
	}

	panic(fmt.Sprintf("unsupported expression for Series.Select: Name='%s', OpType='%s', Col='%s'", e.Name(), e.OpType(), e.Col()))
}

// Stubs for remaining methods
func (as *arrowSeries) Group() df.GroupedSeries { panic("not implemented") }
func (as *arrowSeries) WhenNil(t df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) When(t map[any]df.Value) df.Series { panic("not implemented") }
func (as *arrowSeries) AsFormat(t df.Format) df.Series { panic("not implemented") }
func (as *arrowSeries) Join(schema df.Format, series df.Series, jointype df.JoinType, f func(df.Value, df.Value) []df.Value) df.Series { panic("not implemented") }

var _ df.Series = (*arrowSeries)(nil)
