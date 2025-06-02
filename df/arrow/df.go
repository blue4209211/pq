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

type arrowDataFrame struct {
	name   string
	schema *arrowDataFrameSchema
	record arrow.Record
	mem    memory.Allocator
}

// dfValueToArrowScalar (ensure this is available at package level, e.g. from types.go or series.go)
func dfValueToArrowScalar(val df.Value, targetType arrow.DataType, mem memory.Allocator) (scalar.Scalar, error) {
	if val == nil || val.IsNil() {
		return scalar.NewNullScalar(targetType), nil
	}
	if av, ok := val.(*arrowValue); ok {
		if arrow.TypeEqual(av.val.DataType(), targetType) {
			return av.val, nil
		}
		// It's important that the context for Cast has an allocator.
		castedScalar, err := scalar.Cast(compute.WithAllocator(context.Background(), mem), av.val, targetType)
		if err != nil { return nil, fmt.Errorf("cast scalar from %s to %s: %w", av.val.DataType(), targetType, err) }
		// castedScalar is a new scalar and its resources are managed by itself or its datum.
		return castedScalar, nil
	}
	// Fallback for generic df.Value
	switch targetType.ID() {
	case arrow.INT64: return scalar.NewInt64Scalar(val.GetAsInt()), nil
	case arrow.FLOAT64: return scalar.NewFloat64Scalar(val.GetAsDouble()), nil
	case arrow.STRING: return scalar.NewStringScalar(val.GetAsString()), nil
	case arrow.BOOL: return scalar.NewBooleanScalar(val.GetAsBool()), nil
	case arrow.TIMESTAMP:
		tsType, _ := targetType.(*arrow.TimestampType); unit := tsType.Unit(); t := val.GetAsDatetime()
		var tsVal arrow.Timestamp
		switch unit {
		case arrow.Nanosecond: tsVal = arrow.Timestamp(t.UnixNano())
		case arrow.Microsecond: tsVal = arrow.Timestamp(t.UnixNano() / 1e3)
		case arrow.Millisecond: tsVal = arrow.Timestamp(t.UnixNano() / 1e6)
		case arrow.Second: tsVal = arrow.Timestamp(t.Unix())
		default: return nil, fmt.Errorf("unsupported timestamp unit: %s", unit)
		}
		return scalar.NewTimestampScalar(tsVal, targetType), nil
	default: return nil, fmt.Errorf("unsupported target type for dfValueToArrowScalar: %s", targetType.Name())
	}
}

func NewArrowDataFrame(name string, record arrow.Record, dfSchema *arrowDataFrameSchema) df.DataFrame {
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, memory.DefaultAllocator)
}
func NewArrowDataFrameWithAllocator(name string, record arrow.Record, dfSchema *arrowDataFrameSchema, mem memory.Allocator) df.DataFrame {
	if record == nil { panic("arrow.Record cannot be nil") }
	if dfSchema == nil { panic("df.DataFrameSchema cannot be nil") }
	if mem == nil { panic("memory.Allocator cannot be nil") }
	isDfSchemaTrulyEmpty := (dfSchema.schema == nil || dfSchema.schema.NumFields() == 0)
	isRecordSchemaTrulyEmpty := (record.Schema() == nil || record.Schema().NumFields() == 0)
	if isDfSchemaTrulyEmpty && isRecordSchemaTrulyEmpty {
		if dfSchema.schema == nil && record.Schema() != nil { dfSchema.schema = record.Schema() }
	} else if dfSchema.schema == nil {
	    panic("dfSchema.schema is nil for a non-empty record schema")
	} else if !dfSchema.schema.Equal(record.Schema()) {
		panic(fmt.Sprintf("schema mismatch. Provided dfSchema.schema: %s, Record's schema: %s", dfSchema.schema, record.Schema()))
	}
	record.Retain(); return &arrowDataFrame{name: name, schema: dfSchema, record: record, mem: mem}
}
func NewArrowDataFrameFromArrays(name string, cols []arrow.Array, schema *arrow.Schema) (df.DataFrame, error) {
	return NewArrowDataFrameFromArraysWithAllocator(name, cols, schema, memory.DefaultAllocator)
}
func NewArrowDataFrameFromArraysWithAllocator(name string, cols []arrow.Array, schema *arrow.Schema, mem memory.Allocator) (df.DataFrame, error) {
	if schema == nil {return nil, fmt.Errorf("arrow.Schema cannot be nil")}
	if mem == nil {return nil, fmt.Errorf("memory.Allocator cannot be nil")}
	if len(cols) != schema.NumFields() {return nil, fmt.Errorf("num cols (%d) != num fields (%d)", len(cols), schema.NumFields())}
	var numRows int64 = -1
	if len(cols) > 0 {
		for i := range cols { cols[i].Retain() }
		numRows = int64(cols[0].Len())
		for i, col := range cols {
			if int64(col.Len()) != numRows {
				for j := 0; j <= i; j++ { cols[j].Release() }; return nil, fmt.Errorf("col %d len %d != %d", i, col.Len(), numRows)
			}
			if !arrow.TypeEqual(col.DataType(), schema.Field(i).Type) {
				for j := 0; j <= i; j++ { cols[j].Release() }; return nil, fmt.Errorf("col %d type %s != schema %s", i, col.DataType(), schema.Field(i).Type)
			}
		}
	} else {numRows = 0}
	record := array.NewRecord(schema, cols, numRows);
	for _, col := range cols { col.Release() }
	dfSchema := NewArrowDataFrameSchema(schema).(*arrowDataFrameSchema)
	defer record.Release()
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, mem), nil
}
func (adf *arrowDataFrame) Schema() df.DataFrameSchema { return adf.schema }
func (adf *arrowDataFrame) Name() string { return adf.name }
func (adf *arrowDataFrame) Len() int64 { if adf.record == nil { return 0 }; return adf.record.NumRows() }
func (adf *arrowDataFrame) Release() { if adf.record != nil { adf.record.Release(); adf.record = nil } }
func (adf *arrowDataFrame) GetSeries(index int) df.Series {
	if adf.record == nil || index < 0 || index >= int(adf.record.NumCols()) { panic(fmt.Sprintf("series index %d out of bounds", index)) }
	return NewArrowSeriesWithAllocator(adf.record.Column(index), adf.schema.Get(index), adf.mem)
}
func (adf *arrowDataFrame) GetSeriesByName(sName string) df.Series {
	idx := adf.schema.GetIndexByName(sName); if idx == -1 { panic(fmt.Sprintf("series '%s' not found", sName)) }; return adf.GetSeries(idx)
}
func (adf *arrowDataFrame) GetRow(i int64) df.Row {
    if adf.record == nil || i < 0 || i >= adf.record.NumRows() { panic(fmt.Sprintf("row index %d out of bounds", i)) }
    r, err := NewArrowRowFromRecord(adf.schema, adf.record, int(i)); if err != nil { panic(err) }; return r
}
func (adf *arrowDataFrame) GetValue(rowIndx, colIndx int) df.Value {
    if adf.record == nil || rowIndx < 0 || int64(rowIndx) >= adf.record.NumRows() || colIndx < 0 || colIndx >= int(adf.record.NumCols()) {
        panic(fmt.Sprintf("GetValue index (row: %d, col: %d) out of bounds", rowIndx, colIndx))
    }
    return NewArrowValue(scalar.MakeScalar(adf.record.Column(colIndx), rowIndx), adf.schema.Get(colIndx).Format)
}
func (adf *arrowDataFrame) Limit(offset int, size int) df.DataFrame {
	if adf.record == nil { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	currentNumRows := adf.record.NumRows(); if offset < 0 { offset = 0 }
	if offset >= int(currentNumRows) { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	if offset+size > int(currentNumRows) { size = int(currentNumRows) - offset }
	if size <= 0 { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	slicedRecord := adf.record.NewSlice(int64(offset), int64(offset+size)); defer slicedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, slicedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesIndex(indices ...int) df.DataFrame {
	numRowsToKeep := int64(0); if adf.record != nil { numRowsToKeep = adf.record.NumRows()}
	if len(indices) == 0 {
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil); emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, numRowsToKeep); defer emptyRecord.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRecord, emptyDfSchema, adf.mem)
	}
	if adf.record == nil { panic("cannot select columns from a nil or released dataframe") }
	newFields := make([]arrow.Field, len(indices)); newCols := make([]arrow.Array, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= int(adf.record.NumCols()) { for j := 0; j < i; j++ { newCols[j].Release() }; panic(fmt.Sprintf("select index %d out of bounds", idx)) }
		newFields[i] = adf.schema.schema.Field(idx); newCols[i] = adf.record.Column(idx); newCols[i].Retain()
	}
	newArrowSchema := arrow.NewSchema(newFields, adf.schema.schema.Metadata()); newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	selectedRecord := array.NewRecord(newArrowSchema, newCols, numRowsToKeep)
	for _, col := range newCols { col.Release() }; defer selectedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, selectedRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesName(colNames ...string) df.DataFrame {
	if adf.record == nil && len(colNames) > 0 { panic("cannot select by name from a nil or released dataframe") }
	if len(colNames) == 0 {
		numRowsToKeep := int64(0); if adf.record != nil { numRowsToKeep = adf.record.NumRows()}
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil); emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, numRowsToKeep); defer emptyRecord.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRecord, emptyDfSchema, adf.mem)
	}
	indices := make([]int, len(colNames))
	for i, name := range colNames { idx := adf.schema.GetIndexByName(name); if idx == -1 { panic(fmt.Sprintf("column '%s' not found", name)) }; indices[i] = idx }
	return adf.SelectBySeriesIndex(indices...)
}
func (adf *arrowDataFrame) WhereRow(f func(df.Row) bool) df.DataFrame {
	if adf.record == nil { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	numCols := int(adf.record.NumCols()); currentSchema := adf.schema.schema
	colBuilders := make([]array.Builder, numCols); for i := 0; i < numCols; i++ { colBuilders[i] = builder.NewBuilder(adf.mem, currentSchema.Field(i).Type) }
	defer func() { for _, b := range colBuilders { if b != nil { b.Release() } } }()
	for r := int64(0); r < adf.record.NumRows(); r++ {
		rowView, _ := NewArrowRowFromRecord(adf.schema, adf.record, int(r))
		if f(rowView) {
			for c := 0; c < numCols; c++ { if err := array.CopyValue(colBuilders[c], adf.record.Column(c), int(r)); err != nil { panic(fmt.Sprintf("error copying value col %d, row %d: %v", c,r,err))}}
		}
	}
	newCols := make([]arrow.Array, numCols); var newRecordLen int64
	if len(colBuilders) > 0 && colBuilders[0] != nil { newRecordLen = int64(colBuilders[0].Len()) } else { newRecordLen = 0 }
	for i, b := range colBuilders { newCols[i] = b.NewArray() }
	filteredRecord := array.NewRecord(currentSchema, newCols, newRecordLen)
	for _, col := range newCols { col.Release() }; defer filteredRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, filteredRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) Sort(orders ...df.SortByIndex) df.DataFrame {
	if adf.record == nil || adf.record.NumRows() == 0 || len(orders) == 0 {
		var recToHandle arrow.Record
		if adf.record != nil { recToHandle = adf.record.NewSlice(0, adf.record.NumRows()) } else { recToHandle = array.NewRecord(adf.schema.schema, nil, 0) }
		defer recToHandle.Release(); return NewArrowDataFrameWithAllocator(adf.name, recToHandle, adf.schema, adf.mem)
	}
	ctx := compute.WithAllocator(context.Background(), adf.mem); sortKeys := make([]compute.SortKey, len(orders))
	for i, order := range orders {
		if order.Series < 0 || order.Series >= int(adf.record.NumCols()) { panic(fmt.Sprintf("sort key index %d out of bounds", order.Series)) }
		arrowSortOrder := arrow.Ascending; if order.Order == df.SortOrderDESC { arrowSortOrder = arrow.Descending }
		sortKeys[i] = compute.SortKey{ Name: adf.schema.schema.Field(order.Series).Name, Order: arrowSortOrder }
	}
	indicesDatum, err := compute.SortIndices(ctx, arrow.NewRecordDatum(adf.record), compute.SortOptions{SortKeys: sortKeys, NullPlacement: arrow.NullsFirst})
	if err != nil { panic(fmt.Sprintf("failed to get sort indices: %v", err)) }; defer indicesDatum.Release()
	indicesArr, ok := indicesDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("SortIndices bad return type") }
	sortedRecordDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrow.NewRecordDatum(adf.record), arrow.NewArrayDatum(indicesArr))
	if err != nil { panic(fmt.Sprintf("failed to take sorted rows: %v", err)) }; defer sortedRecordDatum.Release()
	sortedRecord, ok := sortedRecordDatum.(*arrow.RecordDatum).Value().(arrow.Record); if !ok { panic("Take bad return type") }
    return NewArrowDataFrameWithAllocator(adf.name, sortedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) SortByName(orders ...df.SortByName) df.DataFrame {
	if adf.record == nil && len(orders) > 0 { panic("cannot sort by name on nil dataframe") }
	if len(orders) == 0 {
		var recToHandle arrow.Record
		if adf.record != nil { recToHandle = adf.record.NewSlice(0, adf.record.NumRows()) } else { recToHandle = array.NewRecord(adf.schema.schema, nil, 0) }
		defer recToHandle.Release(); return NewArrowDataFrameWithAllocator(adf.name, recToHandle, adf.schema, adf.mem)
	}
	sortByIdx := make([]df.SortByIndex, len(orders))
	for i, order := range orders { idx := adf.schema.GetIndexByName(order.Series); if idx == -1 { panic(fmt.Sprintf("col '%s' not found", order.Series)) }; sortByIdx[i] = df.SortByIndex{Series: idx, Order: order.Order} }
	return adf.Sort(sortByIdx...)
}
func (adf *arrowDataFrame) AddSeries(colName string, series df.Series) df.DataFrame {
	if adf.record == nil { panic("nil dataframe record") }; if adf.schema.HasName(colName) { panic(fmt.Sprintf("col '%s' exists", colName)) }
	arrowSeries, ok := series.(*arrowSeries); if !ok { panic(fmt.Sprintf("expected *arrowSeries, got %T", series)) }
	if arrowSeries.arr == nil { panic("new series array is nil") }; if arrowSeries.Len() != adf.Len() { panic(fmt.Sprintf("len mismatch: df %d, series %d", adf.Len(), arrowSeries.Len())) }
	existingFields := adf.schema.schema.Fields(); newSchemaFields := make([]arrow.Field, len(existingFields)+1)
	copy(newSchemaFields, existingFields)
	newSchemaFields[len(existingFields)] = arrow.Field{Name: colName, Type: arrowSeries.arr.DataType(), Nullable: arrowSeries.arr.NullN() > 0}
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata()); newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	existingCols := adf.record.Columns(); newRecordCols := make([]arrow.Array, len(existingCols)+1)
	for i, col := range existingCols { col.Retain(); newRecordCols[i] = col }
	arrowSeries.arr.Retain(); newRecordCols[len(existingCols)] = arrowSeries.arr
	newRecord := array.NewRecord(newArrowSchema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer newRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RemoveSeries(index int) df.DataFrame {
	if adf.record == nil { panic("nil dataframe record") }; if index < 0 || index >= int(adf.record.NumCols()) { panic(fmt.Sprintf("index %d out of bounds", index)) }
	numOldCols := int(adf.record.NumCols()); newSchemaFields := make([]arrow.Field,0,numOldCols-1); newRecordCols := make([]arrow.Array,0,numOldCols-1)
	for i := 0; i < numOldCols; i++ {
		if i == index { continue }; newSchemaFields = append(newSchemaFields, adf.schema.schema.Field(i))
		col := adf.record.Column(i); col.Retain(); newRecordCols = append(newRecordCols, col)
	}
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata()); newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	newRecord := array.NewRecord(newArrowSchema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer newRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RemoveSeriesByName(s string) df.DataFrame {
	idx := adf.schema.GetIndexByName(s); if idx == -1 { panic(fmt.Sprintf("col '%s' not found", s))}; return adf.RemoveSeries(idx)
}
func (adf *arrowDataFrame) RenameSeries(index int, newName string, inplace bool) df.DataFrame {
	if adf.record == nil { panic("nil dataframe record") }; if index < 0 || index >= int(adf.record.NumCols()) { panic(fmt.Sprintf("index %d out of bounds", index)) }
	if currentName := adf.schema.schema.Field(index).Name; currentName == newName {
		if inplace { return adf }; newRecView := adf.record.NewSlice(0, adf.record.NumRows()); defer newRecView.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRecView, adf.schema, adf.mem)
	}
	if adf.schema.HasName(newName) { panic(fmt.Sprintf("col '%s' exists", newName)) }
	newSchemaFields := make([]arrow.Field, adf.record.NumCols()); copy(newSchemaFields, adf.schema.schema.Fields())
	newSchemaFields[index].Name = newName
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata()); newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	recordCols := adf.record.Columns(); for _, col := range recordCols { col.Retain() }
	newRecord := array.NewRecord(newArrowSchema, recordCols, adf.record.NumRows()); for _, col := range recordCols { col.Release() }
	if inplace { adf.record.Release(); adf.record = newRecord; adf.schema = newDfSchema; return adf }
	defer newRecord.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RenameSeriesByName(colName string, newName string, inplace bool) df.DataFrame {
	idx := adf.schema.GetIndexByName(colName); if idx == -1 { panic(fmt.Sprintf("col '%s' not found", colName)) }; return adf.RenameSeries(idx, newName, inplace)
}
func (adf *arrowDataFrame) GetSeriesExprByName(sName string) df.Expr {
	idx := adf.schema.GetIndexByName(sName); if idx == -1 { panic(fmt.Sprintf("series '%s' not found", sName)) }
	seriesSchema := adf.schema.Get(idx)
	switch seriesSchema.Format.Name() {
	case df.BoolFormat.Name(): return df.NewBoolColExpr(sName)
	case df.IntegerFormat.Name(): return df.NewIntColExpr(sName)
	case df.DoubleFormat.Name(): return df.NewDoubleColExpr(sName)
	case df.StringFormat.Name(): return df.NewStringColExpr(sName)
	case df.DateTimeFormat.Name(): return df.NewDatetimeColExpr(sName)
	default: panic(fmt.Sprintf("GetSeriesExprByName unsupported format: %s", seriesSchema.Format.Name()))
	}
}
func (adf *arrowDataFrame) MapRow(outputSchemaGiven df.DataFrameSchema, f func(df.Row) df.Row) df.DataFrame {
	if adf.record == nil { panic("MapRow on nil record") }; outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok { panic(fmt.Sprintf("outputSchema must be *arrowDataFrameSchema, got %T", outputSchemaGiven)) }
	outputInternalArrowSchema := outputArrowDFSchema.schema; if outputInternalArrowSchema == nil { panic("outputSchema internal schema is nil") }
	numOutputCols := outputInternalArrowSchema.NumFields(); colBuilders := make([]array.Builder, numOutputCols)
	for i := 0; i < numOutputCols; i++ { colBuilders[i] = builder.NewBuilder(adf.mem, outputInternalArrowSchema.Field(i).Type) }
	defer func() { for _, b := range colBuilders { if b != nil { b.Release() } } }()
	for r := int64(0); r < adf.record.NumRows(); r++ {
		inputRow, err := NewArrowRowFromRecord(adf.schema, adf.record, int(r)); if err != nil { panic(fmt.Sprintf("MapRow create input row %d: %v", r, err)) }
		outputRow := f(inputRow); if outputRow == nil { panic(fmt.Sprintf("MapRow func returned nil df.Row for input %d", r)) }
		if outputRow.Len() != numOutputCols { panic(fmt.Sprintf("MapRow func returned %d cols, expected %d", outputRow.Len(), numOutputCols)) }
		for c := 0; c < numOutputCols; c++ {
			val := outputRow.Get(c)
			if val == nil || val.IsNil() { colBuilders[c].AppendNull(); continue }
			arrowVal, castOk := val.(*arrowValue); if !castOk { panic(fmt.Sprintf("MapRow func value col %d type %T, expected *arrowValue", c, val)) }
			if err := appendScalarToBuilder(colBuilders[c], arrowVal.val); err != nil { panic(fmt.Sprintf("MapRow append col %d (name: %s): %v. Scalar: %s, Builder: %s",c, outputInternalArrowSchema.Field(c).Name, err, arrowVal.val.DataType().Name(), colBuilders[c].Type().Name()))}
		}
	}
	newCols := make([]arrow.Array, numOutputCols); var newRecordLen int64
	if len(colBuilders) > 0 && colBuilders[0] != nil { newRecordLen = int64(colBuilders[0].Len()) }
	for i, b := range colBuilders { newCols[i] = b.NewArray() }
	mappedRecord := array.NewRecord(outputInternalArrowSchema, newCols, newRecordLen)
	for _, col := range newCols { col.Release() }; defer mappedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, mappedRecord, outputArrowDFSchema, adf.mem)
}
func (adf *arrowDataFrame) FlatMapRow(outputSchemaGiven df.DataFrameSchema, f func(df.Row) []df.Row) df.DataFrame {
	if adf.record == nil { panic("FlatMapRow on nil record") }; outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok { panic(fmt.Sprintf("outputSchema must be *arrowDataFrameSchema, got %T", outputSchemaGiven)) }
	outputInternalArrowSchema := outputArrowDFSchema.schema; if outputInternalArrowSchema == nil { panic("outputSchema internal schema is nil") }
	numOutputCols := outputInternalArrowSchema.NumFields(); colBuilders := make([]array.Builder, numOutputCols)
	for i := 0; i < numOutputCols; i++ { colBuilders[i] = builder.NewBuilder(adf.mem, outputInternalArrowSchema.Field(i).Type) }
	defer func() { for _, b := range colBuilders { if b != nil { b.Release() } } }()
	for r := int64(0); r < adf.record.NumRows(); r++ {
		inputRow, err := NewArrowRowFromRecord(adf.schema, adf.record, int(r)); if err != nil { panic(fmt.Sprintf("FlatMapRow create input row %d: %v", r, err)) }
		outputRows := f(inputRow); if outputRows == nil { continue }
		for i, outputRow := range outputRows {
			if outputRow == nil { panic(fmt.Sprintf("FlatMapRow func returned slice with nil df.Row (index %d) for input %d", i, r)) }
			if outputRow.Len() != numOutputCols { panic(fmt.Sprintf("FlatMapRow func returned df.Row (index %d) with %d cols, expected %d, for input %d", i, outputRow.Len(), numOutputCols, r)) }
			for c := 0; c < numOutputCols; c++ {
				val := outputRow.Get(c)
				if val == nil || val.IsNil() { colBuilders[c].AppendNull(); continue }
				arrowVal, castOk := val.(*arrowValue); if !castOk { panic(fmt.Sprintf("FlatMapRow func value (col %d, row %d) type %T, expected *arrowValue, for input %d", c, i, val, r)) }
				if err := appendScalarToBuilder(colBuilders[c], arrowVal.val); err != nil { panic(fmt.Sprintf("FlatMapRow append col %d (name: %s): %v. Scalar: %s, Builder: %s",c, outputInternalArrowSchema.Field(c).Name, err, arrowVal.val.DataType().Name(), colBuilders[c].Type().Name()))}
			}
		}
	}
	newCols := make([]arrow.Array, numOutputCols); var newRecordLen int64
	if len(colBuilders) > 0 && colBuilders[0] != nil { newRecordLen = int64(colBuilders[0].Len()) }
	for i, b := range colBuilders { newCols[i] = b.NewArray() }
	flatMappedRecord := array.NewRecord(outputInternalArrowSchema, newCols, newRecordLen)
	for _, col := range newCols { col.Release() }; defer flatMappedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, flatMappedRecord, outputArrowDFSchema, adf.mem)
}
func (adf *arrowDataFrame) Distinct(cols ...string) df.DataFrame {
	if adf.record == nil || adf.record.NumRows() == 0 { schemaToUse := arrow.NewSchema([]arrow.Field{},nil); if adf.schema != nil && adf.schema.schema != nil { schemaToUse = adf.schema.schema}; newRec := array.NewRecord(schemaToUse, nil, 0); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem)}
	var keyIndices []int
	if len(cols) == 0 { keyIndices = make([]int, adf.record.NumCols()); for i := 0; i < int(adf.record.NumCols()); i++ { keyIndices[i] = i }
	} else { keyIndices = make([]int, len(cols)); for i, name := range cols { idx := adf.schema.GetIndexByName(name); if idx == -1 { panic(fmt.Sprintf("Distinct col '%s' not found", name)) }; keyIndices[i] = idx }}
    if adf.record.NumCols() == 0 { if adf.record.NumRows() > 0 { newRec := adf.record.NewSlice(0,1); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }; newRec := adf.record.NewSlice(0,0); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem)}
	sortOrders := make([]df.SortByIndex, len(keyIndices)); for i, keyIdx := range keyIndices { sortOrders[i] = df.SortByIndex{Series: keyIdx, Order: df.SortOrderASC} }
	sortedDf := adf.Sort(sortOrders...); sortedArrowDf, ok := sortedDf.(*arrowDataFrame); if !ok { panic("Distinct: Sort bad return") }; defer sortedArrowDf.Release()
	sortedRecord := sortedArrowDf.record; if sortedRecord.NumRows() == 0 { newRec := sortedRecord.NewSlice(0, 0); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem)}
	uniqueRowIndices := make([]int64, 0, sortedRecord.NumRows()); uniqueRowIndices = append(uniqueRowIndices, 0)
	for i := int64(1); i < sortedRecord.NumRows(); i++ {
		isDifferent := false
		for _, keyIdx := range keyIndices {
			prevValScalar := scalar.MakeScalar(sortedRecord.Column(keyIdx), int(i-1)); currValScalar := scalar.MakeScalar(sortedRecord.Column(keyIdx), int(i))
			if !scalar.Equals(prevValScalar, currValScalar) { isDifferent = true; break }
		}
		if isDifferent { uniqueRowIndices = append(uniqueRowIndices, i) }
	}
	indicesBuilder := array.NewInt64Builder(adf.mem); defer indicesBuilder.Release(); indicesBuilder.AppendValues(uniqueRowIndices, nil)
	indicesArr := indicesBuilder.NewArray(); defer indicesArr.Release()
	ctx := compute.WithAllocator(context.Background(), adf.mem)
	distinctRecordDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrow.NewRecordDatum(sortedRecord), arrow.NewArrayDatum(indicesArr))
	if err != nil { panic(fmt.Sprintf("Distinct: Take failed: %v", err)) }; defer distinctRecordDatum.Release()
	distinctRecord, okValue := distinctRecordDatum.(*arrow.RecordDatum).Value().(arrow.Record); if !okValue { panic("Distinct: Take bad return") }
	return NewArrowDataFrameWithAllocator(adf.name, distinctRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) Append(otherRaw df.DataFrame) df.DataFrame {
	if otherRaw == nil { panic("Append: other df nil") }; otherArrowDf, ok := otherRaw.(*arrowDataFrame); if !ok { panic(fmt.Sprintf("Append: expected *arrowDataFrame, got %T", otherRaw)) }
	currentIsColEmpty := adf.record == nil || adf.record.NumCols() == 0; otherIsColEmpty := otherArrowDf.record == nil || otherArrowDf.record.NumCols() == 0
	currentSchemaForEmpty := adf.schema.schema; if currentSchemaForEmpty == nil || currentSchemaForEmpty.NumFields() != 0 { currentSchemaForEmpty = arrow.NewSchema([]arrow.Field{}, nil) }
	if currentIsColEmpty {
		if otherIsColEmpty { numRows := adf.Len() + otherArrowDf.Len(); newRec := array.NewRecord(currentSchemaForEmpty, nil, numRows); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
		newOtherRec := otherArrowDf.record.NewSlice(0, otherArrowDf.record.NumRows()); defer newOtherRec.Release(); return NewArrowDataFrameWithAllocator(otherArrowDf.name, newOtherRec, otherArrowDf.schema, adf.mem)
	}
	if otherIsColEmpty { newThisRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newThisRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newThisRec, adf.schema, adf.mem) }
	if !adf.schema.Equals(otherArrowDf.schema) { panic(fmt.Sprintf("Append: schema mismatch. Current: %s, Other: %s", adf.schema.schema, otherArrowDf.schema.schema)) }
	if adf.record.NumRows() == 0 { newOtherRec := otherArrowDf.record.NewSlice(0, otherArrowDf.record.NumRows()); defer newOtherRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newOtherRec, otherArrowDf.schema, adf.mem)  }
	if otherArrowDf.record.NumRows() == 0 { newThisRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newThisRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newThisRec, adf.schema, adf.mem) }
	numCols := int(adf.record.NumCols()); concatenatedCols := make([]arrow.Array, numCols); var err error
	for i := 0; i < numCols; i++ {
		col1 := adf.record.Column(i); col2 := otherArrowDf.record.Column(i)
		concatenatedCols[i], err = array.Concatenate([]arrow.Array{col1, col2}, adf.mem)
		if err != nil { for j := 0; j < i; j++ { if concatenatedCols[j] != nil { concatenatedCols[j].Release() } }; panic(fmt.Sprintf("Append: concat col %d ('%s'): %v", i, adf.schema.Get(i).Name, err)) }
	}
	newNumRows := adf.record.NumRows() + otherArrowDf.record.NumRows()
	appendedRecord := array.NewRecord(adf.schema.schema, concatenatedCols, newNumRows)
	for _, col := range concatenatedCols { if col != nil { col.Release() } }; defer appendedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, appendedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) Union(otherRaw df.DataFrame) df.DataFrame {
	if otherRaw == nil { panic("Union: other df nil") }; appendedDf := adf.Append(otherRaw)
	unionDf := appendedDf.Distinct()
	if appendedArrowDf, ok := appendedDf.(*arrowDataFrame); ok { appendedArrowDf.Release() }
	return unionDf
}
func (adf *arrowDataFrame) WhenNil(fillValues map[string]df.Value) df.DataFrame {
	if adf.record == nil { panic("WhenNil on nil record") }; if len(fillValues) == 0 { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	newRecordCols := make([]arrow.Array, adf.record.NumCols()); modified := false; ctx := compute.WithAllocator(context.Background(), adf.mem)
	for i := 0; i < int(adf.record.NumCols()); i++ {
		col := adf.record.Column(i); colName := adf.schema.schema.Field(i).Name
		fillVal, colNeedsFilling := fillValues[colName]
		if !colNeedsFilling || fillVal == nil { col.Retain(); newRecordCols[i] = col; continue }
		modified = true; targetArrowType := col.DataType()
		fillScalar, err := dfValueToArrowScalar(fillVal, targetArrowType, adf.mem)
		if err != nil { for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }; panic(fmt.Sprintf("WhenNil: convert fill for '%s': %v", colName, err)) }
		if c, needsRelease := fillScalar.(interface{ Release() }); needsRelease { defer c.Release() }
		resultDatum, err := compute.FillNull(ctx, arrow.NewArrayDatum(col), arrow.NewScalarDatum(fillScalar))
		if err != nil { for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }; panic(fmt.Sprintf("WhenNil: FillNull for '%s': %v", colName, err)) }
		newColArr := resultDatum.(*arrow.ArrayDatum).MakeArray(); resultDatum.Release(); newRecordCols[i] = newColArr
	}
	if !modified { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	finalRecord := array.NewRecord(adf.schema.schema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) When(replaceMap map[string]map[any]df.Value) df.DataFrame {
	if adf.record == nil { panic("When on nil record") }; if len(replaceMap) == 0 { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	newRecordCols := make([]arrow.Array, adf.record.NumCols()); modified := false; ctx := compute.WithAllocator(context.Background(), adf.mem) // Context for Cast in dfValueToArrowScalar
	for i := 0; i < int(adf.record.NumCols()); i++ {
		originalCol := adf.record.Column(i); colName := adf.schema.schema.Field(i).Name; colType := originalCol.DataType()
		valueReplacements, colNeedsUpdate := replaceMap[colName]
		if !colNeedsUpdate || len(valueReplacements) == 0 { originalCol.Retain(); newRecordCols[i] = originalCol; continue }
		modified = true; b := builder.NewBuilder(adf.mem, colType); defer b.Release()
		for r := 0; r < originalCol.Len(); r++ {
			var currentGoValue interface{}; isNull := originalCol.IsNull(r)
			if !isNull { currentScalar := scalar.MakeScalar(originalCol, r); if cs, nr := currentScalar.(interface{ Release() }); nr { defer cs.Release() }; currentDfValue := NewArrowValue(currentScalar, adf.schema.Get(i).Format); currentGoValue = currentDfValue.Get() }
			replacementDfVal, shouldReplace := valueReplacements[currentGoValue]
			if shouldReplace {
				replacementScalar, err := dfValueToArrowScalar(replacementDfVal, colType, adf.mem)
				if err != nil { for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }; panic(fmt.Sprintf("When: convert replacement for key %v, col '%s': %v", currentGoValue, colName, err)) }
				if c, nr := replacementScalar.(interface{ Release() }); nr { defer c.Release() }
				if err := appendScalarToBuilder(b, replacementScalar); err != nil { for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }; panic(fmt.Sprintf("When: append replacement for key %v, col '%s': %v", currentGoValue, colName, err)) }
			} else { if err := array.CopyValue(b, originalCol, r); err != nil { for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }; panic(fmt.Sprintf("When: copy original for col '%s', row %d: %v", colName, r, err)) }}
		}
		newRecordCols[i] = b.NewArray()
	}
	if !modified { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	finalRecord := array.NewRecord(adf.schema.schema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) Join(outputSchemaGiven df.DataFrameSchema, otherRaw df.DataFrame, jointype df.JoinType, joinColsMap map[string]string, fUser func(r1 df.Row, r2 df.Row) []df.Row) df.DataFrame {
	if adf.record == nil { panic("Join on nil left record") }; if otherRaw == nil { panic("Join: other df nil") }
	otherArrowDf, ok := otherRaw.(*arrowDataFrame); if !ok { panic(fmt.Sprintf("Join: expected *arrowDataFrame, got %T", otherRaw)) }
	if otherArrowDf.record == nil { panic("Join: other df record nil") }; if outputSchemaGiven == nil { panic("Join: outputSchemaGiven nil") }
	outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok { panic(fmt.Sprintf("Join: outputSchemaGiven not *arrowDataFrameSchema, got %T", outputSchemaGiven)) }
	outputInternalArrowSchema := outputArrowDFSchema.schema; if outputInternalArrowSchema == nil { panic("Join: outputSchemaGiven internal schema nil") }
	if fUser == nil { panic("Join: user function fUser cannot be nil in this implementation") }

	ctx := compute.WithAllocator(context.Background(), adf.mem)

	if jointype == df.JoinCross {
		leftDatum := arrow.NewRecordDatum(adf.record); defer leftDatum.Release()
		rightDatum := arrow.NewRecordDatum(otherArrowDf.record); defer rightDatum.Release()
		_, err := compute.CrossJoin(ctx, leftDatum, rightDatum, compute.CrossJoinOptions{SuffixLeft:"_L", SuffixRight:"_R"})
        if err != nil { panic(fmt.Sprintf("Join: CrossJoin compute failed: %v", err)) };
		panic("Join: CrossJoin with fUser post-processing not fully implemented after Arrow kernel.")
	}
	if jointype != df.JoinEqui { panic(fmt.Sprintf("Join: only JoinEqui (and basic CrossJoin kernel) supported. Got %s", jointype)) }
	if len(joinColsMap) == 0 { panic("JoinEqui requires join columns.") }

	outputInternalArrowSchema = outputArrowDFSchema.schema; numOutputCols := outputInternalArrowSchema.NumFields()
	colBuilders := make([]array.Builder, numOutputCols); for i := 0; i < numOutputCols; i++ { colBuilders[i] = builder.NewBuilder(adf.mem, outputInternalArrowSchema.Field(i).Type) }
	defer func() { for _, b := range colBuilders { if b != nil { b.Release() } } }()

	for r1Idx := int64(0); r1Idx < adf.Len(); r1Idx++ {
		leftRowOriginal := adf.GetRow(r1Idx)
		for r2Idx := int64(0); r2Idx < otherArrowDf.Len(); r2Idx++ {
			rightRowOriginal := otherArrowDf.GetRow(r2Idx)
			match := true
			for lKeyName, rKeyName := range joinColsMap {
				lVal := leftRowOriginal.GetByName(lKeyName); rVal := rightRowOriginal.GetByName(rKeyName)
				if (lVal.IsNil() && !rVal.IsNil()) || (!lVal.IsNil() && rVal.IsNil()) || (!lVal.Equals(rVal)) { match = false; break }
			}
			if match {
				outputRows := fUser(leftRowOriginal, rightRowOriginal)
				for _, outRow := range outputRows {
					if outRow.Len() != numOutputCols { panic("Join: fUser returned row with incorrect col count") }
					for c := 0; c < numOutputCols; c++ {
						val := outRow.Get(c); av, ok_av := val.(*arrowValue)
						if !ok_av && !val.IsNil() { panic(fmt.Sprintf("Join: fUser returned non-*arrowValue: %T", val)) }
						var scalarToAppend scalar.Scalar
						if val.IsNil() || !ok_av { scalarToAppend = scalar.NewNullScalar(colBuilders[c].Type()) } else { scalarToAppend = av.val }

						var finalScalarToAppend scalar.Scalar = scalarToAppend
						if !arrow.TypeEqual(scalarToAppend.DataType(), colBuilders[c].Type()) {
							casted, errCast := scalar.Cast(ctx, scalarToAppend, colBuilders[c].Type())
							if errCast != nil { panic(fmt.Sprintf("Join: cast output for col %d: %v", c, errCast)) };
							if cs, ok_cs := casted.(interface{ Release() }); ok_cs { defer cs.Release() }
							finalScalarToAppend = casted
						}
						if err := appendScalarToBuilder(colBuilders[c], finalScalarToAppend); err != nil { panic(fmt.Sprintf("Join: append col %d: %v", c, err)) }
					}
				}
			}
		}
	}
	newCols := make([]array.Array, numOutputCols); var newRecordLen int64
	if numOutputCols > 0 && colBuilders[0] != nil { newRecordLen = int64(colBuilders[0].Len()) }
	for i, b := range colBuilders { newCols[i] = b.NewArray() }
	finalRecord := array.NewRecord(outputInternalArrowSchema, newCols, newRecordLen)
	for _, col := range newCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, outputArrowDFSchema, adf.mem)
}

// --- Stubs for remaining methods ---
func (adf *arrowDataFrame) Select(e ...df.Expr) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Rename(name string, inplace bool) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) AsFormat(t map[string]df.Format) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeries(index int, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeriesByName(name string, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) ForEachRow(f func(df.Row)) { panic("not implemented") }
func (adf *arrowDataFrame) Group(others ...string) df.GroupedDataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Intersection(d df.DataFrame, col ...string) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) Except(d df.DataFrame, col ...string) df.DataFrame { panic("not implemented") }

var _ df.DataFrame = (*arrowDataFrame)(nil)
