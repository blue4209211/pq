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
	"git.querycap.com/practice/df" // MODIFIED import path
)

// const JoinLeftAnti df.JoinType = "leftanti" // Assuming df package might provide this or it's handled via string.
// For now, if Join uses string types for joinType, this might not be needed here.
// If df.JoinType is an enum, this const would only be valid if "leftanti" is part of that enum.
// Let's assume for now the df package handles the join types adequately.
type arrowDataFrame struct {
	name   string
	schema *arrowDataFrameSchema
	record arrow.Record
	mem    memory.Allocator
}

// REMOVED local dfValueToArrowScalar - will use the one from types.go

func NewArrowDataFrame(name string, record arrow.Record, dfSchema *arrowDataFrameSchema) df.DataFrame {
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, memory.DefaultAllocator)
}
func NewArrowDataFrameWithAllocator(name string, record arrow.Record, dfSchema *arrowDataFrameSchema, mem memory.Allocator) df.DataFrame {
	if dfSchema == nil { panic("NewArrowDataFrameWithAllocator: df.DataFrameSchema cannot be nil") }
	if mem == nil { panic("NewArrowDataFrameWithAllocator: memory.Allocator cannot be nil") }
	if record == nil {
		if !(dfSchema.schema == nil || dfSchema.schema.NumFields() == 0) {
			panic("NewArrowDataFrameWithAllocator: record is nil but schema defines fields")
		}
	} else {
		if dfSchema.schema == nil { panic("NewArrowDataFrameWithAllocator: dfSchema.schema is nil for a non-nil record") }
		if !dfSchema.schema.Equal(record.Schema()) {
			panic(fmt.Sprintf("NewArrowDataFrameWithAllocator: schema mismatch. Provided: %s, Record: %s", dfSchema.schema, record.Schema()))
		}
		record.Retain()
	}
	return &arrowDataFrame{name: name, schema: dfSchema, record: record, mem: mem}
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

// NewArrowDataFrameFromSeries creates a DataFrame from a slice of df.Series.
// All series must be *arrowSeries and have the same length.
// The names for the new DataFrame's columns will be taken from the Series' schemas.
// If series array is empty, a DataFrame with 0 columns and 0 rows is created.
func NewArrowDataFrameFromSeries(name string, series []df.Series, mem memory.Allocator) (df.DataFrame, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	if len(series) == 0 {
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil)
		// NewArrowDataFrameSchema returns df.DataFrameSchema, cast to *arrowDataFrameSchema
		emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		// Create an empty record. NewRecord doesn't retain, but it's fine as it's empty.
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, 0)
		// NewArrowDataFrameWithAllocator will handle its lifecycle.
		return NewArrowDataFrameWithAllocator(name, emptyRecord, emptyDfSchema, mem), nil
	}

	arrowArrays := make([]arrow.Array, len(series))
	arrowFields := make([]arrow.Field, len(series))
	var numRows int = -1 // Changed to int to match series.Len()

	for i, s := range series {
		as, ok := s.(*arrowSeries)
		if !ok {
			// Release any arrays already retained if we error out
			for j := 0; j < i; j++ {
				arrowArrays[j].Release()
			}
			return nil, fmt.Errorf("NewArrowDataFrameFromSeries: all series must be *arrowSeries, found %T at index %d", s, i)
		}
		if numRows == -1 {
			numRows = as.Len() // series.Len() returns int
		} else if as.Len() != numRows {
			for j := 0; j < i; j++ {
				arrowArrays[j].Release()
			}
			return nil, fmt.Errorf("NewArrowDataFrameFromSeries: all series must have the same length (expected %d, got %d for series '%s')", numRows, as.Len(), as.Schema().Name)
		}

		as.arr.Retain() // Retain each array as the record will effectively take ownership via NewRecord
		arrowArrays[i] = as.arr

		// Create arrow.Field from df.SeriesSchema
		sSchema := as.Schema()
		arrowDataType, err := dfFormatToArrowType(sSchema.Format)
		if err != nil {
			for j := 0; j <= i; j++ { // Release all retained arrays up to this point
				arrowArrays[j].Release()
			}
			return nil, fmt.Errorf("NewArrowDataFrameFromSeries: error converting format for series %s: %w", sSchema.Name, err)
		}
		arrowFields[i] = arrow.Field{
			Name:     sSchema.Name,
			Type:     arrowDataType, // Use converted type
			Nullable: sSchema.Nullable,
			Metadata: arrow.MetadataFrom(sSchema.Metadata),
		}
	}

	arrowSchema := arrow.NewSchema(arrowFields, nil) // TODO: DataFrame level metadata?

	// array.NewRecord does not retain the input arrays again, it assumes ownership of the references passed.
	// Since we retained them from the series, this is correct.
	record := array.NewRecord(arrowSchema, arrowArrays, int64(numRows))
	// After NewRecord, the record owns these array references. We can release our temporary holds.
	for _, arr := range arrowArrays {
		arr.Release()
	}

	dfSchema := NewArrowDataFrameSchema(record.Schema()).(*arrowDataFrameSchema)
	// NewArrowDataFrameWithAllocator will retain the record.
	// We must release the record created here after NewArrowDataFrameWithAllocator is done with it.
	defer record.Release()
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, mem), nil
}

func (adf *arrowDataFrame) Schema() df.DataFrameSchema { return adf.schema }
func (adf *arrowDataFrame) Name() string { return adf.name }
func (adf *arrowDataFrame) Len() int { if adf.record == nil { return 0 }; return int(adf.record.NumRows()) } // MODIFIED to return int
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
			// Using 3-argument appendScalarToBuilder from types.go
			if err := appendScalarToBuilder(colBuilders[c], arrowVal.val, colBuilders[c].Type()); err != nil { panic(fmt.Sprintf("MapRow append col %d (name: %s): %v. Scalar: %s, Builder: %s",c, outputInternalArrowSchema.Field(c).Name, err, arrowVal.val.DataType().Name(), colBuilders[c].Type().Name()))}
		}
	}
	newCols := make([]array.Array, numOutputCols); var newRecordLen int64
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
				// Using 3-argument appendScalarToBuilder from types.go
				if err := appendScalarToBuilder(colBuilders[c], arrowVal.val, colBuilders[c].Type()); err != nil { panic(fmt.Sprintf("FlatMapRow append col %d (name: %s): %v. Scalar: %s, Builder: %s",c, outputInternalArrowSchema.Field(c).Name, err, arrowVal.val.DataType().Name(), colBuilders[c].Type().Name()))}
			}
		}
	}
	newCols := make([]array.Array, numOutputCols); var newRecordLen int64
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
			if cs, needsRelease := prevValScalar.(interface{ Release() }); needsRelease { cs.Release() }
			if cs, needsRelease := currValScalar.(interface{ Release() }); needsRelease { cs.Release() }
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
		// Using dfValueToArrowScalar from types.go (does not take allocator)
		fillScalar, err := dfValueToArrowScalar(fillVal, targetArrowType)
		if err != nil {
			// Release previously retained/created columns before panicking
			for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }
			panic(fmt.Sprintf("WhenNil: convert fill for '%s': %v", colName, err))
		}
		// dfValueToArrowScalar from types.go does not return retained scalars needing release by caller.

		resultDatum, err := compute.FillNull(ctx, arrow.NewArrayDatum(col), arrow.NewScalarDatum(fillScalar))
		if err != nil {
			for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }
			panic(fmt.Sprintf("WhenNil: FillNull for '%s': %v", colName, err))
		}
		newColArr := resultDatum.Value().(arrow.Array); newColArr.Retain() // Retain for newRecordCols
		resultDatum.Release();
		newRecordCols[i] = newColArr
	}
	if !modified { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	finalRecord := array.NewRecord(adf.schema.schema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) When(replaceMap map[string]map[any]df.Value) df.DataFrame {
	if adf.record == nil { panic("When on nil record") }; if len(replaceMap) == 0 { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	newRecordCols := make([]arrow.Array, adf.record.NumCols()); modified := false; ctx := compute.WithAllocator(context.Background(), adf.mem)
	for i := 0; i < int(adf.record.NumCols()); i++ {
		originalCol := adf.record.Column(i); colName := adf.schema.schema.Field(i).Name; colType := originalCol.DataType()
		valueReplacements, colNeedsUpdate := replaceMap[colName]
		if !colNeedsUpdate || len(valueReplacements) == 0 { originalCol.Retain(); newRecordCols[i] = originalCol; continue }
		modified = true; b := builder.NewBuilder(adf.mem, colType); defer b.Release()
		for r := 0; r < originalCol.Len(); r++ {
			currentDfVal := adf.GetValue(r, i)
			var goKeyForLookup any
			if currentDfVal.IsNil() { goKeyForLookup = nil } else { goKeyForLookup = currentDfVal.Get() }
			replacementDfVal, shouldReplace := valueReplacements[goKeyForLookup]
			if shouldReplace {
				// Using dfValueToArrowScalar from types.go (does not take allocator)
				replacementScalar, err := dfValueToArrowScalar(replacementDfVal, colType)
				if err != nil {
					for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }
					panic(fmt.Sprintf("When: convert replacement for key %v, col '%s': %v", goKeyForLookup, colName, err))
				}
				// Using appendScalarToBuilder from types.go
				errAppend := appendScalarToBuilder(b, replacementScalar, colType)
				// dfValueToArrowScalar from types.go does not return retained scalars.
				if errAppend != nil {
					for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }
					panic(fmt.Sprintf("When: append replacement for key %v, col '%s': %v", goKeyForLookup, colName, errAppend))
				}
			} else {
				originalScalarToAppend := currentDfVal.(*arrowValue).val
				// Using appendScalarToBuilder from types.go
				if err := appendScalarToBuilder(b, originalScalarToAppend, colType); err != nil {
					for j := 0; j < i; j++ { if newRecordCols[j] != nil {newRecordCols[j].Release()} }
					panic(fmt.Sprintf("When: copy original for col '%s', row %d: %v", colName, r, err))
				}
			}
		}
		newRecordCols[i] = b.NewArray()
	}
	if !modified { newRec := adf.record.NewSlice(0, adf.record.NumRows()); defer newRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRec, adf.schema, adf.mem) }
	finalRecord := array.NewRecord(adf.schema.schema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) Join(outputSchemaGiven df.DataFrameSchema, otherRaw df.DataFrame, jointype df.JoinType, joinColsMap map[string]string, fUser func(r1 df.Row, r2 df.Row) []df.Row) df.DataFrame {
	if adf.record == nil && !(jointype == df.JoinRight || jointype == df.JoinOuter || df.JoinType(string(jointype)) == "leftanti" ) { // Assuming JoinLeftAnti is a string const
		if jointype == df.JoinEqui || jointype == df.JoinLeft {
			outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok {panic(fmt.Sprintf("Join: outputSchemaGiven must be *arrowDataFrameSchema, got %T", outputSchemaGiven))}
			emptyOutputRec := array.NewRecord(outputArrowDFSchema.schema, nil, 0); defer emptyOutputRec.Release()
			return NewArrowDataFrameWithAllocator(adf.name, emptyOutputRec, outputArrowDFSchema, adf.mem)
		}
	}
	if otherRaw == nil { panic("Join: other dataframe cannot be nil") }
	otherArrowDf, ok := otherRaw.(*arrowDataFrame); if !ok { panic(fmt.Sprintf("Join: expected *arrowDataFrame, got %T", otherRaw)) }

	if (otherArrowDf.record == nil || otherArrowDf.Len() == 0) && (jointype == df.JoinEqui || jointype == df.JoinLeft || df.JoinType(string(jointype)) == "leftanti") {
		outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok {panic(fmt.Sprintf("Join: outputSchemaGiven must be *arrowDataFrameSchema, got %T", outputSchemaGiven))}
		schemaForEmpty := outputArrowDFSchema.schema; if df.JoinType(string(jointype)) == "leftanti" { schemaForEmpty = adf.schema.schema }
		emptyOutputRec := array.NewRecord(schemaForEmpty, nil, 0); defer emptyOutputRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyOutputRec, outputArrowDFSchema, adf.mem)
	}
	if (adf.record == nil || adf.Len() == 0) && (jointype == df.JoinRight || jointype == df.JoinEqui) {
		outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok {panic(fmt.Sprintf("Join: outputSchemaGiven must be *arrowDataFrameSchema, got %T", outputSchemaGiven))}
		emptyOutputRec := array.NewRecord(outputArrowDFSchema.schema, nil, 0); defer emptyOutputRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyOutputRec, outputArrowDFSchema, adf.mem)
	}
	// For LeftAnti: if adf is empty, result is empty (matching adf schema)
	if (adf.record == nil || adf.Len() == 0) && df.JoinType(string(jointype)) == "leftanti" {
		emptyOutputRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyOutputRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyOutputRec, adf.schema, adf.mem)
	}


	if outputSchemaGiven == nil { panic("Join: outputSchemaGiven cannot be nil") }
	outputArrowDFSchema, ok := outputSchemaGiven.(*arrowDataFrameSchema); if !ok { panic(fmt.Sprintf("Join: outputSchemaGiven must be *arrowDataFrameSchema, got %T", outputSchemaGiven)) }
	if outputArrowDFSchema.schema == nil { panic("Join: outputSchemaGiven's internal arrow.Schema is nil") }

	isSemiOrAntiJoin := (jointype == JoinLeftAnti || jointype == df.JoinRightAnti || jointype == df.JoinLeftSemi || jointype == df.JoinRightSemi)
	if fUser == nil && !isSemiOrAntiJoin {
		panic("Join: user function fUser cannot be nil for this join type")
	}


	ctx := compute.WithAllocator(context.Background(), adf.mem)

	if jointype == df.JoinCross {
		panic("Join: CrossJoin with fUser adaptation is not fully implemented in this pass.")
	}

	leftKeyDatums := make([]arrow.Datum, 0, len(joinColsMap))
	rightKeyDatums := make([]arrow.Datum, 0, len(joinColsMap))
	if (adf.record == nil || otherArrowDf.record == nil) && len(joinColsMap) > 0 {
		panic("Join: Cannot prepare keys for join as one or both records are nil")
	}

	for lKeyName, rKeyName := range joinColsMap {
		lIdx := adf.schema.GetIndexByName(lKeyName); if lIdx == -1 { panic(fmt.Sprintf("Join: left key '%s' not found", lKeyName)) }
		leftKeyDatums = append(leftKeyDatums, arrow.NewArrayDatum(adf.record.Column(lIdx)))
		rIdx := otherArrowDf.schema.GetIndexByName(rKeyName); if rIdx == -1 { panic(fmt.Sprintf("Join: right key '%s' not found", rKeyName)) }
		rightKeyDatums = append(rightKeyDatums, arrow.NewArrayDatum(otherArrowDf.record.Column(rIdx)))
		if !arrow.TypeEqual(adf.record.Column(lIdx).DataType(), otherArrowDf.record.Column(rIdx).DataType()) {
			panic(fmt.Sprintf("Join: type mismatch for key. L:'%s'(%s) R:'%s'(%s)", lKeyName, adf.record.Column(lIdx).DataType(), rKeyName, otherArrowDf.record.Column(rIdx).DataType()))
		}
	}
	if len(leftKeyDatums) == 0 { panic("JoinEqui and other key-based joins require join columns.") }
	defer func() { for _,d := range leftKeyDatums { d.Release() }; for _,d := range rightKeyDatums {d.Release()} }()

	var hjComputeJoinType compute.JoinType
	switch jointype {
	case df.JoinEqui: hjComputeJoinType = compute.InnerJoin
	case df.JoinLeft: hjComputeJoinType = compute.LeftOuterJoin
	case df.JoinRight: hjComputeJoinType = compute.RightOuterJoin
	case df.JoinOuter: hjComputeJoinType = compute.FullOuterJoin
	case df.JoinType("leftanti"): hjComputeJoinType = compute.LeftAntiJoin // Assuming string comparison for custom types
	default: panic(fmt.Sprintf("Join: unsupported join type %s for HashJoin path", jointype))
	}

	hjIndicesTable, err := compute.HashJoin(ctx, leftKeyDatums, rightKeyDatums,
		arrow.NewRecordDatum(adf.record), arrow.NewRecordDatum(otherArrowDf.record),
		hjComputeJoinType, compute.HashJoinOptions{LeftSuffix:"_L", RightSuffix:"_R"})
	if err != nil { panic(fmt.Sprintf("Join: HashJoin compute failed for type %s: %v", jointype, err)) }
	defer hjIndicesTable.Release()

	if isSemiOrAntiJoin {
		if hjIndicesTable.NumCols() != 1 { panic(fmt.Sprintf("Join: %s HashJoin result expected 1 col indices, got %d", jointype, hjIndicesTable.NumCols())) }
		hjTr, errTr := array.NewTableReader(hjIndicesTable, -1); if errTr != nil { panic(errTr) }; defer hjTr.Release()
		var finalRecord arrow.Record
		if hjTr.Next() {
			indicesRecord := hjTr.Record()
			leftIndicesArr := indicesRecord.Column(0)
			takenDatum, errTake := compute.Take(ctx, compute.TakeOptions{}, arrow.NewRecordDatum(adf.record), arrow.NewArrayDatum(leftIndicesArr))
			if errTake != nil { panic(fmt.Sprintf("Join: %s Take failed: %v", jointype, errTake)) }; defer takenDatum.Release()
			resultRecord, okRec := takenDatum.(*arrow.RecordDatum).Value().(arrow.Record); if !okRec { panic(fmt.Sprintf("Join: %s Take bad return", jointype)) }
			finalRecord = resultRecord
		} else {
		    if hjTr.Err() != nil { panic(fmt.Sprintf("Join: error reading %s HashJoin indices: %v", jointype, hjTr.Err())) }
			finalRecord = array.NewRecord(adf.schema.schema, nil, 0)
		}
		defer finalRecord.Release() // NewArrowDataFrameWithAllocator will retain it
		return NewArrowDataFrameWithAllocator(adf.name, finalRecord, adf.schema, adf.mem)
	}

	// Path for INNER, LEFT, RIGHT, FULL OUTER (uses fUser)
	if hjIndicesTable.NumCols() != 2 { panic(fmt.Sprintf("Join: HashJoin result expected 2 index cols for %s, got %d", jointype, hjIndicesTable.NumCols()))}
	outputInternalSchema := outputArrowDFSchema.schema; numOutputCols := outputInternalSchema.NumFields()
	finalBuilders := make([]array.Builder, numOutputCols);
	for i:=0; i<numOutputCols; i++ { finalBuilders[i] = builder.NewBuilder(adf.mem, outputInternalSchema.Field(i).Type) }
	defer func() { for _,b := range finalBuilders { if b != nil { b.Release() } } }()

	hjTr, errTr := array.NewTableReader(hjIndicesTable, -1); if errTr != nil { panic(errTr) }; defer hjTr.Release()
	var nilLeftRow, nilRightRow df.Row

	for hjTr.Next() {
		indicesRecord := hjTr.Record()
		leftIndicesArr := indicesRecord.Column(0).(*array.Int64); rightIndicesArr := indicesRecord.Column(1).(*array.Int64)
		for i := 0; i < int(indicesRecord.NumRows()); i++ {
			var leftRowView, rightRowView df.Row
			if leftIndicesArr.IsNull(i) {
				if nilLeftRow == nil { nilLeftRow = newNilArrowRow(adf.schema, adf.mem) }; leftRowView = nilLeftRow
			} else { lIdx := leftIndicesArr.Value(i); leftRowView, _ = NewArrowRowFromRecord(adf.schema, adf.record, int(lIdx)) }
			if rightIndicesArr.IsNull(i) {
				if nilRightRow == nil { nilRightRow = newNilArrowRow(otherArrowDf.schema, adf.mem) }; rightRowView = nilRightRow
			} else { rIdx := rightIndicesArr.Value(i); rightRowView, _ = NewArrowRowFromRecord(otherArrowDf.schema, otherArrowDf.record, int(rIdx)) }

			outputRows := fUser(leftRowView, rightRowView)
			for _, outRow := range outputRows {
				if outRow.Len() != numOutputCols { panic("Join: fUser row col count mismatch") }
				for c := 0; c < numOutputCols; c++ {
					val := outRow.Get(c); av, ok_av := val.(*arrowValue)
					if !ok_av && !val.IsNil() { panic(fmt.Sprintf("Join: fUser non-*arrowValue: %T", val)) }
					var scalarToAppend scalar.Scalar
					if val.IsNil() || !ok_av { scalarToAppend = scalar.NewNullScalar(finalBuilders[c].Type()) } else { scalarToAppend = av.val }

					// Using 3-argument appendScalarToBuilder from types.go
					errAppend := appendScalarToBuilder(finalBuilders[c], scalarToAppend, finalBuilders[c].Type())
					if errAppend != nil {
						panic(fmt.Sprintf("Join: append col %d (name: %s): %v. Scalar: %s, BuilderType: %s",c, outputInternalSchema.Field(c).Name, errAppend, scalarToAppend.DataType().Name(), finalBuilders[c].Type().Name()))
					}
				}
			}
		}
	}
	if hjTr.Err() != nil { panic(fmt.Sprintf("Join: error reading HashJoin indices: %v", hjTr.Err())) }

	finalCols := make([]arrow.Array, numOutputCols); var finalNumRows int64
	if numOutputCols > 0 && finalBuilders[0] != nil { finalNumRows = int64(finalBuilders[0].Len()) }
	for i, b := range finalBuilders { if b == nil {panic(fmt.Sprintf("Join: nil builder at index %d",i))}; finalCols[i] = b.NewArray() }
	finalRecord := array.NewRecord(outputInternalSchema, finalCols, finalNumRows)
	for _, col := range finalCols { col.Release() }; defer finalRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, finalRecord, outputArrowDFSchema, adf.mem)
}

func (adf *arrowDataFrame) Intersection(otherRaw df.DataFrame, cols ...string) df.DataFrame {
	if adf.record == nil { schemaToUse := adf.schema; if adf.schema == nil || adf.schema.schema == nil { emptyFields := []arrow.Field{}; schemaToUse = NewArrowDataFrameSchema(arrow.NewSchema(emptyFields,nil)).(*arrowDataFrameSchema) }; emptyRec := array.NewRecord(schemaToUse.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, schemaToUse, adf.mem) }
	if otherRaw == nil { panic("Intersection: other dataframe cannot be nil") }
	otherArrowDf, ok := otherRaw.(*arrowDataFrame); if !ok { panic(fmt.Sprintf("Intersection: expected *arrowDataFrame, got %T", otherRaw)) }
	if otherArrowDf.record == nil { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	actualJoinColsMap := make(map[string]string)
	if len(cols) == 0 {
		commonColsFound := false
		for _, name1 := range adf.schema.Names() {
			idx2 := otherArrowDf.schema.GetIndexByName(name1)
			if idx2 != -1 {
				type1 := adf.schema.schema.Field(adf.schema.GetIndexByName(name1)).Type; type2 := otherArrowDf.schema.schema.Field(idx2).Type
				if arrow.TypeEqual(type1, type2) { actualJoinColsMap[name1] = name1; commonColsFound = true }
			}
		}
		if !commonColsFound { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	} else {
		for _, colName := range cols {
			if !adf.schema.HasName(colName) { panic(fmt.Sprintf("Intersection: key col '%s' not in left df", colName)) }
			if !otherArrowDf.schema.HasName(colName) { panic(fmt.Sprintf("Intersection: key col '%s' not in right df", colName)) }
			type1 := adf.schema.schema.Field(adf.schema.GetIndexByName(colName)).Type; type2 := otherArrowDf.schema.schema.Field(otherArrowDf.schema.GetIndexByName(colName)).Type
			if !arrow.TypeEqual(type1, type2) { panic(fmt.Sprintf("Intersection: type mismatch for key col '%s'. L: %s, R: %s", colName, type1, type2)) }
			actualJoinColsMap[colName] = colName
		}
	}
	if len(actualJoinColsMap) == 0 { emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release(); return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem) }
	fSelectLeftRow := func(r1 df.Row, r2 df.Row) []df.Row { return []df.Row{r1} }
	joinedDf := adf.Join(adf.schema, otherArrowDf, df.JoinEqui, actualJoinColsMap, fSelectLeftRow)
	distinctResultDf := joinedDf.Distinct()
	if arrowJoinedDf, ok_join := joinedDf.(*arrowDataFrame); ok_join { arrowJoinedDf.Release() }
	return distinctResultDf
}
func (adf *arrowDataFrame) GroupBy(cols ...string) df.GroupedDataFrame {
	if adf.record == nil { panic("GroupBy called on nil dataframe record") }
	if len(cols) == 0 { panic("GroupBy requires at least one column name") }
	keyIndices := make([]int, len(cols))
	for i, name := range cols {
		idx := adf.schema.GetIndexByName(name)
		if idx == -1 { panic(fmt.Sprintf("GroupBy: column '%s' not found", name)) }
		keyIndices[i] = idx
	}
	ctx := compute.WithAllocator(context.Background(), adf.mem)
	keyColsFromRec := make([]arrow.Array, len(keyIndices)); keyFieldsForSchema := make([]arrow.Field, len(keyIndices))
	for i, ki := range keyIndices { keyColsFromRec[i] = adf.record.Column(ki); keyColsFromRec[i].Retain(); keyFieldsForSchema[i] = adf.schema.schema.Field(ki) }
	keysOnlyRecSchema := arrow.NewSchema(keyFieldsForSchema, nil)
	keysOnlyRec := array.NewRecord(keysOnlyRecSchema, keyColsFromRec, adf.record.NumRows())
	for _, col := range keyColsFromRec { col.Release() }; defer keysOnlyRec.Release()
	tblReader, err := array.NewRecordReader(keysOnlyRecSchema, []arrow.Record{keysOnlyRec})
	if err != nil { panic(fmt.Sprintf("GroupBy: failed to create record reader for keys: %v", err)) }; defer tblReader.Release()
	keysOnlyTable, err := array.NewTableFromReader(tblReader, -1)
	if err != nil { panic(fmt.Sprintf("GroupBy: failed to create keysOnlyTable: %v", err)) };	defer keysOnlyTable.Release()
	keyColNames := make([]string, len(keyIndices)); for i, ki := range keyIndices { keyColNames[i] = adf.schema.schema.Field(ki).Name }
	uniqueKeysResultTable, err := keysOnlyTable.Distinct(ctx, keyColNames...)
	if err != nil { panic(fmt.Sprintf("GroupBy: failed to get distinct keys: %v", err)) };
	adf.record.Retain()
	return &arrowGroupedDataFrame{ originalRecord: adf.record, originalSchema: adf.schema, groupingColNames: cols, uniqueKeysTable: uniqueKeysResultTable, mem: adf.mem, }
}

func (adf *arrowDataFrame) Except(otherRaw df.DataFrame, cols ...string) df.DataFrame {
	if adf.record == nil || adf.record.NumRows() == 0 {
		schemaToUse := adf.schema
		if adf.schema == nil || adf.schema.schema == nil {
			emptyFields := []arrow.Field{}
			schemaToUse = NewArrowDataFrameSchema(arrow.NewSchema(emptyFields,nil)).(*arrowDataFrameSchema)
		}
		emptyRec := array.NewRecord(schemaToUse.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, schemaToUse, adf.mem)
	}
	if otherRaw == nil { panic("Except: other dataframe cannot be nil") }
	otherArrowDf, ok := otherRaw.(*arrowDataFrame)
	if !ok { panic(fmt.Sprintf("Except: expected *arrowDataFrame, got %T", otherRaw)) }
	if otherArrowDf.record == nil || otherArrowDf.record.NumRows() == 0 { return adf.Distinct() }

	actualJoinColsMap := make(map[string]string)
	if len(cols) == 0 {
		commonColsFound := false
		for _, name1 := range adf.schema.Names() {
			idx2 := otherArrowDf.schema.GetIndexByName(name1)
			if idx2 != -1 {
				type1 := adf.schema.schema.Field(adf.schema.GetIndexByName(name1)).Type
				type2 := otherArrowDf.schema.schema.Field(idx2).Type
				if arrow.TypeEqual(type1, type2) { actualJoinColsMap[name1] = name1; commonColsFound = true }
			}
		}
		// If no common columns with same type found for implicit join, result is all distinct rows of left.
		if !commonColsFound { return adf.Distinct() }
	} else {
		for _, colName := range cols {
			if !adf.schema.HasName(colName) { panic(fmt.Sprintf("Except: key col '%s' not in left df", colName)) }
			if !otherArrowDf.schema.HasName(colName) { panic(fmt.Sprintf("Except: key col '%s' not in right df", colName)) }
			type1 := adf.schema.schema.Field(adf.schema.GetIndexByName(colName)).Type
			type2 := otherArrowDf.schema.schema.Field(otherArrowDf.schema.GetIndexByName(colName)).Type
			if !arrow.TypeEqual(type1, type2) { panic(fmt.Sprintf("Except: type mismatch for key '%s'", colName)) }
			actualJoinColsMap[colName] = colName
		}
	}
	// If no join columns specified AND no common columns found (for implicit all-column join),
	// or if explicit cols were empty, it implies an except based on full row comparison.
	// The current HashJoin path requires specific key columns.
	// If actualJoinColsMap is empty but cols was also empty (meaning all-cols implicit join),
	// this means we need to use all columns as keys if they are compatible.
	// This part of logic for "all columns" except might need more specific handling if actualJoinColsMap remains empty.
	// For now, if no join keys, and it's not a zero-column DF, result is adf.Distinct().
	if len(actualJoinColsMap) == 0 && adf.record.NumCols() > 0 { return adf.Distinct() }
    if adf.record.NumCols() == 0 { return adf.Distinct() } // Except on an empty-column DF is itself distinct.

	// Use Join with JoinLeftAnti. fUser is nil as LeftAntiJoin produces rows from left table.
	// Output schema is the left table's schema.
	// Assuming JoinLeftAnti is defined in the df package or as a recognized string const by Join.
	leftAntiJoinedDf := adf.Join(adf.schema, otherArrowDf, df.JoinType("leftanti"), actualJoinColsMap, nil)

	// The result of LeftAntiJoin already contains rows from 'adf' not in 'other'.
	// Now, make these rows distinct.
	resultDf := leftAntiJoinedDf.Distinct()

	if arrowJoinedDf, ok_join := leftAntiJoinedDf.(*arrowDataFrame); ok_join {
		arrowJoinedDf.Release()
	}
	return resultDf
}


func (adf *arrowDataFrame) Select(expressions ...df.Expr) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) Rename(name string, inplace bool) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) AsFormat(t map[string]df.Format) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) UpdateSeries(index int, series df.Series) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) UpdateSeriesByName(name string, series df.Series) df.DataFrame { /* ... */ }
func (adf *arrowDataFrame) ForEachRow(f func(df.Row)) { /* ... */ }

var _ df.DataFrame = (*arrowDataFrame)(nil)
