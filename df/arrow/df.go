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

// arrowDataFrame struct and existing constructors/methods (Schema, Name, Len, etc.) are assumed here.
// For brevity, only new/modified methods are shown.
// --- Re-include necessary parts of arrowDataFrame and its constructors ---
type arrowDataFrame struct {
	name   string
	schema *arrowDataFrameSchema
	record arrow.Record
	mem    memory.Allocator
}
func NewArrowDataFrame(name string, record arrow.Record, dfSchema *arrowDataFrameSchema) df.DataFrame {
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, memory.DefaultAllocator)
}
func NewArrowDataFrameWithAllocator(name string, record arrow.Record, dfSchema *arrowDataFrameSchema, mem memory.Allocator) df.DataFrame {
	if record == nil { panic("arrow.Record cannot be nil") }
	if dfSchema == nil { panic("df.DataFrameSchema cannot be nil") }
	if mem == nil { panic("memory.Allocator cannot be nil") }
	if !dfSchema.schema.Equal(record.Schema()) {
		panic(fmt.Sprintf("provided df.DataFrameSchema's internal arrow.Schema does not match record schema.\nProvided: %s\nRecord: %s", dfSchema.schema, record.Schema()))
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
		// Retain columns before length/type checks, release if checks fail
		for i := range cols {
			cols[i].Retain()
		}
		numRows = int64(cols[0].Len())
		for i, col := range cols {
			if int64(col.Len()) != numRows {
				for j := 0; j <= i; j++ { cols[j].Release() } // Release already retained columns
				return nil, fmt.Errorf("col %d len %d != %d", i, col.Len(), numRows)
			}
			if !arrow.TypeEqual(col.DataType(), schema.Field(i).Type) {
				for j := 0; j <= i; j++ { cols[j].Release() } // Release already retained columns
				return nil, fmt.Errorf("col %d type %s != schema %s", i, col.DataType(), schema.Field(i).Type)
			}
		}
	} else {numRows = 0}
	record := array.NewRecord(schema, cols, numRows) // NewRecord retains columns
	for _, col := range cols { col.Release() } // Release initial retain

	dfSchema := NewArrowDataFrameSchema(schema).(*arrowDataFrameSchema)
	// NewArrowDataFrameWithAllocator will retain the record again.
	// Defer release for the record created here.
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
	if adf.record == nil {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	currentNumRows := adf.record.NumRows(); if offset < 0 { offset = 0 }
	if offset >= int(currentNumRows) {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	if offset+size > int(currentNumRows) { size = int(currentNumRows) - offset }
	if size <= 0 {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	slicedRecord := adf.record.NewSlice(int64(offset), int64(offset+size)); defer slicedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, slicedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesIndex(indices ...int) df.DataFrame {
	numRowsToKeep := int64(0); if adf.record != nil { numRowsToKeep = adf.record.NumRows()}
	if len(indices) == 0 {
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil)
		emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, numRowsToKeep); defer emptyRecord.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRecord, emptyDfSchema, adf.mem)
	}
	if adf.record == nil { panic("cannot select columns from a nil or released dataframe") }
	newFields := make([]arrow.Field, len(indices)); newCols := make([]arrow.Array, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= int(adf.record.NumCols()) {
			for j := 0; j < i; j++ { newCols[j].Release() }
			panic(fmt.Sprintf("select index %d out of bounds", idx))
		}
		newFields[i] = adf.schema.schema.Field(idx); newCols[i] = adf.record.Column(idx); newCols[i].Retain()
	}
	newArrowSchema := arrow.NewSchema(newFields, adf.schema.schema.Metadata())
	newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	selectedRecord := array.NewRecord(newArrowSchema, newCols, numRowsToKeep)
	for _, col := range newCols { col.Release() }; defer selectedRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, selectedRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesName(colNames ...string) df.DataFrame {
	if adf.record == nil && len(colNames) > 0 { panic("cannot select by name from a nil or released dataframe") }
	if len(colNames) == 0 {
		numRowsToKeep := int64(0); if adf.record != nil { numRowsToKeep = adf.record.NumRows()}
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil)
		emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, numRowsToKeep); defer emptyRecord.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRecord, emptyDfSchema, adf.mem)
	}
	indices := make([]int, len(colNames))
	for i, name := range colNames { idx := adf.schema.GetIndexByName(name); if idx == -1 { panic(fmt.Sprintf("column '%s' not found", name)) }; indices[i] = idx }
	return adf.SelectBySeriesIndex(indices...)
}
func (adf *arrowDataFrame) WhereRow(f func(df.Row) bool) df.DataFrame {
	if adf.record == nil {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
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
	if err != nil { panic(fmt.Sprintf("failed to get sort indices for dataframe: %v", err)) }; defer indicesDatum.Release()
	indicesArr, ok := indicesDatum.(*arrow.ArrayDatum).Value.(arrow.Array); if !ok { panic("SortIndices on record did not return an array datum") }
	sortedRecordDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrow.NewRecordDatum(adf.record), arrow.NewArrayDatum(indicesArr))
	if err != nil { panic(fmt.Sprintf("failed to take sorted rows for dataframe: %v", err)) }; defer sortedRecordDatum.Release()
	sortedRecord, ok := sortedRecordDatum.(*arrow.RecordDatum).Value().(arrow.Record); if !ok { panic("Take on record did not return a record datum") }
    return NewArrowDataFrameWithAllocator(adf.name, sortedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) SortByName(orders ...df.SortByName) df.DataFrame {
	if adf.record == nil && len(orders) > 0 { panic("cannot sort by name on a nil or released dataframe") }
	if len(orders) == 0 {
		var recToHandle arrow.Record
		if adf.record != nil { recToHandle = adf.record.NewSlice(0, adf.record.NumRows()) } else { recToHandle = array.NewRecord(adf.schema.schema, nil, 0) }
		defer recToHandle.Release(); return NewArrowDataFrameWithAllocator(adf.name, recToHandle, adf.schema, adf.mem)
	}
	sortByIdx := make([]df.SortByIndex, len(orders))
	for i, order := range orders { idx := adf.schema.GetIndexByName(order.Series); if idx == -1 { panic(fmt.Sprintf("column '%s' not found for SortByName", order.Series)) }; sortByIdx[i] = df.SortByIndex{Series: idx, Order: order.Order} }
	return adf.Sort(sortByIdx...)
}
func (adf *arrowDataFrame) AddSeries(colName string, series df.Series) df.DataFrame {
	if adf.record == nil { panic("cannot add series to a nil dataframe") }
	if adf.schema.HasName(colName) { panic(fmt.Sprintf("dataframe already has a column named '%s'", colName)) }
	arrowSeries, ok := series.(*arrowSeries); if !ok { panic(fmt.Sprintf("cannot add series of type %T, expected *arrowSeries", series)) }
	if arrowSeries.arr == nil { panic("cannot add a nil arrowSeries array") }
	if arrowSeries.Len() != adf.Len() { panic(fmt.Sprintf("length mismatch: dataframe has %d rows, series has %d elements", adf.Len(), arrowSeries.Len())) }
	existingFields := adf.schema.schema.Fields(); newSchemaFields := make([]arrow.Field, len(existingFields)+1)
	copy(newSchemaFields, existingFields)
	newSchemaFields[len(existingFields)] = arrow.Field{Name: colName, Type: arrowSeries.arr.DataType(), Nullable: arrowSeries.arr.NullN() > 0}
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata())
	newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	existingCols := adf.record.Columns(); newRecordCols := make([]arrow.Array, len(existingCols)+1)
	for i, col := range existingCols { col.Retain(); newRecordCols[i] = col }
	arrowSeries.arr.Retain(); newRecordCols[len(existingCols)] = arrowSeries.arr
	newRecord := array.NewRecord(newArrowSchema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer newRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RemoveSeries(index int) df.DataFrame {
	if adf.record == nil { panic("cannot remove series from a nil dataframe") }
	if index < 0 || index >= int(adf.record.NumCols()) { panic(fmt.Sprintf("index %d out of bounds for RemoveSeries", index)) }
	numOldCols := int(adf.record.NumCols()); newSchemaFields := make([]arrow.Field, 0, numOldCols-1); newRecordCols := make([]arrow.Array, 0, numOldCols-1)
	for i := 0; i < numOldCols; i++ {
		if i == index { continue }
		newSchemaFields = append(newSchemaFields, adf.schema.schema.Field(i))
		col := adf.record.Column(i); col.Retain(); newRecordCols = append(newRecordCols, col)
	}
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata())
	newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	newRecord := array.NewRecord(newArrowSchema, newRecordCols, adf.record.NumRows())
	for _, col := range newRecordCols { col.Release() }; defer newRecord.Release()
	return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RemoveSeriesByName(s string) df.DataFrame {
	idx := adf.schema.GetIndexByName(s); if idx == -1 { panic(fmt.Sprintf("column '%s' not found for RemoveSeriesByName", s))}; return adf.RemoveSeries(idx)
}
func (adf *arrowDataFrame) RenameSeries(index int, newName string, inplace bool) df.DataFrame {
	if adf.record == nil { panic("cannot rename series in a nil dataframe") }
	if index < 0 || index >= int(adf.record.NumCols()) { panic(fmt.Sprintf("index %d out of bounds for RenameSeries", index)) }
	if currentName := adf.schema.schema.Field(index).Name; currentName == newName {
		if inplace { return adf }
		newRecView := adf.record.NewSlice(0, adf.record.NumRows()); defer newRecView.Release()
		return NewArrowDataFrameWithAllocator(adf.name, newRecView, adf.schema, adf.mem)
	}
	if adf.schema.HasName(newName) { panic(fmt.Sprintf("dataframe already has a column named '%s'", newName)) }
	newSchemaFields := make([]arrow.Field, adf.record.NumCols())
	for i, field := range adf.schema.schema.Fields() {
		if i == index { newSchemaFields[i] = arrow.Field{Name: newName, Type: field.Type, Nullable: field.Nullable, Metadata: field.Metadata}
		} else { newSchemaFields[i] = field }
	}
	newArrowSchema := arrow.NewSchema(newSchemaFields, adf.schema.schema.Metadata())
	newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	recordCols := adf.record.Columns(); for _, col := range recordCols { col.Retain() }
	newRecord := array.NewRecord(newArrowSchema, recordCols, adf.record.NumRows())
	for _, col := range recordCols { col.Release() }
	if inplace { adf.record.Release(); adf.record = newRecord; adf.schema = newDfSchema; return adf }
	defer newRecord.Release(); return NewArrowDataFrameWithAllocator(adf.name, newRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) RenameSeriesByName(colName string, newName string, inplace bool) df.DataFrame {
	idx := adf.schema.GetIndexByName(colName); if idx == -1 { panic(fmt.Sprintf("column '%s' not found for RenameSeriesByName", colName)) }; return adf.RenameSeries(idx, newName, inplace)
}

func (adf *arrowDataFrame) GetSeriesExprByName(sName string) df.Expr {
	idx := adf.schema.GetIndexByName(sName)
	if idx == -1 { panic(fmt.Sprintf("series with name '%s' not found for GetSeriesExprByName", sName)) }
	seriesSchema := adf.schema.Get(idx)
	switch seriesSchema.Format.Name() {
	case df.BoolFormat.Name(): return df.NewBoolColExpr(sName)
	case df.IntegerFormat.Name(): return df.NewIntColExpr(sName)
	case df.DoubleFormat.Name(): return df.NewDoubleColExpr(sName)
	case df.StringFormat.Name(): return df.NewStringColExpr(sName)
	case df.DateTimeFormat.Name(): return df.NewDatetimeColExpr(sName)
	default: panic(fmt.Sprintf("GetSeriesExprByName not supported for series format: %s", seriesSchema.Format.Name()))
	}
}

// --- Stubs for remaining methods ---
func (adf *arrowDataFrame) Select(e ...df.Expr) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) MapRow(schema df.DataFrameSchema, f func(df.Row) df.Row) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Distinct(cols ...string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Rename(name string, inplace bool) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) FlatMapRow(schema df.DataFrameSchema, f func(df.Row) []df.Row) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) WhenNil(t map[string]df.Value) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) When(t map[string]map[any]df.Value) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) AsFormat(t map[string]df.Format) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeries(index int, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeriesByName(name string, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) ForEachRow(f func(df.Row)) { panic("not implemented") }
func (adf *arrowDataFrame) Group(others ...string) df.GroupedDataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Append(d df.DataFrame) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Union(d df.DataFrame) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Intersection(d df.DataFrame, col ...string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Except(d df.DataFrame, col ...string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Join(schema df.DataFrameSchema, d df.DataFrame, jointype df.JoinType, cols map[string]string, f func(df.Row, df.Row) []df.Row) df.DataFrame { panic("not implemented") }

var _ df.DataFrame = (*arrowDataFrame)(nil)
