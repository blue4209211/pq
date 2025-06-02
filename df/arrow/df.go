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

// arrowDataFrame is the Arrow-based implementation of the df.DataFrame interface.
type arrowDataFrame struct {
	name   string
	schema *arrowDataFrameSchema
	record arrow.Record
	mem    memory.Allocator
}

// NewArrowDataFrame creates a new Arrow-based DataFrame from an arrow.Record.
func NewArrowDataFrame(name string, record arrow.Record, dfSchema *arrowDataFrameSchema) df.DataFrame {
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, memory.DefaultAllocator)
}

// NewArrowDataFrameWithAllocator creates a new DataFrame with a specific allocator.
func NewArrowDataFrameWithAllocator(name string, record arrow.Record, dfSchema *arrowDataFrameSchema, mem memory.Allocator) df.DataFrame {
	if record == nil {
		panic("arrow.Record cannot be nil")
	}
	if dfSchema == nil {
		panic("df.DataFrameSchema cannot be nil")
	}
	if mem == nil {
		panic("memory.Allocator cannot be nil")
	}
	if !dfSchema.schema.Equal(record.Schema()) {
		panic(fmt.Sprintf("provided df.DataFrameSchema's internal arrow.Schema does not match record schema.\nProvided: %s\nRecord: %s", dfSchema.schema, record.Schema()))
	}

	record.Retain()
	return &arrowDataFrame{
		name:   name,
		schema: dfSchema,
		record: record,
		mem:    mem,
	}
}


// NewArrowDataFrameFromArrays creates a DataFrame from a slice of columns (arrow.Array).
func NewArrowDataFrameFromArrays(name string, cols []arrow.Array, schema *arrow.Schema) (df.DataFrame, error) {
	return NewArrowDataFrameFromArraysWithAllocator(name, cols, schema, memory.DefaultAllocator)
}

// NewArrowDataFrameFromArraysWithAllocator creates a DataFrame from arrays with a specific allocator.
func NewArrowDataFrameFromArraysWithAllocator(name string, cols []arrow.Array, schema *arrow.Schema, mem memory.Allocator) (df.DataFrame, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow.Schema cannot be nil")
	}
	if mem == nil {
		return nil, fmt.Errorf("memory.Allocator cannot be nil")
	}
	if len(cols) != schema.NumFields() {
		return nil, fmt.Errorf("number of columns (%d) does not match number of fields in schema (%d)", len(cols), schema.NumFields())
	}

	var numRows int64 = -1
	if len(cols) > 0 {
		// Retain columns before length/type checks, release if checks fail
		for i := range cols {
			cols[i].Retain()
		}

		numRows = int64(cols[0].Len())
		for i, col := range cols {
			if int64(col.Len()) != numRows {
				for j := 0; j <= i; j++ { // Release already retained columns
					cols[j].Release()
				}
				return nil, fmt.Errorf("column %d (%s) has length %d, expected %d", i, schema.Field(i).Name, col.Len(), numRows)
			}
			if !arrow.TypeEqual(col.DataType(), schema.Field(i).Type) {
				for j := 0; j <= i; j++ { // Release already retained columns
					cols[j].Release()
				}
				return nil, fmt.Errorf("column %d (%s) has type %s, schema expects %s", i, schema.Field(i).Name, col.DataType(), schema.Field(i).Type)
			}
		}
	} else {
		numRows = 0
	}

	// array.NewRecord retains the columns passed to it.
	record := array.NewRecord(schema, cols, numRows)
	// Since NewRecord has retained them, we can release our initial retains.
	for _, col := range cols {
		col.Release()
	}

	dfSchema := NewArrowDataFrameSchema(schema).(*arrowDataFrameSchema)
	// NewArrowDataFrameWithAllocator will retain the record again.
	// We defer Release on the record created here as it's an intermediate object before
	// being passed to the constructor.
	defer record.Release()
	return NewArrowDataFrameWithAllocator(name, record, dfSchema, mem), nil
}


func (adf *arrowDataFrame) Schema() df.DataFrameSchema { return adf.schema }
func (adf *arrowDataFrame) Name() string { return adf.name }
func (adf *arrowDataFrame) Len() int64 {
	if adf.record == nil { return 0 }
	return adf.record.NumRows()
}
func (adf *arrowDataFrame) Release() {
	if adf.record != nil {
		adf.record.Release()
		adf.record = nil
	}
}
func (adf *arrowDataFrame) GetSeries(index int) df.Series {
	if adf.record == nil || index < 0 || index >= int(adf.record.NumCols()) {
		panic(fmt.Sprintf("series index %d out of bounds", index))
	}
	// The column itself is not retained here, NewArrowSeriesWithAllocator will retain it.
	return NewArrowSeriesWithAllocator(adf.record.Column(index), adf.schema.Get(index), adf.mem)
}
func (adf *arrowDataFrame) GetSeriesByName(sName string) df.Series {
	idx := adf.schema.GetIndexByName(sName)
	if idx == -1 { panic(fmt.Sprintf("series '%s' not found", sName)) }
	return adf.GetSeries(idx)
}
func (adf *arrowDataFrame) GetRow(i int64) df.Row {
    if adf.record == nil || i < 0 || i >= adf.record.NumRows() {
        panic(fmt.Sprintf("row index %d out of bounds", i))
    }
    r, err := NewArrowRowFromRecord(adf.schema, adf.record, int(i))
    if err != nil { panic(err) } // Should ideally not happen with bounds check
    return r
}
func (adf *arrowDataFrame) GetValue(rowIndx, colIndx int) df.Value {
    if adf.record == nil || rowIndx < 0 || int64(rowIndx) >= adf.record.NumRows() || colIndx < 0 || colIndx >= int(adf.record.NumCols()) {
        panic(fmt.Sprintf("GetValue index (row: %d, col: %d) out of bounds", rowIndx, colIndx))
    }
    // scalar.MakeScalar does not retain the column, which is fine as the column is part of the record.
    return NewArrowValue(scalar.MakeScalar(adf.record.Column(colIndx), rowIndx), adf.schema.Get(colIndx).Format)
}
func (adf *arrowDataFrame) Limit(offset int, size int) df.DataFrame {
	if adf.record == nil {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0) // Assuming adf.schema is valid
		defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	currentNumRows := adf.record.NumRows()
	if offset < 0 { offset = 0 }
	if offset >= int(currentNumRows) {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0)
		defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	if offset+size > int(currentNumRows) { size = int(currentNumRows) - offset }
	if size <= 0 {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0)
		defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	slicedRecord := adf.record.NewSlice(int64(offset), int64(offset+size))
	defer slicedRecord.Release() // NewArrowDataFrameWithAllocator will retain it.
	return NewArrowDataFrameWithAllocator(adf.name, slicedRecord, adf.schema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesIndex(indices ...int) df.DataFrame {
	numRowsToKeep := int64(0)
	if adf.record != nil {
		numRowsToKeep = adf.record.NumRows()
	}

	if len(indices) == 0 {
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil)
		emptyDfSchema := NewArrowDataFrameSchema(emptyArrowSchema).(*arrowDataFrameSchema)
		emptyRecord := array.NewRecord(emptyArrowSchema, nil, numRowsToKeep)
		defer emptyRecord.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRecord, emptyDfSchema, adf.mem)
	}
	if adf.record == nil { // Cannot select columns if record is nil and indices are provided
		panic("cannot select columns from a nil or released dataframe")
	}

	newFields := make([]arrow.Field, len(indices))
	newCols := make([]arrow.Array, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= int(adf.record.NumCols()) {
			for j := 0; j < i; j++ { newCols[j].Release() } // Release already retained columns
			panic(fmt.Sprintf("select index %d out of bounds", idx))
		}
		newFields[i] = adf.schema.schema.Field(idx)
		newCols[i] = adf.record.Column(idx)
		newCols[i].Retain()
	}
	newArrowSchema := arrow.NewSchema(newFields, adf.schema.schema.Metadata())
	newDfSchema := NewArrowDataFrameSchema(newArrowSchema).(*arrowDataFrameSchema)
	selectedRecord := array.NewRecord(newArrowSchema, newCols, numRowsToKeep)
	for _, col := range newCols { col.Release() } // NewRecord has retained them.
	defer selectedRecord.Release() // NewArrowDataFrameWithAllocator will retain it.
	return NewArrowDataFrameWithAllocator(adf.name, selectedRecord, newDfSchema, adf.mem)
}
func (adf *arrowDataFrame) SelectBySeriesName(colNames ...string) df.DataFrame {
	if adf.record == nil && len(colNames) > 0 {
		panic("cannot select by name from a nil or released dataframe")
	}
	// If adf.record is nil and colNames is empty, SelectBySeriesIndex will handle it.

	indices := make([]int, len(colNames))
	for i, name := range colNames {
		idx := adf.schema.GetIndexByName(name)
		if idx == -1 { panic(fmt.Sprintf("column '%s' not found", name)) }
		indices[i] = idx
	}
	return adf.SelectBySeriesIndex(indices...)
}
func (adf *arrowDataFrame) WhereRow(f func(df.Row) bool) df.DataFrame {
	if adf.record == nil {
		emptyRec := array.NewRecord(adf.schema.schema, nil, 0)
		defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(adf.name, emptyRec, adf.schema, adf.mem)
	}
	numCols := int(adf.record.NumCols())
	currentSchema := adf.schema.schema
	colBuilders := make([]array.Builder, numCols)
	for i := 0; i < numCols; i++ { colBuilders[i] = builder.NewBuilder(adf.mem, currentSchema.Field(i).Type) }
	defer func() { for _, b := range colBuilders { if b != nil { b.Release() } } }()
	for r := int64(0); r < adf.record.NumRows(); r++ {
		rowView, _ := NewArrowRowFromRecord(adf.schema, adf.record, int(r))
		if f(rowView) {
			for c := 0; c < numCols; c++ {
				if err := array.CopyValue(colBuilders[c], adf.record.Column(c), int(r)); err != nil {
					panic(fmt.Sprintf("error copying value for col %d, row %d: %v", c, r, err))
				}
			}
		}
	}
	newCols := make([]arrow.Array, numCols)
	var newRecordLen int64 = 0
	if len(colBuilders) > 0 && colBuilders[0] != nil {
		newRecordLen = int64(colBuilders[0].Len())
	}
	for i, b := range colBuilders { newCols[i] = b.NewArray() }
	filteredRecord := array.NewRecord(currentSchema, newCols, newRecordLen)
	for _, col := range newCols { col.Release() } // NewRecord has retained them.
	defer filteredRecord.Release() // NewArrowDataFrameWithAllocator will retain it.
	return NewArrowDataFrameWithAllocator(adf.name, filteredRecord, adf.schema, adf.mem)
}

func (adf *arrowDataFrame) Sort(orders ...df.SortByIndex) df.DataFrame {
	if adf.record == nil || adf.record.NumRows() == 0 || len(orders) == 0 {
		// Create a new record with the same schema and columns but potentially a new reference.
		// If adf.record is nil (e.g. after Release), this needs to be handled.
		// Let's assume if adf.record is nil, schema might still be valid for creating an empty record.
		var recToCopy arrow.Record
		if adf.record != nil {
			recToCopy = adf.record
		} else {
			// Create an empty record with the schema if the original record is nil
			emptyInnerRec := array.NewRecord(adf.schema.schema, nil, 0)
			defer emptyInnerRec.Release()
			return NewArrowDataFrameWithAllocator(adf.name, emptyInnerRec, adf.schema, adf.mem)
		}
		// NewSlice creates a new view, which should be retained by the new DataFrame.
		newRecView := recToCopy.NewSlice(0, recToCopy.NumRows())
		defer newRecView.Release() // NewArrowDataFrameWithAllocator will retain it.
		return NewArrowDataFrameWithAllocator(adf.name, newRecView, adf.schema, adf.mem)
	}

	ctx := compute.WithAllocator(context.Background(), adf.mem)

	sortKeys := make([]compute.SortKey, len(orders))
	for i, order := range orders {
		if order.Series < 0 || order.Series >= int(adf.record.NumCols()) {
			panic(fmt.Sprintf("sort key index %d out of bounds", order.Series))
		}
		arrowSortOrder := arrow.Ascending
		if order.Order == df.SortOrderDESC {
			arrowSortOrder = arrow.Descending
		}
		sortKeys[i] = compute.SortKey{
			Name:  adf.schema.schema.Field(order.Series).Name,
			Order: arrowSortOrder,
		}
	}

	indicesDatum, err := compute.SortIndices(ctx, arrow.NewRecordDatum(adf.record), compute.SortOptions{SortKeys: sortKeys, NullPlacement: arrow.NullsFirst})
	if err != nil {
		panic(fmt.Sprintf("failed to get sort indices for dataframe: %v", err))
	}
	defer indicesDatum.Release()

	indicesArr, ok := indicesDatum.(*arrow.ArrayDatum).Value.(arrow.Array)
	if !ok {
		panic("SortIndices on record did not return an array datum as expected")
	}

	sortedRecordDatum, err := compute.Take(ctx, compute.TakeOptions{}, arrow.NewRecordDatum(adf.record), arrow.NewArrayDatum(indicesArr))
	if err != nil {
		panic(fmt.Sprintf("failed to take sorted rows for dataframe: %v", err))
	}
	defer sortedRecordDatum.Release()

	sortedRecord, ok := sortedRecordDatum.(*arrow.RecordDatum).Value().(arrow.Record)
	if !ok {
		panic("Take on record did not return a record datum as expected")
	}
    // NewArrowDataFrameWithAllocator will Retain the sortedRecord.
	return NewArrowDataFrameWithAllocator(adf.name, sortedRecord, adf.schema, adf.mem)
}

func (adf *arrowDataFrame) SortByName(orders ...df.SortByName) df.DataFrame {
	if adf.record == nil && len(orders) > 0 {
        panic("cannot sort by name on a nil or released dataframe")
    }
	if len(orders) == 0 {
		var recToCopy arrow.Record
		if adf.record != nil {
			recToCopy = adf.record
		} else {
			emptyInnerRec := array.NewRecord(adf.schema.schema, nil, 0)
			defer emptyInnerRec.Release()
			return NewArrowDataFrameWithAllocator(adf.name, emptyInnerRec, adf.schema, adf.mem)
		}
		newRecView := recToCopy.NewSlice(0, recToCopy.NumRows())
		defer newRecView.Release()
		return NewArrowDataFrameWithAllocator(adf.name, newRecView, adf.schema, adf.mem)
	}

	sortByIdx := make([]df.SortByIndex, len(orders))
	for i, order := range orders {
		idx := adf.schema.GetIndexByName(order.Series)
		if idx == -1 {
			panic(fmt.Sprintf("column '%s' not found for SortByName", order.Series))
		}
		sortByIdx[i] = df.SortByIndex{Series: idx, Order: order.Order}
	}
	return adf.Sort(sortByIdx...)
}


// Placeholders for other methods
func (adf *arrowDataFrame) Rename(name string, inplace bool) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Select(e ...df.Expr) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) MapRow(schema df.DataFrameSchema, f func(df.Row) df.Row) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) FlatMapRow(schema df.DataFrameSchema, f func(df.Row) []df.Row) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) WhenNil(t map[string]df.Value) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) When(t map[string]map[any]df.Value) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) AsFormat(t map[string]df.Format) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) GetSeriesExprByName(s string) df.Expr { panic("not implemented") }
func (adf *arrowDataFrame) AddSeries(name string, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeries(index int, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) UpdateSeriesByName(name string, series df.Series) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) RenameSeries(index int, name string, inplace bool) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) RenameSeriesByName(col string, name string, inplace bool) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) RemoveSeries(index int) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) RemoveSeriesByName(s string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) ForEachRow(f func(df.Row)) { panic("not implemented") }
func (adf *arrowDataFrame) Group(others ...string) df.GroupedDataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Append(d df.DataFrame) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Distinct(cols ...string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Join(schema df.DataFrameSchema, d df.DataFrame, jointype df.JoinType, cols map[string]string, f func(df.Row, df.Row) []df.Row) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Union(d df.DataFrame) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Intersection(d df.DataFrame, col ...string) df.DataFrame { panic("not implemented") }
func (adf *arrowDataFrame) Except(d df.DataFrame, col ...string) df.DataFrame { panic("not implemented") }

var _ df.DataFrame = (*arrowDataFrame)(nil)
