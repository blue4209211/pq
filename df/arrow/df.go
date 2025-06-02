//go:build arrow

package arrow

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	// "github.com/apache/arrow/go/v14/arrow/memory" // We might need a memory allocator
	"github.com/blue4209211/pq/df"
)

// arrowDataFrame is the Arrow-based implementation of the df.DataFrame interface.
type arrowDataFrame struct {
	name   string
	schema *arrowDataFrameSchema // Store the schema for the DataFrame
	record arrow.Record          // For now, assume a single record holds all data.
	                            // This can be extended to []arrow.Record or arrow.Table.
}

// NewArrowDataFrame creates a new Arrow-based DataFrame from an arrow.Record.
// The dfSchema should correspond to the record.Schema().
func NewArrowDataFrame(name string, record arrow.Record, dfSchema *arrowDataFrameSchema) df.DataFrame {
	if record == nil {
		panic("arrow.Record cannot be nil")
	}
	if dfSchema == nil {
		// Or, construct dfSchema from record.Schema()
		panic("df.DataFrameSchema cannot be nil")
	}
	// It would be good to validate that dfSchema.schema is equivalent to record.Schema()
	if !dfSchema.schema.Equal(record.Schema()) {
		panic(fmt.Sprintf("provided df.DataFrameSchema does not match record schema.\nProvided: %s\nRecord: %s", dfSchema.schema, record.Schema()))
	}

	record.Retain() // Retain the record as we are storing it.
	return &arrowDataFrame{
		name:   name,
		schema: dfSchema,
		record: record,
	}
}

// NewArrowDataFrameFromArrays creates a DataFrame from a slice of columns (arrow.Array).
// This is a common way to construct tables/records.
func NewArrowDataFrameFromArrays(name string, cols []arrow.Array, schema *arrow.Schema) (df.DataFrame, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow.Schema cannot be nil")
	}
	if len(cols) != schema.NumFields() {
		return nil, fmt.Errorf("number of columns (%d) does not match number of fields in schema (%d)", len(cols), schema.NumFields())
	}

	// Validate that all columns have the same length
	var numRows int64 = -1
	if len(cols) > 0 {
		numRows = int64(cols[0].Len())
		for i, col := range cols {
			if int64(col.Len()) != numRows {
				return nil, fmt.Errorf("column %d (%s) has length %d, expected %d", i, schema.Field(i).Name, col.Len(), numRows)
			}
			if !arrow.TypeEqual(col.DataType(), schema.Field(i).Type) {
				return nil, fmt.Errorf("column %d (%s) has type %s, schema expects %s", i, schema.Field(i).Name, col.DataType(), schema.Field(i).Type)
			}
			col.Retain() // Retain each column
		}
	} else {
		numRows = 0
	}


	record := array.NewRecord(schema, cols, numRows)
	// NewArrowDataFrame expects a *arrowDataFrameSchema, so we create one.
	dfSchema := NewArrowDataFrameSchema(schema).(*arrowDataFrameSchema)

	// No need to call record.Release() here if NewArrowDataFrame retains it.
	// However, cols were retained, and NewRecord also retains them.
	// If NewArrowDataFrame makes its own retain on record, then cols can be released here.
	// For safety, let NewArrowDataFrame manage the record's lifecycle.
	// The caller of NewArrowDataFrameFromArrays should release cols if they are no longer needed after this call.
	// After record is created with array.NewRecord, it holds references to the columns.
    // The individual column arrays (cols) passed into this function can be released by the caller
    // if they are not needed anymore, as the record now has its own references.
    // Releasing them here would be premature if the caller still needs them.
    // However, if this function is the definitive constructor and takes ownership,
    // then releasing cols after record creation (and its own retain) would be correct.
    // For now, this is okay, assuming record handles its column references.
	defer record.Release() // Release the record created by NewRecord as NewArrowDataFrame will retain it again.

	return NewArrowDataFrame(name, record, dfSchema), nil
}


func (adf *arrowDataFrame) Schema() df.DataFrameSchema {
	return adf.schema
}

func (adf *arrowDataFrame) Name() string {
	return adf.name
}

func (adf *arrowDataFrame) Len() int64 {
	if adf.record == nil {
		return 0
	}
	return adf.record.NumRows()
}

func (adf *arrowDataFrame) GetSeries(index int) df.Series {
	if adf.record == nil || index < 0 || index >= int(adf.record.NumCols()) {
		panic(fmt.Sprintf("series index %d out of bounds for dataframe with %d columns", index, adf.record.NumCols()))
	}
	col := adf.record.Column(index)
	// The SeriesSchema needs to be derived from the DataFrameSchema for this specific column
	seriesSchema := adf.schema.Get(index) // This is df.SeriesSchema
	return NewArrowSeries(col, seriesSchema)
}

func (adf *arrowDataFrame) GetSeriesByName(sName string) df.Series {
	idx := adf.schema.GetIndexByName(sName)
	if idx == -1 {
		panic(fmt.Sprintf("series with name '%s' not found", sName))
	}
	return adf.GetSeries(idx)
}

// Placeholder implementations for remaining df.DataFrame methods

func (adf *arrowDataFrame) Rename(name string, inplace bool) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Limit(offset int, size int) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Sort(order ...df.SortByIndex) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) SortByName(order ...df.SortByName) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Select(e ...df.Expr) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) SelectBySeriesIndex(index ...int) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) SelectBySeriesName(col ...string) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) MapRow(schema df.DataFrameSchema, f func(df.Row) df.Row) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) FlatMapRow(schema df.DataFrameSchema, f func(df.Row) []df.Row) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) WhereRow(f func(df.Row) bool) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) WhenNil(t map[string]df.Value) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) When(t map[string]map[any]df.Value) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) AsFormat(t map[string]df.Format) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) GetSeriesExprByName(s string) df.Expr {
	panic("not implemented")
}

func (adf *arrowDataFrame) AddSeries(name string, series df.Series) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) UpdateSeries(index int, series df.Series) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) UpdateSeriesByName(name string, series df.Series) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) RenameSeries(index int, name string, inplace bool) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) RenameSeriesByName(col string, name string, inplace bool) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) RemoveSeries(index int) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) RemoveSeriesByName(s string) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) GetRow(i int64) df.Row {
    if adf.record == nil || i < 0 || i >= adf.record.NumRows() {
        panic(fmt.Sprintf("row index %d out of bounds for dataframe with %d rows", i, adf.record.NumRows()))
    }
    // Use the NewArrowRowFromRecord constructor we defined in types.go
    row, err := NewArrowRowFromRecord(adf.schema, adf.record, int(i))
    if err != nil {
        // This should ideally not happen if bounds are checked, but good practice.
        panic(fmt.Sprintf("failed to create arrowRow from record: %v", err))
    }
    return row
}

func (adf *arrowDataFrame) ForEachRow(f func(df.Row)) {
	panic("not implemented")
}

func (adf *arrowDataFrame) Group(others ...string) df.GroupedDataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Append(d df.DataFrame) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Distinct(cols ...string) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Join(schema df.DataFrameSchema, d df.DataFrame, jointype df.JoinType, cols map[string]string, f func(df.Row, df.Row) []df.Row) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Union(d df.DataFrame) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Intersection(d df.DataFrame, col ...string) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) Except(d df.DataFrame, col ...string) df.DataFrame {
	panic("not implemented")
}

func (adf *arrowDataFrame) GetValue(rowIndx, colIndx int) df.Value {
    if adf.record == nil || rowIndx < 0 || int64(rowIndx) >= adf.record.NumRows() || colIndx < 0 || colIndx >= int(adf.record.NumCols()) {
        panic(fmt.Sprintf("GetValue index (row: %d, col: %d) out of bounds", rowIndx, colIndx))
    }
    column := adf.record.Column(colIndx)
    scalarValue := scalar.MakeScalar(column.Data(), rowIndx)
    seriesSchema := adf.schema.Get(colIndx) // df.SeriesSchema
    return NewArrowValue(scalarValue, seriesSchema.Format)
}

// Ensure arrowDataFrame implements the df.DataFrame interface.
var _ df.DataFrame = (*arrowDataFrame)(nil)

// Destructor-like method to release the record.
// This is not part of the df.DataFrame interface but useful for managing Arrow resources.
func (adf *arrowDataFrame) Release() {
	if adf.record != nil {
		adf.record.Release()
		adf.record = nil
	}
}
