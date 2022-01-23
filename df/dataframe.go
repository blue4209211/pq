package df

import (
	"reflect"
)

// Column metadata for dataframe column
type Column struct {
	Name   string
	Format DataFrameFormat
}

// DataFrameFormat Datatype of Dataframe Column
type DataFrameFormat interface {
	Name() string
	Type() reflect.Kind
	Convert(i interface{}) (interface{}, error)
}

// ForeachDataframeData type for looping over Dataframe
type ForeachDataframeData func(DataFrameRow)

// DataFrame Data container for storing tabular data
type DataFrame interface {
	Column(i int) DataFrameColumn
	Get(i int) DataFrameRow
	Len() int64
	Schema() []Column
	Name() string
	ForEach(f ForeachDataframeData)
}

// ForeachColumnData Type for looping over Dataframe column
type ForeachColumnData func(interface{})

// DataFrameColumn Type for Storing column data of Dataframe
type DataFrameColumn interface {
	Schema() Column
	Len() int64
	Get(i int) interface{}
	ForEach(f ForeachColumnData)
}

// DataFrameRow Type representing row data of Dataframe
type DataFrameRow interface {
	Schema() []Column
	Get(i int) interface{}
	Data() []interface{}
	Len() int64
}
