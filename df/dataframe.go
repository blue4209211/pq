package df

import (
	"reflect"
)

// Column metadata for dataframe column
type Column struct {
	Name   string
	Format DataFrameSeriesFormat
}

// SortOrder defines type for sorting
type SortOrder int

// SortOrderASC ascending sort order
const SortOrderASC SortOrder = 0

// SortOrderDESC descendin sort order
const SortOrderDESC SortOrder = 1

// DataFrameSeriesFormat Datatype of Dataframe Column
type DataFrameSeriesFormat interface {
	Name() string
	Type() reflect.Kind
	Convert(i interface{}) (interface{}, error)
}

type SortByIndex struct {
	Column int
	Order  SortOrder
}

type SortByName struct {
	Column string
	Order  SortOrder
}

// DataFrame Data container for storing tabular data
type DataFrame interface {
	Schema() DataFrameSchema
	Name() string
	Get(i int64) DataFrameRow
	Len() int64

	Rename(name string, inplace bool) DataFrame
	Column(index int) DataFrameSeries
	ColumnByName(s string) DataFrameSeries
	AddColumn(name string, series DataFrameSeries) (DataFrame, error)
	RemoveColumn(index int) DataFrame
	RemoveColumnByName(s string) DataFrame
	RenameColumn(index int, name string, inplace bool) (DataFrame, error)
	RenameColumnByName(col string, name string, inplace bool) (DataFrame, error)
	SelectColumn(index ...int) (DataFrame, error)
	SelectColumnByName(col ...string) (DataFrame, error)

	ForEach(function func(DataFrameRow))
	Sort(order ...SortByIndex) DataFrame
	SortByName(order ...SortByName) DataFrame
	Map(schema []Column, function func(DataFrameRow) []interface{}) DataFrame
	FlatMap(schema []Column, function func(DataFrameRow) [][]interface{}) DataFrame
	Filter(function func(DataFrameRow) bool) DataFrame
	Limit(offset int, size int) DataFrame
}

// DataFrameSeries Type for Storing column data of Dataframe
type DataFrameSeries interface {
	Schema() DataFrameSeriesFormat
	Len() int64
	Get(index int64) interface{}
	ForEach(f func(interface{}))
	Sort(order SortOrder) DataFrameSeries
	Map(schema DataFrameSeriesFormat, function func(interface{}) interface{}) DataFrameSeries
	FlatMap(schema DataFrameSeriesFormat, function func(interface{}) []interface{}) DataFrameSeries
	Filter(function func(interface{}) bool) DataFrameSeries
	Limit(offset int, size int) DataFrameSeries
	Distinct() DataFrameSeries
}

// DataFrameRow Type representing row data of Dataframe
type DataFrameRow interface {
	Schema() DataFrameSchema
	Get(i int) interface{}
	GetByName(s string) interface{}
	Data() []interface{}
	Len() int
}

// DataFrameSchema Type representing schema of Dataframe
type DataFrameSchema interface {
	Columns() []Column
	GetByName(s string) (Column, error)
	GetIndexByName(s string) (int, error)
	Get(i int) Column
	Len() int
}
