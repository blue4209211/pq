package df

import (
	"reflect"
	"time"
)

// Series metadata for dataframe column
type SeriesSchema struct {
	Name   string
	Format Format
}

// SortOrder defines type for sorting
type SortOrder int

// SortOrderASC ascending sort order
const (
	SortOrderASC  SortOrder = 0
	SortOrderDESC SortOrder = 1
)

// JoinType defines type for joining
type JoinType string

const (
	JoinLeft  JoinType = "left"
	JoinEqui  JoinType = "equi"
	JoinRight JoinType = "right"
	JoinOuter JoinType = "outer"
	JoinCross JoinType = "cross"
)

// Format Datatype of Dataframe Column
type Format interface {
	Name() string
	Type() reflect.Kind
	Convert(i any) (any, error)
}

type SortByIndex struct {
	Series int
	Order  SortOrder
}

type SortByName struct {
	Series string
	Order  SortOrder
}

// DataFrame Data container for storing tabular data
type DataFrame interface {
	Schema() DataFrameSchema
	Name() string
	Len() int64
	Rename(name string, inplace bool) DataFrame

	Limit(offset int, size int) DataFrame
	Sort(order ...SortByIndex) DataFrame
	SortByName(order ...SortByName) DataFrame

	SelectSeries(index ...int) DataFrame
	SelectSeriesByName(col ...string) DataFrame
	MapRow(schema DataFrameSchema, f func(Row) Row) DataFrame
	FlatMapRow(schema DataFrameSchema, f func(Row) []Row) DataFrame
	WhereRow(f func(Row) bool) DataFrame
	SelectRow(b Series) DataFrame

	GetSeries(index int) Series
	GetSeriesByName(s string) Series

	AddSeries(name string, series Series) DataFrame
	UpdateSeries(index int, series Series) DataFrame
	UpdateSeriesByName(name string, series Series) DataFrame
	RenameSeries(index int, name string, inplace bool) DataFrame
	RenameSeriesByName(col string, name string, inplace bool) DataFrame
	RemoveSeries(index int) DataFrame
	RemoveSeriesByName(s string) DataFrame

	GetRow(i int64) Row
	ForEachRow(f func(Row))

	Group(others ...string) GroupedDataFrame
	Append(df DataFrame) DataFrame
	Distinct() DataFrame
	Join(schema DataFrameSchema, df DataFrame, jointype JoinType, cols map[string]string, f func(Row, Row) []Row) DataFrame

	GetValue(rowIndx, colIndx int) Value
}

type GroupedDataFrame interface {
	GetGroupColumns() []string
	Get(index Row) DataFrame
	GetKeys() []Row
	ForEach(f func(Row, DataFrame))
	Map(f func(Row, DataFrame) DataFrame) GroupedDataFrame
	Where(f func(Row, DataFrame) bool) GroupedDataFrame
	Len() int64
}

// Series Type for Storing column data of Dataframe
type Series interface {
	Schema() SeriesSchema
	Len() int64
	Get(index int64) Value
	ForEach(f func(Value))
	Sort(order SortOrder) Series
	//TODO remove type args ?
	Map(schema Format, f func(Value) Value) Series
	FlatMap(schema Format, f func(Value) []Value) Series
	Reduce(f func(Value, Value) Value, startValue Value) Value
	Where(f func(Value) bool) Series
	Select(b Series) Series
	Limit(offset int, size int) Series
	Distinct() Series
	Copy() Series
	Group() GroupedSeries

	Append(series Series) Series
	Join(schema Format, series Series, jointype JoinType, f func(Value, Value) []Value) Series
}

type Value interface {
	Schema() Format
	Get() any
	GetAsString() string
	GetAsInt() int64
	GetAsDouble() float64
	GetAsBool() bool
	GetAsDatetime() time.Time
	IsNil() bool
	Equals(other Value) bool
}

type GroupedSeries interface {
	Get(index Value) Series
	GetKeys() []Value
	ForEach(f func(Value, Series))
	Map(f func(Value, Series) Series) GroupedSeries
	Where(f func(Value, Series) bool) GroupedSeries
	Len() int64
}

// Row Type representing row data of Dataframe
type Row interface {
	Schema() DataFrameSchema
	GetRaw(i int) any
	Get(i int) Value
	GetByName(s string) Value
	Len() int
	GetAsString(i int) string
	GetAsInt(i int) int64
	GetAsDouble(i int) float64
	GetAsBool(i int) bool
	GetAsDatetime(i int) time.Time
	GetMap() (r map[string]Value)
	IsAnyNil() bool
	IsNil(i int) bool
	Copy() Row
	Select(i ...int) Row
	Append(name string, v Value) Row
}

// DataFrameSchema Type representing schema of Dataframe
type DataFrameSchema interface {
	Series() []SeriesSchema
	Names() []string
	GetByName(s string) SeriesSchema
	GetIndexByName(s string) int
	HasName(s string) bool
	Get(i int) SeriesSchema
	Len() int
	Equals(other DataFrameSchema) bool
}
