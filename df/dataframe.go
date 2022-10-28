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
	JoinReft  JoinType = "right"
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

	SelectSeries(index ...int) (DataFrame, error)
	SelectSeriesByName(col ...string) (DataFrame, error)
	MapRow(schema DataFrameSchema, f func(Row) Row) DataFrame
	FlatMapRow(schema DataFrameSchema, f func(Row) []Row) DataFrame
	Where(f func(Row) bool) DataFrame
	Select(b Series) DataFrame

	GetSeries(index int) Series
	GetSeriesByName(s string) Series

	AddSeries(name string, series Series) (DataFrame, error)
	UpdateSeries(index int, series Series) (DataFrame, error)
	UpdateSeriesByName(name string, series Series) (DataFrame, error)
	RenameSeries(index int, name string, inplace bool) (DataFrame, error)
	RenameSeriesByName(col string, name string, inplace bool) (DataFrame, error)
	RemoveSeries(index int) DataFrame
	RemoveSeriesByName(s string) DataFrame

	GetRow(i int64) Row
	ForEachRow(f func(Row))

	Group(key string, others ...string) GroupedDataFrame
	Join(schema DataFrameSchema, series DataFrame, jointype JoinType, f func(Row, Row) []Row) DataFrame
}

type GroupedDataFrame interface {
	GetGroupKeys() []string
	Get(index Row) DataFrame
	GetKeys() []Row
	ForEach(f func(Row, DataFrame))
	Map(f func(Row, DataFrame) DataFrame) GroupedDataFrame
	Where(f func(Row, DataFrame) bool) GroupedDataFrame
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
}

type GroupedSeries interface {
	Get(index Value) Series
	GetKeys() []Value
	ForEach(f func(Value, Series))
	Map(f func(Value, Series) Series) GroupedSeries
	Where(f func(Value, Series) bool) GroupedSeries
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
	GetByName(s string) (SeriesSchema, error)
	GetIndexByName(s string) (int, error)
	Get(i int) SeriesSchema
	Len() int
}
