package df

import (
	"reflect"
	"time"
)

// Series metadata for dataframe column
type SeriesSchema struct {
	Name   string
	Format DataFrameSeriesFormat
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

// DataFrameSeriesFormat Datatype of Dataframe Column
type DataFrameSeriesFormat interface {
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
	MapRow(schema []SeriesSchema, f func(DataFrameRow) DataFrameRow) DataFrame
	FlatMapRow(schema []SeriesSchema, f func(DataFrameRow) []DataFrameRow) DataFrame
	FilterRow(f func(DataFrameRow) bool) DataFrame

	GetSeries(index int) DataFrameSeries
	GetSeriesByName(s string) DataFrameSeries

	AddSeries(name string, series DataFrameSeries) (DataFrame, error)
	UpdateSeries(index int, series DataFrameSeries) (DataFrame, error)
	UpdateSeriesByName(name string, series DataFrameSeries) (DataFrame, error)
	RenameSeries(index int, name string, inplace bool) (DataFrame, error)
	RenameSeriesByName(col string, name string, inplace bool) (DataFrame, error)
	RemoveSeries(index int) DataFrame
	RemoveSeriesByName(s string) DataFrame

	GetRow(i int64) DataFrameRow
	ForEachRow(f func(DataFrameRow))

	Join(schema DataFrameSchema, series DataFrame, jointype JoinType, f func(DataFrameRow, DataFrameRow) []DataFrameRow) DataFrame
}

type DataFrameGrouped interface {
	Get(index any) DataFrame
	GetKeys() []any
	ForEach(f func(any, DataFrame))
	Map(schema DataFrameSeriesFormat, f func(any, DataFrame) DataFrame) DataFrameGrouped
	Filter(f func(any, DataFrame) bool) DataFrameGrouped
}

// DataFrameSeries Type for Storing column data of Dataframe
type DataFrameSeries interface {
	Schema() SeriesSchema
	Len() int64
	Get(index int64) DataFrameSeriesValue
	ForEach(f func(DataFrameSeriesValue))
	Sort(order SortOrder) DataFrameSeries
	Map(schema DataFrameSeriesFormat, f func(DataFrameSeriesValue) DataFrameSeriesValue) DataFrameSeries
	FlatMap(schema DataFrameSeriesFormat, f func(DataFrameSeriesValue) []DataFrameSeriesValue) DataFrameSeries
	Reduce(f func(DataFrameSeriesValue, DataFrameSeriesValue) DataFrameSeriesValue, startValue DataFrameSeriesValue) DataFrameSeriesValue
	Filter(f func(DataFrameSeriesValue) bool) DataFrameSeries
	Limit(offset int, size int) DataFrameSeries
	Distinct() DataFrameSeries
	Copy() DataFrameSeries
	Group() DataFrameGroupedSeries

	Append(series DataFrameSeries) DataFrameSeries
	Join(schema DataFrameSeriesFormat, series DataFrameSeries, jointype JoinType, f func(DataFrameSeriesValue, DataFrameSeriesValue) []DataFrameSeriesValue) DataFrameSeries
}

type DataFrameSeriesValue interface {
	Schema() DataFrameSeriesFormat
	Get() any
	GetAsString() string
	GetAsInt() int64
	GetAsDouble() float64
	GetAsBool() bool
	GetAsDatetime() time.Time
	IsNil() bool
}

type DataFrameGroupedSeries interface {
	Get(index any) DataFrameSeries
	GetKeys() []any
	ForEach(f func(any, DataFrameSeries))
	Map(schema DataFrameSeriesFormat, f func(any, DataFrameSeries) DataFrameSeries) DataFrameGroupedSeries
	Filter(f func(any, DataFrameSeries) bool) DataFrameGroupedSeries
}

// DataFrameRow Type representing row data of Dataframe
type DataFrameRow interface {
	Schema() DataFrameSchema
	Get(i int) any
	GetVal(i int) DataFrameSeriesValue
	GetByName(s string) any
	Data() []any
	Len() int
	GetAsString(i int) string
	GetAsInt(i int) int64
	GetAsDouble(i int) float64
	GetAsBool(i int) bool
	GetAsDatetime(i int) time.Time
	GetMap() (r map[string]any)
	IsAnyNil() bool
	IsNil(i int) bool
}

// DataFrameSchema Type representing schema of Dataframe
type DataFrameSchema interface {
	Series() []SeriesSchema
	GetByName(s string) (SeriesSchema, error)
	GetIndexByName(s string) (int, error)
	Get(i int) SeriesSchema
	Len() int
}
