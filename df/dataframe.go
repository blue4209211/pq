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
	Where(f func(DataFrameRow) bool) DataFrame
	Select(b DataFrameSeries) DataFrame

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

	Group(key string, others ...string) DataFrameGrouped
	Join(schema DataFrameSchema, series DataFrame, jointype JoinType, f func(DataFrameRow, DataFrameRow) []DataFrameRow) DataFrame
}

type DataFrameGrouped interface {
	GetGroupKeys() []string
	Get(index DataFrameRow) DataFrame
	GetKeys() []DataFrameRow
	ForEach(f func(DataFrameRow, DataFrame))
	Map(s DataFrameSchema, f func(DataFrameRow, DataFrame) DataFrame) DataFrameGrouped
	Where(f func(DataFrameRow, DataFrame) bool) DataFrameGrouped
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
	Where(f func(DataFrameSeriesValue) bool) DataFrameSeries
	Select(b DataFrameSeries) DataFrameSeries
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
	Get(index DataFrameSeriesValue) DataFrameSeries
	GetKeys() []DataFrameSeriesValue
	ForEach(f func(DataFrameSeriesValue, DataFrameSeries))
	Map(schema DataFrameSeriesFormat, f func(DataFrameSeriesValue, DataFrameSeries) DataFrameSeries) DataFrameGroupedSeries
	Where(f func(DataFrameSeriesValue, DataFrameSeries) bool) DataFrameGroupedSeries
}

// DataFrameRow Type representing row data of Dataframe
type DataFrameRow interface {
	Schema() DataFrameSchema
	GetRaw(i int) any
	Get(i int) DataFrameSeriesValue
	GetByName(s string) DataFrameSeriesValue
	Len() int
	GetAsString(i int) string
	GetAsInt(i int) int64
	GetAsDouble(i int) float64
	GetAsBool(i int) bool
	GetAsDatetime(i int) time.Time
	GetMap() (r map[string]DataFrameSeriesValue)
	IsAnyNil() bool
	IsNil(i int) bool
	Copy() DataFrameRow
	Select(i ...int) DataFrameRow
	Append(name string, v DataFrameSeriesValue) DataFrameRow
}

// DataFrameSchema Type representing schema of Dataframe
type DataFrameSchema interface {
	Series() []SeriesSchema
	GetByName(s string) (SeriesSchema, error)
	GetIndexByName(s string) (int, error)
	Get(i int) SeriesSchema
	Len() int
}
