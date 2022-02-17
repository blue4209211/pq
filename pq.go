package main

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/engine"
	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/blue4209211/pq/internal/sources"
)

// QuerySources Create Dataframe based on given sources
func QuerySources(query string, config map[string]string, srcs ...string) (data df.DataFrame, err error) {
	dfs, err := sources.ReadDataFrames(config, srcs...)
	if err != nil {
		return data, err
	}
	return engine.QueryDataFrames(query, dfs, config)
}

// QueryDataFrames Create Dataframe based on given sources
func QueryDataFrames(query string, config map[string]string, dfs ...df.DataFrame) (data df.DataFrame, err error) {
	return engine.QueryDataFrames(query, dfs, config)
}

// ReadSources Create Dataframe based on given schema and data
func ReadSources(config map[string]string, srcs ...string) (data []df.DataFrame, err error) {
	return sources.ReadDataFrames(config, srcs...)
}

// WriteSource Write Dataframe to Given Source
func WriteSource(data df.DataFrame, config map[string]string, src string) (err error) {
	return sources.WriteDataFrame(data, src, config)
}

// NewDataframe Create Dataframe based on given schema and data
func NewDataframe(cols []df.Column, data [][]any) df.DataFrame {
	return inmemory.NewDataframe(cols, data)
}

// NewDataframeWithName Create Dataframe based on given name, schema and data
func NewDataframeWithName(name string, cols []df.Column, data [][]any) df.DataFrame {
	return inmemory.NewDataframeWithName(name, cols, data)
}

// NewDataframeWithNameFromSeries Create Dataframe based on given name, schema and data
func NewDataframeWithNameFromSeries(name string, colNames []string, data []df.DataFrameSeries) df.DataFrame {
	return inmemory.NewDataframeWithNameFromSeries(name, colNames, data)
}

// NewStringSeries returns a column of type string
func NewStringSeries(data []string) df.DataFrameSeries {
	return inmemory.NewStringSeries(data)
}

// NewIntSeries returns a column of type int
func NewIntSeries(data []int64) df.DataFrameSeries {
	return inmemory.NewIntSeries(data)
}

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []bool) df.DataFrameSeries {
	return inmemory.NewBoolSeries(data)
}

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []float64) df.DataFrameSeries {
	return inmemory.NewDoubleSeries(data)
}

// NewDataFrameRow returns new Row based on schema and data
func NewDataFrameRow(schema df.DataFrameSchema, data []any) df.DataFrameRow {
	return inmemory.NewDataFrameRow(schema, data)
}
