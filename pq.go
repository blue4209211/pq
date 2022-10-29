package main

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/engine"
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
