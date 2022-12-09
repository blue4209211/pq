package sql

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/engine"
	"github.com/blue4209211/pq/sources"
)

// QuerySources Create Dataframe based on given sources
func QuerySources(query string, config map[string]string, srcs ...string) (data df.DataFrame, err error) {
	dfs, err := sources.ReadSources(config, srcs...)
	if err != nil {
		return data, err
	}
	return QueryDataFrames(query, config, dfs...)
}

// QueryDataFrames Create Dataframe based on given sources
func QueryDataFrames(query string, config map[string]string, dfs ...df.DataFrame) (data df.DataFrame, err error) {
	return engine.QueryDataFrames(query, dfs, config)
}
