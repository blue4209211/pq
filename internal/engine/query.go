package engine

import (
	"strings"
	"sync"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/log"
)

type queryEngine interface {
	Query(query string) (df.DataFrame, error)
	RegisterDataFrame(df.DataFrame) error
	Close()
}

func modifyQuery(query string) string {
	if query == "" {
		return query
	}

	query2 := strings.ToLower(query)
	if !(strings.HasPrefix(query2, "select") || strings.HasPrefix(query2, "with")) {
		query = "select " + query
	}
	return query
}

// QueryDataFrames on given files or directories
func QueryDataFrames(query string, dfs []df.DataFrame, config map[string]string) (data df.DataFrame, err error) {
	startTime := time.Now()

	defer func() {
		log.Debug("Query Execution Time ", time.Since(startTime).String())
	}()

	log.Debug("Starting Querying engine")
	engine, err := newSQLiteEngine(config, dfs)

	if err != nil {
		return data, err
	}

	defer engine.Close()

	for _, d := range dfs {
		err = engine.RegisterDataFrame(d)
		if err != nil {
			return data, err
		}
	}

	// some kind of DB issue which is not working correctly when  using multiple channels

	// if len(dfs) <= 1 {
	// 	err = engine.RegisterDataFrame(dfs[0])
	// 	if err != nil {
	// 		return data, err
	// 	}
	// } else {
	// 	jobs := make(chan df.DataFrame, len(dfs))
	// 	results := make(chan error, len(dfs))
	// 	wg := new(sync.WaitGroup)

	// 	for w := 0; w < len(dfs); w++ {
	// 		wg.Add(1)
	// 		go registerDfAsync(&engine, jobs, results, wg, &config)
	// 	}

	// 	for _, f := range dfs {
	// 		jobs <- f
	// 	}

	// 	close(jobs)
	// 	wg.Wait()
	// 	close(results)

	// 	for e := range results {
	// 		if e != nil {
	// 			return data, e
	// 		}
	// 	}
	// }

	return engine.Query(modifyQuery(query))
}

func registerDfAsync(qe *queryEngine, jobs <-chan df.DataFrame, results chan<- error, wg *sync.WaitGroup, config *map[string]string) {
	defer wg.Done()

	for data := range jobs {
		err := (*qe).RegisterDataFrame(data)
		results <- err
	}
}
