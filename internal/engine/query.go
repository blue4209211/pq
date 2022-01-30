package engine

import (
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

// QueryFiles on given files or directories
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

	if len(dfs) <= 1 {
		err = engine.RegisterDataFrame(dfs[0])
		if err != nil {
			return data, err
		}
	} else {
		jobs := make(chan df.DataFrame, len(dfs))
		results := make(chan error, len(dfs))
		wg := new(sync.WaitGroup)

		for w := 1; w <= len(dfs); w++ {
			wg.Add(1)
			go registerDfAsync(&engine, jobs, results, wg, &config)
		}

		for _, f := range dfs {
			jobs <- f
		}

		close(jobs)
		wg.Wait()
		close(results)

		for e := range results {
			if e != nil {
				return data, e
			}
		}
	}

	return engine.Query(query)
}

func registerDfAsync(qe *queryEngine, jobs <-chan df.DataFrame, results chan<- error, wg *sync.WaitGroup, config *map[string]string) {
	defer wg.Done()

	for data := range jobs {
		err := (*qe).RegisterDataFrame(data)
		results <- err
	}
}
