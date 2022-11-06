package sources

import (
	"context"
	"errors"
	"net/url"
	"sync"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/sources/fs"
	"github.com/blue4209211/pq/sources/rdbms"
	"github.com/blue4209211/pq/sources/std"
)

// WriteDataFrame Write Dataframe to Given Source
func writeDataFrame(data df.DataFrame, src string, config map[string]string) (err error) {

	s, err := GetDataFrameSource(src)
	if err != nil {
		return err
	}

	return s.Write(context.Background(), data, src, config)
}

// ReadDataFrames on given files or directories
func readDataFrames(config map[string]string, sourceUrls ...string) (data []df.DataFrame, err error) {
	dfs := make([]df.DataFrame, len(sourceUrls))
	ers := make([]error, len(sourceUrls))

	var wg sync.WaitGroup
	for i, sourceURL := range sourceUrls {
		wg.Add(1)

		go func(idx int, sourceURL string) {
			defer wg.Done()
			dfSource, err := GetDataFrameSource(sourceURL)
			if err != nil {
				ers[idx] = err
				return
			}

			mergedDf, err := dfSource.Read(context.Background(), sourceURL, config)
			if err != nil {
				ers[idx] = err
				return
			}
			dfs[idx] = mergedDf
		}(i, sourceURL)
	}
	wg.Wait()

	for _, err := range ers {
		if err != nil {
			return data, err
		}
	}

	return dfs, nil
}

var sources = []df.DataFrameSource{
	&fs.DataSource{}, &rdbms.DataSource{}, &std.DataSource{},
}

//GetDataFrameSource returns DF source based on given sourceurl
func GetDataFrameSource(sourceURL string) (s df.DataFrameSource, err error) {
	u, err := url.Parse(sourceURL)
	if err != nil {
		return s, err
	}

	proto := "file"
	if u.Scheme == "" && u.Path == "-" {
		proto = "std"
	} else if u.Scheme != "" {
		proto = u.Scheme
	}

	for _, s := range sources {
		if s.IsSupported(proto) {
			return s, err
		}
	}

	return s, errors.New("unsupported format - " + proto)
}

// ReadSources Create Dataframe based on given schema and data
func ReadSources(config map[string]string, srcs ...string) (data []df.DataFrame, err error) {
	return readDataFrames(config, srcs...)
}

// WriteSource Write Dataframe to Given Source
func WriteSource(data df.DataFrame, config map[string]string, src string) (err error) {
	return writeDataFrame(data, src, config)
}
