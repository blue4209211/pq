package engine

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/sources"
)

type QueryEngine interface {
	Query(query string, df []df.DataFrame) (df.DataFrame, error)
	Close()
}

func GetQueryEngine(config map[string]string) (QueryEngine, error) {
	queryEngine, err := NewSQLiteEngine(config)
	return &queryEngine, err
}

func Query(query string, dfs []df.DataFrame, config map[string]string) (data df.DataFrame, err error) {
	engine, err := GetQueryEngine(config)
	if err != nil {
		return data, err
	}
	defer engine.Close()

	return engine.Query(query, dfs)
}

func getFileDetails(fileName string) (path string, name string, ext string, err error) {

	nameAndAlias := strings.Split(fileName, "#")
	if len(nameAndAlias) == 2 {
		path = nameAndAlias[0]
		name = nameAndAlias[1]
	} else {
		name = strings.Split(filepath.Base(fileName), ".")[0]
		path = fileName
	}

	ext = strings.ReplaceAll(filepath.Ext(path), ".", "")

	return
}

func QueryFiles(query string, files []string, config map[string]string) (data df.DataFrame, err error) {
	dfs := make([]df.DataFrame, len(files))

	for i, f := range files {

		path, name, ext, err := getFileDetails(f)
		var reader io.Reader
		if path == "-" {
			reader = bufio.NewReader(os.Stdin)
			ext1, ok := config["fmt.std.type"]
			if !ok {
				ext1 = "json"
			}
			ext = ext1
			name = "stdin"
		} else {
			reader, err = os.Open(path)
			if err != nil {
				return data, err
			}
		}
		streamSource, err := sources.GetSource(ext)
		if err != nil {
			return data, err
		}

		dataframeReader, err := streamSource.Reader(reader, config)
		if err != nil {
			return data, err
		}

		dataframe := sources.NewDatasourceDataFrame(name, dataframeReader)

		dfs[i] = &dataframe
	}

	return Query(query, dfs, config)

}
