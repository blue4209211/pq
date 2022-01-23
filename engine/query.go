package engine

import (
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/log"
	"github.com/blue4209211/pq/sources"
)

type queryEngine interface {
	Query(query string) (df.DataFrame, error)
	RegisterTable(df.DataFrame) error
	Close()
}

func queryDataFrames(query string, dfs []df.DataFrame, config map[string]string) (data df.DataFrame, err error) {
	engine, err := newSQLiteEngine(config, dfs)
	if err != nil {
		return data, err
	}
	defer engine.Close()

	if len(dfs) <= 1 {
		err = engine.RegisterTable(dfs[0])
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
		err := (*qe).RegisterTable(data)
		results <- err
	}
}

func getFileDetails(fileName string) (path string, name string, ext string, comrpression string, err error) {

	nameAndAlias := strings.Split(fileName, "#")
	if len(nameAndAlias) == 2 {
		path = nameAndAlias[0]
		name = nameAndAlias[1]
	} else {
		path = fileName
		name = strings.Split(filepath.Base(path), ".")[0]
	}

	if strings.Index(path, ".csv") >= 0 || strings.Index(path, ".tsv") >= 0 {
		ext = "csv"
	} else if strings.Index(path, ".json") >= 0 {
		ext = "json"
	}

	if strings.Index(path, ".gz") >= 0 {
		comrpression = "gz"
	} else if strings.Index(path, ".zip") >= 0 {
		comrpression = "zip"
	}

	return
}

// QueryFiles on given files or directories
func QueryFiles(query string, fileOrDrs []string, config map[string]string) (data df.DataFrame, err error) {
	dfs := make([]df.DataFrame, 0)

	for _, fileOrDir := range fileOrDrs {

		var fileOrDirPath, fileOrDirName string

		var files []string
		if fileOrDir == "-" {
			fileOrDirName = "stdin"
			fileOrDirPath = "-"
			files = []string{"-"}
		} else {
			fileOrDirPath, fileOrDirName, _, _, err = getFileDetails(fileOrDir)
			if err != nil {
				return data, err
			}

			fileInfo, fileStateErr := os.Stat(fileOrDirPath)
			if fileStateErr == nil {
				if fileInfo.IsDir() {
					err = filepath.Walk(fileOrDirPath, func(path string, info os.FileInfo, err error) error {
						if !info.IsDir() {
							files = append(files, path)
						}
						return nil
					})
					if err != nil {
						return data, err
					}
				} else {
					files = []string{fileOrDirPath}
				}
			} else {
				files, err = filepath.Glob(fileOrDirPath)
				if err != nil {
					return data, err
				}

				if len(files) > 0 && strings.Index(fileOrDir, "#") == -1 {
					return data, errors.New("Regex is defined as filePath but missing alias name, use # to define alias name")
				}

			}

			if len(files) == 0 {
				return data, errors.New("File/Dir Not found or Invalid pattern - " + fileOrDir)
			}
		}

		var mergedDf df.DataFrame
		if len(files) <= 1 {
			mergedDf, err = readFilesToDataframeSync(fileOrDirName, files, &config)
		} else {
			mergedDf, err = readFilesToDataframeAsync(fileOrDirName, files, &config)
		}
		if err != nil {
			return data, err
		}

		dfs = append(dfs, mergedDf)
	}

	return queryDataFrames(query, dfs, config)
}

func readFilesToDataframeSync(fileOrDirName string, files []string, config *map[string]string) (data df.DataFrame, err error) {

	dfsFiles := make([]df.DataFrame, 0, len(files))
	for _, f := range files {
		path, name, ext, compression, err := getFileDetails(f)

		if err != nil {
			return data, err
		}

		if ext == "" {
			log.Debug("unable to detect fileType for (%s), falling back to json", f)
			ext = "json"
		}

		if path == "-" {
			ext1, ok := (*config)["fmt.std.type"]
			if !ok {
				ext1 = "json"
			}
			ext = ext1
			name = "stdin"

			ds, err := getDataframeFromSource(name, ext, os.Stdin, config)
			if err != nil {
				return data, err
			}

			dfsFiles = append(dfsFiles, ds)
		} else if compression == "gz" {
			f, err := os.Open(path)
			if err != nil {
				return data, err
			}
			defer f.Close()
			reader, err := gzip.NewReader(f)
			if err != nil {
				return data, err
			}

			ds, err := getDataframeFromSource(name, ext, reader, config)
			if err != nil {
				return data, err
			}
			dfsFiles = append(dfsFiles, ds)

		} else if compression == "zip" {
			zipReader, err := zip.OpenReader(path)
			if err != nil {
				return data, err
			}
			for _, f := range zipReader.File {
				zipFile, err := f.Open()
				if err != nil {
					return data, err
				}
				defer zipFile.Close()

				ds, err := getDataframeFromSource(name, ext, zipFile, config)
				if err != nil {
					return data, err
				}
				dfsFiles = append(dfsFiles, ds)

			}
		} else {
			f, err := os.Open(path)
			if err != nil {
				return data, err
			}
			defer f.Close()

			ds, err := getDataframeFromSource(name, ext, f, config)
			if err != nil {
				return data, err
			}
			dfsFiles = append(dfsFiles, ds)
		}
	}
	return mergeDfs(fileOrDirName, dfsFiles...)
}

type asyncReaderResult struct {
	data df.DataFrame
	err  error
}

func readFilesToDataframeAsync(fileOrDirName string, files []string, config *map[string]string) (data df.DataFrame, err error) {
	jobs := make(chan string, 5)
	// TODO fornow static value and we are not reading data in seprate channel
	// this will hang is files are more than 100
	results := make(chan asyncReaderResult, 100)
	wg := new(sync.WaitGroup)

	for w := 1; w <= len(files); w++ {
		wg.Add(1)
		go readFileToDataframeAsyncWorker(jobs, results, wg, config)
	}

	for _, f := range files {
		jobs <- f
	}

	close(jobs)
	wg.Wait()
	close(results)
	dfsFiles := make([]df.DataFrame, 0)
	for ds := range results {
		if ds.err != nil {
			return data, ds.err
		}
		dfsFiles = append(dfsFiles, ds.data)
	}
	return mergeDfs(fileOrDirName, dfsFiles...)
}

func readFileToDataframeAsyncWorker(jobs <-chan string, results chan<- asyncReaderResult, wg *sync.WaitGroup, config *map[string]string) {
	defer wg.Done()

	for f := range jobs {
		path, name, ext, compression, err := getFileDetails(f)

		if err != nil {
			results <- asyncReaderResult{err: err}
			break
		}

		if ext == "" {
			log.Debug("unable to detect fileType for (%s), falling back to json", f)
			ext = "json"
		}

		if path == "-" {
			ext1, ok := (*config)["fmt.std.type"]
			if !ok {
				ext1 = "json"
			}
			ext = ext1
			name = "stdin"

			ds, err := getDataframeFromSource(name, ext, os.Stdin, config)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			results <- asyncReaderResult{data: ds}
		} else if compression == "gz" {
			f, err := os.Open(path)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			defer f.Close()
			reader, err := gzip.NewReader(f)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}

			ds, err := getDataframeFromSource(name, ext, reader, config)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			results <- asyncReaderResult{data: ds}
		} else if compression == "zip" {
			zipReader, err := zip.OpenReader(path)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			for _, f := range zipReader.File {
				zipFile, err := f.Open()
				if err != nil {
					results <- asyncReaderResult{err: err}
					break
				}
				defer zipFile.Close()

				ds, err := getDataframeFromSource(name, ext, zipFile, config)
				if err != nil {
					results <- asyncReaderResult{err: err}
					break
				}
				results <- asyncReaderResult{data: ds}
			}
		} else {
			f, err := os.Open(path)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			defer f.Close()

			ds, err := getDataframeFromSource(name, ext, f, config)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			results <- asyncReaderResult{data: ds}
		}
	}

}

func mergeDfs(name string, dfs ...df.DataFrame) (data df.DataFrame, err error) {

	if len(dfs) == 0 {
		return data, errors.New("Empty data")
	}

	cols, err := dfs[0].Schema()
	if err != nil {
		return
	}

	var records [][]interface{}
	if len(dfs) == 1 {
		records, err = dfs[0].Data()
		if err != nil {
			return
		}
	} else {
		records = make([][]interface{}, 0)

		for _, df := range dfs {
			dfRecords, err := df.Data()
			if err != nil {
				return data, err
			}
			records = append(records, dfRecords...)
		}
	}

	inMemoryDf := df.NewInmemoryDataframeWithName(name, cols, records)
	data = &inMemoryDf
	return
}

func getDataframeFromSource(name string, ext string, reader io.Reader, config *map[string]string) (data df.DataFrame, err error) {
	streamSource, err := sources.GetSource(ext)
	if err != nil {
		return data, err
	}
	dataframeReader, err := streamSource.Reader(reader, *config)
	if err != nil {
		return data, err
	}
	dataframe := sources.NewDatasourceDataFrame(name, dataframeReader)
	_, err = dataframe.Schema()

	if err != nil {
		return data, err
	}

	data = &dataframe
	return
}
