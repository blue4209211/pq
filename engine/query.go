package engine

import (
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/log"
	"github.com/blue4209211/pq/sources"
)

type queryEngine interface {
	Query(query string, df []df.DataFrame) (df.DataFrame, error)
	Close()
}

func getQueryEngine(config map[string]string) (queryEngine, error) {
	queryEngine, err := newSQLiteEngine(config)
	return &queryEngine, err
}

func queryDataFrames(query string, dfs []df.DataFrame, config map[string]string) (data df.DataFrame, err error) {
	engine, err := getQueryEngine(config)
	if err != nil {
		return data, err
	}
	defer engine.Close()

	return engine.Query(query, dfs)
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
		dfsFiles := make([]df.DataFrame, 0)

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
				ext1, ok := config["fmt.std.type"]
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

		mergedDfs, err := mergeDfs(fileOrDirName, dfsFiles...)
		if err != nil {
			return data, err
		}
		dfs = append(dfs, mergedDfs)
	}

	return queryDataFrames(query, dfs, config)
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

func getDataframeFromSource(name string, ext string, reader io.Reader, config map[string]string) (data df.DataFrame, err error) {
	streamSource, err := sources.GetSource(ext)
	if err != nil {
		return data, err
	}
	dataframeReader, err := streamSource.Reader(reader, config)
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
