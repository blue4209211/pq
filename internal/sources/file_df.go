package sources

import (
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/blue4209211/pq/internal/log"
)

// WriteDataFrame Write Dataframe to Given Source
func WriteDataFrame(data df.DataFrame, src string, config map[string]string) (err error) {
	if src == "-" {
		format, ok := config[ConfigStdType]
		if !ok {
			format = "json"
		}
		dfs, err := GetSource(format)
		writer, err := dfs.Writer(data, config)
		if err != nil {
			return err
		}
		return writer.Write(os.Stdout)
	}

	fileOrDirPath, _, ext, _, err := getFileDetails(src)
	dfs, err := GetSource(ext)
	if err != nil {
		return err
	}
	writer, err := dfs.Writer(data, config)
	if err != nil {
		return err
	}

	f, err := os.Create(fileOrDirPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return writer.Write(f)
}

// ReadDataFrame Create Dataframe based on given schema and data
func ReadDataFrame(path string, config map[string]string) (data df.DataFrame, err error) {
	dfa, err := ReadDataFrames([]string{path}, config)
	if err != nil {
		return data, err
	}

	return dfa[0], err
}

// ReadDataFrames on given files or directories
func ReadDataFrames(fileOrDrs []string, config map[string]string) (data []df.DataFrame, err error) {
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
		startTime := time.Now()
		log.Debug("Reading data from FS - ", fileOrDirName)
		if len(files) <= 1 {
			mergedDf, err = readFilesToDataframeSync(fileOrDirName, files, &config)
		} else {
			mergedDf, err = readFilesToDataframeAsync(fileOrDirName, files, &config)
		}
		log.Debugf("Completed data read from FS (%s) in (%s) ", fileOrDirName, time.Since(startTime).String())
		if err != nil {
			return data, err
		}

		dfs = append(dfs, mergedDf)
	}

	return dfs, nil
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
	} else if strings.Index(path, ".xml") >= 0 {
		ext = "xml"
	} else if strings.Index(path, ".parquet") >= 0 {
		ext = "parquet"
	}

	if strings.Index(path, ".gz") >= 0 {
		comrpression = "gz"
	} else if strings.Index(path, ".zip") >= 0 {
		comrpression = "zip"
	}

	return
}

func readFilesToDataframeSync(fileOrDirName string, files []string, config *map[string]string) (data df.DataFrame, err error) {

	dfsFiles := make([]df.DataFrame, 0, len(files))
	for _, f := range files {
		path, name, ext, compression, err := getFileDetails(f)

		if err != nil {
			return data, err
		}

		if ext == "" {
			log.Debugf("unable to detect fileType for (%s), falling back to json", f)
			ext = "json"
		}

		if path == "-" {
			ext1, ok := (*config)[ConfigStdType]
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
	return inmemory.NewMergeDataframe(fileOrDirName, dfsFiles...)
}

type asyncReaderResult struct {
	data df.DataFrame
	err  error
}

func readFilesToDataframeAsync(fileOrDirName string, files []string, config *map[string]string) (data df.DataFrame, err error) {
	// TODO fornow static value and we are not reading data in seprate channel
	if len(files) > 200 {
		return data, errors.New("More than 200 files are not supported")
	}
	jobs := make(chan string, len(files))
	results := make(chan asyncReaderResult, len(files))
	wg := new(sync.WaitGroup)

	for w := 0; w < 5; w++ {
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
	return inmemory.NewMergeDataframe(fileOrDirName, dfsFiles...)
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
			ext = "json"
		}

		log.Debugf("reading file path(%s), name(%s), ext(%s), compression(%s)", path, name, ext, compression)

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

func getDataframeFromSource(name string, ext string, reader io.Reader, config *map[string]string) (data df.DataFrame, err error) {
	streamSource, err := GetSource(ext)
	if err != nil {
		return data, err
	}
	dataframeReader, err := streamSource.Reader(reader, *config)
	if err != nil {
		return data, err
	}
	datsourceDf := inmemory.NewDataframeWithName(name, dataframeReader.Schema(), dataframeReader.Data())
	return datsourceDf, nil
}
