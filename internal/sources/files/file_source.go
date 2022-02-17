package files

import (
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/blue4209211/pq/internal/log"
	"github.com/golang/snappy"
)

// StreamSource Provides interface for all the data sources
type StreamSource interface {
	Name() string
	// TODO remove reader, use filepaths, currently hard to decide when to close reader
	Reader(reader io.Reader, args map[string]string) (StreamReader, error)
	// TODO remove reader, use filepaths
	Writer(data df.DataFrame, args map[string]string) (StreamWriter, error)
	Args() map[string]string
}

type StreamReader interface {
	Schema() []df.Column
	Data() [][]any
}

// StreamWriter Writes dataframe to write
type StreamWriter interface {
	Write(writer io.Writer) error
}

// GetStreamHandler Factory method to get source based on given string
func GetStreamHandler(fmt string) (src StreamSource, err error) {
	fmt = strings.ToLower(fmt)

	if fmt == "csv" {
		return &csvDataSource{}, err
	} else if fmt == "json" {
		return &jsonDataSource{}, err
	} else if fmt == "xml" {
		return &xmlDataSource{}, err
	} else if fmt == "parquet" {
		return &parquetDataSource{}, err
	} else {
		return &textDataSource{}, err
	}
}

// DataSource files datasource handler
type DataSource struct {
}

//IsSupported IsSupported returns supported protocols by file sources
func (t *DataSource) IsSupported(protocol string) bool {
	return protocol == "file" || protocol == "csv" || protocol == "xml" || protocol == "json" || protocol == "parquet" || protocol == "text" || protocol == "log"
}

func updateConfigFromSourceURL(sourceURL string, config map[string]string) map[string]string {
	updatedConfig := map[string]string{}
	for k, v := range config {
		updatedConfig[k] = v
	}

	parsedURL, err := url.Parse(sourceURL)
	if err != nil {
		return updatedConfig
	}

	for k, v := range parsedURL.Query() {
		if v == nil || len(v) == 0 {
			updatedConfig[k] = ""
		} else {
			updatedConfig[k] = v[0]
		}
	}
	return updatedConfig
}

func (t *DataSource) Read(sourceURL string, config map[string]string) (data df.DataFrame, err error) {
	config = updateConfigFromSourceURL(sourceURL, config)

	fileOrDirPath, fileOrDirName, _, _, err := getFileDetails(sourceURL)
	if err != nil {
		return data, err
	}
	var files []string

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

		if len(files) > 0 && strings.Index(sourceURL, "#") == -1 {
			return data, errors.New("Regex is defined as filePath but missing alias name, use # to define alias name")
		}

	}

	if len(files) == 0 {
		return data, errors.New("File/Dir Not found or Invalid pattern - " + sourceURL)
	}

	var mergedDf df.DataFrame
	startTime := time.Now()
	log.Debug("Reading data from FS - ", fileOrDirName)
	if len(files) <= 1 {
		mergedDf, err = readSourcesToDataframeSync(fileOrDirName, files, &config)
	} else {
		mergedDf, err = readSourcesToDataframeAsync(fileOrDirName, files, &config)
	}
	log.Debugf("Completed data read from FS (%s) in (%s) ", fileOrDirName, time.Since(startTime).String())
	if err != nil {
		return data, err
	}

	return mergedDf, nil

}

func (t *DataSource) Write(data df.DataFrame, path string, config map[string]string) (err error) {
	config = updateConfigFromSourceURL(path, config)
	fileOrDirPath, _, ext, _, err := getFileDetails(path)
	dfs, err := GetStreamHandler(ext)
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

func getDataframeFromSource(name string, ext string, reader io.Reader, config *map[string]string) (data df.DataFrame, err error) {
	streamSource, err := GetStreamHandler(ext)
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

func getFileDetails(fileName string) (path string, name string, format string, comrpression string, err error) {

	parsedURL, err := url.Parse(fileName)
	if parsedURL.Scheme != "" && parsedURL.Scheme != "file" {
		format = parsedURL.Scheme
	}

	path = parsedURL.Path
	if parsedURL.Fragment != "" {
		name = parsedURL.Fragment
	} else {
		name = strings.Split(filepath.Base(path), ".")[0]
	}

	if format == "" {
		if strings.Index(path, ".csv") >= 0 || strings.Index(path, ".tsv") >= 0 {
			format = "csv"
		} else if strings.Index(path, ".json") >= 0 {
			format = "json"
		} else if strings.Index(path, ".xml") >= 0 {
			format = "xml"
		} else if strings.Index(path, ".parquet") >= 0 {
			format = "parquet"
		} else if strings.Index(path, ".txt") >= 0 || strings.Index(path, ".text") >= 0 || strings.Index(path, ".log") >= 0 {
			format = "text"
		}
	}

	if strings.Index(path, ".gz") >= 0 {
		comrpression = "gz"
	} else if strings.Index(path, ".zip") >= 0 {
		comrpression = "zip"
	} else if strings.Index(path, ".snappy") >= 0 {
		comrpression = "snappy"
	}

	return
}

func readSourceToDataframeAsyncWorker(jobs <-chan string, results chan<- asyncReaderResult, wg *sync.WaitGroup, config *map[string]string) {
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
		} else if compression == "snappy" {
			f, err := os.Open(path)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			defer f.Close()
			reader := snappy.NewReader(f)
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

type asyncReaderResult struct {
	data df.DataFrame
	err  error
}

func readSourcesToDataframeAsync(aliasName string, sources []string, config *map[string]string) (data df.DataFrame, err error) {
	// TODO fornow static value and we are not reading data in seprate channel
	if len(sources) > 200 {
		return data, errors.New("More than 200 files are not supported")
	}
	jobs := make(chan string, len(sources))
	results := make(chan asyncReaderResult, len(sources))
	wg := new(sync.WaitGroup)

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go readSourceToDataframeAsyncWorker(jobs, results, wg, config)
	}

	for _, f := range sources {
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
	return inmemory.NewMergeDataframe(aliasName, dfsFiles...)
}

func readSourcesToDataframeSync(aliasName string, sources []string, config *map[string]string) (data df.DataFrame, err error) {

	dfsFiles := make([]df.DataFrame, 0, len(sources))
	for _, f := range sources {
		path, name, ext, compression, err := getFileDetails(f)

		if err != nil {
			return data, err
		}

		if ext == "" {
			log.Warnf("unable to detect fileType for (%s), falling back to json", f)
			ext = "json"
		}

		if compression == "gz" {
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
	return inmemory.NewMergeDataframe(aliasName, dfsFiles...)
}
