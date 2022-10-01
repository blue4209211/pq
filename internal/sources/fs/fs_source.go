package fs

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/blue4209211/pq/internal/log"
	"github.com/blue4209211/pq/internal/sources/fs/formats"
	"github.com/blue4209211/pq/internal/sources/fs/vfs"
	"github.com/golang/snappy"
)

// DataSource files datasource handler
type DataSource struct {
}

//IsSupported IsSupported returns supported protocols by file sources
func (t *DataSource) IsSupported(protocol string) bool {
	return protocol == "file" || protocol == "gs" || protocol == "s3" || protocol == "csv" || protocol == "xml" || protocol == "json" || protocol == "parquet" || protocol == "text" || protocol == "log"
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
		if len(v) == 0 {
			updatedConfig[k] = ""
		} else {
			updatedConfig[k] = v[0]
		}
	}
	return updatedConfig
}

func (t *DataSource) Read(sourceURL string, config map[string]string) (data df.DataFrame, err error) {
	config = updateConfigFromSourceURL(sourceURL, config)

	filePath, fileOrDirName, _, _, err := getFileDetails(sourceURL)
	if err != nil {
		return data, err
	}

	filesystem, err := vfs.GetVFS(sourceURL)
	if err != nil {
		return data, err
	}

	// vfs will be using base as parent path
	if filePath != "" {
		filePath = path.Base(filePath)
	}
	var files []string

	file, err := filesystem.Open(filePath)
	if err != nil && !strings.Contains(filePath, "*") {
		return data, err
	}
	var fileInfo fs.FileInfo
	if file != nil {
		fileInfo, err = file.Stat()
	}
	if err == nil {
		if fileInfo.IsDir() {
			dirEntries, err := fs.ReadDir(filesystem, fileInfo.Name())
			if err != nil {
				return data, err
			}
			for _, de := range dirEntries {
				if de.Type().IsRegular() {
					files = append(files, path.Join(fileInfo.Name(), de.Name()))
				}
			}
		} else {
			files = []string{fileInfo.Name()}
		}
	} else if strings.Contains(filePath, "*") {
		files, err = fs.Glob(filesystem, filePath)
		if err != nil {
			return data, err
		}

		if len(files) > 0 && !strings.Contains(sourceURL, "#") {
			return data, errors.New("regex is defined as filePath but missing alias name, use # to define alias name")
		}

	}

	if len(files) == 0 {
		return data, errors.New("File/Dir Not found or Invalid pattern - " + sourceURL)
	}

	var mergedDf df.DataFrame
	startTime := time.Now()
	log.Debug("Reading data from FS - ", files)
	if len(files) <= 1 {
		mergedDf, err = readSourcesToDataframeSync(filesystem, fileOrDirName, files, &config)
	} else {
		mergedDf, err = readSourcesToDataframeAsync(filesystem, fileOrDirName, files, &config)
	}
	log.Debugf("Completed data read from FS (%s) in (%s) ", fileOrDirName, time.Since(startTime).String())
	if err != nil {
		return data, err
	}

	return mergedDf, nil

}

func (t *DataSource) Write(data df.DataFrame, path string, config map[string]string) (err error) {
	config = updateConfigFromSourceURL(path, config)
	_, name, ext, _, err := getFileDetails(path)
	dfs, err := formats.GetFormatHandler(ext)
	if err != nil {
		return err
	}
	writer, err := dfs.Writer(data, config)
	if err != nil {
		return err
	}

	fs, err := vfs.GetVFS(path)
	if err != nil {
		return err
	}
	if ext != "" {
		name = name + "." + ext
	}
	f, err := fs.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return writer.Write(f)

}

func getDataframeFromSource(name string, ext string, reader io.Reader, config *map[string]string) (data df.DataFrame, err error) {
	streamSource, err := formats.GetFormatHandler(ext)
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
	if parsedURL.Scheme != "" && parsedURL.Scheme != "file" && parsedURL.Scheme != "s3" && parsedURL.Scheme != "gs" {
		format = parsedURL.Scheme
	}

	if parsedURL.Query().Has("format") {
		format = parsedURL.Query().Get("format")
	}

	path = parsedURL.Path
	if parsedURL.Fragment != "" {
		name = parsedURL.Fragment
	} else {
		name = strings.Split(filepath.Base(path), ".")[0]
	}

	if format == "" {
		if strings.Contains(path, ".csv") || strings.Contains(path, ".tsv") {
			format = "csv"
		} else if strings.Contains(path, ".json") {
			format = "json"
		} else if strings.Contains(path, ".xml") {
			format = "xml"
		} else if strings.Contains(path, ".parquet") {
			format = "parquet"
		} else if strings.Contains(path, ".txt") || strings.Contains(path, ".text") || strings.Contains(path, ".log") {
			format = "text"
		}
	}

	if strings.Contains(path, ".gz") {
		comrpression = "gz"
	} else if strings.Contains(path, ".zip") {
		comrpression = "zip"
	} else if strings.Contains(path, ".snappy") {
		comrpression = "snappy"
	}

	return
}

func readSourceToDataframeAsyncWorker(jobs <-chan string, results chan<- asyncReaderResult, wg *sync.WaitGroup, config *map[string]string, filesystem vfs.VFS) {
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
			f, err := filesystem.Open(path)
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
			f, err := filesystem.Open(path)
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
			f, err := filesystem.Open(path)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}
			defer f.Close()
			buff := bytes.NewBuffer([]byte{})
			_, err = io.Copy(buff, f)
			if err != nil {
				results <- asyncReaderResult{err: err}
				break
			}

			zipReader, err := zip.NewReader(bytes.NewReader(buff.Bytes()), int64(buff.Len()))
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
			f, err := filesystem.Open(path)
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

func readSourcesToDataframeAsync(filesystem vfs.VFS, aliasName string, sources []string, config *map[string]string) (data df.DataFrame, err error) {
	// TODO fornow static value and we are not reading data in seprate channel
	if len(sources) > 200 {
		return data, errors.New("more than 200 files are not supported")
	}
	jobs := make(chan string, len(sources))
	results := make(chan asyncReaderResult, len(sources))
	wg := new(sync.WaitGroup)

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go readSourceToDataframeAsyncWorker(jobs, results, wg, config, filesystem)
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

func readSourcesToDataframeSync(filesystem vfs.VFS, aliasName string, sources []string, config *map[string]string) (data df.DataFrame, err error) {

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
			f, err := filesystem.Open(path)
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
			f, err := filesystem.Open(path)
			if err != nil {
				return data, err
			}
			defer f.Close()
			buff := bytes.NewBuffer([]byte{})
			_, err = io.Copy(buff, f)
			if err != nil {
				return data, err
			}

			zipReader, err := zip.NewReader(bytes.NewReader(buff.Bytes()), int64(buff.Len()))
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
			f, err := filesystem.Open(path)
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
