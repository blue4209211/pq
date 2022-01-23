package sources

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/blue4209211/pq/df"
)

func jsonReadByLine(reader io.Reader) (objMapList []map[string]interface{}, err error) {
	bufferedReader := bufio.NewReader(reader)
	jobs := make(chan []byte, 1000)
	results := make(chan jsonAsyncReadResult, 1000)
	wg := new(sync.WaitGroup)
	objMapListChannel := make(chan jsonAsyncReadResult, 1)
	defer close(objMapListChannel)

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go jsonReadByArrayAsync(jobs, results, wg)
	}

	go jsonResultCollector(objMapListChannel, results)

	// in somecases line size gets bigger than default scanner settings
	// so using reader to handle those scenarios
	jsonData := make([]byte, 0, 10000)
	for err == nil {
		jsonTextArr, isPrefix, err := bufferedReader.ReadLine()
		jsonData = append(jsonData, jsonTextArr...)
		if isPrefix {
			continue
		}
		if err == io.EOF {
			break
		}

		jobs <- jsonData
		jsonData = make([]byte, 0, 10000)
	}

	close(jobs)
	wg.Wait()
	close(results)
	for r := range objMapListChannel {
		return r.data, r.err
	}
	return objMapList, errors.New("Unable to read data")
}

type jsonAsyncReadResult struct {
	data []map[string]interface{}
	err  error
}

func jsonResultCollector(collector chan<- jsonAsyncReadResult, results <-chan jsonAsyncReadResult) {
	objMapList := make([]map[string]interface{}, 0)
	for r := range results {
		if r.err != nil {
			collector <- jsonAsyncReadResult{data: objMapList, err: r.err}
			break
		} else {
			objMapList = append(objMapList, r.data...)
		}
	}

	collector <- jsonAsyncReadResult{data: objMapList, err: nil}
}

func jsonReadByArrayAsync(jobs <-chan []byte, results chan<- jsonAsyncReadResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for data := range jobs {
		r, e := jsonReadToArray(data, jsonIsArray(data))
		results <- jsonAsyncReadResult{data: r, err: e}
	}

}

func jsonReadToArray(byteArr []byte, isArray bool) (objMapList []map[string]interface{}, err error) {
	if isArray {
		objMapList = make([]map[string]interface{}, 0)
		err = json.Unmarshal(byteArr, &objMapList)
		return
	}
	objMap := make(map[string]interface{})
	objMapList = make([]map[string]interface{}, 1)
	err = json.Unmarshal(byteArr, &objMap)
	if err != nil {
		return objMapList, err
	}
	objMapList[0] = objMap

	return
}

// ConfigJSONSingleLine While parsing Input, treat eachline as JSON object or Single Object/Array in the file
const ConfigJSONSingleLine = "json.objectOnEachLine"

var jsonConfig = map[string]string{
	ConfigJSONSingleLine: "true",
}

type jsonDataSource struct {
}

func (t *jsonDataSource) Args() map[string]string {
	return jsonConfig
}

func (t *jsonDataSource) Name() string {
	return "json"
}

func (t *jsonDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &jsonDataSourceWriter{data: data, args: args}, nil
}

func (t *jsonDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	jsonReader := jsonDataSourceReader{args: args}
	err := jsonReader.init(reader)
	return &jsonReader, err
}

type jsonDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *jsonDataSourceWriter) Write(writer io.Writer) (err error) {
	schema := t.data.Schema()
	jsonRecords := make([]map[string]interface{}, t.data.Len())
	for index := 0; index < int(t.data.Len()); index++ {
		row := t.data.Get(index)
		obj := make(map[string]interface{})
		for i, c := range schema {
			obj[c.Name] = row.Data()[i]
		}
		jsonRecords[index] = obj
	}

	singlelineParse, err := jsonIsSingleLineParse(t.args)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(writer)
	if singlelineParse {
		for _, r := range jsonRecords {
			err = encoder.Encode(r)
			if err != nil {
				return err
			}
		}
	} else {
		err = encoder.Encode(jsonRecords)
	}

	return
}

type jsonDataSourceReader struct {
	args    map[string]string
	cols    []df.Column
	records [][]interface{}
}

func (t *jsonDataSourceReader) Schema() (columns []df.Column) {
	return t.cols
}

func (t *jsonDataSourceReader) Data() (data [][]interface{}) {
	return t.records
}

func (t *jsonDataSourceReader) readJSON(reader io.Reader) (objMapList []map[string]interface{}, err error) {

	singlelineParse, err := jsonIsSingleLineParse(t.args)
	if err != nil {
		return objMapList, err
	}
	if singlelineParse {
		return jsonReadByLine(reader)
	}
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	return jsonReadToArray(buf, jsonIsArray(buf))
}

func jsonIsArray(data []byte) (isArray bool) {
	return data[0] == '['
}

func (t *jsonDataSourceReader) init(reader io.Reader) (err error) {

	objMapList, err := t.readJSON(reader)
	if err != nil {
		return err
	}

	colMap := map[string]reflect.Type{}

	for _, row := range objMapList {
		for k, v := range row {
			if _, ok := colMap[k]; !ok {
				colMap[k] = reflect.TypeOf(v)
			}
		}
	}
	colMapKeys := make([]string, 0, len(colMap))
	for k := range colMap {
		colMapKeys = append(colMapKeys, k)
	}
	sort.Strings(colMapKeys)

	t.cols = make([]df.Column, len(colMap))
	index := 0
	for _, k := range colMapKeys {
		v := colMap[k]
		typeStr := "string"
		if v != nil {
			typeStr = v.Kind().String()
		}
		if typeStr == "slice" || typeStr == "array" || typeStr == "map" {
			typeStr = "string"
		}

		dfFormat, err := df.GetFormat(typeStr)
		if err != nil {
			return errors.New("json : unable to get format for - " + k + ", " + typeStr)
		}
		t.cols[index] = df.Column{Name: k, Format: dfFormat}
		index = index + 1
	}

	t.records = make([][]interface{}, len(objMapList))

	for i, objMap := range objMapList {

		row := make([]interface{}, len(t.cols))

		for j, c := range t.cols {
			if v, ok := objMap[c.Name]; ok {
				row[j], err = c.Format.Convert(v)
				if err != nil {
					return err
				}
			} else {
				row[j] = nil
			}
		}

		t.records[i] = row

	}

	return err
}

func jsonIsSingleLineParse(config map[string]string) (singlelineParse bool, err error) {
	singleline, ok := config[ConfigJSONSingleLine]
	singlelineParse = true
	if ok {
		singlelineParse, err = strconv.ParseBool(singleline)
		if err != nil {
			return singlelineParse, err
		}
	}

	return
}
