package formats

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"encoding/json"

	"github.com/blue4209211/pq/df"
)

func jsonReadByLine(reader io.Reader, jsonRootNode string) (objMapList []map[string]any, err error) {
	bufferedReader := bufio.NewReader(reader)
	jobs := make(chan []byte, 1000)
	results := make(chan jsonAsyncReadResult, 1000)
	wg := new(sync.WaitGroup)
	objMapListChannel := make(chan jsonAsyncReadResult, 1)
	defer close(objMapListChannel)

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go jsonReadByArrayAsync(jobs, results, wg, jsonRootNode)
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

		copiedData := make([]byte, len(jsonData))
		copy(copiedData, jsonData)
		jobs <- copiedData
		jsonData = jsonData[:0]
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
	data []map[string]any
	err  error
}

func jsonResultCollector(collector chan<- jsonAsyncReadResult, results <-chan jsonAsyncReadResult) {
	objMapList := make([]map[string]any, 0)
	sentToCollector := false
	for r := range results {
		if r.err != nil {
			if !sentToCollector {
				collector <- jsonAsyncReadResult{data: objMapList, err: r.err}
				sentToCollector = true
			}
		} else {
			objMapList = append(objMapList, r.data...)
		}
	}
	if !sentToCollector {
		collector <- jsonAsyncReadResult{data: objMapList, err: nil}
	}
}

func jsonReadByArrayAsync(jobs <-chan []byte, results chan<- jsonAsyncReadResult, wg *sync.WaitGroup, jsonRootNode string) {
	defer wg.Done()

	for data := range jobs {
		r, e := jsonReadToArray(data, jsonIsArray(data), jsonRootNode)
		results <- jsonAsyncReadResult{data: r, err: e}
	}

}

// customJSONUnmarshaller is custom unmarshaller to handle only single level
type customJSONUnmarshaller struct {
	data any
}

func (a *customJSONUnmarshaller) UnmarshalJSON(b []byte) (err error) {
	switch b[0] {
	// true
	case 't':
		a.data = true
	// false
	case 'f':
		a.data = false
	// null
	case 'n':
		a.data = nil
	// strings
	case '"':
		if len(b) == 2 {
			a.data = ""
		} else {
			rawStr := string(b)
			rawStr = rawStr[1 : len(rawStr)-1]
			rawStr = strings.ReplaceAll(rawStr, `\"`, `"`)
			a.data = rawStr
		}
	// arrays/objects
	case '[', '{':
		a.data = string(b)
	// numbers
	default:
		a.data, err = strconv.ParseFloat(string(b), 64)
	}
	return err
}

func jsonReadToArray(byteArr []byte, isArray bool, jsonRootNode string) (objMapList []map[string]any, err error) {
	if isArray {
		objMapListCustom := make([]map[string]customJSONUnmarshaller, 0)
		err = json.Unmarshal(byteArr, &objMapListCustom)
		if err != nil {
			return objMapList, err
		}

		objMapList = make([]map[string]any, len(objMapListCustom))

		for i, objCustom := range objMapListCustom {
			obj := make(map[string]any)
			for k, v := range objCustom {
				obj[k] = v.data
			}
			objMapList[i] = obj
		}
	} else {
		objMapCustom := make(map[string]*customJSONUnmarshaller)
		objMapList = make([]map[string]any, 1)
		err = json.Unmarshal(byteArr, &objMapCustom)
		if err != nil {
			return objMapList, err
		}

		objMap := make(map[string]any)
		for k, v := range objMapCustom {
			objMap[k] = v.data
		}

		objMapList[0] = objMap
	}

	if jsonRootNode != "" {
		jsonRootNodeList := strings.Split(jsonRootNode, ".")
		jsonRootNodeItem := jsonRootNodeList[0]
		if len(jsonRootNodeList) > 1 {
			jsonRootNode = strings.Join(jsonRootNodeList[1:], ".")
		} else {
			jsonRootNode = ""
		}
		objMapListCustom := make([]map[string]any, 0)
		for _, objMap := range objMapList {
			for k, v := range objMap {
				if jsonRootNodeItem == k {
					newData := []byte(v.(string))
					newDataArr, err := jsonReadToArray(newData, jsonIsArray(newData), jsonRootNode)
					if err != nil {
						return objMapList, err
					}
					objMapListCustom = append(objMapListCustom, newDataArr...)
					break
				}
			}
		}

		objMapList = objMapListCustom
	}

	return
}

// ConfigJSONSingleLine While parsing Input, treat eachline as JSON object or Single Object/Array in the file
const ConfigJSONSingleLine = "json.objectOnEachLine"

// ConfigJSONRootNode root node to use while reading data
const ConfigJSONRootNode = "json.rootNode"

var jsonConfig = map[string]string{
	ConfigJSONSingleLine: "true",
	ConfigJSONRootNode:   "",
}

type JsonDataSource struct {
}

func (t *JsonDataSource) Args() map[string]string {
	return jsonConfig
}

func (t *JsonDataSource) Name() string {
	return "json"
}

func (t *JsonDataSource) Writer(data df.DataFrame, args map[string]string) (FormatWriter, error) {
	return &jsonDataSourceWriter{data: data, args: args}, nil
}

func (t *JsonDataSource) Reader(reader io.Reader, args map[string]string) (FormatReader, error) {
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
	jsonRecords := make([]map[string]any, t.data.Len())
	for index := int64(0); index < t.data.Len(); index++ {
		row := t.data.GetRow(index)
		obj := make(map[string]any)
		for i, c := range schema.Series() {
			obj[c.Name] = row.Data()[i].Get()
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
	cols    []df.SeriesSchema
	records [][]any
}

func (t *jsonDataSourceReader) Schema() (columns []df.SeriesSchema) {
	return t.cols
}

func (t *jsonDataSourceReader) Data() (data [][]any) {
	return t.records
}

func (t *jsonDataSourceReader) readJSON(reader io.Reader) (objMapList []map[string]any, err error) {

	singlelineParse, err := jsonIsSingleLineParse(t.args)
	if err != nil {
		return objMapList, err
	}

	jsonRootNode, ok := t.args[ConfigJSONRootNode]
	if !ok {
		jsonRootNode = ""
	}

	if singlelineParse {
		return jsonReadByLine(reader, jsonRootNode)
	}
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	return jsonReadToArray(buf, jsonIsArray(buf), jsonRootNode)
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

	t.cols = make([]df.SeriesSchema, len(colMap))
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
		t.cols[index] = df.SeriesSchema{Name: k, Format: dfFormat}
		index = index + 1
	}

	t.records = make([][]any, len(objMapList))

	for i, objMap := range objMapList {

		row := make([]any, len(t.cols))

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
