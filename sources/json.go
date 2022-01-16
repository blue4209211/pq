package sources

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/blue4209211/pq/df"
)

func readJSONByLine(reader io.Reader) (objMapList []map[string]interface{}, err error) {
	scanner := bufio.NewScanner(reader)

	objMapList = make([]map[string]interface{}, 0)

	for scanner.Scan() {
		jsonText := scanner.Text()
		objs, err := readJSONByArray(jsonText)
		if err != nil {
			return objMapList, err
		}
		objMapList = append(objMapList, objs...)

	}
	return objMapList, err
}

func readJSONByArray(jsonText string) (objMapList []map[string]interface{}, err error) {
	isArray := false
	if strings.Index(jsonText, "[") == 0 {
		isArray = true
	}
	byetArr := []byte(jsonText)
	if isArray {
		objMapList = make([]map[string]interface{}, 0)
		err = json.Unmarshal(byetArr, &objMapList)
		return
	}
	objMap := make(map[string]interface{})
	objMapList = make([]map[string]interface{}, 1)
	err = json.Unmarshal(byetArr, &objMap)
	if err != nil {
		return
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
	return &jsonDataSourceReader{reader: reader, args: args}, nil
}

type jsonDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *jsonDataSourceWriter) Write(writer io.Writer) (err error) {
	records, err := t.data.Data()
	if err != nil {
		return
	}

	schema, err := t.data.Schema()
	if err != nil {
		return
	}

	jsonRecords := make([]map[string]interface{}, len(records))
	for index, row := range records {
		obj := make(map[string]interface{})
		for i, c := range schema {
			obj[c.Name] = row[i]
		}
		jsonRecords[index] = obj
	}

	singlelineParse, err := isSingleLineParse(t.args)
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
	reader  io.Reader
	args    map[string]string
	cols    []df.Column
	records [][]interface{}
}

func (t *jsonDataSourceReader) Schema() (columns []df.Column, err error) {
	err = t.init()
	if err != nil {
		return columns, err
	}
	return t.cols, err
}

func (t *jsonDataSourceReader) Data() (data [][]interface{}, err error) {
	err = t.init()
	if err != nil {
		return data, err
	}

	return t.records, err

}

func (t *jsonDataSourceReader) readJSON() (objMapList []map[string]interface{}, err error) {

	singlelineParse, err := isSingleLineParse(t.args)
	if err != nil {
		return objMapList, err
	}
	if singlelineParse {
		return readJSONByLine(t.reader)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, t.reader)
	if err != nil {
		return
	}
	jsonStr := buf.String()
	return readJSONByArray(jsonStr)

}

func (t *jsonDataSourceReader) init() (err error) {
	if len(t.cols) != 0 {
		return err
	}

	objMapList, err := t.readJSON()
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

func isSingleLineParse(config map[string]string) (singlelineParse bool, err error) {
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
