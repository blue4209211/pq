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
		return objMapList, err
	} else {
		objMap := make(map[string]interface{})
		objMapList = make([]map[string]interface{}, 1)
		err = json.Unmarshal(byetArr, &objMap)
		if err != nil {
			return objMapList, err
		}
		objMapList[0] = objMap
		return objMapList, err
	}
}

const ConfigJsonSingleLine = "json.singleline"

var jsonConfig = map[string]string{
	ConfigJsonSingleLine: "true",
}

type JSONDataSource struct {
}

func (self *JSONDataSource) Args() map[string]string {
	return jsonConfig
}

func (self *JSONDataSource) Name() string {
	return "json"
}

func (self *JSONDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &JSONDataSourceWriter{data: data, args: args}, nil
}

func (self *JSONDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	return &JSONDataSourceReader{reader: reader, args: args}, nil
}

type JSONDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (self *JSONDataSourceWriter) Write(writer io.Writer) (err error) {
	records, err := self.data.Data()
	if err != nil {
		return
	}

	schema, err := self.data.Schema()
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

	encoder := json.NewEncoder(writer)
	err = encoder.Encode(jsonRecords)

	return
}

type JSONDataSourceReader struct {
	reader  io.Reader
	args    map[string]string
	cols    []df.Column
	records [][]interface{}
}

func (self *JSONDataSourceReader) Schema() (columns []df.Column, err error) {
	err = self.init()
	if err != nil {
		return columns, err
	}
	return self.cols, err
}

func (self *JSONDataSourceReader) Data() (data [][]interface{}, err error) {
	err = self.init()
	if err != nil {
		return data, err
	}

	return self.records, err

}

func (self *JSONDataSourceReader) readJSON() (objMapList []map[string]interface{}, err error) {

	singleline, ok := self.args[ConfigJsonSingleLine]
	singlelineParse := false
	if ok {
		singlelineParse, err = strconv.ParseBool(singleline)
		if err != nil {
			return objMapList, err
		}
	}
	if singlelineParse {
		return readJSONByLine(self.reader)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, self.reader)
	if err != nil {
		return
	}
	jsonStr := buf.String()
	return readJSONByArray(jsonStr)

}

func (self *JSONDataSourceReader) init() (err error) {
	if len(self.cols) != 0 {
		return err
	}

	objMapList, err := self.readJSON()
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

	self.cols = make([]df.Column, len(colMap))
	index := 0
	for _, k := range colMapKeys {
		v := colMap[k]
		typeStr := v.Kind().String()
		if typeStr == "slice" || typeStr == "array" || typeStr == "map" {
			typeStr = "string"
		}

		dfFormat, err := df.GetFormat(typeStr)
		if err != nil {
			return errors.New("json : unable to get format for - " + k + ", " + typeStr)
		}
		self.cols[index] = df.Column{Name: k, Format: dfFormat}
		index = index + 1
	}

	self.records = make([][]interface{}, len(objMapList))

	for i, objMap := range objMapList {

		row := make([]interface{}, len(self.cols))

		for j, c := range self.cols {
			if v, ok := objMap[c.Name]; ok {
				row[j], err = c.Format.Convert(v)
				if err != nil {
					return err
				}
			} else {
				row[j] = nil
			}
		}

		self.records[i] = row

	}

	return err
}
