package files

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blue4209211/pq/df"
)

func xmlReadByLine(reader io.Reader, config map[string]string) (objMapList []map[string]interface{}, err error) {
	bufferedReader := bufio.NewReader(reader)
	jobs := make(chan string, 5)
	results := make(chan xmlAsyncReadResult, 100)
	wg := new(sync.WaitGroup)
	objMapListChannel := make(chan xmlAsyncReadResult, 1)
	defer close(objMapListChannel)

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go xmlReadByArrayAsync(jobs, results, wg, config)
	}

	go xmlResultCollector(objMapListChannel, results)

	// in somecases line size gets bigger than default scanner settings
	// so using reader to handle those scenarios
	xmlText := ""
	for err == nil {
		xmlTextArr, isPrefix, err := bufferedReader.ReadLine()
		xmlText = xmlText + string(xmlTextArr)
		if isPrefix {
			continue
		}
		if err == io.EOF {
			break
		}

		jobs <- xmlText
		xmlText = ""
	}

	close(jobs)
	wg.Wait()
	close(results)
	for r := range objMapListChannel {
		return r.data, r.err
	}
	return objMapList, errors.New("Unable to read data")
}

type xmlAsyncReadResult struct {
	data []map[string]interface{}
	err  error
}

func xmlResultCollector(collector chan<- xmlAsyncReadResult, results <-chan xmlAsyncReadResult) {
	objMapList := make([]map[string]interface{}, 0)
	for r := range results {
		if r.err != nil {
			collector <- xmlAsyncReadResult{data: objMapList, err: r.err}
			break
		} else {
			objMapList = append(objMapList, r.data...)
		}
	}

	collector <- xmlAsyncReadResult{data: objMapList, err: nil}
}

func xmlReadByArrayAsync(jobs <-chan string, results chan<- xmlAsyncReadResult, wg *sync.WaitGroup, config map[string]string) {
	defer wg.Done()

	for data := range jobs {
		r, e := xmlReadToArray(data, config)
		results <- xmlAsyncReadResult{data: r, err: e}
	}

}

type xmlCustomMarsher struct {
	data        []map[string]interface{}
	currentData map[string]interface{}
	elName      string
}

func (c *xmlCustomMarsher) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch tt := t.(type) {
		case xml.StartElement:
			if tt.Name.Local == c.elName {
				if c.currentData != nil {
					c.data = append(c.data, c.currentData)
					c.currentData = nil
				}
				if c.currentData == nil {
					c.currentData = make(map[string]interface{})
					for _, a := range tt.Attr {
						c.currentData["_"+a.Name.Local] = a.Value
					}
				}
			} else {
				if c.currentData != nil {
					var value string
					d.DecodeElement(&value, &start)
					c.currentData[tt.Name.Local] = value
				}
			}
		case xml.EndElement:
			// do nothing
		}
	}

	if c.currentData != nil {
		c.data = append(c.data, c.currentData)
		c.currentData = nil
	}

	return nil
}

func xmlReadToArray(xmlText string, config map[string]string) (objMapList []map[string]interface{}, err error) {
	byetArr := []byte(xmlText)
	customMarshaller := xmlCustomMarsher{elName: config[ConfigXMLElementName], data: make([]map[string]interface{}, 0)}
	err = xml.Unmarshal(byetArr, &customMarshaller)
	if err != nil {
		return customMarshaller.data, err
	}
	return customMarshaller.data, err
}

// ConfigXMLSingleLine While parsing Input, treat eachline as XML object or Single Object/Array in the file
const ConfigXMLSingleLine = "xml.objectOnEachLine"

// ConfigXMLElementName XML element to use for parsing
const ConfigXMLElementName = "xml.elementName"

var xmlConfig = map[string]string{
	ConfigXMLSingleLine:  "true",
	ConfigXMLElementName: "",
}

type xmlDataSource struct {
}

func (t *xmlDataSource) Args() map[string]string {
	return xmlConfig
}

func (t *xmlDataSource) Name() string {
	return "xml"
}

func (t *xmlDataSource) Writer(data df.DataFrame, args map[string]string) (StreamWriter, error) {
	return &xmlDataSourceWriter{data: data, args: args}, nil
}

func (t *xmlDataSource) Reader(reader io.Reader, args map[string]string) (StreamReader, error) {
	xmlReader := xmlDataSourceReader{args: args}
	err := xmlReader.init(reader)
	return &xmlReader, err
}

type xmlDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *xmlDataSourceWriter) Write(writer io.Writer) (err error) {
	schema := t.data.Schema()
	xmlElementName := t.args[ConfigXMLElementName]

	singlelineParse, err := xmlIsSingleLineParse(t.args)
	if err != nil {
		return err
	}
	if !singlelineParse {
		writer.Write([]byte("<root>"))
	}

	for i := int64(0); i < t.data.Len(); i++ {
		rf := "<%s%s>%s</%s>"
		attrs := ""
		nestElements := ""

		for j, c := range schema.Columns() {
			if strings.Index(c.Name, "_") == 0 {
				attrs = attrs + fmt.Sprintf(" %s=\"%s\"", c.Name[1:], t.data.Get(i).Data()[j])
			} else {
				nestElements = nestElements + fmt.Sprintf("<%s>%s</%s>", c.Name, t.data.Get(i).Data()[j], c.Name)
			}
		}
		writer.Write([]byte(fmt.Sprintf(rf, xmlElementName, attrs, nestElements, xmlElementName)))
		if singlelineParse {
			writer.Write([]byte("\n"))
		}
	}

	if !singlelineParse {
		writer.Write([]byte("</root>\n"))
	}

	return
}

type xmlDataSourceReader struct {
	args    map[string]string
	cols    []df.Column
	records [][]interface{}
}

func (t *xmlDataSourceReader) Schema() (columns []df.Column) {
	return t.cols
}

func (t *xmlDataSourceReader) Data() (data [][]interface{}) {
	return t.records
}

func (t *xmlDataSourceReader) readXML(reader io.Reader) (objMapList []map[string]interface{}, err error) {

	singlelineParse, err := xmlIsSingleLineParse(t.args)
	if err != nil {
		return objMapList, err
	}
	if singlelineParse {
		return xmlReadByLine(reader, t.args)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		return
	}
	xmlStr := buf.String()
	return xmlReadToArray(xmlStr, t.args)

}

func (t *xmlDataSourceReader) init(reader io.Reader) (err error) {
	objMapList, err := t.readXML(reader)
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
			return errors.New("xml : unable to get format for - " + k + ", " + typeStr)
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

func xmlIsSingleLineParse(config map[string]string) (singlelineParse bool, err error) {
	singleline, ok := config[ConfigXMLSingleLine]
	singlelineParse = true
	if ok {
		singlelineParse, err = strconv.ParseBool(singleline)
		if err != nil {
			return singlelineParse, err
		}
	}

	return
}
