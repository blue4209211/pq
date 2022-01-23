package sources

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/compress"
	"github.com/apache/arrow/go/v7/parquet/file"
	"github.com/apache/arrow/go/v7/parquet/schema"
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/log"
)

func parquetReadByLine(reader io.Reader) (schema []df.Column, data [][]interface{}, err error) {
	bufferedReader := bufio.NewReader(reader)
	jobs := make(chan string, 5)
	results := make(chan parquetAsyncReadResult, 100)
	wg := new(sync.WaitGroup)
	objMapListChannel := make(chan parquetAsyncReadResult, 1)
	defer close(objMapListChannel)

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go parquetReadByArrayAsync(jobs, results, wg)
	}

	go parquetResultCollector(objMapListChannel, results)

	// in somecases line size gets bigger than default scanner settings
	// so using reader to handle those scenarios
	parquetText := ""
	for err == nil {
		parquetTextArr, isPrefix, err := bufferedReader.ReadLine()
		parquetText = parquetText + string(parquetTextArr)
		if isPrefix {
			continue
		}
		if err == io.EOF {
			break
		}

		jobs <- parquetText
		parquetText = ""
	}

	close(jobs)
	wg.Wait()
	close(results)
	for r := range objMapListChannel {
		return r.schema, r.data, r.err
	}
	return schema, data, errors.New("Unable to read data")
}

type parquetAsyncReadResult struct {
	schema []df.Column
	data   [][]interface{}
	err    error
}

func parquetResultCollector(collector chan<- parquetAsyncReadResult, results <-chan parquetAsyncReadResult) {
	var schema []df.Column
	records := make([][]interface{}, 0)
	for r := range results {
		if r.err != nil {
			collector <- parquetAsyncReadResult{err: r.err}
			break
		} else {
			if schema == nil {
				schema = r.schema
			}
			dfRecords := r.data
			records = append(records, dfRecords...)
		}
	}

	collector <- parquetAsyncReadResult{schema: schema, data: records, err: nil}
}

func parquetReadByArrayAsync(jobs <-chan string, results chan<- parquetAsyncReadResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for data := range jobs {
		s, r, e := parquetReadToArray(data)
		results <- parquetAsyncReadResult{schema: s, data: r, err: e}
	}

}

func parquetReadToArray(parquetText string) (schema []df.Column, data [][]interface{}, err error) {
	parquetReader, err := file.NewParquetReader(bytes.NewReader([]byte(parquetText)))
	if err != nil {
		log.Error("unable to read parquet", err)
		return schema, data, err
	}
	defer parquetReader.Close()

	dfSchema := make([]df.Column, parquetReader.MetaData().Schema.NumColumns())
	dataArr := make([][]interface{}, parquetReader.NumRows())
	for i := int64(0); i < parquetReader.NumRows(); i++ {
		dataArr[i] = make([]interface{}, parquetReader.MetaData().Schema.NumColumns())
	}

	for r := 0; r < parquetReader.NumRowGroups(); r++ {
		rowGroupReader := parquetReader.RowGroup(r)
		for c := 0; c < rowGroupReader.NumColumns(); c++ {
			colReader := rowGroupReader.Column(c)
			dfType, err := df.GetFormatFromKind(parquetParquetTypeToKindMap[colReader.Descriptor().PhysicalType()])
			if err != nil {
				log.Error("unable to get schema", colReader.Descriptor().PhysicalType(), err)
				return schema, data, err
			}
			dfSchema[c] = df.Column{Name: colReader.Descriptor().Name(), Format: dfType}

			switch colReader.Descriptor().PhysicalType() {
			case parquet.Types.FixedLenByteArray:
				fixedByteReader := colReader.(*file.FixedLenByteArrayColumnChunkReader)
				fixedByteValues := make([]parquet.FixedLenByteArray, parquetReader.NumRows())
				_, _, err := fixedByteReader.ReadBatch(parquetReader.NumRows(), fixedByteValues, nil, nil)
				if err != nil {
					log.Error("unable to read fixedbytearray", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = string(fixedByteValues[j])
				}
			case parquet.Types.Double:
				doubleReader := colReader.(*file.Float64ColumnChunkReader)
				doubleValues := make([]float64, parquetReader.NumRows())
				_, _, err := doubleReader.ReadBatch(parquetReader.NumRows(), doubleValues, nil, nil)
				if err != nil {
					log.Error("unable to read double", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = doubleValues[j]
				}
			case parquet.Types.Float:
				floatReader := colReader.(*file.Float32ColumnChunkReader)
				floatValues := make([]float32, parquetReader.NumRows())
				_, _, err := floatReader.ReadBatch(parquetReader.NumRows(), floatValues, nil, nil)
				if err != nil {
					log.Error("unable to read float", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = float64(floatValues[j])
				}
			case parquet.Types.ByteArray:
				byteReader := colReader.(*file.ByteArrayColumnChunkReader)
				byteValues := make([]parquet.ByteArray, parquetReader.NumRows())
				_, _, err := byteReader.ReadBatch(parquetReader.NumRows(), byteValues, nil, nil)
				if err != nil {
					log.Error("unable to read bytearray", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = string(byteValues[j])
				}
			case parquet.Types.Int32:
				intReader := colReader.(*file.Int32ColumnChunkReader)
				intValues := make([]int32, parquetReader.NumRows())
				_, _, err := intReader.ReadBatch(parquetReader.NumRows(), intValues, nil, nil)
				if err != nil {
					log.Error("unable to read int32", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = int64(intValues[j])
				}

			case parquet.Types.Int64:
				int64Reader := colReader.(*file.Int64ColumnChunkReader)
				int64Values := make([]int64, parquetReader.NumRows())
				_, _, err := int64Reader.ReadBatch(parquetReader.NumRows(), int64Values, nil, nil)

				if err != nil {
					log.Error("unable to read int64", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = int64Values[j]
				}
			case parquet.Types.Int96:
				int96Reader := colReader.(*file.Int96ColumnChunkReader)
				int96Values := make([]parquet.Int96, parquetReader.NumRows())
				_, _, err := int96Reader.ReadBatch(parquetReader.NumRows(), int96Values, nil, nil)
				if err != nil {
					log.Error("unable to read int96", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = int96Values[j].String()
				}
			case parquet.Types.Boolean:
				boolReader := colReader.(*file.BooleanColumnChunkReader)
				boolValues := make([]bool, parquetReader.NumRows())
				_, _, err := boolReader.ReadBatch(parquetReader.NumRows(), boolValues, nil, nil)
				if err != nil {
					log.Error("unable to read boolean", err)
					return schema, data, err
				}
				for j := int64(0); j < parquetReader.NumRows(); j++ {
					dataArr[j][c] = boolValues[j]
				}
			}

		}
	}
	return dfSchema, dataArr, err
}

// ConfigParquetSingleLine While parsing Input, treat eachline as parquet object or Single Object/Array in the file
const ConfigParquetSingleLine = "parquet.objectOnEachLine"

var parquetConfig = map[string]string{
	ConfigParquetSingleLine: "false",
}

type parquetDataSource struct {
}

func (t *parquetDataSource) Args() map[string]string {
	return parquetConfig
}

func (t *parquetDataSource) Name() string {
	return "parquet"
}

func (t *parquetDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &parquetDataSourceWriter{data: data, args: args}, nil
}

func (t *parquetDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	parquetReader := &parquetDataSourceReader{args: args}
	err := parquetReader.init(reader)
	return parquetReader, err
}

type parquetDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *parquetDataSourceWriter) Write(writer io.Writer) (err error) {
	dfSchema := t.data.Schema()
	fields := make([]schema.Node, len(dfSchema))
	for i, f := range dfSchema {
		fields[i], _ = schema.NewPrimitiveNode(f.Name, parquet.Repetitions.Optional, parquetKindToParquetTypeMap[f.Format.Type()], int32(i), -1)
	}

	nodeGroup, _ := schema.NewGroupNode("root", parquet.Repetitions.Optional, fields, -1)
	descr := schema.NewSchema(nodeGroup)

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < descr.NumColumns(); i++ {
		opts = append(opts, parquet.WithCompressionFor(descr.Column(i).Name(), compress.Codecs.Uncompressed))
	}
	opts = append(opts, parquet.WithDataPageSize(t.data.Len()))
	opts = append(opts, parquet.WithVersion(parquet.V2_LATEST))
	opts = append(opts, parquet.WithDataPageVersion(parquet.DataPageV2))
	opts = append(opts, parquet.WithDictionaryDefault(false))

	props := parquet.NewWriterProperties(opts...)

	parquertWriter := file.NewParquetWriter(writer, nodeGroup, file.WithWriterProps(props))
	rowGroupWriter := parquertWriter.AppendBufferedRowGroup()

	for col := 0; col < descr.NumColumns(); col++ {
		columnChunkWriter, err := rowGroupWriter.Column(col)
		if err != nil {
			log.Error("unable to get next coulmn ", err)
			return err
		}

		defValues := make([]int16, 3)
		for idx := range defValues {
			defValues[idx] = 1
		}

		var writerValue interface{}

		switch dfSchema[col].Format.Type() {
		case reflect.Int64:
			writerValue = make([]int64, t.data.Len(), t.data.Len())
		case reflect.Float64:
			writerValue = make([]float64, t.data.Len(), t.data.Len())
		case reflect.String:
			writerValue = make([]string, t.data.Len(), t.data.Len())
		case reflect.Bool:
			writerValue = make([]bool, t.data.Len(), t.data.Len())
		}

		for i := 0; i < int(t.data.Len()); i++ {
			r := t.data.Get(i)
			if r.Data()[col] == nil {
				continue
			}

			switch arr := writerValue.(type) {
			case []bool:
				arr[i] = r.Data()[col].(bool)
			case []int64:
				arr[i] = r.Data()[col].(int64)
			case []string:
				arr[i] = r.Data()[col].(string)
			case []float64:
				arr[i] = r.Data()[col].(float64)
			}
		}

		_, err = parquetWriteBatchValues(columnChunkWriter, writerValue, defValues, nil)
		if err != nil {
			log.Error("unable to write data", err)
			return err
		}
		columnChunkWriter.Close()
	}
	err = rowGroupWriter.Close()
	if err != nil {
		log.Error("Unable to close rowGroupWriter", err)
		return
	}
	err = parquertWriter.Close()
	if err != nil {
		log.Error("Unable to close parquertWriter", err)
		return
	}

	return
}

type parquetDataSourceReader struct {
	args    map[string]string
	cols    []df.Column
	records [][]interface{}
}

func (t *parquetDataSourceReader) Schema() (columns []df.Column) {
	return t.cols
}

func (t *parquetDataSourceReader) Data() (data [][]interface{}) {
	return t.records

}

func (t *parquetDataSourceReader) readParquet(reader io.Reader) (schema []df.Column, data [][]interface{}, err error) {

	singlelineParse, err := parquetIsSingleLineParse(t.args)
	if err != nil {
		return schema, data, err
	}
	if singlelineParse {
		return parquetReadByLine(reader)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		return
	}
	parquetStr := buf.String()
	return parquetReadToArray(parquetStr)

}

func (t *parquetDataSourceReader) init(reader io.Reader) (err error) {
	schema, records, err := t.readParquet(reader)
	if err != nil {
		return err
	}

	t.cols = schema
	t.records = records
	return err
}

func parquetIsSingleLineParse(config map[string]string) (singlelineParse bool, err error) {
	singleline, ok := config[ConfigParquetSingleLine]
	singlelineParse = false
	if ok {
		singlelineParse, err = strconv.ParseBool(singleline)
		if err != nil {
			return singlelineParse, err
		}
	}

	return
}

func parquetWriteBatchValues(writer file.ColumnChunkWriter, vals interface{}, defLevels, repLevels []int16) (int64, error) {

	switch w := writer.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch(vals.([]int32), defLevels, repLevels)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch(vals.([]int64), defLevels, repLevels)
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch(vals.([]float32), defLevels, repLevels)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch(vals.([]float64), defLevels, repLevels)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch(vals.([]parquet.Int96), defLevels, repLevels)
	case *file.ByteArrayColumnChunkWriter:
		if reflect.TypeOf(vals) == reflect.TypeOf([]string{}) {
			stringData := vals.([]string)
			byteArrayData := make([]parquet.ByteArray, len(stringData))
			for i, s := range stringData {
				byteArrayData[i] = parquet.ByteArray(s)
			}
			return w.WriteBatch(byteArrayData, defLevels, repLevels)
		}
		return w.WriteBatch(vals.([]parquet.ByteArray), defLevels, repLevels)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch(vals.([]bool), defLevels, repLevels)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch(vals.([]parquet.FixedLenByteArray), defLevels, repLevels)
	default:
		panic("unimplemented")
	}
}

func parquetReadBatch(reader file.ColumnChunkReader, batch int64, valueOut interface{}, valuesRead int64, defLevels, repLevels []int16) int64 {
	switch r := reader.(type) {
	case *file.Int32ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]int32)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Int64ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]int64)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Float32ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]float32)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Float64ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]float64)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Int96ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]parquet.Int96)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.ByteArrayColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]parquet.ByteArray)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.BooleanColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]bool)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.FixedLenByteArrayColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, valueOut.([]parquet.FixedLenByteArray)[valuesRead:], defLevels, repLevels)
		return int64(read)
	default:
		panic("unimplemented")
	}
}

var parquetKindToParquetTypeMap = map[reflect.Kind]parquet.Type{
	reflect.Bool:    parquet.Types.Boolean,
	reflect.Int32:   parquet.Types.Int32,
	reflect.Int64:   parquet.Types.Int64,
	reflect.Float32: parquet.Types.Float,
	reflect.Float64: parquet.Types.Double,
	reflect.String:  parquet.Types.ByteArray,
}

var parquetParquetTypeToKindMap = map[parquet.Type]reflect.Kind{
	parquet.Types.Boolean:   reflect.Bool,
	parquet.Types.Int32:     reflect.Int32,
	parquet.Types.Int64:     reflect.Int64,
	parquet.Types.Int96:     reflect.String,
	parquet.Types.Float:     reflect.Float32,
	parquet.Types.Double:    reflect.Float64,
	parquet.Types.ByteArray: reflect.String,
}
