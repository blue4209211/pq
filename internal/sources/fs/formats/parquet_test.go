package formats

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/blue4209211/pq/df/inmemory"

	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/compress"
	"github.com/apache/arrow/go/v7/parquet/file"
	"github.com/apache/arrow/go/v7/parquet/schema"
	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestParquetDataSource(t *testing.T) {
	source := ParquetDataSource{}
	assert.Equal(t, source.Name(), "parquet")
}

func TestParquetDataSourceReader(t *testing.T) {

	data := map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{1.0, 2.0, 3.0},
		"c": []string{"c1", "c2", ""},
		"d": []string{"d1", "d,2", "d2"},
	}

	fields := make([]schema.Node, len(data))
	fields[0], _ = schema.NewPrimitiveNode("a", parquet.Repetitions.Optional, parquetKindToParquetTypeMap[reflect.Int64], 0, -1)
	fields[1], _ = schema.NewPrimitiveNode("b", parquet.Repetitions.Optional, parquetKindToParquetTypeMap[reflect.Float64], 1, -1)
	fields[2], _ = schema.NewPrimitiveNode("c", parquet.Repetitions.Optional, parquetKindToParquetTypeMap[reflect.String], 2, -1)
	fields[3], _ = schema.NewPrimitiveNode("d", parquet.Repetitions.Optional, parquetKindToParquetTypeMap[reflect.String], 3, -1)

	nodeGroup, _ := schema.NewGroupNode("root", parquet.Repetitions.Optional, fields, -1)
	descr := schema.NewSchema(nodeGroup)

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < descr.NumColumns(); i++ {
		opts = append(opts, parquet.WithCompressionFor(descr.Column(i).Name(), compress.Codecs.Uncompressed))
	}
	opts = append(opts, parquet.WithDataPageSize(3))
	opts = append(opts, parquet.WithVersion(parquet.V2_LATEST))
	opts = append(opts, parquet.WithDataPageVersion(parquet.DataPageV2))
	opts = append(opts, parquet.WithDictionaryDefault(false))

	props := parquet.NewWriterProperties(opts...)

	var buff bytes.Buffer
	parquertWriter := file.NewParquetWriter(&buff, nodeGroup, file.WithWriterProps(props))
	rowGroupWriter := parquertWriter.AppendBufferedRowGroup()

	for col := 0; col < descr.NumColumns(); col++ {
		columnChunkWriter, err := rowGroupWriter.Column(col)
		if err != nil {
			t.Log("unable to get next coulmn", err)
			return
		}

		defValues := make([]int16, 3)
		for idx := range defValues {
			defValues[idx] = 1
		}

		count, err := parquetWriteBatchValues(columnChunkWriter, data[descr.Column(col).Name()], defValues, nil)
		if err != nil {
			t.Log("unable to write data", err)
			return
		}
		columnChunkWriter.Close()
		t.Log("rows written", descr.Column(col).Name(), count)
	}
	err := rowGroupWriter.Close()
	if err != nil {
		t.Log("Unable to close rowGroupWriter", err)
		return
	}
	err = parquertWriter.Close()
	if err != nil {
		t.Log("Unable to close parquertWriter", err)
		return
	}

	t.Log(rowGroupWriter.NumColumns())
	t.Log(rowGroupWriter.NumRows())
	t.Log(rowGroupWriter.TotalBytesWritten())

	source := ParquetDataSource{}
	parquetReader, err := source.Reader(bytes.NewReader(buff.Bytes()), map[string]string{
		ConfigParquetSingleLine: "false",
	})
	assert.NoError(t, err)

	schema := parquetReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "a", schema.Get(0).Name)
	assert.Equal(t, "integer", schema.Get(0).Format.Name())
	assert.Equal(t, "b", schema.Get(1).Name)
	assert.Equal(t, "double", schema.Get(1).Format.Name())
	assert.Equal(t, "c", schema.Get(2).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	assert.Equal(t, "d", schema.Get(3).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	dfData := *(parquetReader.Data())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(dfData))
	assert.Equal(t, int64(1), dfData[0].GetRaw(0))
	assert.Equal(t, 1.0, dfData[0].GetRaw(1))
	assert.Equal(t, "c1", dfData[0].GetRaw(2))
	assert.Equal(t, "d1", dfData[0].GetRaw(3))
	assert.Equal(t, 3.0, dfData[2].GetRaw(1))
	assert.Equal(t, "", dfData[2].GetRaw(2))
}

func TestParquetDataSourceWriter(t *testing.T) {
	schema := df.NewSchema([]df.SeriesSchema{{Name: "a", Format: df.IntegerFormat}, {Name: "b", Format: df.DoubleFormat}})
	rows := []df.Row{
		inmemory.NewRow(schema, &([]df.Value{inmemory.NewIntValue(1), inmemory.NewDoubleValue(1.0)})),
		inmemory.NewRow(schema, &([]df.Value{inmemory.NewIntValue(2), inmemory.NewDoubleValue(2.0)})),
		inmemory.NewRow(schema, &([]df.Value{inmemory.NewIntValue(3), inmemory.NewDoubleValue(3.0)})),
	}

	dataframe := inmemory.NewDataframeFromRow(schema, &rows)

	source := ParquetDataSource{}
	writer, err := source.Writer(dataframe, map[string]string{
		ConfigParquetSingleLine: "false",
	})
	assert.NoError(t, err)
	buff := new(strings.Builder)
	err = writer.Write(buff)
	assert.NoError(t, err)

	parquetReader, err := source.Reader(strings.NewReader(buff.String()), map[string]string{
		ConfigParquetSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.NotNil(t, parquetReader)

	schema = parquetReader.Schema()
	assert.NoError(t, err)
	assert.NoError(t, err)

	schema2 := dataframe.Schema()
	assert.Equal(t, schema, schema2)

}

func BenchmarkParquetParsing(b *testing.B) {

	schema := df.NewSchema([]df.SeriesSchema{{Name: "a", Format: df.IntegerFormat}, {Name: "b", Format: df.DoubleFormat}, {Name: "c", Format: df.StringFormat}, {Name: "d", Format: df.BoolFormat}})
	records := make([]df.Row, 1000)

	for i := range records {
		records[i] = inmemory.NewRow(schema, &([]df.Value{
			inmemory.NewIntValue(1),
			inmemory.NewDoubleValue(1.0),
			inmemory.NewStringValue("abc"),
			inmemory.NewBoolValue(true),
		}))
	}

	dataframe := inmemory.NewDataframeFromRow(schema, &records)

	source := ParquetDataSource{}
	writer, _ := source.Writer(dataframe, map[string]string{
		ConfigParquetSingleLine: "false",
	})
	buff := new(strings.Builder)
	_ = writer.Write(buff)

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parquetReader, _ := source.Reader(strings.NewReader(buff.String()), map[string]string{
				ConfigParquetSingleLine: "false",
			})
			parquetReader.Schema()
			parquetReader.Data()
		}
	})
}
