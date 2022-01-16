package sources

import (
	"github.com/blue4209211/pq/df"
)

// DatasourceDataFrame Struct to store data and schema
type DatasourceDataFrame struct {
	name   string
	reader DataFrameReader
}

// Data return data stored in Dataframe
func (t *DatasourceDataFrame) Data() ([][]interface{}, error) {
	return t.reader.Data()
}

// Schema Return dataframe schema
func (t *DatasourceDataFrame) Schema() ([]df.Column, error) {
	return t.reader.Schema()
}

// Name Return dataframe name
func (t *DatasourceDataFrame) Name() string {
	return t.name
}

// NewDatasourceDataFrame Return new data frame from datasource
func NewDatasourceDataFrame(name string, reader DataFrameReader) DatasourceDataFrame {
	return DatasourceDataFrame{name: name, reader: reader}
}
