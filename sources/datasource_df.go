package sources

import (
	"github.com/blue4209211/pq/df"
)

type DatasourceDataFrame struct {
	name   string
	reader DataFrameReader
}

func (self *DatasourceDataFrame) Data() ([][]interface{}, error) {
	return self.reader.Data()
}

func (self *DatasourceDataFrame) Schema() ([]df.Column, error) {
	return self.reader.Schema()
}

func (self *DatasourceDataFrame) Name() string {
	return self.name
}

func NewDatasourceDataFrame(name string, reader DataFrameReader) DatasourceDataFrame {
	return DatasourceDataFrame{name: name, reader: reader}
}
