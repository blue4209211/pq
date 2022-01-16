package df

import "strconv"

type InmemoryDataFrame struct {
	name   string
	schema []Column
	data   [][]interface{}
}

func (self *InmemoryDataFrame) Data() ([][]interface{}, error) {
	return self.data, nil
}

func (self *InmemoryDataFrame) Schema() ([]Column, error) {
	return self.schema, nil
}

func (self *InmemoryDataFrame) Name() string {
	return self.name
}

var dfCounter = 0

func NewInmemoryDataframe(cols []Column, data [][]interface{}) InmemoryDataFrame {
	dfCounter = dfCounter + 1
	return NewInmemoryDataframeWithName("df_"+strconv.Itoa(dfCounter), cols, data)
}

func NewInmemoryDataframeWithName(name string, cols []Column, data [][]interface{}) InmemoryDataFrame {
	return InmemoryDataFrame{name: name, schema: cols, data: data}
}
