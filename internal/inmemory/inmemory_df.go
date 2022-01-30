package inmemory

import (
	"errors"
	"reflect"
	"sort"
	"strconv"

	"github.com/blue4209211/pq/df"
)

type inmemoryDataFrame struct {
	name   string
	schema df.DataFrameSchema
	data   [][]interface{}
}

func (t *inmemoryDataFrame) Schema() df.DataFrameSchema {
	return t.schema
}

func (t *inmemoryDataFrame) Name() string {
	return t.name
}

func (t *inmemoryDataFrame) Rename(name string, inplace bool) df.DataFrame {
	if inplace {
		t.name = name
		return t
	}

	data := make([][]interface{}, t.Len())
	for i, r := range t.data {
		r2 := make([]interface{}, len(r))
		copy(r2, r)
		data[i] = r2
	}
	return NewDataframeWithName(name, t.schema.Columns(), data)
}

func (t *inmemoryDataFrame) Column(i int) df.DataFrameSeries {
	series := make([]interface{}, t.Len(), t.Len())
	for j, e := range t.data {
		series[j] = e[i]
	}
	return NewSeries(series, t.schema.Get(i).Format)
}

func (t *inmemoryDataFrame) ColumnByName(s string) df.DataFrameSeries {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.Column(index)
}

func (t *inmemoryDataFrame) Get(r int64) df.DataFrameRow {
	return NewDataFrameRow(t.schema, t.data[r])
}

func (t *inmemoryDataFrame) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrame) ForEach(f func(df.DataFrameRow)) {
	for r := int64(0); r < t.Len(); r++ {
		f(t.Get(r))
	}
}

func (t *inmemoryDataFrame) AddColumn(name string, series df.DataFrameSeries) (d df.DataFrame, e error) {
	if t.Len() != series.Len() {
		return d, errors.New("Data length mismatch")
	}
	_, e = t.schema.GetByName(name)
	if e == nil {
		return d, errors.New("Column Already Exists - " + name)
	}
	e = nil
	cols := make([]df.Column, 0, t.schema.Len()+1)
	cols = append(cols, t.schema.Columns()...)
	cols = append(cols, df.Column{Name: name, Format: series.Schema()})
	data := make([][]interface{}, len(cols), len(cols))
	for i, e := range t.data {
		data[i] = append(e, series.Get(int64(i)))
	}
	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) RemoveColumn(index int) df.DataFrame {
	cols := make([]df.Column, 0, t.schema.Len()-1)
	cols = append(cols, t.schema.Columns()[:index]...)
	cols = append(cols, t.schema.Columns()[index+1:]...)
	data := make([][]interface{}, t.Len(), t.Len())
	for i, e := range t.data {
		row := make([]interface{}, 0, t.schema.Len()-1)
		row = append(row, e[:index]...)
		row = append(row, e[index+1:]...)
		data[i] = row
	}
	return NewDataframe(cols, data)
}

func (t *inmemoryDataFrame) RemoveColumnByName(s string) df.DataFrame {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.RemoveColumn(index)
}

func (t *inmemoryDataFrame) RenameColumn(index int, name string, inplace bool) (d df.DataFrame, e error) {
	_, e = t.schema.GetByName(name)
	if e == nil {
		return d, errors.New("Column Already Exists")
	}
	e = nil

	cols := t.schema.Columns()
	cols[index] = df.Column{Name: name, Format: cols[index].Format}
	if inplace {
		t.schema = df.NewSchema(cols)
		return t, nil
	}
	data := make([][]interface{}, len(cols), len(cols))
	for i, e := range t.data {
		r := make([]interface{}, len(cols))
		copy(r, e)
		data[i] = r
	}
	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) RenameColumnByName(col string, name string, inplace bool) (d df.DataFrame, e error) {
	index, err := t.schema.GetIndexByName(col)
	if err != nil {
		panic(err)
	}
	return t.RenameColumn(index, name, inplace)
}

func (t *inmemoryDataFrame) SelectColumn(index ...int) (d df.DataFrame, e error) {
	cols := make([]df.Column, 0, len(index))
	for _, c := range index {
		cols = append(cols, t.Schema().Get(c))
	}

	data := make([][]interface{}, t.Len())
	for i, v := range t.data {
		r := make([]interface{}, 0, len(index))
		for _, c := range index {
			r = append(r, v[c])
		}
		data[i] = r
	}

	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) SelectColumnByName(col ...string) (d df.DataFrame, e error) {
	idexes := make([]int, len(col))

	for i, e := range col {
		index, err := t.schema.GetIndexByName(e)
		if err != nil {
			panic(err)
		}
		idexes[i] = index
	}

	return t.SelectColumn(idexes...)
}

func (t *inmemoryDataFrame) Sort(orders ...df.SortByIndex) df.DataFrame {
	data := make([][]interface{}, t.Len())
	for i, r := range t.data {
		r2 := make([]interface{}, len(r))
		copy(r2, r)
		data[i] = r2
	}

	isLessFunc := func(f df.Column, order df.SortOrder, c1 interface{}, c2 interface{}) bool {
		if f.Format.Type() == reflect.Int64 {
			if order == df.SortOrderASC {
				return c1.(int64) < c2.(int64)
			}
			return c1.(int64) > c2.(int64)
		} else if f.Format.Type() == reflect.Float64 {
			if order == df.SortOrderASC {
				return c1.(float64) < c2.(float64)
			}
			return c1.(float64) > c2.(float64)
		} else if f.Format.Type() == reflect.String {
			if order == df.SortOrderASC {
				return c1.(string) < c2.(string)
			}
			return c1.(string) > c2.(string)
		} else if f.Format.Type() == reflect.Bool {
			if order == df.SortOrderASC {
				return c1.(bool) == false
			}
			return c1.(bool) == true
		}
		return false
	}

	sort.Slice(data, func(i, j int) bool {
		r1 := t.data[i]
		r2 := t.data[j]

		isLess := true

		for _, o := range orders {
			isLess = isLess && isLessFunc(t.schema.Get(o.Column), o.Order, r1[o.Column], r2[o.Column])
		}
		return isLess

	})

	return NewDataframe(t.schema.Columns(), data)
}

func (t *inmemoryDataFrame) SortByName(order ...df.SortByName) df.DataFrame {
	idexes := make([]df.SortByIndex, len(order))

	for i, e := range order {
		index, err := t.schema.GetIndexByName(e.Column)
		if err != nil {
			panic(err)
		}
		idexes[i] = df.SortByIndex{Column: index, Order: e.Order}
	}

	return t.Sort(idexes...)
}

func (t *inmemoryDataFrame) Map(cols []df.Column, f func(df.DataFrameRow) []interface{}) df.DataFrame {

	data := make([][]interface{}, t.Len())
	for i, r := range t.data {
		data[i] = f(NewDataFrameRow(t.schema, r))
	}

	return NewDataframe(cols, data)
}

func (t *inmemoryDataFrame) FlatMap(cols []df.Column, f func(df.DataFrameRow) [][]interface{}) df.DataFrame {
	data := make([][]interface{}, 0, t.Len())
	for _, r := range t.data {
		data = append(data, f(NewDataFrameRow(t.schema, r))...)
	}

	return NewDataframe(cols, data)
}

func (t *inmemoryDataFrame) Filter(f func(df.DataFrameRow) bool) df.DataFrame {
	data := make([][]interface{}, 0, t.Len())
	for _, r := range t.data {
		if f(NewDataFrameRow(t.schema, r)) {
			data = append(data, r)
		}
	}

	return NewDataframe(t.schema.Columns(), data)
}

func (t *inmemoryDataFrame) Limit(offset int, size int) df.DataFrame {
	return NewDataframe(t.schema.Columns(), t.data[offset:offset+size])
}

var dfCounter = 0

// NewDataframe Create Dataframe based on given schema and data
func NewDataframe(cols []df.Column, data [][]interface{}) df.DataFrame {
	dfCounter = dfCounter + 1
	return NewDataframeWithName("df_"+strconv.Itoa(dfCounter), cols, data)
}

// NewDataframeWithName Create Dataframe based on given name, schema and data
func NewDataframeWithName(name string, cols []df.Column, data [][]interface{}) df.DataFrame {
	return &inmemoryDataFrame{name: name, schema: df.NewSchema(cols), data: data}
}

// NewDataframeWithNameFromSeries Create Dataframe based on given name, schema and data
func NewDataframeWithNameFromSeries(name string, colNames []string, data []df.DataFrameSeries) df.DataFrame {
	dfData := make([][]interface{}, 0, 10)
	for i := int64(0); i < data[0].Len(); i++ {
		r := make([]interface{}, len(colNames))
		for j := 0; j < len(colNames); j++ {
			r[j] = data[j].Get(i)
		}
		dfData = append(dfData, r)
	}

	cols := make([]df.Column, len(colNames))
	for i, e := range colNames {
		cols[i] = df.Column{Name: e, Format: data[i].Schema()}
	}

	return NewDataframeWithName(name, cols, dfData)
}
