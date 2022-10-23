package inmemory

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/blue4209211/pq/df"
)

type inmemoryDataFrame struct {
	name   string
	schema df.DataFrameSchema
	data   [][]any
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

	data := make([][]any, t.Len())
	for i, r := range t.data {
		r2 := make([]any, len(r))
		copy(r2, r)
		data[i] = r2
	}
	return NewDataframeWithName(name, t.schema.Series(), data)
}

func (t *inmemoryDataFrame) GetSeries(i int) df.DataFrameSeries {
	series := make([]any, t.Len())
	for j, e := range t.data {
		series[j] = e[i]
	}
	return NewSeries(series, t.schema.Get(i).Format)
}

func (t *inmemoryDataFrame) GetSeriesByName(s string) df.DataFrameSeries {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.GetSeries(index)
}

func (t *inmemoryDataFrame) GetRow(r int64) df.DataFrameRow {
	return NewDataFrameRow(t.schema, t.data[r])
}

func (t *inmemoryDataFrame) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrame) ForEachRow(f func(df.DataFrameRow)) {
	for r := int64(0); r < t.Len(); r++ {
		f(t.GetRow(r))
	}
}

func (t *inmemoryDataFrame) AddSeries(name string, series df.DataFrameSeries) (d df.DataFrame, e error) {
	if t.Len() != series.Len() {
		return d, errors.New("Data length mismatch")
	}
	_, e = t.schema.GetByName(name)
	if e == nil {
		return d, errors.New("Column Already Exists - " + name)
	}
	e = nil
	cols := make([]df.SeriesSchema, 0, t.schema.Len()+1)
	cols = append(cols, t.schema.Series()...)
	cols = append(cols, df.SeriesSchema{Name: name, Format: series.Schema().Format})
	data := make([][]any, len(cols))
	for i, e := range t.data {
		data[i] = append(e, series.Get(int64(i)))
	}
	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) UpdateSeries(index int, series df.DataFrameSeries) (d df.DataFrame, e error) {
	if index < 0 || index >= t.Schema().Len() {
		return d, fmt.Errorf(fmt.Sprintf("Column Doesnt Exists - %d", index))
	}

	e = nil
	cols := make([]df.SeriesSchema, 0, t.schema.Len())
	cols = append(cols, t.schema.Series()...)
	cols[index] = df.SeriesSchema{Name: cols[index].Name, Format: series.Schema().Format}
	data := make([][]any, len(cols))
	for i, e := range t.data {
		e2 := make([]any, len(e))
		e2[index] = series.Get(int64(i))
		data[i] = e2
	}
	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) UpdateSeriesByName(name string, series df.DataFrameSeries) (d df.DataFrame, e error) {
	index, e := t.schema.GetIndexByName(name)
	if e != nil {
		return d, fmt.Errorf("unable to find column - %s", name)
	}
	return t.UpdateSeries(index, series)
}

func (t *inmemoryDataFrame) RemoveSeries(index int) df.DataFrame {
	cols := make([]df.SeriesSchema, 0, t.schema.Len()-1)
	cols = append(cols, t.schema.Series()[:index]...)
	cols = append(cols, t.schema.Series()[index+1:]...)
	data := make([][]any, t.Len())
	for i, e := range t.data {
		row := make([]any, 0, t.schema.Len()-1)
		row = append(row, e[:index]...)
		row = append(row, e[index+1:]...)
		data[i] = row
	}
	return NewDataframe(cols, data)
}

func (t *inmemoryDataFrame) RemoveSeriesByName(s string) df.DataFrame {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.RemoveSeries(index)
}

func (t *inmemoryDataFrame) RenameSeries(index int, name string, inplace bool) (d df.DataFrame, e error) {
	_, e = t.schema.GetByName(name)
	if e == nil {
		return d, errors.New("column already exists")
	}
	e = nil

	cols := t.schema.Series()
	cols[index] = df.SeriesSchema{Name: name, Format: cols[index].Format}
	if inplace {
		t.schema = df.NewSchema(cols)
		return t, nil
	}
	data := make([][]any, len(cols))
	for i, e := range t.data {
		r := make([]any, len(cols))
		copy(r, e)
		data[i] = r
	}
	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) RenameSeriesByName(col string, name string, inplace bool) (d df.DataFrame, e error) {
	index, err := t.schema.GetIndexByName(col)
	if err != nil {
		panic(err)
	}
	return t.RenameSeries(index, name, inplace)
}

func (t *inmemoryDataFrame) SelectSeries(index ...int) (d df.DataFrame, e error) {
	cols := make([]df.SeriesSchema, 0, len(index))
	for _, c := range index {
		cols = append(cols, t.Schema().Get(c))
	}

	data := make([][]any, t.Len())
	for i, v := range t.data {
		r := make([]any, 0, len(index))
		for _, c := range index {
			r = append(r, v[c])
		}
		data[i] = r
	}

	return NewDataframe(cols, data), e
}

func (t *inmemoryDataFrame) SelectSeriesByName(col ...string) (d df.DataFrame, e error) {
	idexes := make([]int, len(col))

	for i, e := range col {
		index, err := t.schema.GetIndexByName(e)
		if err != nil {
			panic(err)
		}
		idexes[i] = index
	}

	return t.SelectSeries(idexes...)
}

func (t *inmemoryDataFrame) Sort(orders ...df.SortByIndex) df.DataFrame {
	data := make([][]any, t.Len())
	for i, r := range t.data {
		r2 := make([]any, len(r))
		copy(r2, r)
		data[i] = r2
	}

	isLessFunc := func(f df.SeriesSchema, order df.SortOrder, c1 any, c2 any) bool {
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
				return !c1.(bool)
			}
			return c1.(bool)
		}
		return false
	}

	sort.Slice(data, func(i, j int) bool {
		r1 := t.data[i]
		r2 := t.data[j]

		isLess := true

		for _, o := range orders {
			isLess = isLess && isLessFunc(t.schema.Get(o.Series), o.Order, r1[o.Series], r2[o.Series])
		}
		return isLess

	})

	return NewDataframe(t.schema.Series(), data)
}

func (t *inmemoryDataFrame) SortByName(order ...df.SortByName) df.DataFrame {
	idexes := make([]df.SortByIndex, len(order))

	for i, e := range order {
		index, err := t.schema.GetIndexByName(e.Series)
		if err != nil {
			panic(err)
		}
		idexes[i] = df.SortByIndex{Series: index, Order: e.Order}
	}

	return t.Sort(idexes...)
}

func (t *inmemoryDataFrame) MapRow(cols []df.SeriesSchema, f func(df.DataFrameRow) df.DataFrameRow) df.DataFrame {

	data := make([]df.DataFrameRow, t.Len())
	for i, r := range t.data {
		data[i] = f(NewDataFrameRow(t.schema, r))
	}

	return NewDataframeFromRow(cols, data)
}

func (t *inmemoryDataFrame) FlatMapRow(cols []df.SeriesSchema, f func(df.DataFrameRow) []df.DataFrameRow) df.DataFrame {
	data := make([]df.DataFrameRow, 0, t.Len())
	for _, r := range t.data {
		data = append(data, f(NewDataFrameRow(t.schema, r))...)
	}

	return NewDataframeFromRow(cols, data)
}

func (t *inmemoryDataFrame) FilterRow(f func(df.DataFrameRow) bool) df.DataFrame {
	data := make([][]any, 0, t.Len())
	for _, r := range t.data {
		if f(NewDataFrameRow(t.schema, r)) {
			data = append(data, r)
		}
	}

	return NewDataframe(t.schema.Series(), data)
}

func (t *inmemoryDataFrame) Limit(offset int, size int) df.DataFrame {
	return NewDataframe(t.schema.Series(), t.data[offset:offset+size])
}

func (t *inmemoryDataFrame) Join(schema df.DataFrameSchema, data df.DataFrame, jointype df.JoinType, f func(df.DataFrameRow, df.DataFrameRow) []df.DataFrameRow) (r df.DataFrame) {
	val := []df.DataFrameRow{}
	if jointype == df.JoinLeft || jointype == df.JoinReft || jointype == df.JoinEqui {
		min := int64(len(t.data))
		if data.Len() < min {
			min = data.Len()
		}
		for i := int64(0); i < min; i++ {
			val = append(val, f(t.GetRow(i), data.GetRow(i))...)
		}
		if jointype == df.JoinLeft {
			for i := int64(min); i < int64(len(t.data)); i++ {
				val = append(val, f(t.GetRow(i), nil)...)
			}
		} else if jointype == df.JoinReft {
			for i := int64(min); i < int64(len(t.data)); i++ {
				val = append(val, f(nil, data.GetRow(i))...)
			}
		}
	} else if jointype == df.JoinCross {
		for i := int64(0); i < t.Len(); i++ {
			for j := int64(0); j < data.Len(); j++ {
				val = append(val, f(t.GetRow(i), data.GetRow(j))...)
			}
		}
	}
	return NewDataframeFromRow(schema.Series(), val)
}

var dfCounter = 0

// NewDataframe Create Dataframe based on given schema and data
func NewDataframe(cols []df.SeriesSchema, data [][]any) df.DataFrame {
	dfCounter = dfCounter + 1
	return NewDataframeWithName("df_"+strconv.Itoa(dfCounter), cols, data)
}

// NewDataframe Create Dataframe based on given schema and data
func NewDataframeFromRow(cols []df.SeriesSchema, data []df.DataFrameRow) df.DataFrame {
	data2 := make([][]any, 0, len(data))
	for _, r := range data {
		data2 = append(data2, r.Data())
	}

	dfCounter = dfCounter + 1
	return NewDataframeWithName("df_"+strconv.Itoa(dfCounter), cols, data2)
}

// NewDataframeWithName Create Dataframe based on given name, schema and data
func NewDataframeWithName(name string, cols []df.SeriesSchema, data [][]any) df.DataFrame {
	return &inmemoryDataFrame{name: name, schema: df.NewSchema(cols), data: data}
}

// NewDataframeWithNameFromSeries Create Dataframe based on given name, schema and data
func NewDataframeWithNameFromSeries(name string, colNames []string, data []df.DataFrameSeries) df.DataFrame {
	dfData := make([][]any, 0, 10)
	for i := int64(0); i < data[0].Len(); i++ {
		r := make([]any, len(colNames))
		for j := 0; j < len(colNames); j++ {
			r[j] = data[j].Get(i)
		}
		dfData = append(dfData, r)
	}

	cols := make([]df.SeriesSchema, len(colNames))
	for i, e := range colNames {
		cols[i] = df.SeriesSchema{Name: e, Format: data[i].Schema().Format}
	}

	return NewDataframeWithName(name, cols, dfData)
}
