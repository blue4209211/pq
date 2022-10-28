package inmemory

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/blue4209211/pq/df"
)

type inmemoryDataFrame struct {
	name   string
	schema df.DataFrameSchema
	data   []df.Row
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

	data := make([]df.Row, t.Len())
	for i, r := range t.data {
		data[i] = r.Copy()
	}
	return NewDataframeFromRowAndName(name, t.schema, &data)
}

func (t *inmemoryDataFrame) GetSeries(i int) df.Series {
	series := make([]df.Value, t.Len())
	for j, e := range t.data {
		series[j] = e.Get(i)
	}
	return NewSeriesWihNameAndCopy(&series, t.schema.Get(i).Format, t.schema.Get(i).Name, false)
}

func (t *inmemoryDataFrame) GetSeriesByName(s string) df.Series {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.GetSeries(index)
}

func (t *inmemoryDataFrame) GetRow(r int64) df.Row {
	return t.data[r]
}

func (t *inmemoryDataFrame) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrame) ForEachRow(f func(df.Row)) {
	for _, r := range t.data {
		f(r)
	}
}

func (t *inmemoryDataFrame) AddSeries(name string, series df.Series) (d df.DataFrame, e error) {
	if t.Len() != series.Len() {
		return d, errors.New("data length mismatch")
	}
	_, e = t.schema.GetByName(name)
	if e == nil {
		return d, errors.New("column Already Exists - " + name)
	}
	e = nil
	cols := make([]df.SeriesSchema, 0, t.schema.Len()+1)
	cols = append(cols, t.schema.Series()...)
	cols = append(cols, df.SeriesSchema{Name: name, Format: series.Schema().Format})
	data := make([]df.Row, len(cols))
	for i, e := range t.data {
		data[i] = e.Append(name, series.Get(int64(i)))
	}
	return NewDataframeFromRow(df.NewSchema(cols), &data), e
}

func (t *inmemoryDataFrame) UpdateSeries(index int, series df.Series) (d df.DataFrame, e error) {
	if index < 0 || index >= t.Schema().Len() {
		return d, fmt.Errorf(fmt.Sprintf("Column Doesnt Exists - %d", index))
	}

	e = nil
	cols := make([]df.SeriesSchema, 0, t.schema.Len())
	cols = append(cols, t.schema.Series()...)
	cols[index] = df.SeriesSchema{Name: cols[index].Name, Format: series.Schema().Format}
	schema := df.NewSchema(cols)
	data := make([]df.Row, len(cols))
	for i, e := range t.data {
		e2 := make([]df.Value, e.Len())
		for i := 0; i < e.Len(); i++ {
			e2[i] = e.Get(i)
		}
		e2[index] = series.Get(int64(i))
		data[i] = NewRow(schema, &e2)
	}
	return NewDataframeFromRow(schema, &data), e
}

func (t *inmemoryDataFrame) UpdateSeriesByName(name string, series df.Series) (d df.DataFrame, e error) {
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
	schema := df.NewSchema(cols)
	data := make([]df.Row, t.Len())
	for i, e := range t.data {
		row := make([]df.Value, t.schema.Len()-1)
		for j := 0; j < t.schema.Len(); j++ {
			if j == index {
				continue
			}
			if j < index {
				row[j] = e.Get(j)
			}
			if j > index {
				row[j-1] = e.Get(j)
			}
		}
		data[i] = NewRow(schema, &row)
	}
	return NewDataframeFromRow(schema, &data)
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
	schema := df.NewSchema(cols)
	data := make([]df.Row, len(cols))
	for i, e := range t.data {
		r := make([]df.Value, len(cols))
		for i = 0; i < e.Len(); i++ {
			r[i] = e.Get(i)
		}
		data[i] = NewRow(schema, &r)
	}
	return NewDataframeFromRow(schema, &data), e
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

	data := make([]df.Row, t.Len())
	for i, v := range t.data {
		data[i] = v.Select(index...)
	}

	return NewDataframeFromRow(df.NewSchema(cols), &data), e
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
	data := make([]df.Row, t.Len())
	for i, r := range t.data {
		data[i] = r.Copy()
	}

	isLessFunc := func(f df.SeriesSchema, order df.SortOrder, c1 df.Value, c2 df.Value) bool {
		if f.Format == df.IntegerFormat {
			if order == df.SortOrderASC {
				return c1.GetAsInt() < c2.GetAsInt()
			}
			return c1.GetAsInt() > c2.GetAsInt()
		} else if f.Format == df.DoubleFormat {
			if order == df.SortOrderASC {
				return c1.GetAsDouble() < c2.GetAsDouble()
			}
			return c1.GetAsDouble() > c2.GetAsDouble()
		} else if f.Format == df.StringFormat {
			if order == df.SortOrderASC {
				return c1.GetAsString() < c2.GetAsString()
			}
			return c1.GetAsString() > c2.GetAsString()
		} else if f.Format == df.BoolFormat {
			if order == df.SortOrderASC {
				return !c1.GetAsBool()
			}
			return c1.GetAsBool()
		} else if f.Format == df.DateTimeFormat {
			if order == df.SortOrderASC {
				return c1.GetAsDatetime().UnixMilli() < c2.GetAsDatetime().UnixMilli()
			}
			return c1.GetAsDatetime().UnixMilli() > c2.GetAsDatetime().UnixMilli()
		}
		return false
	}

	sort.Slice(data, func(i, j int) bool {
		r1 := t.data[i]
		r2 := t.data[j]

		isLess := true

		for _, o := range orders {
			isLess = isLess && isLessFunc(t.schema.Get(o.Series), o.Order, r1.Get(o.Series), r2.Get(o.Series))
		}
		return isLess

	})

	return NewDataframeFromRow(t.schema, &data)
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

func (t *inmemoryDataFrame) MapRow(cols df.DataFrameSchema, f func(df.Row) df.Row) df.DataFrame {

	data := make([]df.Row, t.Len())
	for i, r := range t.data {
		data[i] = f(r)
	}

	return NewDataframeFromRow(cols, &data)
}

func (t *inmemoryDataFrame) FlatMapRow(cols df.DataFrameSchema, f func(df.Row) []df.Row) df.DataFrame {
	data := make([]df.Row, 0, t.Len())
	for _, r := range t.data {
		data = append(data, f(r)...)
	}
	return NewDataframeFromRow(cols, &data)
}

func (t *inmemoryDataFrame) WhereRow(f func(df.Row) bool) df.DataFrame {
	data := make([]df.Row, 0, t.Len())
	for _, r := range t.data {
		if f(r) {
			data = append(data, r)
		}
	}
	return NewDataframeFromRow(t.schema, &data)
}

func (t *inmemoryDataFrame) SelectRow(b df.Series) df.DataFrame {
	if b.Schema().Format != df.BoolFormat {
		panic("Only bool series supported")
	}
	data := make([]df.Row, 0, len(t.data))
	seriesLength := b.Len()
	for i, d := range t.data {
		if int64(i) < seriesLength && b.Get(int64(i)).GetAsBool() {
			data = append(data, d)
		}
	}
	return NewDataframeFromRow(t.schema, &data)
}

func (t *inmemoryDataFrame) Limit(offset int, size int) df.DataFrame {
	v := t.data[offset : offset+size]
	return NewDataframeFromRow(t.schema, &v)
}

func (t *inmemoryDataFrame) Append(series df.DataFrame) df.DataFrame {
	return t
}

func (t *inmemoryDataFrame) Group(key string, others ...string) df.GroupedDataFrame {
	return NewGroupedDf(t, key, others...)
}

func (t *inmemoryDataFrame) Join(schema df.DataFrameSchema, data df.DataFrame, jointype df.JoinType, cols map[string]string, f func(df.Row, df.Row) []df.Row) (r df.DataFrame) {
	val := []df.Row{}
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
	return NewDataframeFromRow(schema, &val)
}

var dfCounter = 0

// NewDataframe Create Dataframe based on given schema and data
func NewDataframeFromRow(cols df.DataFrameSchema, data *[]df.Row) df.DataFrame {
	dfCounter = dfCounter + 1
	return NewDataframeFromRowAndName("df_"+strconv.Itoa(dfCounter), cols, data)
}

// NewDataframe Create Dataframe based on given schema and data
func NewDataframeFromRowAndName(name string, cols df.DataFrameSchema, data *[]df.Row) df.DataFrame {
	return &inmemoryDataFrame{name: name, schema: cols, data: *data}
}

// NewDataframe Create Dataframe based on given schema and data
func NewDataframeFromRowAndNameAndCopy(name string, cols df.DataFrameSchema, data *[]df.Row, copyData bool) df.DataFrame {
	return &inmemoryDataFrame{name: name, schema: cols, data: *data}
}

// NewDataframeWithNameFromSeries Create Dataframe based on given name, schema and data
func NewDataframeWithNameFromSeries(name string, colNames []string, data *[]df.Series) df.DataFrame {
	if len(*data) == 0 || len(colNames) == 0 {
		panic("data/col is empty")
	}
	if len(*data) != len(colNames) {
		panic("data/col len is not equal")
	}
	cols := make([]df.SeriesSchema, len(colNames))
	for i, e := range colNames {
		cols[i] = df.SeriesSchema{Name: e, Format: (*data)[i].Schema().Format}
	}

	schema := df.NewSchema(cols)

	dfData := make([]df.Row, 0, (*data)[0].Len())
	for i := int64(0); i < (*data)[0].Len(); i++ {
		r := make([]df.Value, len(colNames))
		for j := 0; j < len(colNames); j++ {
			r[j] = (*data)[j].Get(i)
		}
		dfData = append(dfData, NewRow(schema, &r))
	}

	return &inmemoryDataFrame{name: name, schema: df.NewSchema(cols), data: dfData}
}
