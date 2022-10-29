package inmemory

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/blue4209211/pq/df"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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
	index := t.schema.GetIndexByName(s)
	if index < 0 {
		panic("col not found - " + s)
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

func (t *inmemoryDataFrame) AddSeries(name string, series df.Series) (d df.DataFrame) {
	if t.Len() != series.Len() {
		panic("data length mismatch")
	}
	s1 := t.schema.GetByName(name)
	if s1.Name != "" {
		panic("column Already Exists - " + name)
	}
	cols := make([]df.SeriesSchema, 0, t.schema.Len()+1)
	cols = append(cols, t.schema.Series()...)
	cols = append(cols, df.SeriesSchema{Name: name, Format: series.Schema().Format})
	data := make([]df.Row, len(cols))
	for i, e := range t.data {
		data[i] = e.Append(name, series.Get(int64(i)))
	}
	return NewDataframeFromRow(df.NewSchema(cols), &data)
}

func (t *inmemoryDataFrame) UpdateSeries(index int, series df.Series) (d df.DataFrame) {
	if index < 0 || index >= t.Schema().Len() {
		panic(fmt.Sprintf("Column Doesnt Exists - %d", index))
	}

	cols := make([]df.SeriesSchema, 0, t.schema.Len())
	cols = append(cols, t.schema.Series()...)
	cols[index] = df.SeriesSchema{Name: cols[index].Name, Format: series.Schema().Format}
	schema := df.NewSchema(cols)
	data := make([]df.Row, len(t.data))
	for i, e := range t.data {
		e2 := make([]df.Value, e.Len())
		for i := 0; i < e.Len(); i++ {
			e2[i] = e.Get(i)
		}
		e2[index] = series.Get(int64(i))
		data[i] = NewRow(schema, &e2)
	}
	return NewDataframeFromRow(schema, &data)
}

func (t *inmemoryDataFrame) UpdateSeriesByName(name string, series df.Series) (d df.DataFrame) {
	index := t.schema.GetIndexByName(name)
	if index < 0 {
		panic("col not found - " + name)
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
	index := t.schema.GetIndexByName(s)
	if index < 0 {
		panic("col not found - " + s)
	}
	return t.RemoveSeries(index)
}

func (t *inmemoryDataFrame) RenameSeries(index int, name string, inplace bool) (d df.DataFrame) {
	s1 := t.schema.GetByName(name)
	if s1.Name != "" {
		panic("column already exists")
	}
	cols := t.schema.Series()
	cols[index] = df.SeriesSchema{Name: name, Format: cols[index].Format}
	if inplace {
		t.schema = df.NewSchema(cols)
		return t
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
	return NewDataframeFromRow(schema, &data)
}

func (t *inmemoryDataFrame) RenameSeriesByName(col string, name string, inplace bool) (d df.DataFrame) {
	index := t.schema.GetIndexByName(col)
	if index < 0 {
		panic("col not found - " + col)
	}
	return t.RenameSeries(index, name, inplace)
}

func (t *inmemoryDataFrame) SelectSeries(index ...int) (d df.DataFrame) {
	if len(index) == 0 {
		return d
	}
	cols := make([]df.SeriesSchema, 0, len(index))
	for _, c := range index {
		cols = append(cols, t.Schema().Get(c))
	}

	data := make([]df.Row, t.Len())
	for i, v := range t.data {
		data[i] = v.Select(index...)
	}

	return NewDataframeFromRow(df.NewSchema(cols), &data)
}

func (t *inmemoryDataFrame) SelectSeriesByName(col ...string) (d df.DataFrame) {
	if len(col) == 0 {
		return d
	}

	idexes := make([]int, len(col))

	for i, e := range col {
		index := t.schema.GetIndexByName(e)
		if index < 0 {
			panic("col not found - " + e)
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
		index := t.schema.GetIndexByName(e.Series)
		if index < 0 {
			panic("col not found - " + e.Series)
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
		panic("only bool series supported")
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

func (t *inmemoryDataFrame) Append(d df.DataFrame) df.DataFrame {
	if d.Schema().Len() != t.schema.Len() {
		panic("schema are not same")
	}

	for i, s := range t.schema.Series() {
		if s != d.Schema().Get(i) {
			panic("schema are not same")
		}
	}

	s1 := make([]df.Row, t.Len()+d.Len())
	copy(s1, t.data)

	for i := int64(0); i < d.Len(); i++ {
		s1[i+t.Len()] = d.GetRow(i)
	}

	return NewDataframeFromRow(t.schema, &s1)
}

func (t *inmemoryDataFrame) Group(key string, others ...string) df.GroupedDataFrame {
	return NewGroupedDf(t, key, others...)
}

func (t *inmemoryDataFrame) GetValue(rowIndx, colIndx int) (v df.Value) {
	return t.data[rowIndx].Get(colIndx)
}

func (t *inmemoryDataFrame) Join(schema df.DataFrameSchema, data df.DataFrame, jointype df.JoinType, cols map[string]string, f func(df.Row, df.Row) []df.Row) (r df.DataFrame) {
	if len(cols) == 0 || jointype == df.JoinCross {
		val := []df.Row{}
		if jointype == df.JoinLeft || jointype == df.JoinRight || jointype == df.JoinEqui {
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
			} else if jointype == df.JoinRight {
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
	} else {
		colIdx := map[int]int{}
		for k, v := range cols {
			i1 := t.schema.GetIndexByName(k)
			if i1 < 0 {
				panic("col not found - " + k)
			}
			i2 := data.Schema().GetIndexByName(v)
			if i2 < 0 {
				panic("col not found - " + v)
			}
			colIdx[i1] = i2
		}

		matched := map[int64]int64{}

		val := []df.Row{}
		for i := int64(0); i < t.Len(); i++ {
			r1 := t.GetRow(i)
			for j := int64(0); j < data.Len(); j++ {
				r2 := data.GetRow(j)

				b2 := true
				for k, v := range colIdx {
					if r1.Get(k) != r2.Get(v) {
						b2 = false
						break
					}
				}
				if b2 {
					val = append(val, f(r1, r2)...)
					matched[i] = j
				}
			}
		}

		if jointype == df.JoinLeft {
			leftMatched := maps.Keys(matched)
			for i := int64(0); i < t.Len(); i++ {
				if slices.Contains(leftMatched, i) {
					continue
				}
				val = append(val, f(t.GetRow(i), nil)...)
			}
		} else if jointype == df.JoinRight {
			rightMatched := maps.Values(matched)
			for i := int64(0); i < data.Len(); i++ {
				if slices.Contains(rightMatched, i) {
					continue
				}
				val = append(val, f(data.GetRow(i), nil)...)
			}
		}

		return NewDataframeFromRow(schema, &val)
	}
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
		panic("data/col len is not empty")
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
