package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrame struct {
	data   map[df.DataFrameRow]df.DataFrame
	schema df.DataFrameSchema
	keys   []string
}

func (t *inmemoryGroupedDataFrame) GetGroupKeys() []string {
	s1 := make([]string, len(t.keys))
	copy(s1, t.keys)
	return s1
}

func (t *inmemoryGroupedDataFrame) Get(index df.DataFrameRow) (d df.DataFrame) {
	return t.data[index]
}

func (t *inmemoryGroupedDataFrame) GetKeys() (d []df.DataFrameRow) {
	d = make([]df.DataFrameRow, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedDataFrame) ForEach(f func(df.DataFrameRow, df.DataFrame)) {
	for k, v := range t.data {
		f(k, v)
	}
}

func (t *inmemoryGroupedDataFrame) Map(s df.DataFrameSchema, f func(df.DataFrameRow, df.DataFrame) df.DataFrame) (d df.DataFrameGrouped) {
	d1 := map[df.DataFrameRow]df.DataFrame{}
	for k, v := range t.data {
		dfr := f(k, v)
		d1[k] = dfr
	}
	return &inmemoryGroupedDataFrame{data: d1, schema: s}
}

func (t *inmemoryGroupedDataFrame) Where(f func(df.DataFrameRow, df.DataFrame) bool) (d df.DataFrameGrouped) {
	d1 := map[df.DataFrameRow]df.DataFrame{}
	for k, v := range t.data {
		if f(k, v) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedDataFrame{data: d1, schema: t.schema}
}

func NewGroupedDf(data df.DataFrame, key string, others ...string) df.DataFrameGrouped {
	groupedData := map[df.DataFrameRow][]df.DataFrameRow{}
	indexes := []int{}
	i, err := data.Schema().GetIndexByName(key)
	if err != nil {
		panic(err)
	}
	indexes = append(indexes, i)

	for _, o := range others {
		i, err := data.Schema().GetIndexByName(o)
		if err != nil {
			panic(err)
		}
		indexes = append(indexes, i)
	}

	data.ForEachRow(func(dfr df.DataFrameRow) {
		groupedData[dfr.Select(indexes...)] = append(groupedData[dfr.Select(indexes...)], dfr)
	})

	groupedData2 := map[df.DataFrameRow]df.DataFrame{}
	for k, v := range groupedData {
		groupedData2[k] = NewDataframeFromRow(data.Schema().Series(), v)
	}

	return &inmemoryGroupedDataFrame{data: groupedData2, schema: data.Schema(), keys: []string{key}}
}
