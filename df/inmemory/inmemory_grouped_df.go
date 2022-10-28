package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrame struct {
	data map[df.Row]df.DataFrame
	keys []string
}

func (t *inmemoryGroupedDataFrame) GetGroupKeys() []string {
	s1 := make([]string, len(t.keys))
	copy(s1, t.keys)
	return s1
}

func (t *inmemoryGroupedDataFrame) Get(index df.Row) (d df.DataFrame) {
	return t.data[index]
}

func (t *inmemoryGroupedDataFrame) GetKeys() (d []df.Row) {
	d = make([]df.Row, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedDataFrame) ForEach(f func(df.Row, df.DataFrame)) {
	for k, v := range t.data {
		f(k, v)
	}
}

func (t *inmemoryGroupedDataFrame) Map(f func(df.Row, df.DataFrame) df.DataFrame) (d df.GroupedDataFrame) {
	d1 := map[df.Row]df.DataFrame{}
	for k, v := range t.data {
		dfr := f(k, v)
		d1[k] = dfr
	}
	return &inmemoryGroupedDataFrame{data: d1}
}

func (t *inmemoryGroupedDataFrame) Where(f func(df.Row, df.DataFrame) bool) (d df.GroupedDataFrame) {
	d1 := map[df.Row]df.DataFrame{}
	for k, v := range t.data {
		if f(k, v) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedDataFrame{data: d1}
}

func NewGroupedDf(data df.DataFrame, key string, others ...string) df.GroupedDataFrame {
	groupedData := map[df.Row][]df.Row{}
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

	data.ForEachRow(func(dfr df.Row) {
		groupedData[dfr.Select(indexes...)] = append(groupedData[dfr.Select(indexes...)], dfr)
	})

	groupedData2 := map[df.Row]df.DataFrame{}
	for k, v := range groupedData {
		groupedData2[k] = NewDataframeFromRow(data.Schema(), &v)
	}

	return &inmemoryGroupedDataFrame{data: groupedData2, keys: []string{key}}
}
