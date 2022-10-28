package inmemory

import (
	"fmt"
	"strings"

	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrame struct {
	data         map[string]df.DataFrame
	keys         map[string]df.Row
	groupColumns []string
}

func (t *inmemoryGroupedDataFrame) GetGroupColumns() []string {
	s1 := make([]string, len(t.groupColumns))
	copy(s1, t.groupColumns)
	return s1
}

func (t *inmemoryGroupedDataFrame) Get(index df.Row) (d df.DataFrame) {
	return t.data[getKey(index)]
}

func (t *inmemoryGroupedDataFrame) GetKeys() (d []df.Row) {
	d = make([]df.Row, 0, len(t.data))
	for _, v := range t.keys {
		d = append(d, v)
	}
	return d
}

func (t *inmemoryGroupedDataFrame) ForEach(f func(df.Row, df.DataFrame)) {
	for k, v := range t.data {
		f(t.keys[k], v)
	}
}

func (t *inmemoryGroupedDataFrame) Map(f func(df.Row, df.DataFrame) df.DataFrame) (d df.GroupedDataFrame) {
	d1 := map[string]df.DataFrame{}
	for k, v := range t.data {
		dfr := f(t.keys[k], v)
		d1[k] = dfr
	}
	return &inmemoryGroupedDataFrame{data: d1, keys: t.keys, groupColumns: t.groupColumns}
}

func (t *inmemoryGroupedDataFrame) Where(f func(df.Row, df.DataFrame) bool) (d df.GroupedDataFrame) {
	d1 := map[string]df.DataFrame{}
	d2 := map[string]df.Row{}
	for k, v := range t.data {
		if f(t.keys[k], v) {
			d1[k] = v
			d2[k] = t.keys[k]
		}
	}
	return &inmemoryGroupedDataFrame{data: d1, keys: d2, groupColumns: t.groupColumns}
}

func (t *inmemoryGroupedDataFrame) Len() int64 {
	return int64(len(t.data))
}

func getKey(r df.Row) string {
	var b strings.Builder
	for i := 0; i < r.Len(); i++ {
		fmt.Fprintf(&b, "%v", r.Get(i))
	}
	return b.String()
}

func NewGroupedDf(data df.DataFrame, key string, others ...string) df.GroupedDataFrame {
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

	groupedData := map[string][]df.Row{}
	groupedRowKey := map[string]df.Row{}
	data.ForEachRow(func(dfr df.Row) {
		k := dfr.Select(indexes...)
		k1 := getKey(k)
		groupedData[k1] = append(groupedData[k1], dfr)
		groupedRowKey[k1] = k
	})

	groupedData2 := map[string]df.DataFrame{}
	for k, v := range groupedData {
		groupedData2[k] = NewDataframeFromRow(data.Schema(), &v)
	}

	return &inmemoryGroupedDataFrame{data: groupedData2, groupColumns: []string{key}, keys: groupedRowKey}
}
