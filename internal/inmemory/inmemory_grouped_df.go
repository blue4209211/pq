package inmemory

import (
	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrame struct {
	data   map[any][][]any
	schema df.DataFrameSchema
}

func (t *inmemoryGroupedDataFrame) Get(index any) (d df.DataFrame) {
	r := t.data[index]
	return NewDataframe(t.schema.Series(), r)
}

func (t *inmemoryGroupedDataFrame) GetKeys() (d []any) {
	d = make([]any, 0, len(t.data))
	for k := range t.data {
		d = append(d, k)
	}
	return d
}

func (t *inmemoryGroupedDataFrame) ForEach(f func(any, df.DataFrame)) {
	for k, v := range t.data {
		f(k, NewDataframe(t.schema.Series(), v))
	}
}

func (t *inmemoryGroupedDataFrame) Map(schema df.DataFrameSeriesFormat, f func(any, df.DataFrame) df.DataFrame) (d df.DataFrameGrouped) {
	d1 := map[any][][]any{}
	for k, v := range t.data {
		dfr := f(k, NewDataframe(t.schema.Series(), v))
		dfr.ForEachRow(func(dfr df.DataFrameRow) {
			d1[k] = append(d1[k], dfr.Data())
		})
	}
	return &inmemoryGroupedDataFrame{data: d1, schema: t.schema}
}

func (t *inmemoryGroupedDataFrame) Filter(f func(any, df.DataFrame) bool) (d df.DataFrameGrouped) {
	d1 := map[any][][]any{}
	for k, v := range t.data {
		if f(k, NewDataframe(t.schema.Series(), v)) {
			d1[k] = v
		}
	}
	return &inmemoryGroupedDataFrame{data: d1, schema: t.schema}
}

func NewGroupedDf(data df.DataFrameSeries) df.DataFrameGrouped {
	return &inmemoryGroupedDataFrame{}
}
