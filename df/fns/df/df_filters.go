package df

import (
	"fmt"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func FNil(s df.DataFrame) (r df.DataFrame) {
	return s.FilterRow(func(dfr df.DataFrameRow) bool {
		return dfr.IsAnyNil()
	})
}

func FBoolSeries(s df.DataFrame, bs df.DataFrameSeries) (r df.DataFrame) {
	sname := fmt.Sprintf("s_%d", time.Now().Nanosecond())
	dfs := inmemory.NewDataframeWithNameFromSeries(sname, []string{sname}, []df.DataFrameSeries{bs})
	return s.Join(s.Schema(), dfs, df.JoinEqui, func(dfr1, dfr2 df.DataFrameRow) (r []df.DataFrameRow) {
		if dfr2.GetAsBool(0) {
			r = append(r, dfr1)
		}
		return r
	})
}

func FQuery(d df.DataFrame, q string) (r df.DataFrame) {
	return r
}
