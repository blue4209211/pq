package df

import (
	"github.com/blue4209211/pq/df"
)

func IsNil(s df.DataFrame) (r df.DataFrame) {
	return s.Where(func(dfr df.DataFrameRow) bool {
		return dfr.IsAnyNil()
	})
}

func Query(d df.DataFrame, q string) (r df.DataFrame) {
	return r
}
