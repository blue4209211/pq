package df

import (
	"github.com/blue4209211/pq/df"
)

func IsRowHasNil() (r func(df.Row) bool) {
	return func(dfr df.Row) bool {
		return dfr.IsAnyNil()
	}
}

func Query(q string) (r df.DataFrame) {
	return r
}
