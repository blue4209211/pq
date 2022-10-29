package df

import (
	"github.com/blue4209211/pq/df"
)

func IsRowHasNil(d df.DataFrame) (r df.DataFrame) {
	return d.WhereRow(func(dfr df.Row) bool {
		return dfr.IsAnyNil()
	})
}

func IsRowHasNilBool(d df.DataFrame) (r bool) {
	return IsRowHasNil(d).Len() > 0
}

func IsRowHasNonNil(d df.DataFrame) (r df.DataFrame) {
	return d.WhereRow(func(dfr df.Row) bool {
		return !dfr.IsAnyNil()
	})
}

func IsRowHasNonNilBool(d df.DataFrame) (r bool) {
	return IsRowHasNonNil(d).Len() > 0
}

func Query(q string) (r df.DataFrame) {
	return r
}
