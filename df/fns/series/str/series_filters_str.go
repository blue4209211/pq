package str

import (
	"strings"

	"github.com/blue4209211/pq/df"
)

func IsContains(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Where(func(v df.DataFrameSeriesValue) bool {
		return strings.Contains(v.GetAsString(), q)
	})
	return r
}

func IsStartsWith(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Where(func(v df.DataFrameSeriesValue) bool {
		return strings.HasPrefix(v.GetAsString(), q)
	})
	return r
}

func IsEndsWith(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Where(func(v df.DataFrameSeriesValue) bool {
		return strings.HasSuffix(v.GetAsString(), q)
	})
	return r
}
