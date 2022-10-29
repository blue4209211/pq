package str

import (
	"strings"

	"github.com/blue4209211/pq/df"
)

func IsContains(s df.Series, q string) (r df.Series) {
	r = s.Where(func(v df.Value) bool {
		if v == nil || v.IsNil() {
			return false
		}
		return strings.Contains(v.GetAsString(), q)
	})
	return r
}

func IsStartsWith(s df.Series, q string) (r df.Series) {
	r = s.Where(func(v df.Value) bool {
		if v == nil || v.IsNil() {
			return false
		}
		return strings.HasPrefix(v.GetAsString(), q)
	})
	return r
}

func IsEndsWith(s df.Series, q string) (r df.Series) {
	r = s.Where(func(v df.Value) bool {
		if v == nil || v.IsNil() {
			return false
		}
		return strings.HasSuffix(v.GetAsString(), q)
	})
	return r
}
