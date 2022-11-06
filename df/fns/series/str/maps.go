package str

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func MaskNil(s df.Series, v string) (r df.Series) {
	if s.Schema().Format != df.StringFormat {
		panic("only supported for string format")
	}
	r = s.Map(df.StringFormat, func(sv df.Value) df.Value {
		if sv.IsNil() {
			return inmemory.NewStringValueConst(v)
		}
		return sv
	})
	return r
}

func Concat(s df.Series, vs string, v any) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return inmemory.NewValue(df.StringFormat, fmt.Sprintf("%v%v%v", "", vs, v))
		}
		return inmemory.NewValue(df.StringFormat, fmt.Sprintf("%v%v%v", dfsv.GetAsString(), vs, v))
	})
}

func Substring(s df.Series, start int, end int) (r df.Series) {
	if start < 0 || end < 0 {
		panic("start/end cannot be  < 0")
	}
	if start > end {
		panic("start cannot be > end")
	}

	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() || len(dfsv.GetAsString()) < end {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, dfsv.GetAsString()[start:end])
	})
}

func Upper(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.ToUpper(dfsv.GetAsString()))
	})
}

func Lower(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.ToLower(dfsv.GetAsString()))
	})
}

func Title(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.Title(dfsv.GetAsString()))
	})
}

func ReplaceAll(s df.Series, match string, replcae string) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.ReplaceAll(dfsv.GetAsString(), match, replcae))
	})
}

func Replace(s df.Series, match string, replcae string, n int) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.Replace(dfsv.GetAsString(), match, replcae, n))
	})
}

func Trim(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.TrimSpace(dfsv.GetAsString()))
	})
}

func RTrim(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.TrimRight(dfsv.GetAsString(), " \t\f\v"))
	})
}

func LTrim(s df.Series) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.TrimLeft(dfsv.GetAsString(), " \t\f\v"))
	})
}

func Split(s df.Series, sep string, index int) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.Split(dfsv.GetAsString(), sep)[index])
	})
}

func Extract(s df.Series, pattern string) (r df.Series) {
	exp, err := regexp.Compile(pattern)
	if err != nil {
		panic(err)
	}
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, exp.FindString(dfsv.GetAsString()))
	})
}

func Repeat(s df.Series, n int) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.Repeat(dfsv.GetAsString(), n))
	})
}

func TrimSuffix(s df.Series, suf string) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.TrimSuffix(dfsv.GetAsString(), suf))
	})
}

func TrimPrefix(s df.Series, suf string) (r df.Series) {
	return s.Map(df.StringFormat, func(dfsv df.Value) df.Value {
		if dfsv == nil || dfsv.IsNil() {
			return dfsv
		}
		return inmemory.NewValue(df.StringFormat, strings.TrimPrefix(dfsv.GetAsString(), suf))
	})
}

func ConcatSeries(s df.Series, sep string, s1 df.Series) (r df.Series) {
	r = s.Join(df.StringFormat, s1, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		return append(r, inmemory.NewStringValueConst(fmt.Sprintf("%v%v%v", dfsv1.Get(), sep, dfsv2.Get())))
	})
	return r
}
