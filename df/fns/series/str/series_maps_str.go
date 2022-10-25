package str

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func WhereNil(s df.DataFrameSeries, v string) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}
	r = s.Map(df.IntegerFormat, func(sv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if sv.Get() == nil {
			return inmemory.NewDataFrameSeriesStringValue(v)
		}
		return inmemory.NewDataFrameSeriesStringValue(sv.GetAsString())
	})
	return r
}

func Concat(s df.DataFrameSeries, vs string, v any) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, fmt.Sprintf("%v%v%v", dfsv.GetAsString(), vs, v))
	})
}

func Substring(s df.DataFrameSeries, start int, end int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, dfsv.GetAsString()[start:end])
	})
}

func Upper(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ToUpper(dfsv.GetAsString()))
	})
}

func Lower(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ToLower(dfsv.GetAsString()))
	})
}

func Title(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Title(dfsv.GetAsString()))
	})
}

func ReplaceAll(s df.DataFrameSeries, match string, replcae string) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.ReplaceAll(dfsv.GetAsString(), match, replcae))
	})
}

func Replace(s df.DataFrameSeries, match string, replcae string, n int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Replace(dfsv.GetAsString(), match, replcae, n))
	})
}

func Trim(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimSpace(dfsv.GetAsString()))
	})
}

func RTrim(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimRight(dfsv.GetAsString(), " \t\f\v"))
	})
}

func LTrim(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimLeft(dfsv.GetAsString(), " \t\f\v"))
	})
}

func Split(s df.DataFrameSeries, sep string, index int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Split(dfsv.GetAsString(), sep)[index])
	})
}

func Extract(s df.DataFrameSeries, pattern string) (r df.DataFrameSeries) {
	exp, err := regexp.Compile(pattern)
	if err != nil {
		panic(err)
	}
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, exp.FindString(dfsv.GetAsString()))
	})
}

func Repeat(s df.DataFrameSeries, n int) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.Repeat(dfsv.GetAsString(), n))
	})
}

func TrimSuffix(s df.DataFrameSeries, suf string) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimSuffix(dfsv.GetAsString(), suf))
	})
}

func TrimPrefix(s df.DataFrameSeries, suf string) (r df.DataFrameSeries) {
	return s.Map(df.StringFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesValue(df.StringFormat, strings.TrimPrefix(dfsv.GetAsString(), suf))
	})
}

func ConcatSeries(s df.DataFrameSeries, sep string, s1 df.DataFrameSeries) (r df.DataFrameSeries) {
	r = s.Join(df.StringFormat, s1, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesStringValue(fmt.Sprintf("%v%v%v", dfsv1.Get(), sep, dfsv2.Get())))
	})
	return r
}
