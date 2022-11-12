package inmemory

import (
	"strings"
	"testing"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewStringSeries(t *testing.T) {

	data := []string{
		"abc", "def", "geh", "ijk", "lmn", "abc",
	}

	s := NewStringSeriesVarArg(data...)

	//len
	assert.Equal(t, int64(len(data)), s.Len())

	//where
	sf := s.Where(func(i df.Value) bool {
		return i.Get() == "abc"
	})
	assert.Equal(t, int64(2), sf.Len())

	//map
	sm := s.Map(df.StringFormat, func(i df.Value) df.Value {
		return NewStringValueConst(i.GetAsString() + "1")
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+"1", sm.Get(1).Get())

	//flatmap
	sfm := s.FlatMap(df.StringFormat, func(i df.Value) []df.Value {
		return []df.Value{
			NewStringValueConst(i.GetAsString() + "1"),
			NewStringValueConst(i.GetAsString() + "2"),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+"2", sfm.Get(1).Get())

	//distinct
	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	//sort
	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, "lmn", ss.Get(0).Get())

	//limit
	ss = s.Limit(1, 2)
	assert.Equal(t, int64(2), ss.Len())
	assert.Equal(t, "def", ss.Get(0).GetAsString())
	assert.Equal(t, "geh", ss.Get(1).GetAsString())

	//copy
	ss = s.Copy()
	assert.Equal(t, int64(6), ss.Len())
	assert.Equal(t, "abc", ss.Get(0).GetAsString())

	//select
	// ss = s.Select(NewBoolSeriesVarArg(false, true, false, true, false, false))
	// assert.Equal(t, int64(2), ss.Len())
	// assert.Equal(t, "def", ss.Get(0).GetAsString())
	// assert.Equal(t, "ijk", ss.Get(1).GetAsString())

	//reduce
	s1 := s.Reduce(func(v1, v2 df.Value) df.Value {
		return NewStringValueConst(v1.GetAsString() + v2.GetAsString())
	}, NewStringValueConst(""))
	assert.Equal(t, strings.Join(data, ""), s1.GetAsString())

	//group
	sg := s.Group()
	assert.Equal(t, len(sg.GetKeys()), 5)

	//append
	ss = s.Append(NewStringSeriesVarArg("1", "2"))
	assert.Equal(t, int64(8), ss.Len())
	assert.Equal(t, int64(6), s.Len())

	//join
	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinEqui, func(v1, v2 df.Value) []df.Value {
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(2), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinLeft, func(v1, v2 df.Value) []df.Value {
		if v2 == nil {
			return []df.Value{v1}
		}
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(6), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinCross, func(v1, v2 df.Value) []df.Value {
		if v2 == nil {
			return []df.Value{v1}
		}
		if v1 == nil {
			return []df.Value{v2}
		}
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(12), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

}

func TestNewStringExpressionSeries(t *testing.T) {
	data := []string{
		"abc", "def", "geh", "abc", "ijk", "lmn", " xyz ",
	}
	s := NewStringSeriesVarArg(data...)
	snil := s.Append(NewStringSeries([]*string{nil}))

	s1 := s.Select(NewStringExpr().ConcatConst(",", "1", "2"))
	assert.Equal(t, "abc,1,2", s1.Get(0).GetAsString())

	s1 = snil.Select(NewStringExpr().NonNil())
	assert.Equal(t, snil.Len()-1, s1.Len())

	s1 = s.Select(NewStringExpr().SubstringConst(0, 2))
	assert.Equal(t, "ab", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().Upper())
	assert.Equal(t, "ABC", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().Lower())
	assert.Equal(t, "abc", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().Title())
	assert.Equal(t, "Abc", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().ReplaceAllConst("abc", "abc1"))
	assert.Equal(t, "abc1", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().ReplaceConst("a", "x", 1))
	assert.Equal(t, "xbc", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().Trim())
	assert.Equal(t, "xyz", s1.Get(s1.Len()-1).Get())

	s1 = s.Select(NewStringExpr().RTrim())
	assert.Equal(t, " xyz", s1.Get(s1.Len()-1).Get())

	s1 = s.Select(NewStringExpr().LTrim())
	assert.Equal(t, "xyz ", s1.Get(s1.Len()-1).Get())

	s1 = s.Select(NewStringExpr().SplitConst("b", 0))
	assert.Equal(t, "a", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().RepeatConst(2))
	assert.Equal(t, "abcabc", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().TrimSuffixConst("bc"))
	assert.Equal(t, "a", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().TrimPrefixConst("ab"))
	assert.Equal(t, "c", s1.Get(0).Get())

	s1 = s.Select(NewStringExpr().WhenConst(map[string]string{"abc": "xyz"}))
	assert.Equal(t, "xyz", s1.Get(0).Get())

	s1 = snil.Select(NewStringExpr().WhenNilConst("kbc"))
	assert.Equal(t, "kbc", s1.Get(s1.Len()-1).Get())

	s1 = snil.Select(NewStringExpr().InConst("abc"))
	assert.Equal(t, int64(2), s1.Len())

	s1 = snil.Select(NewStringExpr().ContainsConst("abc"))
	assert.Equal(t, int64(2), s1.Len())

	s1 = snil.Select(NewStringExpr().StartsWithConst("ab"))
	assert.Equal(t, int64(2), s1.Len())

	s1 = snil.Select(NewStringExpr().EndsWithConst("bc"))
	assert.Equal(t, int64(2), s1.Len())

	s1 = snil.Select(NewStringExpr().EqConst("abc"))
	assert.Equal(t, int64(2), s1.Len())

	s1 = snil.Select(NewStringExpr().NeConst("abc"))
	assert.Equal(t, int64(5), s1.Len())

	s1 = snil.Select(NewStringExpr().Len())
	assert.Equal(t, int64(3), s1.Get(0).Get())

	data1 := []string{
		"2017-12-01",
	}
	datas1 := NewStringSeriesVarArg(data1...)
	s1 = datas1.Select(NewStringExpr().ParseDatetimeConst("2006-01-02"))
	assert.Equal(t, time.Date(2017, 12, 01, 0, 0, 0, 0, time.UTC), s1.Get(0).Get())

}
