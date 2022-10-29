package str

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestMaskNil(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("1"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := MaskNil(s1, "hello")
	assert.Equal(t, "hello", s2.Get(2).GetAsString())
}

func TestConcat(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("1"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Concat(s1, ",", "hello")
	assert.Equal(t, "1,hello", s2.Get(0).GetAsString())
	assert.Equal(t, ",hello", s2.Get(2).GetAsString())
}

func TestSubstring(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Substring(s1, 0, 2)
	assert.Equal(t, "11", s2.Get(0).GetAsString())
	assert.Equal(t, true, s2.Get(2).IsNil())
}

func TestUpper(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("a"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Upper(s1)
	assert.Equal(t, "A", s2.Get(0).GetAsString())
}

func TestLower(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("A"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Lower(s1)
	assert.Equal(t, "a", s2.Get(0).GetAsString())
}

func TestTitle(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("ABC def"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Title(s1)
	assert.Equal(t, "ABC Def", s2.Get(0).GetAsString())
}

func TestReplaceAll(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("ABC def"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := ReplaceAll(s1, "ABC", "kbc")
	assert.Equal(t, "kbc def", s2.Get(0).GetAsString())
}

func TestReplace(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("ABC def ABC"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Replace(s1, "ABC", "kbc", 1)
	assert.Equal(t, "kbc def ABC", s2.Get(0).GetAsString())
}

func TestTrim(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("ABC "), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Trim(s1)
	assert.Equal(t, "ABC", s2.Get(0).GetAsString())
}

func TestRTrim(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue(" ABC "), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := RTrim(s1)
	assert.Equal(t, " ABC", s2.Get(0).GetAsString())
}

func TestLTrim(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue(" ABC "), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := LTrim(s1)
	assert.Equal(t, "ABC ", s2.Get(0).GetAsString())
}

func TestSplit(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("ABC DEF"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Split(s1, " ", 0)
	assert.Equal(t, "ABC", s2.Get(0).GetAsString())
}

func TestExtract(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Extract(s1, `^[a-z]+`)
	assert.Equal(t, int64(4), s2.Len())
	assert.Equal(t, "abc", s2.Get(0).GetAsString())
	assert.Equal(t, "", s2.Get(1).GetAsString())
}

func TestRepeat(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := Repeat(s1, 2)
	assert.Equal(t, "abcabc", s2.Get(0).GetAsString())
}

func TestTrimSuffix(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := TrimSuffix(s1, "bc")
	assert.Equal(t, "a", s2.Get(0).GetAsString())
}

func TestTrimPrefix(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := TrimPrefix(s1, "ab")
	assert.Equal(t, "c", s2.Get(0).GetAsString())
}

func TestConcatSeries(t *testing.T) {
	s1 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s2 := inmemory.NewSeries(&[]df.Value{inmemory.NewStringValue("abc"), inmemory.NewStringValue("111"), inmemory.NewStringValue("2"), inmemory.NewValue(df.StringFormat, nil)}, df.StringFormat)
	s3 := ConcatSeries(s1, "", s2)
	assert.Equal(t, "abcabc", s3.Get(0).GetAsString())
}
