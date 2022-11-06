package dt

import (
	"testing"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestMaskNil(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{
		inmemory.NewDatetimeValueConst(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)),
		inmemory.NewDatetimeValueConst(time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC)),
		inmemory.NewDatetimeValue(nil),
	}, df.DateTimeFormat)
	s2 := MaskNil(s1, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC))
	assert.Equal(t, 2009, s2.Get(2).GetAsDatetime().Year())
}

func TestYear(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Year(s1)
	assert.Equal(t, int64(2009), s2.Get(0).GetAsInt())
}

func TestMonth(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Month(s1)
	assert.Equal(t, int64(11), s2.Get(0).GetAsInt())
}

func TestDay(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Day(s1)
	assert.Equal(t, int64(10), s2.Get(0).GetAsInt())
}

func TestHour(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Hour(s1)
	assert.Equal(t, int64(23), s2.Get(0).GetAsInt())
}

func TestMinute(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Minute(s1)
	assert.Equal(t, int64(0), s2.Get(0).GetAsInt())
}

func TestSecond(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Second(s1)
	assert.Equal(t, int64(0), s2.Get(0).GetAsInt())
}

func TestUnixMilli(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := UnixMilli(s1)
	assert.Equal(t, int64(s1.Get(0).GetAsDatetime().UnixMilli()), s2.Get(0).GetAsInt())
}

func TestAddDate(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := AddDate(s1, 0, 0, 1)
	assert.Equal(t, (s1.Get(0).GetAsDatetime().AddDate(0, 0, 1)), s2.Get(0).GetAsDatetime())
}

func TestAddTime(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := AddTime(s1, 0, 1, 0)
	assert.Equal(t, (s1.Get(0).GetAsDatetime().Add(time.Minute * 1)), s2.Get(0).GetAsDatetime())
}

func TestParse(t *testing.T) {
	s1 := inmemory.NewStringSeriesVarArg(
		"2022-01-23",
	)
	s2 := Parse(s1, "2006-01-02")
	assert.Equal(t, int(2022), s2.Get(0).GetAsDatetime().Year())
}

func TestFormat(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := Format(s1, "2006-01-02")
	assert.Equal(t, ("2009-11-10"), s2.Get(0).GetAsString())
}

func TestToUnixMilli(t *testing.T) {
	s1 := inmemory.NewDatetimeSeriesVarArg(
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2010, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2011, time.November, 10, 23, 0, 0, 0, time.UTC),
		time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	s2 := UnixMilli(s1)
	assert.Equal(t, (s1.Get(0).GetAsDatetime().UnixMilli()), s2.Get(0).GetAsInt())
}

func TestFromUnixMilli(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(
		time.Now().UnixMilli(),
	)
	s2 := FromUnixMilli(s1)
	assert.Equal(t, (time.UnixMilli(s1.Get(0).GetAsInt())), s2.Get(0).GetAsDatetime())
}
