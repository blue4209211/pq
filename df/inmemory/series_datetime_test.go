package inmemory

import (
	"testing"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewDatetimeSeries(t *testing.T) {

	data := []time.Time{
		time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 02, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 03, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 04, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 05, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 06, 0, 0, 0, 0, time.UTC),
	}

	s := NewDatetimeSeriesVarArg(data...)
	assert.Equal(t, int64(len(data)), s.Len())

}

func TestNewDatetimeExpressionSeries(t *testing.T) {
	data := []time.Time{
		time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 02, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 03, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 04, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 05, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC),
		time.Date(2011, 11, 06, 0, 0, 0, 0, time.UTC),
	}
	s := NewDatetimeSeriesVarArg(data...)
	snil := s.Append(NewDatetimeSeries([]*time.Time{nil}))
	assert.Equal(t, int64(snil.Len()), snil.Len())

	s1 := s.Select(NewDatetimeExpr().WhenConst(map[time.Time]time.Time{time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC): time.Date(2011, 12, 01, 0, 0, 0, 0, time.UTC)}))
	assert.Equal(t, time.Date(2011, 12, 01, 0, 0, 0, 0, time.UTC), s1.Get(0).Get())

	s2 := s.Select(NewDatetimeExpr().Year())
	assert.Equal(t, int64(2011), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().Month())
	assert.Equal(t, int64(11), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().Month())
	assert.Equal(t, int64(11), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().Hour())
	assert.Equal(t, int64(0), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().Minute())
	assert.Equal(t, int64(0), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().Second())
	assert.Equal(t, int64(0), s2.Get(0).Get())

	s2 = s.Select(NewDatetimeExpr().UnixMilli())
	assert.Equal(t, data[0].UnixMilli(), s2.Get(0).Get())

	s1 = s.Select(NewDatetimeExpr().AddDateConst(int64(0), int64(1), int64(0)))
	assert.Equal(t, time.Date(2011, 12, 01, 0, 0, 0, 0, time.UTC), s1.Get(0).Get())

	s1 = s.Select(NewDatetimeExpr().AddTimeConst(int64(0), int64(1), int64(0)))
	assert.Equal(t, time.Date(2011, 11, 01, 0, 1, 0, 0, time.UTC), s1.Get(0).Get())

	s3 := s.Select(NewDatetimeExpr().FormatConst("2006-01-02"))
	assert.Equal(t, "2011-11-01", s3.Get(0).Get())

	s1 = s.Select(NewDatetimeExpr().BetweenConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC), time.Date(2011, 11, 02, 0, 0, 0, 0, time.UTC), df.ExprBetweenIncludeBoth))
	assert.Equal(t, int64(3), s1.Len())

	s1 = s.Select(NewDatetimeExpr().InConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(2), s1.Len())
	s1 = s.Select(NewDatetimeExpr().NotInConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(5), s1.Len())

	s1 = s.Select(NewDatetimeExpr().EqConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(2), s1.Len())

	s1 = s.Select(NewDatetimeExpr().LtConst(time.Date(2011, 11, 02, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(2), s1.Len())

	s1 = s.Select(NewDatetimeExpr().GtConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(5), s1.Len())

	s1 = s.Select(NewDatetimeExpr().LeConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(2), s1.Len())

	s1 = s.Select(NewDatetimeExpr().GeConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(7), s1.Len())

	s1 = s.Select(NewDatetimeExpr().NeConst(time.Date(2011, 11, 01, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, int64(5), s1.Len())

}
