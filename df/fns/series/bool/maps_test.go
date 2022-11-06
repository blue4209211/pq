package bool

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestMaskNil(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{
		inmemory.NewBoolValueConst(true),
		inmemory.NewBoolValueConst(false),
		inmemory.NewValue(df.BoolFormat, nil),
	}, df.BoolFormat)
	s2 := MaskNil(s1, false)
	assert.Equal(t, false, s2.Get(2).GetAsBool())
}

func TestNot(t *testing.T) {
	s1 := inmemory.NewBoolSeriesVarArg(true, false, true)
	s2 := Not(s1)
	assert.Equal(t, false, s2.Get(0).GetAsBool())
}

func TestAnd(t *testing.T) {
	s1 := inmemory.NewBoolSeriesVarArg(true, false, true)
	s2 := And(s1, true)
	assert.Equal(t, true, s2.Get(0).GetAsBool())
}

func TestOr(t *testing.T) {
	s1 := inmemory.NewBoolSeriesVarArg(true, false, true)
	s2 := Or(s1, true)
	assert.Equal(t, true, s2.Get(1).GetAsBool())
}

func TestAndSeries(t *testing.T) {
	s1 := inmemory.NewBoolSeriesVarArg(true, false, true)
	s2 := inmemory.NewBoolSeriesVarArg(true, false, false)
	s3 := AndSeries(s1, s2)
	assert.Equal(t, false, s3.Get(1).GetAsBool())
	assert.Equal(t, false, s3.Get(2).GetAsBool())
}

func TestOrSeries(t *testing.T) {
	s1 := inmemory.NewBoolSeriesVarArg(true, false, true)
	s2 := inmemory.NewBoolSeriesVarArg(true, false, false)
	s3 := OrSeries(s1, s2)
	assert.Equal(t, false, s3.Get(1).GetAsBool())
	assert.Equal(t, true, s3.Get(2).GetAsBool())
}
