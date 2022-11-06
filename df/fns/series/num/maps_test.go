package num

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestIntOp(t *testing.T) {
	s1 := inmemory.NewIntSeriesVarArg(1, 2, 3, 4)
	s2 := IntOp(s1, int64(1), NumAddOp)
	assert.Equal(t, int64(2), s2.Get(0).GetAsInt())
	s2 = IntOp(s1, int64(1), NumSubOp)
	assert.Equal(t, int64(0), s2.Get(0).GetAsInt())
	s2 = IntOp(s1, int64(2), NumMulOp)
	assert.Equal(t, int64(2), s2.Get(0).GetAsInt())
	s2 = IntOp(s1, int64(2), NumDivOp)
	assert.Equal(t, int64(0), s2.Get(0).GetAsInt())

}

func TestDoubleOp(t *testing.T) {
	s1 := inmemory.NewDoubleSeriesVarArg(1, 2, 3, 4)
	assert.Equal(t, float64(2), s1.Get(1).GetAsDouble())
	s2 := DoubleOp(s1, float64(1), NumAddOp)
	assert.Equal(t, float64(2), s2.Get(0).GetAsDouble())
	s2 = DoubleOp(s1, float64(1), NumSubOp)
	assert.Equal(t, float64(0), s2.Get(0).GetAsDouble())
	s2 = DoubleOp(s1, float64(2), NumMulOp)
	assert.Equal(t, float64(2), s2.Get(0).GetAsDouble())
	s2 = DoubleOp(s1, float64(2), NumDivOp)
	assert.Equal(t, float64(0.5), s2.Get(0).GetAsDouble())
}

func TestMaskNilDouble(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1), inmemory.NewDoubleValueConst(2), inmemory.NewDoubleValue(nil)}, df.DoubleFormat)
	s2 := MaskNilDouble(s1, float64(1))
	assert.Equal(t, float64(1), s2.Get(2).GetAsDouble())
}

func TestMaskNilInt(t *testing.T) {
	s1 := inmemory.NewSeries([]df.Value{inmemory.NewIntValueConst(1), inmemory.NewIntValueConst(2), inmemory.NewIntValue(nil)}, df.IntegerFormat)
	s2 := MaskNilInt(s1, int64(1))
	assert.Equal(t, int64(1), s2.Get(2).GetAsInt())
}

func TestParseInt(t *testing.T) {
	s1 := inmemory.NewStringSeriesVarArg("1", "2")
	s2 := ParseInt(s1)
	assert.Equal(t, int64(1), s2.Get(0).GetAsInt())
}

func TestParseDouble(t *testing.T) {
	s1 := inmemory.NewStringSeriesVarArg("1", "2")
	s2 := ParseDouble(s1)
	assert.Equal(t, float64(1), s2.Get(0).GetAsDouble())
}

func TestNumOpSeries(t *testing.T) {
	s1 := inmemory.NewDoubleSeriesVarArg(1, 2, 3, 4)
	s2 := inmemory.NewDoubleSeriesVarArg(1, 2, 3, 4)
	s3 := NumOpSeries(s1, s2, NumAddOp)
	assert.Equal(t, float64(4), s3.Get(1).GetAsDouble())
	assert.Equal(t, int64(4), s3.Len())
}