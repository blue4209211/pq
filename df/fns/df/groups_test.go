package df

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestDistinct(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, 2.0), inmemory.NewValue(df.DoubleFormat, 2.0)}, df.DoubleFormat),
	})
	d2 := Distinct(d)
	assert.Equal(t, int64(2), d2.Len())
}

func TestUnion(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, 2.0), inmemory.NewValue(df.DoubleFormat, 2.0)}, df.DoubleFormat),
	})
	d1 := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, 2.0), inmemory.NewValue(df.DoubleFormat, 2.0)}, df.DoubleFormat),
	})
	d2 := Union(d, d1, true)
	assert.Equal(t, int64(6), d2.Len())

	d2 = Union(d, d1, false)
	assert.Equal(t, int64(2), d2.Len())
}

func TestIntersection(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, 2.0), inmemory.NewValue(df.DoubleFormat, 2.0)}, df.DoubleFormat),
	})
	d1 := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, 2.0), inmemory.NewValue(df.DoubleFormat, 2.0)}, df.DoubleFormat),
	})
	d2 := Intersection(d, d1)
	assert.Equal(t, int64(2), d2.Len())
}

func TestSubstract(t *testing.T) {
}
