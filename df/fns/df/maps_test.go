package df

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestAsType(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, nil)}, df.DoubleFormat),
	})
	d2 := AsType(d, map[string]df.Format{"c1": df.StringFormat})
	assert.Equal(t, df.StringFormat, d2.Schema().GetByName("c1").Format)
	assert.Equal(t, "1", d2.GetValue(0, 0).Get())
}

func TestMaskNill(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, nil)}, df.DoubleFormat),
	})
	d2 := MaskNill(d, map[string]any{"c2": 2.0})
	assert.Equal(t, 2.0, d2.GetValue(1, 1).Get())
}

func TestMask(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, nil)}, df.DoubleFormat),
	})
	d2 := Mask(d, map[string]map[any]any{"c2": {1.0: 11.0}})
	assert.Equal(t, 11.0, d2.GetValue(0, 1).Get())
}
