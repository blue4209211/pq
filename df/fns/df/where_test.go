package df

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestIsRowHasNil(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, nil)}, df.DoubleFormat),
	})
	d2 := IsRowHasNil(d)
	assert.Equal(t, int64(1), d2.Len())
}

func TestIsRowHasNonNil(t *testing.T) {
	d := inmemory.NewDataframeWithNameFromSeries("df", []string{"c1", "c2"}, &[]df.Series{
		inmemory.NewDoubleSeriesVarArg(1.0, 2.0),
		inmemory.NewSeries([]df.Value{inmemory.NewDoubleValueConst(1.0), inmemory.NewValue(df.DoubleFormat, nil)}, df.DoubleFormat),
	})
	d2 := IsRowHasNonNil(d)
	assert.Equal(t, int64(1), d2.Len())
}

func TestQuery(t *testing.T) {
}
