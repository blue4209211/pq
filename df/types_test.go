package df

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetType(t *testing.T) {
	f, err := GetFormat("string")
	assert.NoError(t, err)
	assert.Equal(t, "string", f.Name())
	assert.Equal(t, reflect.String, f.Type())

	f, err = GetFormat("string1")
	assert.Nil(t, f)
	assert.Error(t, err)

	f, err = GetFormatFromKind(reflect.Int32)
	assert.Equal(t, reflect.Int64, f.Type())
	assert.Nil(t, err)

}

func TestIntType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Int)
	assert.NoError(t, err)

	assert.Equal(t, f.Name(), "integer")

	c, err := f.Convert("1")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), c)

	c, err = f.Convert("1.0")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), c)

	c, err = f.Convert(true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), c)

	c, err = f.Convert("xyz")
	assert.Error(t, err)
	assert.Equal(t, c, int64(0))

	c, err = f.Convert(nil)
	assert.Nil(t, c)
	assert.Nil(t, err)

}

func TestDoubleType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Float64)
	assert.NoError(t, err)
	assert.Equal(t, f.Name(), "double")

	c, err := f.Convert("1")
	assert.NoError(t, err)
	assert.Equal(t, float64(1.0), c)

	c, err = f.Convert("1.0")
	assert.NoError(t, err)
	assert.Equal(t, float64(1.0), c)

	c, err = f.Convert(true)
	assert.NoError(t, err)
	assert.Equal(t, float64(1.0), c)

	c, err = f.Convert("xyz")
	assert.Error(t, err)
	assert.Equal(t, c, float64(0))

	c, err = f.Convert(nil)
	assert.Nil(t, c)
	assert.Nil(t, err)

}

func TestStringType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.String)
	assert.NoError(t, err)
	assert.Equal(t, f.Name(), "string")

	c, err := f.Convert(1)
	assert.NoError(t, err)
	assert.Equal(t, "1", c)

	//TODO ideally should be 1.0
	c, err = f.Convert(1.0)
	assert.NoError(t, err)
	assert.Equal(t, "1", c)

	c, err = f.Convert(1.1)
	assert.NoError(t, err)
	assert.Equal(t, "1.1", c)

	c, err = f.Convert(true)
	assert.NoError(t, err)
	assert.Equal(t, "true", c)

	c, err = f.Convert("xyz")
	assert.Equal(t, "xyz", c)
	assert.Nil(t, err)

	c, err = f.Convert(nil)
	assert.Nil(t, c)
	assert.Nil(t, err)
}

func TestBoolType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Bool)
	assert.NoError(t, err)
	assert.Equal(t, f.Name(), "boolean")

	c, err := f.Convert("1")
	assert.NoError(t, err)
	assert.Equal(t, true, c)

	c, err = f.Convert("1.0")
	assert.Error(t, err)
	assert.Equal(t, c, false)

	c, err = f.Convert(true)
	assert.NoError(t, err)
	assert.Equal(t, true, c)

	c, err = f.Convert("xyz")
	assert.Error(t, err)
	assert.Equal(t, c, false)

	c, err = f.Convert(nil)
	assert.Nil(t, c)
	assert.Nil(t, err)

}

func TestNewSchema(t *testing.T) {
	s := NewSchema([]SeriesSchema{
		{Name: "c1", Format: IntegerFormat},
		{Name: "c2", Format: DoubleFormat},
		{Name: "c3", Format: StringFormat},
		{Name: "c4", Format: BoolFormat},
	})

	c := s.GetByName("c1")
	assert.Equal(t, "c1", c.Name)
	c = s.GetByName("c11")
	assert.Equal(t, "", c.Name)

	assert.Equal(t, "c3", s.Get(2).Name)
	assert.Equal(t, 4, s.Len())
}
