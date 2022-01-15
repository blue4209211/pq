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
	assert.Error(t, err)

	f, err = GetFormatFromKind(reflect.Int32)
	assert.Equal(t, reflect.Int64, f.Type())

}

func TestIntType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Int)
	assert.NoError(t, err)

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

	c, err = f.Convert(nil)
	assert.Nil(t, c)

}

func TestDoubleType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Float64)
	assert.NoError(t, err)

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

	c, err = f.Convert(nil)
	assert.Nil(t, c)

}

func TestStringType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.String)
	assert.NoError(t, err)

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

	c, err = f.Convert(nil)
	assert.Nil(t, c)
}

func TestBoolType(t *testing.T) {
	f, err := GetFormatFromKind(reflect.Bool)
	assert.NoError(t, err)

	c, err := f.Convert("1")
	assert.NoError(t, err)
	assert.Equal(t, true, c)

	c, err = f.Convert("1.0")
	assert.Error(t, err)

	c, err = f.Convert(true)
	assert.NoError(t, err)
	assert.Equal(t, true, c)

	c, err = f.Convert("xyz")
	assert.Error(t, err)

	c, err = f.Convert(nil)
	assert.Nil(t, c)

}
