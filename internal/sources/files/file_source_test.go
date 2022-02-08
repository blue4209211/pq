package files

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSource(t *testing.T) {
	source, err := GetStreamHandler("json")
	assert.Nil(t, err)
	assert.Equal(t, "json", source.Name())

	source, err = GetStreamHandler("json1")
	assert.Nil(t, err)
	assert.Equal(t, "text", source.Name())
}
