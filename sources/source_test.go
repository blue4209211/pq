package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSource(t *testing.T) {
	source, err := GetSource("json")
	assert.Nil(t, err)
	assert.Equal(t, "json", source.Name())

	source, err = GetSource("json1")
	assert.Error(t, err)
}
