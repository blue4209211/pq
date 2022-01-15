package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdDataSource(t *testing.T) {
	source := StdDataSource{}
	assert.Equal(t, source.Name(), "std")
}
