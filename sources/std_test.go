package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdDataSource(t *testing.T) {
	source := stdDataSource{}
	assert.Equal(t, source.Name(), "std")
}
