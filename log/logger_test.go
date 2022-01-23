package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	Warnf("%s %s", "xyz", "abc")

	SetLogger("warning")
	assert.Equal(t, isDebugEnabled, false)
	assert.Equal(t, isInfoEnabled, false)
	assert.Equal(t, isWarningEnabled, true)
	assert.Equal(t, isErrorEnabled, true)

}
