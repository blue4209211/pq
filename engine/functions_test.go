package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextExtract(t *testing.T) {
	extracted := textExtract("hello hl hello addasdwasscsd dcsddssd", 1)
	assert.Equal(t, "hl", extracted)

	extracted = textExtract("hello hl hello addasdwasscsd dcsddssd", 11)
	assert.Equal(t, "", extracted)

}
