package fns

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextExtract(t *testing.T) {
	extracted := TextExtract("hello hl hello addasdwasscsd dcsddssd", 1)
	assert.Equal(t, "hl", extracted)

	extracted = TextExtract("hello hl hello addasdwasscsd dcsddssd", 11)
	assert.Equal(t, "", extracted)

}

func TestRegexp(t *testing.T) {
	extracted := Regexp("he", "hello")
	assert.Equal(t, true, extracted)

	extracted = Regexp("be", "hello")
	assert.Equal(t, false, extracted)

}

func TestMatch(t *testing.T) {
	extracted := Matches("he", "hello")
	assert.Equal(t, true, extracted)

	extracted = Matches("be", "hello")
	assert.Equal(t, false, extracted)

}
