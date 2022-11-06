package vfs

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOSFSOpen(t *testing.T) {
	u, err := url.Parse("../../../testdata/json1.json")
	assert.NoError(t, err)
	fs, err := NewOsFS(u)
	assert.NoError(t, err)
	f, err := fs.Open("json1.json")
	assert.NoError(t, err)
	fi, err := f.Stat()
	assert.NoError(t, err)
	assert.False(t, fi.IsDir())
}

func TestOSFSCreate(t *testing.T) {
	u, err := url.Parse("/tmp/json1.json")
	assert.NoError(t, err)
	fs, err := NewOsFS(u)
	assert.NoError(t, err)
	w, err := fs.Create("json1.json")
	assert.NoError(t, err)
	_, err = w.Write([]byte("xyz"))
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)
	f, err := fs.Open("json1.json")
	assert.NoError(t, err)
	fi, err := f.Stat()
	assert.NoError(t, err)
	assert.False(t, fi.IsDir())
}
