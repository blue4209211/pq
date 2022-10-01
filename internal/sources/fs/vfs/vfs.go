package vfs

import (
	"errors"
	"io"
	"io/fs"
	"net/url"
)

type VFS interface {
	fs.FS
	Create(name string) (io.WriteCloser, error)
}

func GetVFS(u string) (VFS, error) {
	base, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	if base.Scheme == "" || base.Scheme == "file" {
		return NewOsFS(base)
	} else if base.Scheme == "s3" {
		return NewS3FS(base)
	} else if base.Scheme == "gs" {
		return NewGSFS(base)
	} else {
		return nil, errors.New("unknown file system - " + base.Scheme)
	}
}
