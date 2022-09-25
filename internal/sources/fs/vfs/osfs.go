package vfs

import (
	"io"
	"io/fs"
	"net/url"
	"os"
)

func pathForDirFS(u *url.URL) string {
	if u.Path == "" {
		return ""
	}

	rootPath := u.Path
	if len(rootPath) >= 3 {
		if rootPath[0] == '/' && rootPath[2] == ':' {
			rootPath = rootPath[1:]
		}
	}

	// a file:// URL with a host part should be interpreted as a UNC
	switch u.Host {
	case ".":
		rootPath = "//./" + rootPath
	case "":
		// nothin'
	default:
		rootPath = "//" + u.Host + rootPath
	}

	return rootPath
}

type osFS struct {
	root fs.FS
	base string
}

func NewOsFS(u *url.URL) (VFS, error) {
	rootPath := pathForDirFS(u)
	dirFS := os.DirFS(rootPath)
	return &osFS{root: dirFS, base: rootPath}, nil
}

func (f *osFS) Create(name string) (io.WriteCloser, error) {
	return os.Create(name)
}

func (f *osFS) Open(name string) (fs.File, error) {
	return f.root.Open(name)
}

func (f *osFS) ReadFile(name string) ([]byte, error) {
	return fs.ReadFile(f.root, name)
}

func (f *osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return fs.ReadDir(f.root, name)
}

func (f *osFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.root, name)
}

func (f *osFS) Glob(name string) ([]string, error) {
	return fs.Glob(f.root, name)
}

func (f *osFS) Sub(name string) (fs.FS, error) {
	return fs.Sub(f.root, name)
}
