package vfs

import (
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/blue4209211/pq/internal/log"
)

type osFS struct {
	root fs.FS
	base string
}

func NewOsFS(u *url.URL) (VFS, error) {
	absPath, err := filepath.Abs(u.Path)
	if err != nil {
		return nil, err
	}
	fileinfo, err := os.Stat(absPath)
	if err != nil && !strings.Contains(absPath, "*") {
		return nil, err
	}

	var base string
	if fileinfo != nil && fileinfo.IsDir() {
		base = filepath.Dir(absPath)
	} else {
		base = filepath.Dir(absPath)
	}

	log.Debugf("using %s as basepath ", base)

	return &osFS{root: os.DirFS(base), base: base}, nil
}

func (f *osFS) Create(name string) (io.WriteCloser, error) {
	return os.Create(path.Join(f.base, name))
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
