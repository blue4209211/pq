package vfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func NewGSFS(u *url.URL) (VFS, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	prefix := path.Dir(u.Path)
	if strings.Index(prefix, "/") == 0 {
		prefix = prefix[1:]
	}
	return &gsFS{root: client.Bucket(u.Host), ctx: ctx, prefix: prefix}, nil
}

type gsFS struct {
	root        *storage.BucketHandle
	ctx         context.Context
	bucketAttrs *storage.BucketAttrs
	prefix      string
}

func (f *gsFS) Create(name string) (io.WriteCloser, error) {
	return f.writeFile(path.Join(f.prefix, name))
}

func (fsys *gsFS) dirExists(name string) bool {
	if name == "." || name == "" {
		return true
	}

	iter := fsys.dirIter(name)
	if _, err := iter.Next(); err != nil {
		return false
	}

	return true
}

func (fsys *gsFS) readFile(name string) (f *File, err error) {
	obj := fsys.root.Object(name)
	r, err := obj.NewReader(fsys.ctx)
	if err != nil {
		return f, fsys.errorWrap(err)
	}

	attrs, err := obj.Attrs(fsys.ctx)
	if err != nil {
		return f, fsys.errorWrap(err)
	}

	return &File{reader: r, writer: nil, attrs: attrs}, nil
}

func (fsys *gsFS) writeFile(name string) (f *File, err error) {
	obj := fsys.root.Object(name)
	w := obj.NewWriter(fsys.ctx)

	return &File{reader: nil, writer: w}, nil
}

func (f *gsFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	name = filepath.Join(f.prefix, name)

	if f.dirExists(name) {
		if f.bucketAttrs == nil {
			attrs, err := f.root.Attrs(f.ctx)
			if err != nil {
				return nil, f.errorWrap(err)
			}
			f.bucketAttrs = attrs
		}
		return f.dir(name), nil
	}

	return f.readFile(name)
}

func (f *gsFS) errorWrap(err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
		err = fs.ErrNotExist
	}

	return err
}

func (f *gsFS) dirIter(path string) *storage.ObjectIterator {
	if path == "." {
		path = ""
	}

	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return f.root.Objects(f.ctx, &storage.Query{Prefix: path, StartOffset: path, Delimiter: "/"})
}

func (f *gsFS) dir(path string) *dir {
	return &dir{prefix: path, bucketCreatedAt: f.bucketAttrs.Created, iter: f.dirIter(path)}
}

type File struct {
	reader io.ReadCloser
	writer io.WriteCloser
	attrs  *storage.ObjectAttrs
}

func (f *File) Stat() (fs.FileInfo, error) {
	if f == nil {
		return nil, errors.New("file doesnt exists")
	}
	return &fileInfo{attrs: f.attrs}, nil
}

func (f *File) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *File) Write(p []byte) (int, error) {
	return f.writer.Write(p)
}

func (f *File) Close() error {
	if f.reader != nil {
		return f.reader.Close()
	}
	if f.writer != nil {
		return f.writer.Close()
	}
	return nil
}

func (f *File) ReadDir(count int) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{
		Op:   "read",
		Path: f.attrs.Name,
		Err:  errors.New("is not a directory"),
	}
}

type fileInfo struct {
	dirModTime time.Time
	attrs      *storage.ObjectAttrs
}

func (f *fileInfo) Name() string {
	name := f.attrs.Name
	if f.IsDir() {
		name = f.attrs.Prefix
	}
	return filepath.Base(name)
}

func (f *fileInfo) Type() fs.FileMode {
	return f.Mode().Type()
}

func (f *fileInfo) Info() (fs.FileInfo, error) {
	return f, nil
}

func (f *fileInfo) Size() int64 {
	return f.attrs.Size
}

func (f *fileInfo) Mode() fs.FileMode {
	if f.IsDir() {
		return fs.ModeDir
	}

	return 0
}

func (f *fileInfo) ModTime() time.Time {
	if f.IsDir() {
		return f.dirModTime
	}
	return f.attrs.Updated
}

func (f *fileInfo) IsDir() bool {
	return f.attrs.Prefix != ""
}

func (f *fileInfo) Sys() interface{} {
	return nil
}

type dir struct {
	prefix          string
	bucketCreatedAt time.Time
	iter            *storage.ObjectIterator
}

func (d *dir) Close() error {
	return nil
}

func (d *dir) Read(buf []byte) (int, error) {
	return 0, nil
}

func (d *dir) Stat() (fs.FileInfo, error) {
	return d, nil
}

func (d *dir) IsDir() bool {
	return true
}

func (d *dir) ModTime() time.Time {
	return d.bucketCreatedAt
}

func (d *dir) Mode() fs.FileMode {
	return fs.ModeDir
}

func (d *dir) Type() fs.FileMode {
	return d.Mode().Type()
}

func (d *dir) Name() string {
	return filepath.Base(d.prefix)
}

func (d *dir) Size() int64 {
	return 0
}

func (d *dir) Sys() interface{} {
	return nil
}

func (d *dir) ReadDir(count int) ([]fs.DirEntry, error) {
	var list []fs.DirEntry
	for {
		if count == len(list) {
			break
		}

		attrs, err := d.iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		finfo := &fileInfo{dirModTime: d.bucketCreatedAt, attrs: attrs}
		list = append(list, finfo)
	}

	if len(list) == 0 && count > 0 {
		return nil, io.EOF
	}

	return list, nil
}
