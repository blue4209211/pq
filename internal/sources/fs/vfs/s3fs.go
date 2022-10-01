package vfs

import (
	"bytes"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jszwec/s3fs"
)

func NewS3FS(u *url.URL) (VFS, error) {
	if u.Query().Has("AWS_PROFILE") {
		os.Setenv("AWS_PROFILE", u.Query().Get("AWS_PROFILE"))
	}
	if u.Query().Has("AWS_REGION") {
		os.Setenv("AWS_REGION", u.Query().Get("AWS_REGION"))
	}
	if u.Query().Has("AWS_ACCESS_KEY_ID") {
		os.Setenv("AWS_ACCESS_KEY_ID", u.Query().Get("AWS_ACCESS_KEY_ID"))
	}
	if u.Query().Has("AWS_SECRET_ACCESS_KEY") {
		os.Setenv("AWS_SECRET_ACCESS_KEY", u.Query().Get("AWS_SECRET_ACCESS_KEY"))
	}

	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	s3api := s3.New(s)
	bucket := u.Host
	s3fs := s3fs.New(s3api, bucket)

	base := path.Dir(u.Path)
	if strings.Index(base, "/") == 0 {
		base = base[1:]
	}

	return &s3FS{root: s3fs, bucket: bucket, s3: s3api, base: base}, nil
}

type s3FS struct {
	root   *s3fs.S3FS
	bucket string
	s3     *s3.S3
	base   string
}

func (f *s3FS) Open(name string) (fs.File, error) {
	return f.root.Open(path.Join(f.base, name))
}

func (f *s3FS) ReadDir(name string) ([]fs.DirEntry, error) {
	return f.root.ReadDir(path.Join(f.base, name))
}

func (f *s3FS) Stat(name string) (fs.FileInfo, error) {
	return f.root.Stat(path.Join(f.base, name))
}

func (f *s3FS) Create(name string) (io.WriteCloser, error) {
	return &s3FileWriter{s3fs: f, key: path.Join(f.base, name), data: make([]byte, 0)}, nil
}

//TODO https://mehranjnf.medium.com/s3-multipart-upload-with-goroutines-92a7aebe831b
type s3FileWriter struct {
	s3fs *s3FS
	key  string
	data []byte
}

func (f *s3FileWriter) Write(p []byte) (n int, err error) {
	f.data = append(f.data, p...)
	return len(p), err
}

func (f *s3FileWriter) Close() error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(f.s3fs.bucket),
		Key:    aws.String(f.key),
		Body:   bytes.NewReader(f.data),
	}
	_, err := f.s3fs.s3.PutObject(input)
	return err
}
