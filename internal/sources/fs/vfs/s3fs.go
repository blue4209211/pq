package vfs

import (
	"bytes"
	"io"
	"io/fs"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jszwec/s3fs"
)

func NewS3FS(u *url.URL) (VFS, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	s3api := s3.New(s)
	bucket := u.Host
	s3fs := s3fs.New(s3api, bucket)

	return &s3FS{root: s3fs, bucket: bucket, s3: s3api}, nil
}

type s3FS struct {
	root   *s3fs.S3FS
	bucket string
	s3     *s3.S3
}

func (f *s3FS) Open(name string) (fs.File, error) {
	return f.root.Open(name)
}

func (f *s3FS) Create(name string) (io.WriteCloser, error) {
	return s3FileWriter{s3fs: f, key: name}, nil
}

//TODO https://mehranjnf.medium.com/s3-multipart-upload-with-goroutines-92a7aebe831b
type s3FileWriter struct {
	s3fs *s3FS
	key  string
}

func (f s3FileWriter) Write(p []byte) (n int, err error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(f.s3fs.bucket),
		Key:    aws.String(f.key),
		Body:   bytes.NewReader(p),
	}
	_, err = f.s3fs.s3.PutObject(input)
	return len(p), err
}

func (f s3FileWriter) Close() error {
	return nil
}
