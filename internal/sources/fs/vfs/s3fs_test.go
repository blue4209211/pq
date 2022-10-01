package vfs

// func TestS3FSOpen(t *testing.T) {
// 	u, err := url.Parse("s3:///testdata/json1.json?AWS_ENDPOINT")
// 	assert.NoError(t, err)
// 	fs, err := NewS3FS(u)
// 	assert.NoError(t, err)
// 	f, err := fs.Open("json1.json")
// 	assert.NoError(t, err)
// 	fi, err := f.Stat()
// 	assert.NoError(t, err)
// 	assert.False(t, fi.IsDir())
// }

// func TestS3FSCreate(t *testing.T) {
// 	u, err := url.Parse("/tmp/json1.json")
// 	assert.NoError(t, err)
// 	fs, err := NewS3FS(u)
// 	assert.NoError(t, err)
// 	w, err := fs.Create("json1.json")
// 	assert.NoError(t, err)
// 	_, err = w.Write([]byte("xyz"))
// 	assert.NoError(t, err)
// 	err = w.Close()
// 	assert.NoError(t, err)
// 	f, err := fs.Open("json1.json")
// 	assert.NoError(t, err)
// 	fi, err := f.Stat()
// 	assert.NoError(t, err)
// 	assert.False(t, fi.IsDir())
// }
