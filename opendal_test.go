package opendal_test

import (
	"opendal"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

func TestServicesAliyunDrive(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	path := "/test/path"
	data := []byte("Hello, World!")

	opts := opendal.OperatorOptions{
		"client_id":     os.Getenv("OPENDAL_ALIYUN_DRIVE_CLIENT_ID"),
		"client_secret": os.Getenv("OPENDAL_ALIYUN_DRIVE_CLIENT_SECRET"),
		"refresh_token": os.Getenv("OPENDAL_ALIYUN_DRIVE_REFRESH_TOKEN"),
		"drive_type":    os.Getenv("OPENDAL_ALIYUN_DRIVE_DRIVE_TYPE"),
		"root":          "/opendal/",
	}

	op, err := opendal.NewOperator(aliyun_drive.Scheme, opts)

	err = op.Write(path, data)
	assert.Nil(err)

	_, err = op.Stat("/not_exists")
	assert.NotNil(err)
	assert.Equal(int32(3), err.(*opendal.Error).Code())
	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.Equal(uint64(len(data)), meta.ContentLength())
	// FIXME: incorrect timestamp and path type
	/* assert.False(meta.IsDir())
	assert.True(meta.IsFile())
	t.Logf("%s", meta.LastModified()) */

	_, err = op.Read("/not_exists")
	assert.NotNil(err)
	assert.Equal(int32(3), err.(*opendal.Error).Code())
	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)

	err = op.Delete(path)
	assert.Nil(err)
}
