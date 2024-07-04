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
	data := []byte("Hello OpenDAL-go without CGO!")

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

	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)
}
