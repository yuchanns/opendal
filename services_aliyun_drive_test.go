package opendal_test

import (
	"opendal"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

func TestOpenDALNew(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	path := "/test/path"
	data := []byte("Hello OpenDAL-go without CGO!")

	opts := opendal.NewOptions()

	opts.Set("client_id", os.Getenv("OPENDAL_ALIYUN_DRIVE_CLIENT_ID"))
	opts.Set("client_secret", os.Getenv("OPENDAL_ALIYUN_DRIVE_CLIENT_SECRET"))
	opts.Set("refresh_token", os.Getenv("OPENDAL_ALIYUN_DRIVE_REFRESH_TOKEN"))
	opts.Set("drive_type", os.Getenv("OPENDAL_ALIYUN_DRIVE_DRIVE_TYPE"))
	opts.Set("root", "/opendal/")

	op, err := opendal.NewOperator(aliyun_drive.Scheme{}, opts)

	err = op.Write(path, data)
	assert.Nil(err)

	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)
}
