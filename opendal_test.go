package opendal_test

import (
	"opendal"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDALNew(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	opts, err := opendal.NewOptions()
	assert.Nil(err)

	_, err = opendal.NewOperator("unsupported", opts)
	assert.NotNil(err)
	assert.Equal(int32(1), err.(*opendal.Error).Code())

	op, err := opendal.NewOperator("memory", opts)
	assert.Nil(err)

	path := "/test/path"
	data := []byte("Hello OpenDAL-go without CGO!")

	err = op.Write(path, data)
	assert.Nil(err)

	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)
}
