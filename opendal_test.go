package opendal_test

import (
	"fmt"
	"opendal"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

func TestBehavior(t *testing.T) {
	suite.Run(t, new(BehaviorTestSuite))
}

var schemeMap = map[string]opendal.Schemer{
	"aliyun_drive": aliyun_drive.Scheme,
}

type BehaviorTestSuite struct {
	suite.Suite

	op *opendal.Operator
}

func (s *BehaviorTestSuite) SetupSuite() {
	testScheme := os.Getenv("OPENDAL_TEST")
	scheme, ok := schemeMap[testScheme]
	if !ok {
		panic(fmt.Sprintf("Unsupported scheme: %s", testScheme))
	}

	prefix := fmt.Sprintf("OPENDAL_%s_", strings.ToUpper(testScheme))
	opts := opendal.OperatorOptions{}
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := pair[0]
		value := pair[1]
		if strings.HasPrefix(key, prefix) {
			opts[strings.ToLower(strings.TrimPrefix(key, prefix))] = value
		}
	}
	op, err := opendal.NewOperator(scheme, opts)
	if err != nil {
		panic(fmt.Errorf("cannot create operator: %s", err))
	}
	s.op = op
}

func (s *BehaviorTestSuite) TestStat() {
	assert := s.Require()
	op := s.op

	dir := "/test/dir/"
	path := "/test/path"
	data := []byte("Hello, World!")

	err := op.CreateDir(dir)
	assert.Nil(err)

	err = op.Write(path, data)
	assert.Nil(err)

	_, err = op.Stat("/not_exists")
	assert.NotNil(err)
	assert.Equal(int32(3), err.(*opendal.Error).Code())

	meta, err := op.Stat(strings.TrimRight(dir, "/"))
	assert.Nil(err)
	assert.True(meta.IsDir())
	assert.False(meta.IsFile())

	meta, err = op.Stat(path)
	assert.Nil(err)
	assert.Equal(uint64(len(data)), meta.ContentLength())
	assert.False(meta.IsDir())
	assert.True(meta.IsFile())
	assert.False(meta.LastModified().IsZero())
}

func (s *BehaviorTestSuite) TestWrite() {
	assert := s.Require()
	op := s.op

	path := "/test/path"
	data := []byte("Hello, World!")

	err := op.Write(path, data)
	assert.Nil(err)

	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)

	err = op.Delete(path)
	assert.Nil(err)
}
