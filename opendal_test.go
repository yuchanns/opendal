package opendal_test

import (
	"fmt"
	"opendal"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

func TestBehavior(t *testing.T) {
	suite.Run(t, new(BehaviorTestSuite))
}

var schemes = []opendal.Schemer{
	aliyun_drive.Scheme,
}

type BehaviorTestSuite struct {
	suite.Suite

	op *opendal.Operator
}

func (s *BehaviorTestSuite) SetupSuite() {
	test := os.Getenv("OPENDAL_TEST")
	var scheme opendal.Schemer
	for _, s := range schemes {
		if s.Scheme() != test {
			continue
		}
		scheme = s
		break
	}
	s.Require().NotNil(scheme, "unsupported scheme: %s", test)

	prefix := fmt.Sprintf("OPENDAL_%s_", strings.ToUpper(scheme.Scheme()))

	opts := opendal.OperatorOptions{}
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := pair[0]
		value := pair[1]
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		opts[strings.ToLower(strings.TrimPrefix(key, prefix))] = value
	}

	op, err := opendal.NewOperator(scheme, opts)
	s.Require().Nil(err, "create operator must succeed")

	s.op = op
}

func (s *BehaviorTestSuite) TestStat() {
	s.T().Parallel()
	assert := s.Require()
	op := s.op

	uuid := uuid.NewString()
	dir := fmt.Sprintf("%s/dir/", uuid)
	path := fmt.Sprintf("%s/path", dir)
	data := []byte(uuid)

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

	err = op.Delete(path)
	assert.Nil(err)
	err = op.Delete(dir)
	assert.Nil(err)
}

func (s *BehaviorTestSuite) TestWrite() {
	s.T().Parallel()
	assert := s.Require()
	op := s.op

	uuid := uuid.NewString()
	path := fmt.Sprintf("%s/path", uuid)
	data := []byte(uuid)

	err := op.Write(path, data)
	assert.Nil(err)

	result, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(data, result)

	err = op.Delete(path)
	assert.Nil(err)
}
