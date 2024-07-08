package opendal_test

import (
	"fmt"
	"opendal"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

type behaviorTest = func(assert *require.Assertions, op *opendal.Operator)

func TestBehavior(t *testing.T) {
	op, err := newOperator()
	require.Nil(t, err)

	cap := op.Info().GetFullCapability()

	var tests []behaviorTest

	if cap.Stat() && cap.Write() && cap.Blocking() {
		tests = append(tests,
			testStat,
			testWrite,
		)
	}

	for i := range tests {
		test := tests[i]

		fullName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
		parts := strings.Split(fullName, ".")
		testName := strings.TrimPrefix(parts[len((parts))-1], "test")

		t.Run(testName, func(t *testing.T) {
			// Run all tests in parallel by default.
			// To run synchronously for specific services, set GOMAXPROCS=1.
			t.Parallel()
			assert := require.New(t)

			test(assert, op)
		})
	}
}

func newOperator() (op *opendal.Operator, err error) {
	var schemes = []opendal.Schemer{
		aliyun_drive.Scheme,
	}

	test := os.Getenv("OPENDAL_TEST")
	var scheme opendal.Schemer
	for _, s := range schemes {
		if s.Scheme() != test {
			continue
		}
		scheme = s
		break
	}
	if scheme == nil {
		err = fmt.Errorf("unsupported scheme: %s", test)
		return
	}

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

	op, err = opendal.NewOperator(scheme, opts)
	if err != nil {
		err = fmt.Errorf("create operator must succeed: %s", err)
	}

	return
}

func testStat(assert *require.Assertions, op *opendal.Operator) {
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

func testWrite(assert *require.Assertions, op *opendal.Operator) {
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
