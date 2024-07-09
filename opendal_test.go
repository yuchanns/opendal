package opendal_test

import (
	"fmt"
	"opendal"
	"os"
	"reflect"
	"runtime"
	"slices"
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
			testList,
			testReader,
			testCopy,
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
	path := fmt.Sprintf("%spath", dir)
	data := []byte(uuid)

	err := op.CreateDir(dir)
	assert.Nil(err)

	err = op.Write(path, data)
	assert.Nil(err)

	_, err = op.Stat("/not_exists")
	assert.NotNil(err)
	assert.Equal(int32(3), err.(*opendal.Error).Code())
	exist, err := op.IsExist("/not_exists")
	assert.Nil(err)
	assert.False(exist)

	meta, err := op.Stat(strings.TrimSuffix(dir, "/"))
	assert.Nil(err)
	assert.True(meta.IsDir())
	assert.False(meta.IsFile())
	exist, err = op.IsExist(strings.TrimSuffix(dir, "/"))
	assert.Nil(err)
	assert.True(exist)

	meta, err = op.Stat(path)
	assert.Nil(err)
	assert.Equal(uint64(len(data)), meta.ContentLength())
	assert.False(meta.IsDir())
	assert.True(meta.IsFile())
	assert.False(meta.LastModified().IsZero())
	exist, err = op.IsExist(path)
	assert.Nil(err)
	assert.True(exist)

	assert.Nil(op.Delete(path))
	assert.Nil(op.Delete(dir))
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

	assert.Nil(op.Delete(path))
}

func testList(assert *require.Assertions, op *opendal.Operator) {
	uuid := uuid.NewString()
	dir := fmt.Sprintf("%s/dir/", uuid)
	pathA := fmt.Sprintf("%spath_a", dir)
	pathB := fmt.Sprintf("%spath_b", dir)
	data := []byte(uuid)

	err := op.CreateDir(dir)
	assert.Nil(err)

	err = op.Write(pathA, data)
	assert.Nil(err)

	err = op.Write(pathB, data)
	assert.Nil(err)

	lister, err := op.List(dir)
	assert.Nil(err)

	var (
		names []string
		paths []string

		expectedNames = []string{"path_a", "path_b"}
		expectedPaths = []string{pathA, pathB}
	)
	for lister.Next() {
		entry := lister.Entry()
		assert.Nil(entry.Error())
		names = append(names, entry.Name())
		paths = append(paths, entry.Path())
	}
	slices.Sort(expectedNames)
	slices.Sort(expectedPaths)
	slices.Sort(names)
	slices.Sort(paths)

	assert.Equal(expectedNames, names)
	assert.Equal(expectedPaths, paths)

	assert.Nil(op.Delete(dir))
}

func testReader(assert *require.Assertions, op *opendal.Operator) {
	uuid := uuid.NewString()
	path := fmt.Sprintf("%s/path", uuid)
	data := []byte(uuid)

	err := op.Write(path, data)
	assert.Nil(err)

	reader, err := op.Reader(path)
	assert.Nil(err)
	buf, size, err := reader.Read(uint(len(data)))
	assert.Nil(err)
	assert.Equal(data, buf)
	assert.Equal(uint(len(buf)), size)

	assert.Nil(op.Delete(path))
}

func testCopy(assert *require.Assertions, op *opendal.Operator) {
	uuid := uuid.NewString()
	pathA := fmt.Sprintf("%s/pathA", uuid)
	pathB := fmt.Sprintf("%s/pathB", uuid)
	data := []byte(uuid)

	err := op.Write(pathA, data)
	assert.Nil(err)

	assert.Nil(op.Copy(pathA, pathB))
	result, err := op.Read(pathB)
	assert.Nil(err)
	assert.Equal(data, result)

	assert.Nil(op.Delete(pathA))
	assert.Nil(op.Delete(pathB))
}

func testRename(assert *require.Assertions, op *opendal.Operator) {
	uuid := uuid.NewString()
	pathA := fmt.Sprintf("%s/pathA", uuid)
	pathB := fmt.Sprintf("%s/pathB", uuid)
	data := []byte(uuid)

	err := op.Write(pathA, data)
	assert.Nil(err)

	assert.Nil(op.Rename(pathA, pathB))

	exist, err := op.IsExist(pathA)
	assert.Nil(err)
	assert.False(exist)

	result, err := op.Read(pathB)
	assert.Nil(err)
	assert.Equal(data, result)

	assert.Nil(op.Delete(pathB))
}
