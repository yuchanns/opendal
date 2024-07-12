package opendal_test

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.yuchanns.xyz/opendal"
)

func testsList(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() || !cap.List() {
		return nil
	}
	return []behaviorTest{
		testListCheck,
		testListDir,
		testListPrefix,
		testListRichDir,
		testListEmptyDir,
		testListNonExistDir,
		testListSubDir,
		testListNestedDir,
		testListDirWithFilePath,
	}
}

func testListCheck(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	assert.Nil(op.Check(), "operator check must succeed")
}

func testListDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	path, content, size := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))

	assert.Nil(op.Write(path, content), "write must succeed")

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()

	var found bool
	for obs.Next() {
		entry := obs.Entry()

		if entry.Path() != path {
			continue
		}

		meta, err := op.Stat(entry.Path())
		assert.Nil(err)
		assert.True(meta.IsFile())
		assert.Equal(uint64(size), meta.ContentLength())
		found = true
		break
	}
	assert.Nil(obs.Error())
	assert.True(found, "file must be found in list")
}

func testListPrefix(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	obs, err := op.List(path[:len(path)-1])
	assert.Nil(err)
	defer obs.Close()
	assert.True(obs.Next())
	assert.Nil(obs.Error())

	entry := obs.Entry()
	assert.Equal(path, entry.Path())
}

func testListRichDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	assert.Nil(op.CreateDir(parent))

	var expected []string
	for range 10 {
		path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))
		expected = append(expected, path)
		assert.Nil(op.Write(path, content))
	}

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()
	var actual []string
	for obs.Next() {
		entry := obs.Entry()
		actual = append(actual, entry.Path())
	}
	assert.Nil(obs.Error())

	slices.Sort(expected)
	slices.Sort(actual)

	assert.Equal(expected, actual)
}

func testListEmptyDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir := fixture.NewDirPath()

	assert.Nil(op.CreateDir(dir), "create must succeed")

	obs, err := op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	var paths []string
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
	}
	assert.Nil(obs.Error())
	assert.Equal(0, len(paths), "dir should only return empty")

	obs, err = op.List(strings.TrimSuffix(dir, "/"))
	assert.Nil(err)
	defer obs.Close()
	for obs.Next() {
		entry := obs.Entry()
		path := entry.Path()
		paths = append(paths, path)
		meta, err := op.Stat(path)
		assert.Nil(err, "given dir should exist")
		assert.True(meta.IsDir(), "given dir must be dir, but found: %v", path)
	}
	assert.Nil(obs.Error())
	assert.Equal(1, len(paths), "only return the dir iteself, but found: %v", paths)
}

func testListNonExistDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir := fixture.NewDirPath()

	obs, err := op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	assert.False(obs.Next(), "dir should only return empty")
}

func testListSubDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	obs, err := op.List("/")
	assert.Nil(err)
	defer obs.Close()

	var found bool
	for obs.Next() {
		entry := obs.Entry()
		if path != entry.Path() {
			continue
		}
		found = true
		break
	}
	assert.Nil(obs.Error())
	assert.True(found, "dir should be found in list")
}

func testListNestedDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	dir := fixture.PushPath(fmt.Sprintf("%s%s/", parent, uuid.NewString()))

	filePath := fixture.PushPath(fmt.Sprintf("%s%s", dir, uuid.NewString()))
	dirPath := fixture.PushPath(fmt.Sprintf("%s%s/", dir, uuid.NewString()))

	assert.Nil(op.CreateDir(dir), "create must succeed")
	assert.Nil(op.Write(filePath, []byte("test_list_nested_dir")), "write must succeed")
	assert.Nil(op.CreateDir(dirPath), "create must succeed")

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()
	var paths []string
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
		assert.Equal(dir, entry.Path())
	}
	assert.Nil(obs.Error())
	assert.Equal(1, len(paths), "parent should only got 1 entry")

	obs, err = op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	paths = nil
	var foundFile bool
	var foundDir bool
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
		if entry.Path() == filePath {
			foundFile = true
		} else if entry.Path() == dirPath {
			foundDir = true
		}
	}
	assert.Nil(obs.Error())
	assert.Equal(2, len(paths), "parent should only got 2 entries")

	assert.True(foundFile, "file should be found in list")
	meta, err := op.Stat(filePath)
	assert.Nil(err)
	assert.True(meta.IsFile())
	assert.Equal(uint64(20), meta.ContentLength())

	assert.True(foundDir, "dir should be found in list")
	meta, err = op.Stat(dirPath)
	assert.Nil(err)
	assert.True(meta.IsDir())
}

func testListDirWithFilePath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))

	assert.Nil(op.Write(path, content))

	obs, err := op.List(strings.TrimSuffix(parent, "/"))
	assert.Nil(err)
	defer obs.Close()

	for obs.Next() {
		entry := obs.Entry()
		assert.Equal(parent, entry.Path())
	}
	assert.Nil(obs.Error())
}
