package opendal_test

import (
	"fmt"
	"opendal"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsCreateDir(cap *opendal.Capability) []behaviorTest {
	if !cap.CreateDir() || !cap.Stat() {
		return nil
	}
	return []behaviorTest{
		testCreateDir,
		testCreateDirExisting,
	}
}

func testCreateDir(assert *require.Assertions, op *opendal.Operator) {
	path := fmt.Sprintf("%s/", uuid.NewString())

	assert.Nil(op.CreateDir(path))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsDir())

	assert.Nil(op.Delete(path), "delete must succeed")
}

func testCreateDirExisting(assert *require.Assertions, op *opendal.Operator) {
	path := fmt.Sprintf("%s/", uuid.NewString())

	assert.Nil(op.CreateDir(path))
	assert.Nil(op.CreateDir(path))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsDir())

	assert.Nil(op.Delete(path), "delete must succeed")
}
