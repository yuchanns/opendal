package opendal_test

import (
	"opendal"

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

func testCreateDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsDir())
}

func testCreateDirExisting(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path))
	assert.Nil(op.CreateDir(path))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsDir())
}
