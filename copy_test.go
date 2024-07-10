package opendal_test

import (
	"fmt"
	"opendal"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsCopy(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() || !cap.Copy() {
		return nil
	}
	return []behaviorTest{
		testCopyFileWithASCIIName,
		testCopyFileWithNonASCIIName,
		testCopyNonExistingSource,
		testCopySourceDir,
		testCopyTargetDir,
		testCopySelf,
		testCopyNested,
		testCopyOverwrite,
	}
}

func testCopyFileWithASCIIName(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := uuid.NewString()
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := uuid.NewString()

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
	assert.Nil(op.Delete(targetPath), "delete must succeed")
}

func testCopyFileWithNonASCIIName(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := "🐂🍺中文.docx"
	targetPath := "😈🐅Français.docx"
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))
	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
	assert.Nil(op.Delete(targetPath), "delete must succeed")
}

func testCopyNonExistingSource(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := uuid.NewString()
	targetPath := uuid.NewString()

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
}

func testCopySourceDir(assert *require.Assertions, op *opendal.Operator) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath := fmt.Sprintf("%s/", uuid.NewString())
	targetPath := uuid.NewString()

	assert.Nil(op.CreateDir(sourcePath))

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testCopyTargetDir(assert *require.Assertions, op *opendal.Operator) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath := uuid.NewString()
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := fmt.Sprintf("%s/", uuid.NewString())

	assert.Nil(op.CreateDir(targetPath))

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
	assert.Nil(op.Delete(targetPath), "delete must succeed")
}

func testCopySelf(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := uuid.NewString()
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))

	err := op.Copy(sourcePath, sourcePath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsSameFile, assertErrorCode(err))

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
}

func testCopyNested(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := uuid.NewString()
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := fmt.Sprintf(
		"%s/%s/%s",
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
	)

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
	assert.Nil(op.Delete(targetPath), "delete must succeed")
}

func testCopyOverwrite(assert *require.Assertions, op *opendal.Operator) {
	sourcePath := uuid.NewString()
	sourceContent, _ := genBytes(op.Info().GetFullCapability())

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := uuid.NewString()
	targetContent, _ := genBytes(op.Info().GetFullCapability())
	assert.NotEqual(sourceContent, targetContent)

	assert.Nil(op.Write(targetPath, targetContent))

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)

	assert.Nil(op.Delete(sourcePath), "delete must succeed")
	assert.Nil(op.Delete(targetPath), "delete must succeed")
}
