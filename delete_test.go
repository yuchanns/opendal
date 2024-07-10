package opendal_test

import (
	"opendal"

	"github.com/stretchr/testify/require"
)

func testsDelete(cap *opendal.Capability) []behaviorTest {
	if !cap.Stat() || !cap.Delete() || !cap.Write() {
		return nil
	}
	tests := []behaviorTest{
		testDeleteFile,
		testDeleteEmptyDir,
		testDeleteWithSpecialChars,
		testDeleteNotExisting,
		testDeleteStream,
		testRemoveOneFile,
	}
	if cap.ListWithRecursive() {
		tests = append(tests, testRemoveAllBasic)
		if !cap.CreateDir() {
			tests = append(tests, testRemoveAllWithPrefix)
		}
	}
	return tests
}

func testDeleteFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testDeleteEmptyDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testDeleteWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testDeleteNotExisting(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testDeleteStream(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testRemoveOneFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testRemoveAllBasic(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}

func testRemoveAllWithPrefix(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	//
}
