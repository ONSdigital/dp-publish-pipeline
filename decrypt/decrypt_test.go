package decrypt

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// Only testing the correct pass, if this is moved to production error cases
// need tested.
func TestDecryptCollectionFile(t *testing.T) {
	testFile := "../test-data/collections/test-0001/complete/about/data.json"
	key := "2iyOwMI3YF+fF+SDqMlD8Q=="

	Convey("With a filename and key, test for decryption", t, func() {
		plaintext, err := DecryptFile(testFile, key)
		So(err, ShouldBeNil)
		So(string(plaintext), ShouldContainSubstring, "We are the UK’s largest independent producer of official statistics")
	})

	Convey("With a good filename, but a bad key, ensure we get failure", t, func() {
		plaintext, err := DecryptFile(testFile, "not_a_key")
		So(err, ShouldNotBeNil)
		So(plaintext, ShouldBeNil)
	})

	Convey("With a broken filename, ensure we get failure", t, func() {
		plaintext, err := DecryptFile("/does/not/exist", "not_a_key")
		So(err, ShouldNotBeNil)
		So(plaintext, ShouldBeNil)
	})

}
