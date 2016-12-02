package decrypt

import (
	"strings"
	"testing"
)

// Only testing the correct pass, if this is moved to production error cases
// need tested.
func TestDecyptZebedeeFile(t *testing.T) {
	testFile := "../test-data/collections/test-0001/complete/about/data.json"
	key := "2iyOwMI3YF+fF+SDqMlD8Q=="
	content := DecryptFile(testFile, key)
	expecting := "We are the UK’s largest independent producer of official statistics"
	if !strings.Contains(string(content), expecting) {
		t.Errorf("Decryption failed: %s", string(content))
	}
}
