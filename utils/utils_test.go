package utils

import "testing"
import "os"

func TestGetDefaultVariable(t *testing.T) {
	expected := "Test123"
	results := GetEnvironmentVariable("TEST", expected)
	if expected != results {
		t.Errorf("Test failed, expected: %s got: %s", expected, results)
	}
}

func TestGetEnvironmentVariable(t *testing.T) {
	expected := "Test123"
	os.Setenv("TEST", expected)
	results := GetEnvironmentVariable("TEST", "1234")
	if expected != results {
		t.Errorf("Test failed, expected: %s got: %s", expected, results)
	}
}
