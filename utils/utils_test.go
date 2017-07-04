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

func TestGetEnvironmentVariableAsArray(t *testing.T) {
	expected := []string{"Test123", "Test321"}
	os.Setenv("TEST", "Test123,Test321")
	results := GetEnvironmentVariableAsArray("TEST", "1234")
	if expected[0] != results[0] && expected[1] != results[1] {
		t.Errorf("Test failed, expected: %s got: %s", expected, results)
	}
}

func TestGetEnvironmentVariableAsArrayDefault(t *testing.T) {
	expected := []string{"Test123", "Test321"}
	results := GetEnvironmentVariableAsArray("TEST32", "Test123,Test321")
	if expected[0] != results[0] {
		t.Errorf("Test failed, expected: %s got: %s", expected, results)
	}
}

func TestGetEnvironmentVariableIntArray(t *testing.T) {
	expected := 123
	os.Setenv("TEST", "123")
	results, _ := GetEnvironmentVariableInt("TEST", 321)
	if expected != results {
		t.Errorf("Test failed, expected: %d got: %d", expected, results)
	}
}
