package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStoreFile(t *testing.T) {
	var data DataSet
	data.FileLocation = "test/data.json"
	data.FileContent = "123456789"
	Convey("With a file location and content, test storeFile", t, func() {
		bytes, _ := json.Marshal(data)
		storeData(bytes)
		content, err := ioutil.ReadFile(data.FileLocation)
		So(err, ShouldBeNil)
		So(string(content), ShouldEqual, data.FileContent)
	})
	os.RemoveAll("test")
}
