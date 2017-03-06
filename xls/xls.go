package xls

// brew install xlslib
import (
	"io"
	"os"
	"unsafe"

	uuid "github.com/satori/go.uuid"
)

/*
#cgo LDFLAGS: /usr/local/lib/libxls.dylib
#include <stdlib.h>
#include <stdbool.h>
#include "/usr/local/include/xlslib/xlslib.h"
*/
import "C"

type XLSWorkbook struct {
	workbook  *C.struct__workbook
	worksheet *C.struct__worksheet
	row       int
}

func CreateXLSWorkbook(workSheetName string) XLSWorkbook {
	var wb XLSWorkbook
	wb.row = 0
	wb.workbook = C.xlsNewWorkbook()
	cworkSheetName := C.CString(workSheetName)
	defer C.free(unsafe.Pointer(cworkSheetName))
	wb.worksheet = C.xlsWorkbookSheet(wb.workbook, cworkSheetName)
	return wb
}

func (wb *XLSWorkbook) WriteRow(row []string) error {
	ctitle := C.CString(row[0])
	defer C.free(unsafe.Pointer(ctitle))
	C.xlsWorksheetLabel(wb.worksheet, C.unsigned32_t(wb.row),
		C.unsigned32_t(0), ctitle, nil)
	for i := 1; i < len(row); i++ {
		cvalue := C.CString(row[i])
		defer C.free(unsafe.Pointer(cvalue))
		C.xlsWorksheetLabel(wb.worksheet, C.unsigned32_t(wb.row), C.unsigned32_t(i),
			cvalue, nil)
	}
	wb.row++
	return nil
}

func (wb *XLSWorkbook) DumpToWriter(writer io.Writer) {
	tmpFile := uuid.NewV1()
	fileName := C.CString(tmpFile.String())
	defer C.free(unsafe.Pointer(fileName))
	C.xlsWorkbookDump(wb.workbook, fileName)
	data, _ := os.Open(tmpFile.String())
	io.Copy(writer, data)
	os.Remove(tmpFile.String())

}

func (wb *XLSWorkbook) Close() {
	C.xlsDeleteWorkbook(wb.workbook)
}
