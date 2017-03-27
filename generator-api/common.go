package generator

const xlsFormat = "xls"
const csvFormat = "csv"

type fileWriter func([]string) error

func IsCsv(format string) bool {
        return format == csvFormat
}

func IsXls(format string) bool {
        return format == xlsFormat
}
