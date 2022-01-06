package main

import (
	"C"
	"fmt"
	"github.com/stikkireddy/databricks-sql-extensions/parser"
)

//export parseSql
func parseSql(sqlStr *C.char) (parsedJson *C.char, error *C.char) {
	sql := C.GoString(sqlStr)
	resp, err := parser.ParseAllSqlStatements(sql)
	if err != nil {
		return nil, C.CString(fmt.Sprintf(err.Error()))
	}
	return C.CString(resp), nil
}

func main() {
	return
}
