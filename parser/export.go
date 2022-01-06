package parser

import (
	"C"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/thoas/go-funk"
	"regexp"
	"strings"
)

func PreParseString(fileData string) string {
	//removed lookahead: //https://larrysteinle.com/2011/02/09/use-regular-expressions-to-clean-sql-statements/
	//wont support comments inside of strings for comments
	allComments := regexp.MustCompile(`(--[^\r\n]*)|(/\*[\w\W]*?\*/)`)
	pythonComments := regexp.MustCompile(`#[^\n]*`)
	return pythonComments.ReplaceAllString(allComments.ReplaceAllString(fileData, ""), "")
}

func FetchUniqVariables(strWithVars string) (resp []string) {
	//removed lookahead: //https://larrysteinle.com/2011/02/09/use-regular-expressions-to-clean-sql-statements/
	//wont support comments inside of strings for comments
	allComments := regexp.MustCompile(`\${([^}]+)}`)
	for _, match := range allComments.FindAllString(strWithVars, -1) {
		resp = append(resp, strings.TrimRight(strings.TrimLeft(match, "${var."), "}"))
	}
	return funk.UniqString(resp)

}

func parseSql(sqlStr string) ([]*Statement, error) {
	parsedSqlStatements := &SqlStatements{}
	err := parser.ParseString("", PreParseString(sqlStr), parsedSqlStatements)
	if err != nil {
		parseErr, ok := err.(participle.Error)
		if ok {
			return nil, errors.New(fmt.Sprintf("error at position", parseErr.Position(), parseErr))
		}
		return nil, err
	}
	if parsedSqlStatements.Statements == nil {
		return nil, errors.New("No table ddl statements were parsed!")
	}
	return parsedSqlStatements.Statements, nil
}

func Jsonify(data interface{}) (resp string, err error) {
	bytes, err := json.Marshal(data)
	//repr.Println(ddl)
	if err != nil {
		return "", err
	}
	resp = string(bytes)
	return
}

func getOrNil(a interface{}, b bool) interface{} {
	if b == true {
		return a
	} else {
		return nil
	}
}

func ParseAllSqlStatements(sqlStr string) (jsonString string, err error) {
	parsedStatements, err := parseSql(sqlStr)
	if err != nil {
		return "", err
	}
	type resp struct {
		ParsedStatementsList []*Statement `json:"parsed_statements_list"`
	}
	//parsedStatements := make([]ParsedSqlStatement, 0)
	//for _, ddl := range ddlStatements {
	//	parsedSqlStatement := ddl.getParsedSqlStatements(sqlStr, getAst, getHCL)
	//	if parsedSqlStatement != nil {
	//		parsedStatements = append(parsedStatements, *parsedSqlStatement)
	//	}
	//}
	return Jsonify(resp{parsedStatements})
}
