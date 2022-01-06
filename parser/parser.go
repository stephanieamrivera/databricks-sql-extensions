package parser

import (
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"strings"
)

var (
	anyKeyWord = strings.Join(AllKeyWords, "|")
	sqlLexer   = lexer.Must(lexer.NewSimple([]lexer.Rule{
		//{"Colon", `:`, nil},
		//{"NestedStruct", `([a-zA-Z_][a-zA-Z0-9_]*)(\s+)*<(\s+)*(.*?)(>>>>>|>>>>|>>>|>>|>)`, nil},
		//{`Keyword`, `(?i)\b(MAP|ARRAY|STRUCT|BUCKETS|LOCATION|PARTITIONED|COMMENT|CREATE|TABLE|IF|NOT|EXISTS|SELECT|FROM|TOP|DISTINCT|ALL|WHERE|GROUP|BY|HAVING|UNION|MINUS|EXCEPT|INTERSECT|ORDER|LIMIT|OFFSET|TRUE|FALSE|NULL|IS|NOT|ANY|SOME|BETWEEN|AND|OR|LIKE|AS|IN)\b`, nil},
		//high jack the whole as clause
		//best effort to stop at semicolon
		{"AsClause", `(AS)(\s+)*(SELECT)(.*)[^;]`, nil},
		//{"SelectClause", `(SELECT(([^'"]|"[^'"]*"))*?(;)|SELECT(([^'"]|'[^'"]*'))*?(;))`, nil},
		{`Keyword`, fmt.Sprintf(`(?i)\b(%s)\b`, anyKeyWord), nil},
		{`IdentWithTicks`, "[`][a-zA-Z_][a-zA-Z0-9_@.\\-\\s]*[`]", nil},
		{`Ident`, "[a-zA-Z_][a-zA-Z0-9@._]*", nil},
		{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`, nil},
		{`Backticks`, "`", nil},
		{"JsonLookup", `::`, nil},
		//{"Colon", `:`, nil},
		{`String`, `'[^']*'|"[^"]*"`, nil},
		{`Operators`, `<>|!=|<=|>=|[-+*/%,.()=<>]`, nil},
		{"Term", `[;]`, nil},
		{"whitespace", `[ \n\t]+`, nil},
		//{"anything", ``, nil},
	}))
	//parser = participle.MustBuild(
	//	&SqlStatements{},
	//	participle.Lexer(sqlLexer),
	//	participle.Unquote("String"),
	//	participle.CaseInsensitive("Keyword"),
	//	participle.CaseInsensitive("AsClause"),
	//	participle.Unquote("IdentWithTicks"),
	//	//participle
	//	//participle.Elide("whitespace"),
	//	// Need to solve left recursion detection first, if possible.
	//	participle.UseLookahead(5),
	//
	//)
	parser = participle.MustBuild(
		&SqlStatements{},
		participle.Lexer(sqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
		participle.CaseInsensitive("AsClause"),
		participle.Unquote("IdentWithTicks"),
		//participle.Elide("SqlLineComment"),
		//participle.Elide("whitespace"),
		// Need to solve left recursion detection first, if possible.
		participle.UseLookahead(5),
	)
)
