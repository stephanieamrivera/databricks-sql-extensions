package parser

var typeKeywords = []string{
	"MAP",
	"ARRAY",
	"STRUCT",
}

var principalKeyWords = []string{
	"USER",
	"USERS",
	"GROUP",
	"GROUPS",
	"SERVICE PRINCIPAL",
	"SERVICE PRINCIPALS",
}

var endpointKeywords = []string{
	"SERVERLESS",
	"CLASSIC",
	"ENDPOINT",
	"ENDPOINTS",
	"WAREHOUSE",
	"ALTER",
	"RESUME",
	"SUSPEND",
	"SHOW",
	"USE",
	"OR",
	"REPLACE",
	"DROP",
}

var grantKeyWords = []string{
	"ALL PRIVILIGES",
	"MODIFY",
	"USAGE",
	"GRANT",
}

var keyWords = []string{
	//"ALL PRIVILIGES",
	"SCHEMA",
	"CATALOG",
	"TO",
	//"USAGE",
	//"MODIFY",
	//"GRANT",
	"WITH",
	"STORAGE",
	"CREDENTIAL",
	"URL",
	"EXTERNAL",
	"BUCKETS",
	"LOCATION",
	"PARTITIONED",
	"COMMENT",
	"CREATE",
	"TABLE",
	"GLOBAL",
	"VIEW",
	"IF",
	"NOT",
	"EXISTS",
	"SELECT",
	"FROM",
	"TOP",
	"DISTINCT",
	"ALL",
	"WHERE",
	"GROUP",
	"BY",
	"HAVING",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	"ORDER",
	"LIMIT",
	"OFFSET",
	"TRUE",
	"FALSE",
	"NULL",
	"IS",
	"NOT",
	"ANY",
	"SOME",
	"BETWEEN",
	"AND",
	"OR",
	"LIKE",
	"AS",
	"IN",
}

func Flatten(lists ...[]string) (res []string) {
	for _, v := range lists {
		res = append(res, v...)
	}
	return
}

var AllKeyWords = Flatten(keyWords,
	typeKeywords,
	principalKeyWords,
	grantKeyWords,
	endpointKeywords)

var TypeMap = map[string]string{
	"boolean":   "boolean",
	"byte":      "byte",
	"tinyint":   "byte",
	"short":     "short",
	"smallint":  "short",
	"int":       "integer",
	"integer":   "integer",
	"long":      "long",
	"bigint":    "long",
	"float":     "float",
	"real":      "float",
	"double":    "double",
	"date":      "date",
	"timestamp": "timestamp",
	"string":    "string",
	"binary":    "binary",
	"decimal":   "decimal",
	"dec":       "decimal",
	"numeric":   "decimal",
	"char":      "char",
}

var TypeNameMap = map[string]string{
	"boolean":   "boolean",
	"byte":      "byte",
	"tinyint":   "byte",
	"short":     "short",
	"smallint":  "short",
	"int":       "int",
	"integer":   "int",
	"long":      "long",
	"bigint":    "long",
	"float":     "float",
	"real":      "float",
	"double":    "double",
	"date":      "date",
	"timestamp": "timestamp",
	"string":    "string",
	"binary":    "binary",
	"decimal":   "decimal",
	"dec":       "decimal",
	"numeric":   "decimal",
	"char":      "char",
}
