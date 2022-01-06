package parser

type Flag bool

func (b *Flag) Capture(values []string) error {
	*b = values[0] != ""
	return nil
}

type Identifier struct {
	Value string `json:"value" parser:"@Ident | @IdentWithTicks"`
}

type Setting struct {
	Key   string `json:"key" parser:"@Ident '='?"`
	Value string `json:"value" parser:"@(String | Ident)"`
}

type EndpointCreate struct {
	WithReplacement *Flag       `json:"with_replacement,omitempty" parser:"'CREATE' @( 'OR' 'REPLACE' )?"`
	IsClassic       *Flag       `json:"is_classic,omitempty" parser:"'CLASSIC'?"`
	IfNotExists     *Flag       `json:"if_not_exists" parser:"( 'ENDPOINT' | 'WAREHOUSE') @('IF' 'NOT' 'EXISTS')?"`
	Name            *Identifier `json:"name" parser:"@@" `
	Options         []Setting   `json:"options" parser:"'WITH'? @@*" `
}

type EndpointAlter struct {
	IfExists *Flag       `json:"if_exists,omitempty" parser:"'ALTER' ( 'ENDPOINT' | 'WAREHOUSE') @('IF' 'EXISTS')?"`
	Name     *Identifier ` json:"name" parser:"@@"`
	Suspend  *Flag       `json:"suspend,omitempty" parser:"(@'SUSPEND' |" `
	Resume   *Flag       `json:"resume,omitempty" parser:"@'RESUME')!"`
}

type EndpointShow struct {
	EndpointFilter                string `json:"endpoint_filter,omitempty" parser:"'SHOW' 'ENDPOINTS' ( ('LIKE' @String)?" | `
	EndpointFilterCaseInsensitive string `json:"endpoint_filter_case_insensitive,omitempty" parser:" ('ILIKE' @String)? )" `
}

type EndpointDrop struct {
	IfExists *Flag       `json:"if_exists,omitempty" parser:"'DROP' ( 'ENDPOINT' | 'WAREHOUSE') @('IF' 'EXISTS')?"`
	Name     *Identifier `json:"name" parser:"@@" `
}

type EndpointUse struct {
	Name *Identifier `json:"name" parser:"'USE' 'ENDPOINT' @@ "`
}

type Endpoint struct {
	Create *EndpointCreate `json:"create,omitempty" parser:"(?= ('CREATE' | 'SHOW' | 'ALTER' | 'USE') ('OR' 'REPLACE')? 'ENDPOINT' ) @@ |" `
	Alter  *EndpointAlter  ` json:"alter,omitempty" parser:"@@ |" `
	Show   *EndpointShow   ` json:"show,omitempty" parser:"@@ |" `
	Drop   *EndpointDrop   ` json:"drop,omitempty" parser:"@@ |" `
	Use    *EndpointUse    `json:"use,omitempty" parser:"@@" `
}
