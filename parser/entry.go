package parser

import (
	"strings"
)

type SqlStatements struct {
	// the semicolon is optional due to the hack from the select clause regex
	Statements []*Statement ` json:"statements" parser:"@@ ( ';'+ @@? )*" `
}

const (
	Table              string = "TABLE"
	ExternalLocation          = "EXTERNAL_LOCATION"
	StorageCredentials        = "STORAGE_CREDENTIALS"
	Grant                     = "GRANT"
)

type EP string

func (ac *EP) Capture(value []string) error {
	//fmt.Println(value)
	curr := []string{string(*ac)}
	if ac == nil {
		curr = []string{}
	}
	*ac = EP(strings.TrimSpace(strings.Join(append(curr, value...), " ")))
	//*ac = AsClause(strings.TrimSpace(value[0][2:]))
	return nil
}

type Statement struct {
	//Select      string    `json:"select,omitempty" parser:"( @( SelectClause ) |"`
	Endpoint    *Endpoint `json:"endpoint,omitempty" parser:"@@ |" `
	Passthrough *EP       ` json:"passthrough,omitempty" parser:"@(~';')*"`
}
