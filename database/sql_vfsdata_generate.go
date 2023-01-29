//go:build ignore
// +build ignore

package main

import (
	"github.com/shurcooL/vfsgen"
	"log"
	"net/http"
)

func main() {
	fs := http.Dir("sql")
	err := vfsgen.Generate(fs, vfsgen.Options{
		Filename:     "sql_vfsdata.go",
		PackageName:  "database",
		VariableName: "SQL",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
