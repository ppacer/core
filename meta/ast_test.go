// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package meta

import (
	"embed"
	"testing"
)

//go:embed *.go
var goSourceFiles embed.FS

func TestParsePackagesASTs(t *testing.T) {
	asts, err := ParsePackagesASTs(goSourceFiles)
	if err != nil {
		t.Errorf("Error while parsing ASTs from meta package: %s", err.Error())
	}
	expectedFiles := []string{
		"ast.go", "ast_test.go", "finders.go", "finders_test.go",
	}
	for _, expFileName := range expectedFiles {
		if _, exists := asts["."].FileToASTs[expFileName]; !exists {
			t.Errorf("Expected to have AST for file %s, but it's not there",
				expFileName)
		}
	}
}

func BenchmarkParsingProjectASTs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParsePackagesASTs(goSourceFiles)
	}
}
