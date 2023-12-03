package meta

import (
	"embed"
	"testing"
)

func TestParsingMetaASTs(t *testing.T) {
	t.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	astMap, err := ParsePackagesASTs(embed.FS{})
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	if _, metaExists := astMap["meta"]; !metaExists {
		t.Error("Expected <meta> package to exist in Packages ASTs map, but it does not")
	}

	if len(astMap) < 2 {
		t.Errorf("Expected more then 1 package - meta, got: %d modules", len(astMap))
	}

	astMeta := astMap["meta"]
	if len(astMeta.FileToASTs) < 2 {
		t.Errorf("Expected at least two file ASTs in meta module, got: %d", len(astMeta.FileToASTs))
	}
	for file, astFile := range astMeta.FileToASTs {
		if len(astFile.Decls) == 0 {
			t.Errorf("Expected at least one top-level declaration at each file in meta module. This is not true for %s", file)
		}
	}
}

func BenchmarkParsingProjectASTs(b *testing.B) {
	b.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	for i := 0; i < b.N; i++ {
		ParsePackagesASTs(embed.FS{})
	}
}
