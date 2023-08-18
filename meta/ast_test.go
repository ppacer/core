package meta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParsingMetaASTs(t *testing.T) {
	astMap, err := getASTMapWd()
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	if len(astMap) != 1 {
		t.Errorf("Expected ASTs for 1 module - meta, got: %d modules", len(astMap))
	}

	for pkg := range astMap {
		if len(astMap[pkg].FileToAST) < 2 {
			t.Errorf("Expected at least two file ASTs in meta module, got: %d", len(astMap[pkg].FileToAST))
		}
		for file, astFile := range astMap[pkg].FileToAST {
			if len(astFile.Decls) == 0 {
				t.Errorf("Expected at least one top-level declaration at each file in meta module. This is not true for %s", file)
			}
		}
	}
}

func BenchmarkParsingProjectASTs(b *testing.B) {
	wd, err := os.Getwd()
	if err != nil {
		b.Errorf("Could not get working dir: %s", err.Error())
	}
	cfg := packagesConfigDefault()
	cfg.Dir = filepath.Dir(wd)

	for i := 0; i < b.N; i++ {
		ParsePackagesASTs(cfg)
	}
}

func getASTMapWd() (map[string]PackageASTs, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	cfg := packagesConfigDefault()
	cfg.Dir = wd
	return ParsePackagesASTs(cfg)
}
