package meta

import (
	"go/ast"

	"github.com/rs/zerolog/log"
	"golang.org/x/tools/go/packages"
)

// PackageASTs represents single Go package metadata with FileToAST field which
// is a mapping from package file name to its parsed AST. If you need to use
// token.FileSet corresponding to parsed ASTs, you should reach for
// Package.Fset field.
type PackageASTs struct {
	Package   *packages.Package
	FileToAST map[string]*ast.File
}

// ParsePackagesASTs parses all modules in this project in form of map from
// package ID to PackageASTs (metadata plus mapping file -> AST). Configuration
// on how this parsing should be done can be passed. In case when nil is
// provided, then default configuration will be used. Default configuration
// contains Dir as the current directory. If you want parse all modules in this
// project you should set cfg.Dir to the project root catalog.
// Example: To get AST of file tmp.go in this_proj/sub1 module you can do:
//
//     astMap, _ := ParsePackagesASTs(nil)
//     astFile := astMap["this_proj/sub1"].FileToAST["/full/path/here/this_proj/sub1/tmp.go"]
//
// Currently this function uses golang.org/x/tools/go/packages.Load function
// which is based on actual OS file system. We cannot simply embed project
// source code in the binary, to the actual program needs to be on the same
// machine as source code. TODO(dskrzypiec): implement packages.Load
// alternative using go/ast, go/parser and go/token, which would work for
// embedded files.
func ParsePackagesASTs(cfg *packages.Config) (map[string]PackageASTs, error) {
	pkgs, err := projectPackages(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Could not load project packages")
		return nil, err
	}
	astMap := packagesToASTsMap(pkgs)
	return astMap, nil
}

// Builds package name --> PackageASTs mapping based on package slice.
func packagesToASTsMap(pkgs []*packages.Package) map[string]PackageASTs {
	m := make(map[string]PackageASTs)
	for _, pkg := range pkgs {
		mm := make(map[string]*ast.File)
		for idx, fileAst := range pkg.Syntax {
			mm[pkg.GoFiles[idx]] = fileAst
		}
		m[pkg.ID] = PackageASTs{Package: pkg, FileToAST: mm}
	}
	return m
}

// Loads slice of parsed Packages (in this case internal modules) of the
// project.
func projectPackages(config *packages.Config) ([]*packages.Package, error) {
	var cfg *packages.Config
	if config == nil {
		cfg = packagesConfigDefault()
	} else {
		cfg = config
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, err
	}
	return pkgs, nil
}

func packagesConfigDefault() *packages.Config {
	return &packages.Config{
		Mode:  packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Tests: false,
	}
}
