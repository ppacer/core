// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package meta

import (
	"embed"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

// Packages ASTs map built on embedded Go source files. It's built on init.
var PackagesASTsMap map[string]PackageASTs = map[string]PackageASTs{}

// ParseASTs parses ASTs for packages and its Go files based on given FS.
// Parsed object is assigned to PackagesASTsMap. It's enough to call this
// function just once at the program start.
func ParseASTs(fs embed.FS) error {
	astMap, err := ParsePackagesASTs(fs)
	if err != nil {
		return err
	}
	PackagesASTsMap = astMap
	return nil
}

// PackageASTs represents single Go package metadata with FileToAST field which
// is a mapping from package file name to its parsed AST.
type PackageASTs struct {
	Name       string
	Fset       *token.FileSet
	FileToASTs map[string]*ast.File
}

// ParsePackagesASTs parses all packages under ./src/ in this project in form
// of map from package ID to PackageASTs (metadata plus mapping file -> AST).
func ParsePackagesASTs(fs embed.FS) (map[string]PackageASTs, error) {
	packMap := make(map[string]PackageASTs)
	wErr := walkParsePackages(fs, ".", packMap)
	if wErr != nil {
		return nil, wErr
	}
	return packMap, nil
}

func walkParsePackages(
	fs embed.FS, dirPath string, packages map[string]PackageASTs,
) error {
	pkg, err := parseSinglePackage(dirPath, fs)
	if err != nil {
		return err
	}
	packages[dirPath] = pkg

	dirEntries, dirErr := fs.ReadDir(dirPath)
	if dirErr != nil {
		return dirErr
	}
	for _, entry := range dirEntries {
		if entry.IsDir() {
			wErr := walkParsePackages(
				fs, embedPathJoin(dirPath, entry.Name()), packages,
			)
			if wErr != nil {
				return wErr
			}
		}
	}
	return nil
}

func parseSinglePackage(dirPath string, fs embed.FS) (PackageASTs, error) {
	fileToASTs := make(map[string]*ast.File)
	dirEntries, dirErr := fs.ReadDir(dirPath)
	if dirErr != nil {
		return PackageASTs{}, dirErr
	}
	fset := token.NewFileSet()

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		data, readErr := fs.ReadFile(embedPathJoin(dirPath, entry.Name()))
		if readErr != nil {
			return PackageASTs{}, readErr
		}
		astFile, parseErr := parser.ParseFile(fset, entry.Name(), data,
			parser.AllErrors|parser.ParseComments)
		if parseErr != nil {
			return PackageASTs{}, parseErr
		}
		fileToASTs[entry.Name()] = astFile
	}
	return PackageASTs{
		Name:       dirPath,
		Fset:       fset,
		FileToASTs: fileToASTs,
	}, nil
}

func embedPathJoin(base, child string) string {
	return strings.TrimPrefix(base+"/"+child, "./")
}
