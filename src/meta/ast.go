package meta

import (
	"embed"
	"go/ast"
	"go/parser"
	"go/token"
	"go_shed/src"
	"strings"

	"github.com/rs/zerolog/log"
)

// PackageASTs represents single Go package metadata with FileToAST field which
// is a mapping from package file name to its parsed AST.
type PackageASTs struct {
	Name       string
	Fset       *token.FileSet
	FileToASTs map[string]*ast.File
}

// ParsePackagesASTs parses all packages under ./src/ in this project in form
// of map from package ID to PackageASTs (metadata plus mapping file -> AST).
// Parsing is done on embedded Go project source files defined in
// src.GoSourceFiles.
func ParsePackagesASTs() (map[string]PackageASTs, error) {
	packMap := make(map[string]PackageASTs)
	wErr := walkParsePackages(src.GoSourceFiles, ".", packMap)
	if wErr != nil {
		return nil, wErr
	}
	return packMap, nil
}

func walkParsePackages(fs embed.FS, dirPath string, packages map[string]PackageASTs) error {
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
			wErr := walkParsePackages(fs, embedPathJoin(dirPath, entry.Name()), packages)
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
			log.Error().Err(readErr).Msgf("Could not read content of %s/%s file", dirPath, entry.Name())
			return PackageASTs{}, readErr
		}
		astFile, parseErr := parser.ParseFile(fset, entry.Name(), data, parser.AllErrors|parser.ParseComments)
		if parseErr != nil {
			log.Error().Err(parseErr).Msgf("Error while parsing %s/%s file", dirPath, entry.Name())
			return PackageASTs{}, parseErr
		}
		fileToASTs[entry.Name()] = astFile
	}
	return PackageASTs{Name: src.ModuleName + dirPath, Fset: fset, FileToASTs: fileToASTs}, nil
}

func embedPathJoin(base, child string) string {
	return strings.TrimPrefix(base+"/"+child, "./")
}
