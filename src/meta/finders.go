package meta

import (
	"bytes"
	"errors"
	"go/ast"
	"go/printer"

	"github.com/rs/zerolog/log"
)

var ErrMethodNotFound = errors.New("method not found in ASTs")

// MethodBodySource finds given method for given type in the AST and return it's body AST and source code as string. If
// given type or method does not exist in given ASTs map, then non nil error would be returned.
func MethodBodySource(astMap map[string]PackageASTs, typeName, methodName string) (*ast.BlockStmt, string, error) {
	for pkg := range astMap {
		for fileName, astFile := range astMap[pkg].FileToAST {
			funcDecl := findMethodInAST(astFile, typeName, methodName)
			if funcDecl == nil {
				continue
			}
			var buf bytes.Buffer
			err := printer.Fprint(&buf, astMap[pkg].Package.Fset, funcDecl.Body)
			if err != nil {
				log.Error().Err(err).Msgf("Error while printing source code for %s.%s method from %s", typeName, methodName, fileName)
				return nil, "", err
			}
			return funcDecl.Body, buf.String(), nil
		}
	}
	return nil, "", ErrMethodNotFound
}

func findMethodInAST(astFile *ast.File, typeName, methodName string) *ast.FuncDecl {
	for _, decl := range astFile.Decls {
		funcDecl, isFunc := decl.(*ast.FuncDecl)
		if !isFunc {
			continue
		}
		if funcDecl.Recv == nil || len(funcDecl.Recv.List) != 1 || funcDecl.Name.Name != methodName {
			continue
		}

		ident, isIdent := funcDecl.Recv.List[0].Type.(*ast.Ident)
		if isIdent && ident.Name == typeName {
			return funcDecl
		}

		// Check for *T receivers
		starExpr, isStar := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
		if isStar {
			ident, isIdent := starExpr.X.(*ast.Ident)
			if isIdent && ident.Name == typeName {
				return funcDecl
			}
		}
	}
	return nil
}
