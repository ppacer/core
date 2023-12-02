package meta

import (
	"embed"
	"go/ast"
	"testing"
)

func TestFindMethodBodySource(t *testing.T) {
	t.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	astMap, err := ParsePackagesASTs(embed.FS{})
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	bodyBlockStmt, bodySrc, err := MethodBodySource(astMap, "PointTest", "String")
	if err != nil {
		t.Errorf("Error while getting method's body source code: %s", err.Error())
	}

	const expectedBodySrc = `{
	return fmt.Sprintf("Point(%d, %d)", pt.X, pt.Y)
}`
	if bodySrc != expectedBodySrc {
		t.Errorf("Expected [%s] body, but got [%s]", expectedBodySrc, bodySrc)
	}
	if bodyBlockStmt == nil {
		t.Error("Expected non nil *ast.BlockStmt for found method's body")
	}
	if len(bodyBlockStmt.List) != 1 {
		t.Errorf("Expected single statement in found method's body, got: %d",
			len(bodyBlockStmt.List))
	}
	if _, ok := bodyBlockStmt.List[0].(*ast.ReturnStmt); !ok {
		t.Error("Expected single ast.ReturnStmt in found method's body")
	}
}

func TestFindMethodBodySourcePrivate(t *testing.T) {
	t.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	astMap, err := ParsePackagesASTs(embed.FS{})
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	bodyBlockStmt, bodySrc, err := MethodBodySource(astMap, "PointTest", "privateMethod")
	if err != nil {
		t.Errorf("Error while getting method's body source code: %s", err.Error())
	}
	const expectedBodySrc = `{
	return 42
}`

	if bodySrc != expectedBodySrc {
		t.Errorf("Expected [%s] body, but got [%s]", expectedBodySrc, bodySrc)
	}
	if len(bodyBlockStmt.List) != 1 {
		t.Errorf("Expected single statement in found method's body, got: %d",
			len(bodyBlockStmt.List))
	}
	if _, ok := bodyBlockStmt.List[0].(*ast.ReturnStmt); !ok {
		t.Error("Expected single ast.ReturnStmt in found method's body")
	}
}

func TestFindMethodBodySourceOnPointer(t *testing.T) {
	t.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	astMap, err := ParsePackagesASTs(embed.FS{})
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	bodyBlockStmt, bodySrc, err := MethodBodySource(astMap, "PointTest", "EmptyMethod")
	if err != nil {
		t.Errorf("Error while getting method's body source code: %s", err.Error())
	}
	const expectedBodySrc = `{
}`

	if bodySrc != expectedBodySrc {
		t.Errorf("Expected [%s] body, but got [%s]", expectedBodySrc, bodySrc)
	}
	if bodyBlockStmt == nil {
		t.Error("Expected non nil *ast.BlockStmt for found method's body")
	}
	if len(bodyBlockStmt.List) != 0 {
		t.Errorf("Expected empty body block statements list, but got %d statements", len(bodyBlockStmt.List))
	}
}

func TestFindMethodBodySourceNotExist(t *testing.T) {
	t.Skip("Approach to embedding Go files and parsing the tree will be revisited")
	astMap, err := ParsePackagesASTs(embed.FS{})
	if err != nil {
		t.Errorf("Couldn't get AST map for meta module: %s", err.Error())
	}

	_, source, err := MethodBodySource(astMap, "PointTest", "FakeMethodName")
	if err == nil {
		t.Errorf("Expected to not find FakeMethodName, but got nil error. Source: %s", source)
	}
	if err != ErrMethodNotFound {
		t.Errorf("Expected ErrMethodNotFound error, but got %v error", err)
	}
}
