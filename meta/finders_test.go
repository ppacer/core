// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package meta

import (
	"go/ast"
	"testing"
)

func TestFindMethodBodySource(t *testing.T) {
	astMap, err := ParsePackagesASTs(goSourceFiles)
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
	astMap, err := ParsePackagesASTs(goSourceFiles)
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
	astMap, err := ParsePackagesASTs(goSourceFiles)
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
	astMap, err := ParsePackagesASTs(goSourceFiles)
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

type Task interface {
	Execute() error
}
type MyTask struct{}

func (mt *MyTask) Execute() error {
	return nil
}

func TestTypeNames(t *testing.T) {
	x := 42
	type MyInt int
	mi42 := MyInt(42)
	var a any = MyInt(42)

	mt := MyTask{}
	var task Task = &mt

	data := []struct {
		obj              any
		expectedTypeName string
	}{
		{42, "int"},
		{&x, "int"},
		{"test", "string"},
		{MyInt(42), "MyInt"},
		{&mi42, "MyInt"},
		{a, "MyInt"},
		{task, "MyTask"},
	}

	for _, d := range data {
		tn := TypeName(d.obj)
		if tn != d.expectedTypeName {
			t.Errorf("For obj %+v expected type name %s, got %s", d.obj,
				d.expectedTypeName, tn)
		}
	}
}
