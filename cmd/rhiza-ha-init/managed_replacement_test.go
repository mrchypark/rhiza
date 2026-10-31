package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedsql"
)

func TestManagedReplacementLoopCanceledBeforeHostAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No Host access is permitted once serve owns cancellation/shutdown.
	runManagedReplacementLoop(ctx, nil, managedsql.Config{})
}

// The loop deliberately accepts a concrete Host, so it cannot be replaced by
// a fake that records calls. Inspect the checked-in control flow instead: a
// pending SQL reservation makes Host.Ready fail, but must not gate the
// independent control-plane resume of an already-Closed repair.
func TestManagedReplacementLoopReconcilesClosedRepairWithoutSQLReadiness(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate managed replacement source")
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(file), "serve.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := make(map[string]*ast.FuncDecl)
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = fn
		}
	}
	loop, reconcile := functions["runManagedReplacementLoop"], functions["reconcileManagedReplacement"]
	if loop == nil || reconcile == nil {
		t.Fatal("managed replacement control path is missing")
	}
	if !callsSelector(loop.Body, "reconcileManagedReplacement") {
		t.Fatal("background loop no longer invokes managed replacement reconciliation")
	}
	if callsSelector(loop.Body, "Ready") || callsSelector(reconcile.Body, "Ready") {
		t.Fatal("SQL readiness must not gate control-plane reconciliation of a closed repair")
	}
	if !containsClosedRepairResume(reconcile.Body) {
		t.Fatal("reconciliation no longer resumes the exact Closed repair through Host.ReplaceVoter")
	}
}

func callsSelector(node ast.Node, selector string) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			found = found || fun.Sel.Name == selector
		case *ast.Ident:
			found = found || fun.Name == selector
		}
		return !found
	})
	return found
}

func containsClosedRepairResume(body *ast.BlockStmt) bool {
	for _, stmt := range body.List {
		branch, ok := stmt.(*ast.IfStmt)
		if !ok || !callsClosedRepairTest(branch.Cond) {
			continue
		}
		if callsSelector(branch.Body, "PlanVoterReplacement") && callsSelector(branch.Body, "ReplaceVoter") {
			return true
		}
	}
	return false
}

func callsClosedRepairTest(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		binary, ok := n.(*ast.BinaryExpr)
		if !ok || binary.Op != token.EQL {
			return true
		}
		for _, side := range []ast.Expr{binary.X, binary.Y} {
			ident, ok := side.(*ast.BasicLit)
			if ok && ident.Kind == token.STRING && ident.Value == `"Closed"` {
				found = true
			}
		}
		return !found
	})
	return found
}
