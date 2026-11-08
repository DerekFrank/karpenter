/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package storelint checks where state stores are built and started, as pkg/state/AGENTS.md lays out:
//
//   - state.NewStore, NewObjectStore, NewJoinStore, Join and Derive build stores, so they're called only where stores
//     are built: in pkg/cluster, which builds the shared stores; in pkg/state, which defines them; in a controller's
//     Register(context.Context, manager.Manager) error method, which builds its local stores; and in tests.
//   - A shared store, one reached through a pkg/cluster value, is added to a manager only by cluster.State.Register,
//     so that it starts exactly once whichever controllers read it.
package storelint

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

const (
	statePkg   = "sigs.k8s.io/karpenter/pkg/state"
	clusterPkg = "sigs.k8s.io/karpenter/pkg/cluster"
	managerPkg = "sigs.k8s.io/controller-runtime/pkg/manager"
)

// builders are the pkg/state functions that build a store, or a part of one.
var builders = map[string]bool{"NewStore": true, "NewObjectStore": true, "NewJoinStore": true, "Join": true, "Derive": true}

var Analyzer = &analysis.Analyzer{
	Name:     "storelint",
	Doc:      "checks that state stores are built only in pkg/cluster, pkg/state, a controller's Register or a test, and that shared stores are added to the manager only by cluster.State.Register",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	in := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	in.WithStack([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push {
			return true
		}
		call := n.(*ast.CallExpr)
		if strings.HasSuffix(pass.Fset.File(call.Pos()).Name(), "_test.go") {
			return true
		}
		fn := enclosingFunc(stack)
		if name, ok := builder(pass, call); ok && !buildsStores(pass, fn) {
			pass.Reportf(call.Pos(), "state.%s builds a store: call it in pkg/cluster, or in a controller's Register(context.Context, manager.Manager) error", name)
		}
		if store, ok := addedSharedStore(pass, call); ok && !stateRegister(pass, fn) {
			pass.Reportf(call.Pos(), "%s is a shared store: only cluster.State.Register adds it to the manager", types.ExprString(store))
		}
		return true
	})
	return nil, nil
}

// builder returns the name of the pkg/state store builder call calls, if it calls one.
func builder(pass *analysis.Pass, call *ast.CallExpr) (string, bool) {
	fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != statePkg || !builders[fn.Name()] {
		return "", false
	}
	if sig, ok := fn.Type().(*types.Signature); !ok || sig.Recv() != nil {
		return "", false
	}
	return fn.Name(), true
}

// buildsStores returns whether code in fn of the package being checked may build stores.
func buildsStores(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	path := pass.Pkg.Path()
	for _, pkg := range []string{statePkg, clusterPkg} {
		if path == pkg || strings.HasPrefix(path, pkg+"/") {
			return true
		}
	}
	return registerMethod(pass, fn)
}

// registerMethod returns whether fn is a method Register(context.Context, manager.Manager) error.
func registerMethod(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if fn == nil || fn.Recv == nil || fn.Name.Name != "Register" {
		return false
	}
	obj, ok := pass.TypesInfo.Defs[fn.Name].(*types.Func)
	if !ok {
		return false
	}
	sig := obj.Type().(*types.Signature)
	if sig.Params().Len() != 2 || sig.Results().Len() != 1 {
		return false
	}
	return named(sig.Params().At(0).Type(), "context", "Context") &&
		named(sig.Params().At(1).Type(), managerPkg, "Manager") &&
		types.Identical(sig.Results().At(0).Type(), types.Universe.Lookup("error").Type())
}

// stateRegister returns whether fn is cluster.State's Register.
func stateRegister(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if pass.Pkg.Path() != clusterPkg || fn == nil || fn.Recv == nil || fn.Name.Name != "Register" || len(fn.Recv.List) != 1 {
		return false
	}
	return named(pass.TypesInfo.TypeOf(fn.Recv.List[0].Type), clusterPkg, "State")
}

// addedSharedStore returns the store a manager.Manager's Add call adds, if it's a shared store: a state.Store reached
// through a value of a type pkg/cluster declares.
func addedSharedStore(pass *analysis.Pass, call *ast.CallExpr) (ast.Expr, bool) {
	fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
	if !ok || fn.Name() != "Add" || fn.Pkg() == nil || fn.Pkg().Path() != managerPkg || len(call.Args) != 1 {
		return nil, false
	}
	arg := ast.Unparen(call.Args[0])
	if !named(pass.TypesInfo.TypeOf(arg), statePkg, "Store") {
		return nil, false
	}
	for e := arg; ; {
		sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
		if !ok {
			return nil, false
		}
		if named(pass.TypesInfo.TypeOf(sel.X), clusterPkg, "") {
			return arg, true
		}
		e = sel.X
	}
}

// named returns whether t, or what it points to, is a named type of pkg called name, or of any name if name is empty.
func named(t types.Type, pkg, name string) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := types.Unalias(t).(*types.Named)
	if !ok || n.Obj().Pkg() == nil {
		return false
	}
	return n.Obj().Pkg().Path() == pkg && (name == "" || n.Obj().Name() == name)
}

// enclosingFunc returns the function declaration the innermost node of stack is in, or nil at package level.
func enclosingFunc(stack []ast.Node) *ast.FuncDecl {
	for i := len(stack) - 1; i >= 0; i-- {
		if fn, ok := stack[i].(*ast.FuncDecl); ok {
			return fn
		}
	}
	return nil
}
