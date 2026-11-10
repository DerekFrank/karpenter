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

// Package storelint checks the state rules pkg/state/AGENTS.md lays out:
//
//   - Stores, Derives, Claims and intent handles are built in pkg/state itself, in pkg/cluster, in a controller's
//     Register(context.Context, manager.Manager) error method, or in a test.
//   - A New* function or constructor outside pkg/state and pkg/cluster subscribes to nothing and derives nothing.
//   - A shared store, one reached through a pkg/cluster value, is added to a manager only by cluster.State.Register.
//   - An object a store holds is the informer cache's, so it's never passed to a client write, nor read through
//     StatusConditions() without options.
//   - A controller that reads stores requeues after a wall-clock deadline only, on a line that says so with
//     //storelint:deadline <reason>.
//   - Every Contribution has a statetest.ExpectContribution test in its package.
//   - A map or slice field of a store's Derived or Intent fields says what bounds it with a // bounded: comment.
//   - pkg/cluster exports no raw *state.Store, only Readers and intent handles.
//   - A Derive's target has a reader besides a source: waking a controller on part of an entry is SourceOn's job.
//
// A line carrying //storelint:allow <reason> is exempt from the first two rules, for code that predates them and is
// being moved, such as cost's stores.
package storelint

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

const (
	statePkg     = "sigs.k8s.io/karpenter/pkg/state"
	statetestPkg = "sigs.k8s.io/karpenter/pkg/state/statetest"
	clusterPkg   = "sigs.k8s.io/karpenter/pkg/cluster"
	facadePkg    = "sigs.k8s.io/karpenter/pkg/controllers/state"
	managerPkg   = "sigs.k8s.io/controller-runtime/pkg/manager"
	clientPkg    = "sigs.k8s.io/controller-runtime/pkg/client"
)

// builders are the pkg/state functions that build a store, a Derive, Claims or an intent handle.
var builders = map[string]bool{
	"NewObjectStore": true, "NewObjectStoreBy": true, "NewFedStore": true, "NewSyntheticStore": true,
	"Derive": true, "DeriveObject": true, "DeriveEach": true, "DeriveEachObject": true, "NewClaims": true, "NewIntent": true,
}

// derives are the builders whose second argument is the target they write.
var derives = map[string]bool{"Derive": true, "DeriveObject": true, "DeriveEach": true, "DeriveEachObject": true}

// writes are the client methods that write the object they're passed with the server's response.
var writes = map[string]bool{"Create": true, "Update": true, "Patch": true, "Delete": true}

// readOnlyUses are the methods a Derive target can be called with that read nothing: a source, a subscription, the
// sync gate and a reset.
var readOnlyUses = map[string]bool{"Source": true, "SourceOn": true, "Subscribe": true, "Synced": true, "Clear": true}

var Analyzer = &analysis.Analyzer{
	Name:     "storelint",
	Doc:      "checks where state stores are built and started, that the objects they hold aren't written, and the rules of pkg/state/AGENTS.md",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	in := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	readsStores := importsAny(pass.Pkg, statePkg, clusterPkg, facadePkg) && pass.Pkg.Path() != statePkg
	in.WithStack([]ast.Node{(*ast.CallExpr)(nil), (*ast.KeyValueExpr)(nil), (*ast.AssignStmt)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if !push || isTest(pass, n.Pos()) {
			return true
		}
		switch n := n.(type) {
		case *ast.CallExpr:
			checkCall(pass, n, stack)
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && key.Name == "RequeueAfter" && readsStores {
				checkDeadline(pass, n.Pos())
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if sel, ok := ast.Unparen(lhs).(*ast.SelectorExpr); ok && sel.Sel.Name == "RequeueAfter" && readsStores {
					checkDeadline(pass, n.Pos())
				}
			}
		}
		return true
	})
	checkContributions(pass)
	checkBounded(pass)
	checkExports(pass)
	checkWakeFilters(pass, in)
	return nil, nil
}

func checkCall(pass *analysis.Pass, call *ast.CallExpr, stack []ast.Node) {
	fn := enclosingFunc(stack)
	constructor := isConstructor(fn) && !holdsState(pass.Pkg.Path())
	if name, ok := builder(pass, call); ok && !allowed(pass, call.Pos()) {
		switch {
		case constructor:
			pass.Reportf(call.Pos(), "state.%s in a constructor: build it in the controller's Register(context.Context, manager.Manager) error", name)
		case !buildsStores(pass, fn):
			pass.Reportf(call.Pos(), "state.%s builds a store: call it in pkg/cluster, or in a controller's Register(context.Context, manager.Manager) error", name)
		}
	}
	if constructor && subscribes(pass, call) && !allowed(pass, call.Pos()) {
		pass.Reportf(call.Pos(), "Subscribe in a constructor: subscribe in the controller's Register(context.Context, manager.Manager) error, or wake on a source")
	}
	if store, ok := addedSharedStore(pass, call); ok && !stateRegister(pass, fn) {
		pass.Reportf(call.Pos(), "%s is a shared store: only cluster.State.Register adds it to the manager", types.ExprString(store))
	}
	if obj, ok := writtenHeldObject(pass, call); ok {
		pass.Reportf(call.Pos(), "%s is the informer cache's object, which a client write overwrites: write a DeepCopy", types.ExprString(obj))
	}
	if obj, ok := conditionsOfHeldObject(pass, call); ok {
		pass.Reportf(call.Pos(), "%s is the informer cache's object, which StatusConditions() writes: pass status.WithObservedOnly()", types.ExprString(obj))
	}
}

// builder returns the name of the pkg/state builder call calls, if it calls one.
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

// subscribes returns whether call calls a Subscribe method declared in pkg/state or pkg/cluster.
func subscribes(pass *analysis.Pass, call *ast.CallExpr) bool {
	fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
	if !ok || fn.Name() != "Subscribe" || fn.Pkg() == nil {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	return ok && sig.Recv() != nil && holdsState(fn.Pkg().Path())
}

// holdsState returns whether path is pkg/state itself or pkg/cluster, which build the shared state.
func holdsState(path string) bool {
	return path == statePkg || path == clusterPkg || strings.HasPrefix(path, clusterPkg+"/")
}

// buildsStores returns whether code in fn of the package being checked may build stores: pkg/state itself, and not its
// subpackages; pkg/cluster; and a controller's Register.
func buildsStores(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	return holdsState(pass.Pkg.Path()) || registerMethod(pass, fn)
}

// isConstructor returns whether fn is a New* function or method.
func isConstructor(fn *ast.FuncDecl) bool {
	return fn != nil && strings.HasPrefix(fn.Name.Name, "New")
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
		if call, ok := ast.Unparen(e).(*ast.CallExpr); ok {
			e = call.Fun
		}
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

// writtenHeldObject returns the object a client write is passed, if it's an object a store holds.
func writtenHeldObject(pass *analysis.Pass, call *ast.CallExpr) (ast.Expr, bool) {
	fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
	if !ok || !writes[fn.Name()] || fn.Pkg() == nil || fn.Pkg().Path() != clientPkg || len(call.Args) < 2 {
		return nil, false
	}
	return heldObject(pass, call.Args[1])
}

// conditionsOfHeldObject returns the object StatusConditions() is called on without options, if it's an object a store
// holds.
func conditionsOfHeldObject(pass *analysis.Pass, call *ast.CallExpr) (ast.Expr, bool) {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "StatusConditions" || len(call.Args) != 0 {
		return nil, false
	}
	return heldObject(pass, sel.X)
}

// heldObject returns e if it's a field of a value of a type pkg/cluster declares, such as a StateNode's NodeClaim, which
// holds an object a store holds.
func heldObject(pass *analysis.Pass, e ast.Expr) (ast.Expr, bool) {
	e = ast.Unparen(e)
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return nil, false
	}
	s, ok := pass.TypesInfo.Selections[sel]
	if !ok || s.Kind() != types.FieldVal || s.Obj().Pkg() == nil || s.Obj().Pkg().Path() != clusterPkg {
		return nil, false
	}
	return e, true
}

// checkDeadline reports a RequeueAfter at pos unless its line carries //storelint:deadline with a reason.
func checkDeadline(pass *analysis.Pass, pos token.Pos) {
	if !lineHas(pass, pos, "//storelint:deadline ") {
		pass.Reportf(pos, "RequeueAfter in a controller that reads stores: wake on a source, or mark a wall-clock deadline with //storelint:deadline <reason>")
	}
}

// allowed returns whether the line at pos carries //storelint:allow with a reason.
func allowed(pass *analysis.Pass, pos token.Pos) bool {
	return lineHas(pass, pos, "//storelint:allow ")
}

// lineHas returns whether a comment on the line at pos, or the line before it, starts with directive.
func lineHas(pass *analysis.Pass, pos token.Pos, directive string) bool {
	line := pass.Fset.Position(pos).Line
	for _, f := range pass.Files {
		if pass.Fset.File(f.Pos()) != pass.Fset.File(pos) {
			continue
		}
		for _, group := range f.Comments {
			for _, c := range group.List {
				if l := pass.Fset.Position(c.Pos()).Line; (l == line || l == line-1) && strings.HasPrefix(c.Text, directive) {
					return true
				}
			}
		}
	}
	return false
}

// checkContributions reports each type of the package that implements state.Contribution without a
// statetest.ExpectContribution or ExpectContributionWith call in the package's tests. It reads the test files of the
// package's directory, so a Contribution an external test checks through an export_test.go alias counts too.
func checkContributions(pass *analysis.Pass) {
	if pass.Pkg.Path() == statePkg || len(pass.Files) == 0 {
		return
	}
	covered := testedNames(pass)
	for _, name := range pass.Pkg.Scope().Names() {
		tn, ok := pass.Pkg.Scope().Lookup(name).(*types.TypeName)
		if !ok || tn.IsAlias() || isTest(pass, tn.Pos()) || covered[tn.Name()] || !contribution(tn) {
			continue
		}
		pass.Reportf(tn.Pos(), "%s is a Contribution without a statetest.ExpectContribution test in its package", tn.Name())
	}
}

// testedNames returns the names in the arguments of the statetest.ExpectContribution and ExpectContributionWith calls
// of the test files in the package's directory, and the types the test files alias them to.
func testedNames(pass *analysis.Pass) map[string]bool {
	names := map[string]bool{}
	aliases := map[string]string{}
	dir := filepath.Dir(pass.Fset.File(pass.Files[0].Pos()).Name())
	entries, err := os.ReadDir(dir)
	if err != nil {
		return names
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		// A law check names the contributions it checks in its call or in the test function around it, such as a
		// table of contributions it ranges over.
		var funcs []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				funcs = funcs[:len(funcs)-1]
				return true
			}
			funcs = append(funcs, n)
			switch n := n.(type) {
			case *ast.TypeSpec:
				if id, ok := n.Type.(*ast.Ident); ok && n.Assign.IsValid() {
					aliases[n.Name.Name] = id.Name
				}
			case *ast.CallExpr:
				fun := n.Fun
				if index, ok := fun.(*ast.IndexListExpr); ok {
					fun = index.X
				}
				if sel, ok := fun.(*ast.SelectorExpr); !ok || !strings.HasPrefix(sel.Sel.Name, "ExpectContribution") {
					return true
				}
				var scope ast.Node = n
				for i := len(funcs) - 1; i >= 0; i-- {
					if _, ok := funcs[i].(*ast.FuncLit); ok {
						scope = funcs[i]
						break
					}
					if _, ok := funcs[i].(*ast.FuncDecl); ok {
						scope = funcs[i]
						break
					}
				}
				ast.Inspect(scope, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.Ident:
						names[n.Name] = true
					case *ast.SelectorExpr:
						names[n.Sel.Name] = true
					}
					return true
				})
			}
			return true
		})
	}
	for alias, target := range aliases {
		if names[alias] {
			names[target] = true
		}
	}
	return names
}

// contribution returns whether the named type implements state.Contribution: Update(D, Self) D and Delete(D) D.
func contribution(tn *types.TypeName) bool {
	n, ok := tn.Type().(*types.Named)
	if !ok {
		return false
	}
	if _, ok := n.Underlying().(*types.Interface); ok {
		return false
	}
	var update, del *types.Signature
	for i := range n.NumMethods() {
		switch m := n.Method(i); m.Name() {
		case "Update":
			update = m.Type().(*types.Signature)
		case "Delete":
			del = m.Type().(*types.Signature)
		}
	}
	if update == nil || del == nil || update.Params().Len() != 2 || update.Results().Len() != 1 || del.Params().Len() != 1 || del.Results().Len() != 1 {
		return false
	}
	d := update.Params().At(0).Type()
	self, ok := types.Unalias(update.Params().At(1).Type()).(*types.Named)
	return ok && self.Origin() == n.Origin() && types.Identical(update.Results().At(0).Type(), d) &&
		types.Identical(del.Params().At(0).Type(), d) && types.Identical(del.Results().At(0).Type(), d)
}

// checkBounded reports a map or slice field, declared in the package, of a type the package uses as a store's Derived
// or Intent fields, without a // bounded: comment, and of the structs those fields nest.
func checkBounded(pass *analysis.Pass) {
	fields := map[*types.Var]*ast.Field{}
	for _, f := range pass.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			if field, ok := n.(*ast.Field); ok {
				for _, name := range field.Names {
					if v, ok := pass.TypesInfo.Defs[name].(*types.Var); ok {
						fields[v] = field
					}
				}
			}
			return true
		})
	}
	seen := map[types.Type]bool{}
	var check func(t types.Type)
	check = func(t types.Type) {
		t = types.Unalias(t)
		if seen[t] {
			return
		}
		seen[t] = true
		n, ok := t.(*types.Named)
		if !ok || n.Obj().Pkg() == nil {
			return
		}
		if n.Obj().Pkg().Path() == statePkg {
			// An Intended[V] holds V.
			for i := range n.TypeArgs().Len() {
				check(n.TypeArgs().At(i))
			}
			return
		}
		s, ok := n.Underlying().(*types.Struct)
		if !ok || n.Obj().Pkg() != pass.Pkg {
			return
		}
		for i := range s.NumFields() {
			v := s.Field(i)
			switch types.Unalias(v.Type()).Underlying().(type) {
			case *types.Map, *types.Slice:
				if field, ok := fields[v]; ok && !bounded(field) && !isTest(pass, v.Pos()) {
					pass.Reportf(v.Pos(), "%s.%s is a collection in a store's fields: say what bounds it with a // bounded: <what> comment", n.Obj().Name(), v.Name())
				}
			}
			check(v.Type())
		}
	}
	for _, inst := range pass.TypesInfo.Instances {
		n, ok := types.Unalias(inst.Type).(*types.Named)
		if !ok || n.Obj().Pkg() == nil || n.Obj().Pkg().Path() != statePkg {
			continue
		}
		switch n.Obj().Name() {
		case "Store", "Reader", "Entry":
		default:
			continue
		}
		args := n.TypeArgs()
		// Store and Reader are [K, O, D, I]; Entry is [O, D, I].
		for i := args.Len() - 2; i >= 0 && i < args.Len(); i++ {
			check(args.At(i))
		}
	}
}

func bounded(field *ast.Field) bool {
	for _, group := range []*ast.CommentGroup{field.Doc, field.Comment} {
		if group != nil && strings.Contains(group.Text(), "bounded:") {
			return true
		}
	}
	return false
}

// checkExports reports an exported field, function result or method result of pkg/cluster whose type is a raw
// *state.Store, which would let any holder write it.
func checkExports(pass *analysis.Pass) {
	if pass.Pkg.Path() != clusterPkg {
		return
	}
	for _, name := range pass.Pkg.Scope().Names() {
		obj := pass.Pkg.Scope().Lookup(name)
		if isTest(pass, obj.Pos()) {
			continue
		}
		switch obj := obj.(type) {
		case *types.Func:
			checkResults(pass, obj)
		case *types.TypeName:
			n, ok := obj.Type().(*types.Named)
			if !ok {
				continue
			}
			if s, ok := n.Underlying().(*types.Struct); ok {
				for i := range s.NumFields() {
					if f := s.Field(i); f.Exported() && named(f.Type(), statePkg, "Store") {
						pass.Reportf(f.Pos(), "%s.%s exports a raw store: export its Reader", obj.Name(), f.Name())
					}
				}
			}
			for i := range n.NumMethods() {
				checkResults(pass, n.Method(i))
			}
		}
	}
}

func checkResults(pass *analysis.Pass, fn *types.Func) {
	if !fn.Exported() {
		return
	}
	results := fn.Type().(*types.Signature).Results()
	for i := range results.Len() {
		if named(results.At(i).Type(), statePkg, "Store") {
			pass.Reportf(fn.Pos(), "%s returns a raw store: return its Reader", fn.Name())
		}
	}
}

// checkWakeFilters reports a Derive whose target, a variable or field the package declares, is read only as a source:
// every use of it is the Derive's target, a Source, SourceOn, Subscribe, Synced or Clear call, or an assignment.
func checkWakeFilters(pass *analysis.Pass, in *inspector.Inspector) {
	targets := map[types.Object]*ast.CallExpr{}
	derivations := map[*ast.CallExpr]bool{}
	in.WithStack([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		call := n.(*ast.CallExpr)
		name, ok := builder(pass, call)
		if !push || !ok || !derives[name] || len(call.Args) < 2 || isTest(pass, call.Pos()) {
			return true
		}
		if obj := objectOf(pass, call.Args[1]); obj != nil && obj.Pkg() == pass.Pkg {
			targets[obj] = call
			// A Derivation whose result is kept is read through its Sources.
			if len(stack) > 1 {
				if _, ok := stack[len(stack)-2].(*ast.ExprStmt); !ok {
					derivations[call] = true
				}
			}
		}
		return true
	})
	read := map[types.Object]bool{}
	in.WithStack([]ast.Node{(*ast.Ident)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		ident := n.(*ast.Ident)
		obj := pass.TypesInfo.Uses[ident]
		if !push || obj == nil {
			return true
		}
		if _, ok := targets[obj]; !ok {
			return true
		}
		if !readOnlyUse(pass, ident, stack, targets[obj]) {
			read[obj] = true
		}
		return true
	})
	for obj, call := range targets {
		if !read[obj] && !derivations[call] {
			pass.Reportf(call.Pos(), "%s is read only as a source: wake on the source store with SourceOn instead of deriving a store to filter wakes", obj.Name())
		}
	}
}

// readOnlyUse returns whether the use of a Derive target at ident reads nothing of its entries.
func readOnlyUse(pass *analysis.Pass, ident *ast.Ident, stack []ast.Node, derive *ast.CallExpr) bool {
	// The use is ident itself, or the selector of a field whose Sel is ident.
	i := len(stack) - 1
	var use ast.Node = ident
	if i > 0 {
		if sel, ok := stack[i-1].(*ast.SelectorExpr); ok && sel.Sel == ident {
			use, i = sel, i-1
		}
	}
	if i == 0 {
		return false
	}
	switch parent := stack[i-1].(type) {
	case *ast.CallExpr:
		return parent == derive && len(parent.Args) > 1 && ast.Unparen(parent.Args[1]) == use
	case *ast.AssignStmt:
		for _, lhs := range parent.Lhs {
			if lhs == use {
				return true
			}
		}
	case *ast.SelectorExpr:
		if parent.X == use && readOnlyUses[parent.Sel.Name] && i > 1 {
			_, called := stack[i-2].(*ast.CallExpr)
			return called
		}
	case *ast.KeyValueExpr:
		// A composite literal's field initialized with the store, as a constructor does.
		return parent.Value == use
	}
	return false
}

// objectOf returns the variable or field e names, or nil.
func objectOf(pass *analysis.Pass, e ast.Expr) types.Object {
	switch e := ast.Unparen(e).(type) {
	case *ast.Ident:
		return pass.TypesInfo.Uses[e]
	case *ast.SelectorExpr:
		if s, ok := pass.TypesInfo.Selections[e]; ok && s.Kind() == types.FieldVal {
			return s.Obj()
		}
	}
	return nil
}

// importsAny returns whether pkg imports any of paths directly.
func importsAny(pkg *types.Package, paths ...string) bool {
	for _, imp := range pkg.Imports() {
		for _, path := range paths {
			if imp.Path() == path {
				return true
			}
		}
	}
	return false
}

func isTest(pass *analysis.Pass, pos token.Pos) bool {
	f := pass.Fset.File(pos)
	return f != nil && strings.HasSuffix(f.Name(), "_test.go")
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
