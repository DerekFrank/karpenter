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

package storelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

// checkNodeEntryFields reports, outside pkg/cluster and pkg/state, a read of a node fact NodeView owns straight from a
// node entry: any field of the Node a NodeClaim entry's Derived.Node holds, and the Labels, Spec.Taints,
// Status.Capacity or Status.Allocatable of a NodeClaimEntry's or NodeEntry's Object. Which object each fact comes from
// depends on the node's registration and initialization, so a reader reads it through cluster.ViewOfNodeClaim or
// cluster.ViewOfNode. A NodeClaim's own spec and status, such as its NodeClassRef or conditions, are read from the
// entry's Object.
func checkNodeEntryFields(pass *analysis.Pass, in *inspector.Inspector) {
	if holdsState(pass.Pkg.Path()) {
		return
	}
	in.Preorder([]ast.Node{(*ast.SelectorExpr)(nil)}, func(n ast.Node) {
		sel := n.(*ast.SelectorExpr)
		if isTest(pass, sel.Pos()) {
			return
		}
		if inner, ok := fieldOf(pass, sel.X); ok && inner.Obj().Pkg().Path() == clusterPkg && inner.Obj().Name() == "Node" &&
			named(inner.Recv(), clusterPkg, "NodeClaimDerived") {
			pass.Reportf(sel.Pos(), "%s reads the Node a NodeClaim entry carries: read it through cluster.ViewOfNodeClaim", types.ExprString(sel))
			return
		}
		var object ast.Expr
		switch sel.Sel.Name {
		case "Labels":
			object = sel.X
		case "Taints", "Capacity", "Allocatable":
			parent, ok := ast.Unparen(sel.X).(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name == "Taints") != (parent.Sel.Name == "Spec") || (sel.Sel.Name != "Taints") != (parent.Sel.Name == "Status") {
				return
			}
			object = parent.X
		default:
			return
		}
		if s, ok := fieldOf(pass, object); ok && s.Obj().Pkg().Path() == statePkg && s.Obj().Name() == "Object" && nodeEntry(s.Recv()) {
			pass.Reportf(sel.Pos(), "%s reads a node fact from a node entry's object: read it through the node's cluster.NodeView", types.ExprString(sel))
		}
	})
}

// fieldOf returns the field selection e is, if it's one.
func fieldOf(pass *analysis.Pass, e ast.Expr) (*types.Selection, bool) {
	sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
	if !ok {
		return nil, false
	}
	s, ok := pass.TypesInfo.Selections[sel]
	return s, ok && s.Kind() == types.FieldVal && s.Obj().Pkg() != nil
}

// nodeEntry returns whether t is a cluster.NodeClaimEntry or cluster.NodeEntry: a state.Entry whose Derived is
// cluster.NodeClaimDerived or cluster.NodeDerived.
func nodeEntry(t types.Type) bool {
	n, ok := types.Unalias(t).(*types.Named)
	if !ok || n.Obj().Pkg() == nil || n.Obj().Pkg().Path() != statePkg || n.Obj().Name() != "Entry" || n.TypeArgs().Len() != 3 {
		return false
	}
	d := n.TypeArgs().At(1)
	return named(d, clusterPkg, "NodeClaimDerived") || named(d, clusterPkg, "NodeDerived")
}

// definition is where a local variable gets its value: the expression of an assignment or a declaration, the i-th of
// its results, or the range expression of a range statement's value. A parameter has none, and is held: its caller may
// pass a store's own object.
type definition struct {
	rhs   ast.Expr
	index int
	// ranged is whether rhs is the expression a range statement ranges over, of which the variable is an element.
	ranged bool
	param  bool
}

// definitions maps each local variable and parameter of the package's files to where it gets its value. A variable
// assigned more than once keeps its first definition.
func definitions(pass *analysis.Pass) map[*types.Var]definition {
	defs := map[*types.Var]definition{}
	add := func(ident ast.Expr, d definition) {
		id, ok := ident.(*ast.Ident)
		if !ok {
			return
		}
		v, ok := pass.TypesInfo.ObjectOf(id).(*types.Var)
		if !ok {
			return
		}
		if _, seen := defs[v]; !seen {
			defs[v] = d
		}
	}
	for _, f := range pass.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncType:
				if n.Params != nil {
					for _, field := range n.Params.List {
						for _, name := range field.Names {
							add(name, definition{param: true})
						}
					}
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if len(n.Rhs) == len(n.Lhs) {
						add(lhs, definition{rhs: n.Rhs[i]})
					} else if len(n.Rhs) == 1 {
						add(lhs, definition{rhs: n.Rhs[0], index: i})
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if len(n.Values) == len(n.Names) {
						add(name, definition{rhs: n.Values[i]})
					} else if len(n.Values) == 1 {
						add(name, definition{rhs: n.Values[0], index: i})
					}
				}
			case *ast.RangeStmt:
				if n.Value != nil {
					add(n.Value, definition{rhs: n.X, ranged: true})
				}
			}
			return true
		})
	}
	return defs
}

// held decides whether an expression is an object a store holds, which a client write or StatusConditions() would
// change for every reader.
type held struct {
	pass *analysis.Pass
	defs map[*types.Var]definition
}

func newHeld(pass *analysis.Pass) *held { return &held{pass: pass, defs: definitions(pass)} }

// object returns whether e is an object a store holds: a field of a value of a type pkg/cluster declares, such as a
// NodeClaim entry's Derived.Node; the Object of an entry a read returned without a copy; a NodeView's NodeClaim() or
// Node(); or a local variable holding one of those.
func (h *held) object(e ast.Expr) bool { return h.objectDepth(e, 0) }

func (h *held) objectDepth(e ast.Expr, depth int) bool {
	if depth > 8 {
		return false
	}
	switch e := ast.Unparen(e).(type) {
	case *ast.SelectorExpr:
		s, ok := h.pass.TypesInfo.Selections[e]
		if !ok || s.Kind() != types.FieldVal || s.Obj().Pkg() == nil {
			return false
		}
		switch s.Obj().Pkg().Path() {
		case clusterPkg:
			return true
		case statePkg:
			return s.Obj().Name() == "Object" && h.sharedEntry(e.X, depth+1)
		}
	case *ast.CallExpr:
		fn, ok := typeutil.Callee(h.pass.TypesInfo, e).(*types.Func)
		if !ok || fn.Pkg() == nil || fn.Pkg().Path() != clusterPkg {
			return false
		}
		sig := fn.Type().(*types.Signature)
		return sig.Recv() != nil && named(sig.Recv().Type(), clusterPkg, "NodeView") && (fn.Name() == "NodeClaim" || fn.Name() == "Node")
	case *ast.Ident:
		v, ok := h.pass.TypesInfo.ObjectOf(e).(*types.Var)
		if !ok {
			return false
		}
		d, ok := h.defs[v]
		if !ok || d.param || d.rhs == nil {
			return false
		}
		if d.ranged {
			// An element of what a pkg/cluster read returns, such as NodePoolState.UnlaunchedNodeClaims.
			_, object := v.Type().(*types.Pointer)
			return object && h.sharedEntry(d.rhs, depth+1)
		}
		return d.index == 0 && h.objectDepth(d.rhs, depth+1)
	}
	return false
}

// sharedEntry returns whether e is a state.Entry, or holds entries, whose objects are a store's own: one a read
// returned with client.UnsafeDisableDeepCopy, or a pkg/cluster function returned, such as NodePoolState.LaunchedNodes;
// a parameter, which a caller may pass from either; a field of a struct, such as a disruption candidate's entry, built
// from shared reads; or an element of any of those. An entry a read returned as a copy is the caller's own.
func (h *held) sharedEntry(e ast.Expr, depth int) bool {
	if depth > 8 {
		return false
	}
	switch e := ast.Unparen(e).(type) {
	case *ast.Ident:
		v, ok := h.pass.TypesInfo.ObjectOf(e).(*types.Var)
		if !ok {
			return false
		}
		d, ok := h.defs[v]
		if !ok || d.param {
			return holdsEntry(v.Type())
		}
		if d.rhs == nil {
			return false
		}
		if call, ok := ast.Unparen(d.rhs).(*ast.CallExpr); ok {
			return h.sharedRead(call)
		}
		return h.sharedEntry(d.rhs, depth+1)
	case *ast.SelectorExpr:
		if s, ok := h.pass.TypesInfo.Selections[e]; ok && s.Kind() == types.FieldVal {
			return holdsEntry(s.Type())
		}
	case *ast.IndexExpr:
		return h.sharedEntry(e.X, depth+1)
	case *ast.CallExpr:
		return h.sharedRead(e)
	}
	return false
}

// sharedRead returns whether a call returns entries whose objects are a store's own: a Get or List of pkg/state, or a
// method of pkg/cluster that reads like one, such as Nodes.Unmanaged, passed client.UnsafeDisableDeepCopy, and any other
// function or method of pkg/cluster.
func (h *held) sharedRead(call *ast.CallExpr) bool {
	fn, ok := typeutil.Callee(h.pass.TypesInfo, call).(*types.Func)
	if !ok || fn.Pkg() == nil {
		return false
	}
	switch fn.Pkg().Path() {
	case statePkg:
		if fn.Name() == "Get" || fn.Name() == "List" {
			return h.disablesDeepCopy(call)
		}
	case clusterPkg:
		if fn.Name() == "Unmanaged" {
			return h.disablesDeepCopy(call)
		}
		return true
	}
	return false
}

// disablesDeepCopy returns whether one of call's arguments is client.UnsafeDisableDeepCopy.
func (h *held) disablesDeepCopy(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		sel, ok := ast.Unparen(arg).(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "UnsafeDisableDeepCopy" {
			continue
		}
		if obj := h.pass.TypesInfo.ObjectOf(sel.Sel); obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == clientPkg {
			return true
		}
	}
	return false
}

// matchingFields returns the argument of a store Reader's or an Index's List call that is a client.MatchingFields or
// client.MatchingFieldsSelector, which a store's List panics on: matching a field is an Index's List.
func matchingFields(pass *analysis.Pass, call *ast.CallExpr) (ast.Expr, bool) {
	fn, ok := typeutil.Callee(pass.TypesInfo, call).(*types.Func)
	if !ok || fn.Name() != "List" || fn.Pkg() == nil || fn.Pkg().Path() != statePkg {
		return nil, false
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return nil, false
	}
	for _, arg := range call.Args {
		if named(pass.TypesInfo.TypeOf(arg), clientPkg, "MatchingFields") || named(pass.TypesInfo.TypeOf(arg), clientPkg, "MatchingFieldsSelector") {
			return arg, true
		}
	}
	return nil, false
}

// holdsEntry returns whether t is a state.Entry, or a pointer, slice, array or map of them.
func holdsEntry(t types.Type) bool {
	for {
		switch u := types.Unalias(t).(type) {
		case *types.Pointer:
			t = u.Elem()
		case *types.Slice:
			t = u.Elem()
		case *types.Array:
			t = u.Elem()
		case *types.Map:
			t = u.Elem()
		case *types.Named:
			return u.Obj().Pkg() != nil && u.Obj().Pkg().Path() == statePkg && u.Obj().Name() == "Entry"
		default:
			return false
		}
	}
}
