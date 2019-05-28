package parser

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/parser/ast"
)

// WalkVisitor struct
type WalkVisitor struct{}

// Enter enter the node and visit
func (v WalkVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	fmt.Printf("-> %v\n", spew.Sdump(in))
	return in, false
}

// Leave leave the node
func (v WalkVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	fmt.Printf("<- %T\n", in)
	return in, true
}
