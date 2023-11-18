package tree_with_clone

import (
	"github.com/emirpasic/gods/containers"
	"github.com/emirpasic/gods/trees/avltree"
)

type Tree interface {
	Get(key interface{}) (value interface{}, found bool)
	Put(key interface{}, value interface{})
	Size() int
	Iterator() containers.ReverseIteratorWithKey
	Clone() Tree
}

type TreeWithClone struct {
	tree *avltree.Tree
}

func NewTreeWithClone(tree *avltree.Tree) *TreeWithClone {
	return &TreeWithClone{
		tree: tree,
	}
}

func (twc *TreeWithClone) Init() {
	twc.tree = avltree.NewWithStringComparator()
}

func (twc *TreeWithClone) Get(key interface{}) (value interface{}, found bool) {
	return twc.tree.Get(key)
}

func (twc *TreeWithClone) Put(key interface{}, value interface{}) {
	twc.tree.Put(key, value)
}

func (twc *TreeWithClone) Size() int {
	return twc.tree.Size()
}

func (twc *TreeWithClone) Iterator() containers.ReverseIteratorWithKey {
	return twc.tree.Iterator()
}

func (twc *TreeWithClone) Clone() Tree {
	oldAvl := twc.tree
	twc.Init()
	return NewTreeWithClone(oldAvl)
}
