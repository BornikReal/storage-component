package main

import (
	"fmt"
	"github.com/BornikReal/storage-component/pkg/ss_manager"
	"github.com/BornikReal/storage-component/pkg/storage"
	"github.com/emirpasic/gods/trees/avltree"
)

func main() {
	ssManager := ss_manager.NewSSManager("test", 10, 1)
	if err := ssManager.Init(); err != nil {
		panic(err)
	}
	tree := avltree.NewWithStringComparator()
	mt := storage.NewMemTable(tree, ssManager, 5)

	fmt.Println(mt.Set("a", "1"))
	fmt.Println(mt.Get("a"))
	fmt.Println(mt.Set("b", "2"))
	fmt.Println(mt.Get("b"))
	fmt.Println(mt.Set("c", "3"))
	fmt.Println(mt.Get("c"))
	fmt.Println(mt.Set("d", "4"))
	fmt.Println(mt.Get("d"))
	fmt.Println(mt.Set("e", "5"))
	fmt.Println(mt.Get("e"))

	fmt.Println(mt.Set("f", "6"))
	fmt.Println(mt.Get("f"))
	fmt.Println(mt.Set("g", "7"))
	fmt.Println(mt.Get("g"))
	fmt.Println(mt.Set("h", "8"))
	fmt.Println(mt.Get("h"))
	fmt.Println(mt.Set("j", "9"))
	fmt.Println(mt.Get("j"))
	fmt.Println(mt.Set("a", "10"))
	fmt.Println(mt.Get("a"))

	if err := ssManager.CompressSS(); err != nil {
		panic(err)
	}

	fmt.Println(mt.Get("a"))
	fmt.Println(mt.Get("b"))
	fmt.Println(mt.Get("c"))
	fmt.Println(mt.Get("d"))
	fmt.Println(mt.Get("e"))
}
