package main

//func binarySearch(needle int, haystack []int) (int, bool) {
//
//	low := 0
//	high := len(haystack) - 1
//
//	for low <= high {
//		median := (low + high) / 2
//
//		if haystack[median] < needle {
//			low = median + 1
//		} else {
//			high = median - 1
//		}
//	}
//
//	if low < len(haystack) && low >= 0 && haystack[low] == needle {
//		return haystack[low], true
//	}
//
//	if high >= len(haystack) || high < 0 {
//		return 0, false
//	}
//
//	return haystack[high], true
//}

func main() {
	//items := []int{1, 2, 9, 20, 31, 45, 64, 70, 100}
	//fmt.Println(binarySearch(110, items))
	//pq := priorityqueue.NewWith(utils.IntComparator)
	//pq.Enqueue(1)
	//pq.Enqueue(5)
	//pq.Enqueue(4)
	//fmt.Println(pq.Dequeue())
	//fmt.Println(pq.Dequeue())
	//fmt.Println(pq.Dequeue())
	//fmt.Println(pq.Dequeue())
	//tree := avltree.NewWithStringComparator()
	//tree.Put("1", "a")
	//tree.Put("4", "d")
	//tree.Put("3", "c")
	//tree.Put("2", "b")
	//it := tree.Iterator()
	//next := it.First()
	//for next {
	//	fmt.Println(it.Key(), it.Value())
	//	next = it.Next()
	//}
	//file, err := os.Create("test")
	//if err != nil {
	//	panic(err)
	//}
	//res := append(append([]byte("test"), 31), []byte("v1")...)
	//n, err := file.Write(res)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(n)
	//n, err = file.Write([]byte{30})
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(n)
	//res = append(append([]byte("aboba"), 31), []byte("v2")...)
	//n, err = file.Write(res)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(n)
	//err = file.Close()
	//if err != nil {
	//	panic(err)
	//}
	//
	//file, err = os.Open("test")
	//if err != nil {
	//	panic(err)
	//}
	//stats, err := file.Stat()
	//if err != nil {
	//	panic(err)
	//}
	//
	//var size = stats.Size()
	//r := make([]byte, size)
	//n, err = file.ReadAt(r, 0)
	//if err != nil {
	//	panic(err)
	//}
	//sp := bytes.Split(r, []byte{30})
	//for _, b := range sp {
	//	kv := bytes.Split(b, []byte{31})
	//	if len(kv) != 2 {
	//		panic("invalid kv")
	//	}
	//	fmt.Println(string(kv[0]), string(kv[1]))
	//}
	//
	//r = make([]byte, size)
	//n, err = file.Read(r)
	//if err != nil {
	//	panic(err)
	//}
	//sp = bytes.Split(r, []byte{30})
	//for _, b := range sp {
	//	kv := bytes.Split(b, []byte{31})
	//	if len(kv) != 2 {
	//		panic("invalid kv")
	//	}
	//	fmt.Println(string(kv[0]), string(kv[1]))
	//}
	//var t sync.Mutex
	//fmt.Println(string(r))
	//tree.Iterator()
	//fmt.Println(tree.UpdateSize())
	//fmt.Println(tree.UpdateSize())
	//t, err := tree.MarshalJSON()
	//fmt.Println(string(t), err)
	//tree1 := avl.NewWithStringComparator()
	//err = tree1.UnmarshalJSON(t)
	//fmt.Println(tree1.Values(), err)

	//files, err := os.ReadDir("./")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//for _, f := range files {
	//	fmt.Println(f.Name(), f.IsDir())
	//}
	//f, err := os.Open("/tmp/dat")
	//fmt.Println(files)
}
