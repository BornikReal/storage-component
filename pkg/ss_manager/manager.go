package ss_manager

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/BornikReal/storage-component/pkg/index"
	"github.com/BornikReal/storage-component/pkg/iterator"
	"github.com/BornikReal/storage-component/pkg/ss"
)

type SSWithIndex struct {
	table *ss.SS
	idx   *index.Manager
}

type SSManager struct {
	path      string
	blockSize int64
	batch     int64

	idx []SSWithIndex
}

func NewSSManager(path string, blockSize int64, batch int64) *SSManager {
	return &SSManager{
		path:      path,
		blockSize: blockSize,
		batch:     batch,
	}
}

func (s *SSManager) Init() error {
	files, err := os.ReadDir(s.path)
	if errors.Is(err, os.ErrNotExist) {
		err = os.Mkdir(s.path, os.ModePerm)
		return err
	}
	if err != nil {
		return err
	}

	for _, f := range files {
		name := f.Name()
		ssi := SSWithIndex{
			table: ss.NewSS(s.path, name),
		}

		if err = ssi.table.Init(); err != nil {
			return err
		}
		ssi.idx = index.NewManager(ssi.table, s.blockSize, s.batch)
		if err = ssi.idx.Init(); err != nil {
			return err
		}

		s.idx = append(s.idx, ssi)
	}
	sort.SliceStable(s.idx, func(i, j int) bool {
		return s.idx[i].table.Id < s.idx[j].table.Id
	})
	return nil
}

func (s *SSManager) createNewSS() (SSWithIndex, error) {
	var lastID int64
	if len(s.idx) != 0 {
		lastID = s.idx[len(s.idx)-1].table.Id + 1
	}
	newSS := ss.NewSS(s.path, strconv.FormatInt(lastID, 10))
	if err := newSS.Init(); err != nil {
		return SSWithIndex{}, err
	}
	idx := SSWithIndex{
		table: newSS,
		idx:   index.NewManager(newSS, s.blockSize, s.batch),
	}
	s.idx = append(s.idx, idx)
	return idx, nil
}

func (s *SSManager) SaveTree(it iterator.Iterator) error {
	idx, err := s.createNewSS()
	if err != nil {
		return err
	}

	next := it.First()
	for next {
		key, ok := it.Key().(string)
		if !ok {
			return err
		}
		value, ok := it.Value().(string)
		if !ok {
			return err
		}

		if err = idx.table.WriteKV(ss.KV{
			Key:   key,
			Value: value,
		}); err != nil {
			return err
		}
		next = it.Next()
	}

	if _, err = idx.table.UpdateSize(); err != nil {
		return err
	}

	if err = idx.idx.Init(); err != nil {
		return err
	}
	return nil
}

func (s *SSManager) Get(key string) (string, bool, error) {
	for i := len(s.idx) - 1; i >= 0; i-- {
		value, exist, err := s.idx[i].idx.Get(key)
		if err != nil {
			return "", false, err
		}
		if !exist {
			continue
		}
		return value, true, nil
	}
	return "", false, nil
}

func (s *SSManager) CompressSS() error {
	if len(s.idx) != 2 {
		return nil
	}

	t1 := s.idx[0]
	t2 := s.idx[1]
	newSS := ss.NewSS(s.path, fmt.Sprintf("-%s", t1.table.Name))
	if err := newSS.Init(); err != nil {
		return err
	}

	ok1 := true
	ok2 := true
	read1 := true
	read2 := true

	var kv1, kv2 ss.KV
	var err error

	for {
		if read1 {
			kv1, ok1, err = t1.table.Read(s.batch)
			if err != nil {
				return err
			}
		}

		if read2 {
			kv2, ok2, err = t2.table.Read(s.batch)
			if err != nil {
				return err
			}
		}

		if !(ok1 && ok2) {
			break
		}

		if kv1.Key < kv2.Key {
			read2 = false
			if err = newSS.WriteKV(kv1); err != nil {
				return err
			}
		} else if kv1.Key >= kv2.Key {
			if kv1.Key != kv2.Key {
				read1 = false
			}

			if err = newSS.WriteKV(kv2); err != nil {
				return err
			}
		}
	}

	for ok1 {
		kv1, ok1, err = t1.table.Read(1)
		if err != nil {
			return err
		}

		if err = newSS.WriteKV(kv1); err != nil {
			return err
		}
	}

	for ok2 {
		kv2, ok2, err = t2.table.Read(1)
		if err != nil {
			return err
		}

		if err = newSS.WriteKV(kv2); err != nil {
			return err
		}
	}
	if err = t1.table.Delete(); err != nil {
		return err
	}
	if err = t2.table.Delete(); err != nil {
		return err
	}
	if err = newSS.Rename(t1.table.Name); err != nil {
		return err
	}
	idx := index.NewManager(newSS, s.blockSize, s.batch)
	if err = idx.Init(); err != nil {
		return err
	}
	s.idx[0] = SSWithIndex{
		table: newSS,
		idx:   idx,
	}
	s.idx = append(s.idx[:1], s.idx[2:]...)
	return nil
}
