package ss_manager

import (
	"github.com/BornikReal/storage-component/pkg/ss"
	"os"
	"sort"
	"strconv"
)

type Iterator interface {
	Next() bool
	Value() interface{}
	Key() interface{}
	First() bool
}

type SSManager struct {
	path      string
	maxSSSize int64

	tables []*ss.SS
}

func NewSSManager(path string, maxSSSize int64) *SSManager {
	return &SSManager{
		path:      path,
		maxSSSize: maxSSSize,
	}
}

func (s *SSManager) Init() error {
	files, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	for _, f := range files {
		name := f.Name()
		s.tables = append(s.tables, ss.NewSS(name))
	}
	sort.SliceStable(s.tables, func(i, j int) bool {
		return s.tables[i].Id < s.tables[j].Id
	})
	return nil
}

func (s *SSManager) createNewSS() *ss.SS {
	var lastID int64
	if len(s.tables) != 0 {
		lastID = s.tables[len(s.tables)-1].Id + 1
	}
	newSS := ss.NewSS(strconv.FormatInt(lastID, 10))
	s.tables = append(s.tables, newSS)
	return newSS
}

func (s *SSManager) SaveTree(it *Iterator) error {
	//table := s.createNewSS()
	return nil
}
