package index

import "github.com/BornikReal/storage-component/pkg/ss"

type Value struct {
	Key    string
	Offset int64
}

type Manager struct {
	table     *ss.SS
	idx       []Value
	blockSize int64
	batch     int64
}

func NewManager(table *ss.SS, blockSize int64, batch int64) *Manager {
	return &Manager{
		table:     table,
		blockSize: blockSize,
		batch:     batch,
	}
}

func (m *Manager) Init() error {
	var pos, offset int64
	var key string
	var err error

	for {
		key, pos, offset, err = m.table.FindFirstFromOffset(offset, m.batch)
		if err != nil {
			return err
		}
		m.idx = append(m.idx, Value{
			Key:    key,
			Offset: pos,
		})
		offset += m.blockSize
		if offset >= m.table.Size {
			break
		}
	}
	return nil
}

func (m *Manager) Get(key string) (string, bool, error) {
	i, ok := m.findFirst(key)
	if !ok {
		return "", false, nil
	}
	var limit int64
	if i != len(m.idx)-1 {
		limit = m.idx[i+1].Offset - m.idx[i].Offset
	}
	return m.table.Get(key, m.idx[i].Offset, limit)
}

func (m *Manager) findFirst(key string) (int, bool) {
	begin := 0
	end := len(m.idx) - 1

	for begin <= end {
		mid := (begin + end) / 2

		if m.idx[mid].Key < key {
			begin = mid + 1
		} else {
			end = mid - 1
		}
	}

	if begin < len(m.idx) && begin >= 0 && m.idx[begin].Key == key {
		return begin, true
	}

	if end >= len(m.idx) || end < 0 {
		return 0, false
	}

	return end, true
}
