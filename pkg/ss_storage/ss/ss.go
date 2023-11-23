package ss

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

	"github.com/BornikReal/storage-component/pkg/ss_storage/kv_file"
)

type SS struct {
	kv_file.KVFile
	Id int64
}

func NewSS(path string, name string) *SS {
	return &SS{
		KVFile: kv_file.KVFile{
			Name: name,
			Path: path,
		},
	}
}

func (s *SS) Init() error {
	if err := s.KVFile.Init(); err != nil {
		return err
	}
	id, err := strconv.ParseInt(s.Name, 10, 64)
	if err != nil {
		return err
	}
	s.Id = id

	return nil
}

func (s *SS) Get(key string, offset, limit int64) (string, bool, error) {
	if s.File == nil {
		return "", false, kv_file.NotInitSSTableError
	}

	if offset > s.Size {
		return "", false, kv_file.InvalidOffsetError
	}

	if offset+limit > s.Size || limit == 0 {
		limit = s.Size - offset
	}

	r := make([]byte, limit)
	_, err := s.File.ReadAt(r, offset)
	if err != nil {
		panic(err)
	}
	pairs := bytes.Split(r, []byte{kv_file.FileDelimiter})
	return findValue(key, pairs)
}

func (s *SS) FindFirstFromOffset(offset, batch int64) (string, int64, int64, error) {
	if s.File == nil {
		return "", 0, 0, kv_file.NotInitSSTableError
	}

	if offset > s.Size {
		return "", 0, 0, kv_file.InvalidOffsetError
	}

	r := make([]byte, batch)
	var key []byte
	var keyStart, keyFound bool
	var pos int64

	if offset == 0 {
		keyStart = true
	}

	for i := offset; i < s.Size; i += batch {
		n, err := s.File.ReadAt(r, i)
		if err != nil {
			return "", 0, 0, err
		}
		for j := 0; j < n; j++ {
			if r[j] == kv_file.FileDelimiter {
				if keyFound {
					return string(key), pos, i + int64(j), nil
				}
				keyStart = true
				pos = i + int64(j) + 1
			} else if r[j] == kv_file.KvDelimiter && keyStart {
				keyFound = true
			} else if keyStart && !keyFound {
				key = append(key, r[j])
			}
		}
	}

	if !keyStart && !keyFound {
		return "", 0, 0, kv_file.UnexpectedFileEndError
	}

	return string(key), pos, s.Size, nil
}

func (s *SS) Delete() error {
	if err := s.File.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.GetFullPath()); err != nil {
		return err
	}
	return nil
}

func (s *SS) Rename(newName string) error {
	if err := s.File.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.GetFullPath(), fmt.Sprintf("%s/%s", s.Path, newName)); err != nil {
		return err
	}
	s.Name = newName
	if err := s.Init(); err != nil {
		return err
	}
	return nil
}

func findValue(search string, pairs [][]byte) (string, bool, error) {
	begin := 0
	end := len(pairs) - 1

	for begin <= end {
		mid := (begin + end) / 2

		kv, err := kv_file.GetPair(pairs[mid])
		if err != nil {
			return "", false, err
		}

		if kv.Key < search {
			begin = mid + 1
		} else {
			end = mid - 1
		}
	}

	if begin == len(pairs) {
		return "", false, nil
	}

	kv, err := kv_file.GetPair(pairs[begin])
	if err != nil || kv.Key != search {
		return "", false, err
	}

	return kv.Value, true, nil
}
