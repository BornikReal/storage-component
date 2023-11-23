package ss

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
)

const (
	kvDelimiter   = 31
	fileDelimiter = 30
)

var (
	NotInitSSTableError    = errors.New("ss table not init")
	InvalidOffsetError     = errors.New("offset too big")
	UnexpectedFileEndError = errors.New("unexpected file end error")
)

type KV struct {
	Key   string
	Value string
}

type SS struct {
	Id   int64
	Name string
	Size int64

	file       *os.File
	offset     int64
	path       string
	isNotEmpty bool
}

func NewSS(path string, name string) *SS {
	return &SS{
		Name: name,
		path: path,
	}
}

func (s *SS) getFullPath() string {
	return fmt.Sprintf("%s/%s", s.path, s.Name)
}

func (s *SS) Init() error {
	file, err := os.Open(s.getFullPath())
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(s.getFullPath())
	}
	if err != nil {
		return err
	}
	s.file = file
	id, err := strconv.ParseInt(s.Name, 10, 64)
	if err != nil {
		return err
	}
	s.Id = id

	size, err := s.UpdateSize()
	if err != nil {
		return err
	}
	s.Size = size

	return nil
}

func (s *SS) Close() error {
	return s.file.Close()
}

func (s *SS) WriteKV(kv KV) error {
	if s.file == nil {
		return NotInitSSTableError
	}

	res := make([]byte, 0, len(kv.Key)+len(kv.Value)+2)
	if s.isNotEmpty {
		res = append(res, fileDelimiter)
	} else {
		s.isNotEmpty = true
	}
	res = append(res, []byte(kv.Key)...)
	res = append(res, kvDelimiter)
	res = append(res, []byte(kv.Value)...)
	_, err := s.file.Write(res)
	if err != nil {
		return err
	}
	return nil
}

func (s *SS) Get(key string, offset, limit int64) (string, bool, error) {
	if s.file == nil {
		return "", false, NotInitSSTableError
	}

	if offset > s.Size {
		return "", false, InvalidOffsetError
	}

	if offset+limit > s.Size || limit == 0 {
		limit = s.Size - offset
	}

	r := make([]byte, limit)
	_, err := s.file.ReadAt(r, offset)
	if err != nil {
		panic(err)
	}
	pairs := bytes.Split(r, []byte{fileDelimiter})
	return findValue(key, pairs)
}

func (s *SS) FindFirstFromOffset(offset, batch int64) (string, int64, int64, error) {
	if s.file == nil {
		return "", 0, 0, NotInitSSTableError
	}

	if offset > s.Size {
		return "", 0, 0, InvalidOffsetError
	}

	r := make([]byte, batch)
	var key []byte
	var keyStart, keyFound bool
	var pos int64

	if offset == 0 {
		keyStart = true
	}

	for i := offset; i < s.Size; i += batch {
		n, err := s.file.ReadAt(r, i)
		if err != nil {
			return "", 0, 0, err
		}
		for j := 0; j < n; j++ {
			if r[j] == fileDelimiter {
				if keyFound {
					return string(key), pos, i + int64(j), nil
				}
				keyStart = true
				pos = i + int64(j) + 1
			} else if r[j] == kvDelimiter && keyStart {
				keyFound = true
			} else if keyStart && !keyFound {
				key = append(key, r[j])
			}
		}
	}

	if !keyStart && !keyFound {
		return "", 0, 0, UnexpectedFileEndError
	}

	return string(key), pos, s.Size, nil
}

func (s *SS) Read(batch int64) (KV, bool, error) {
	if s.offset == s.Size {
		return KV{}, false, nil
	}
	r := make([]byte, batch)
	var pair []byte
	var kv KV

	for i := s.offset; i < s.Size; i += batch {
		n, err := s.file.ReadAt(r, i)
		if err != nil {
			return KV{}, false, err
		}
		for j := 0; j < n; j++ {
			if r[j] == fileDelimiter {
				s.offset = i + int64(j) + 1
				kv, err = getPair(pair)
				return kv, true, err
			} else {
				pair = append(pair, r[j])
			}
		}
	}
	s.offset = s.Size
	kv, err := getPair(pair)
	return kv, true, err
}

func (s *SS) Delete() error {
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.getFullPath()); err != nil {
		return err
	}
	return nil
}

func (s *SS) Rename(newName string) error {
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.getFullPath(), fmt.Sprintf("%s/%s", s.path, newName)); err != nil {
		return err
	}
	s.Name = newName
	if err := s.Init(); err != nil {
		return err
	}
	return nil
}

func (s *SS) UpdateSize() (int64, error) {
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	s.Size = info.Size()
	return s.Size, nil
}

func getPair(pair []byte) (KV, error) {
	kv := bytes.Split(pair, []byte{kvDelimiter})
	if len(kv) != 2 {
		return KV{}, errors.New("invalid pair")
	}
	return KV{
		Key:   string(kv[0]),
		Value: string(kv[1]),
	}, nil
}

func findValue(search string, pairs [][]byte) (string, bool, error) {
	begin := 0
	end := len(pairs) - 1

	for begin <= end {
		mid := (begin + end) / 2

		kv, err := getPair(pairs[mid])
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

	kv, err := getPair(pairs[begin])
	if err != nil || kv.Key != search {
		return "", false, err
	}

	return kv.Value, true, nil
}
