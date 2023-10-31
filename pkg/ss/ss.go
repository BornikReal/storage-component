package ss

import (
	"bytes"
	"errors"
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

type SS struct {
	Id   int64
	Name string
	Size int64

	file *os.File
}

func NewSS(name string) *SS {
	return &SS{
		Name: name,
	}
}

func (s *SS) Init() error {
	file, err := os.Open(s.Name)
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(s.Name)
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

func (s *SS) WriteKV(key string, value string) error {
	if s.file == nil {
		return NotInitSSTableError
	}

	res := make([]byte, 0, len(key)+len(value)+2)
	if s.Size != 0 {
		res = append(res, fileDelimiter)
	}
	res = append(res, []byte(key)...)
	res = append(res, kvDelimiter)
	res = append(res, []byte(value)...)
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
				pos = i + int64(j)
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

func (s *SS) UpdateSize() (int64, error) {
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	s.Size = info.Size()
	return s.Size, nil
}

func getPair(pair []byte) (string, string, error) {
	kv := bytes.Split(pair, []byte{kvDelimiter})
	if len(kv) != 2 {
		return "", "", errors.New("invalid pair")
	}
	return string(kv[0]), string(kv[1]), nil
}

func findValue(search string, pairs [][]byte) (string, bool, error) {
	begin := 0
	end := len(pairs) - 1

	for begin <= end {
		mid := (begin + end) / 2

		key, _, err := getPair(pairs[mid])
		if err != nil {
			return "", false, err
		}

		if key < search {
			begin = mid + 1
		} else {
			end = mid - 1
		}
	}

	if begin == len(pairs) {
		return "", false, nil
	}

	key, value, err := getPair(pairs[begin])
	if err != nil || key != search {
		return "", false, err
	}

	return value, true, nil
}
