package kv_file

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

const (
	KvDelimiter   = 31
	FileDelimiter = 30
)

var (
	NotInitSSTableError    = errors.New("file not init")
	InvalidOffsetError     = errors.New("offset too big")
	UnexpectedFileEndError = errors.New("unexpected File end error")
)

type KV struct {
	Key   string
	Value string
}

type KVFile struct {
	Name string
	Size int64
	Path string

	File       *os.File
	offset     int64
	isNotEmpty bool
}

func NewKVFile(path string, name string) *KVFile {
	return &KVFile{
		Name: name,
		Path: path,
	}
}

func (s *KVFile) GetFullPath() string {
	if s.Path == "" {
		return s.Name
	}
	return fmt.Sprintf("%s/%s", s.Path, s.Name)
}

func (s *KVFile) Init() error {
	file, err := os.OpenFile(s.GetFullPath(), os.O_RDWR|os.O_APPEND, os.ModeAppend)
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(s.GetFullPath())
	}
	if err != nil {
		return err
	}
	s.File = file

	size, err := s.UpdateSize()
	if err != nil {
		return err
	}
	s.Size = size
	s.isNotEmpty = size != 0

	return nil
}

func (s *KVFile) Close() error {
	return s.File.Close()
}

func (s *KVFile) WriteKV(kv KV) error {
	if s.File == nil {
		return NotInitSSTableError
	}

	res := make([]byte, 0, len(kv.Key)+len(kv.Value)+2)
	if s.isNotEmpty {
		res = append(res, FileDelimiter)
	} else {
		s.isNotEmpty = true
	}
	res = append(res, []byte(kv.Key)...)
	res = append(res, KvDelimiter)
	res = append(res, []byte(kv.Value)...)
	_, err := s.File.Write(res)
	if err != nil {
		return err
	}
	return nil
}

func (s *KVFile) UpdateSize() (int64, error) {
	info, err := s.File.Stat()
	if err != nil {
		return 0, err
	}
	s.Size = info.Size()
	return s.Size, nil
}

func (s *KVFile) Read(batch int64) (KV, bool, error) {
	if s.offset == s.Size {
		return KV{}, false, nil
	}
	r := make([]byte, batch)
	var pair []byte
	var kv KV

	for i := s.offset; i < s.Size; i += batch {
		n, err := s.File.ReadAt(r, i)
		if err != nil {
			return KV{}, false, err
		}
		for j := 0; j < n; j++ {
			if r[j] == FileDelimiter {
				s.offset = i + int64(j) + 1
				kv, err = GetPair(pair)
				return kv, true, err
			} else {
				pair = append(pair, r[j])
			}
		}
	}
	s.offset = s.Size
	kv, err := GetPair(pair)
	return kv, true, err
}

func (s *KVFile) Clear() error {
	if err := os.Truncate(s.Name, 0); err != nil {
		return err
	}
	if _, err := s.UpdateSize(); err != nil {
		return err
	}
	return nil
}

func GetPair(pair []byte) (KV, error) {
	kv := bytes.Split(pair, []byte{KvDelimiter})
	if len(kv) != 2 {
		return KV{}, errors.New("invalid pair")
	}
	return KV{
		Key:   string(kv[0]),
		Value: string(kv[1]),
	}, nil
}
