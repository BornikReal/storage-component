package storage

import (
	"errors"
	"fmt"
)

var (
	NotInitError   = errors.New("storage not init")
	NotFoundError  = errors.New("key not found")
	NotStringError = func(v interface{}) error { return errors.New(fmt.Sprintf("expected string, but got %T", v)) }
)
