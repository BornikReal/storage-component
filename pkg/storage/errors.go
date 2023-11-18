package storage

import (
	"errors"
)

var (
	NotInitError  = errors.New("storage not init")
	NotFoundError = errors.New("key not found")
)
