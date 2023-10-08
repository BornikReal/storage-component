package storage

import "errors"

var NotInitError = errors.New("storage not init")
var NotFoundError = errors.New("key not found")
