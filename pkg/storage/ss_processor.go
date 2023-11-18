package storage

import (
	"github.com/BornikReal/storage-component/pkg/ss_storage/iterator"
)

type (
	SSSaver interface {
		SaveTree(it iterator.Iterator) error
		Get(key string) (string, bool, error)
	}
)

type SSProcessor struct {
	ssManager SSSaver
}

func NewSSProcessor(ssManager SSSaver) *SSProcessor {
	return &SSProcessor{
		ssManager: ssManager,
	}
}

func (sp *SSProcessor) Listen(listener chan iterator.Iterator) {
	for it := range listener {
		if err := sp.ssManager.SaveTree(it); err != nil {
			panic(err)
		}
	}
}
