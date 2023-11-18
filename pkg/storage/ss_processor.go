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
	errorCh   chan error
}

func NewSSProcessor(ssManager SSSaver, errorCh chan error) *SSProcessor {
	return &SSProcessor{
		ssManager: ssManager,
		errorCh:   errorCh,
	}
}

func (sp *SSProcessor) Start(listener chan iterator.Iterator) {
	for it := range listener {
		if err := sp.ssManager.SaveTree(it); err != nil {
			sp.errorCh <- err
		}
	}
}
