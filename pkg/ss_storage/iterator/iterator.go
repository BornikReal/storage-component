package iterator

type Iterator interface {
	Next() bool
	Value() interface{}
	Key() interface{}
	First() bool
}
