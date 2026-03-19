package storage

type Message struct {
	Offset int64
	Key    string
	Value  string
}
