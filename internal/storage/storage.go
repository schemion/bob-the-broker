package storage

type Storage interface {
	AppendMessage(msg Message) (int64, error)
	FetchMessages(offset int64, limit int) ([]Message, error)
}
