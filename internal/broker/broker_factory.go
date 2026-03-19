package broker

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}
