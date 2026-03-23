package grpcapi

import (
	"bob-the-broker/internal/broker"
	"bob-the-broker/internal/brokerpb"
	"context"
)

type GrpcBroker struct {
	broker broker.Broker
	brokerpb.UnimplementedBrokerServiceServer
}

func NewGrpcBroker(b broker.Broker) brokerpb.BrokerServiceServer {
	return &GrpcBroker{
		broker: b,
	}
}

func (g *GrpcBroker) Produce(ctx context.Context, req *brokerpb.ProduceRequest) (*brokerpb.ProduceResponse, error) {
	return &brokerpb.ProduceResponse{}, g.broker.Produce(req.Topic, req.Key, req.Value)
}

func (g *GrpcBroker) Fetch(ctx context.Context, req *brokerpb.FetchRequest) (*brokerpb.FetchResponse, error) {
	msgs, err := g.broker.Fetch(
		req.Topic,
		int(req.Partition),
		req.Offset,
		int(req.Limit),
	)

	if err != nil {
		return nil, err
	}

	rsp := &brokerpb.FetchResponse{}
	for _, msg := range msgs {
		rsp.Messages = append(rsp.Messages, &brokerpb.Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Partition: int32(msg.Partition),
		})
	}
	return rsp, nil
}

func (g *GrpcBroker) Subscribe(req *brokerpb.SubscribeRequest, srv brokerpb.BrokerService_SubscribeServer) error {
	ch := g.broker.Subscribe(req.Topic)
	defer g.broker.Unsubscribe(req.Topic, ch)
	for {
		select {
		case <-srv.Context().Done():
			return srv.Context().Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			err := srv.Send(&brokerpb.Message{
				Topic:     msg.Topic,
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Partition: int32(msg.Partition),
			})
			if err != nil {
				return err
			}
		}
	}
}

func (s *GrpcBroker) HealthCheck(
	ctx context.Context,
	req *brokerpb.HealthCheckRequest,
) (*brokerpb.HealthCheckResponse, error) {
	return &brokerpb.HealthCheckResponse{Status: "OK"}, nil
}
