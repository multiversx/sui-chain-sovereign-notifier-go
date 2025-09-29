package tracker

import (
	"context"
	"errors"

	"github.com/block-vision/sui-go-sdk/models"
)

type clientWrapper struct {
	wsClient  SUIWSClient
	rpcClient SUIRPCClient
}

func NewClientWrapper(wsClient SUIWSClient, rpcClient SUIRPCClient) (*clientWrapper, error) {
	if wsClient == nil {
		return nil, errors.New("wsClient is nil")
	}
	if rpcClient == nil {
		return nil, errors.New("rpcClient is nil")
	}

	return &clientWrapper{wsClient: wsClient, rpcClient: rpcClient}, nil
}

func (cw *clientWrapper) SubscribeEvent(ctx context.Context, req models.SuiXSubscribeEventsRequest, msgCh chan models.SuiEventResponse) error {
	return cw.wsClient.SubscribeEvent(ctx, req, msgCh)
}
