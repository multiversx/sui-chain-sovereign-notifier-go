package tracker

import (
	"context"

	"github.com/block-vision/sui-go-sdk/models"
	sovCore "github.com/multiversx/mx-chain-core-go/core/sovereign"
	"github.com/multiversx/mx-chain-core-go/data/sovereign"
)

// IncomingHeaderCreator defines an incoming header creator behavior
type IncomingHeaderCreator interface {
	CreateIncomingHeader(checkPoint SUILightCheckpoint) (sovereign.IncomingHeaderHandler, error)
	IsInterfaceNil() bool
}

// IncomingHeadersNotifierHandler defines an incoming header notifier behavior
type IncomingHeadersNotifierHandler interface {
	NotifyHeaderSubscribers(header sovereign.IncomingHeaderHandler) error
	RegisterSubscriber(handler sovCore.IncomingHeaderSubscriber) error
}

type SUIWSClient interface {
	SubscribeEvent(ctx context.Context, req models.SuiXSubscribeEventsRequest, msgCh chan models.SuiEventResponse) error
}

type SUIRPCClient interface {
	SuiGetLatestCheckpointSequenceNumber(ctx context.Context) (uint64, error)
	SuiGetCheckpoint(ctx context.Context, req models.SuiGetCheckpointRequest) (models.CheckpointResponse, error)
	SuiGetCheckpoints(ctx context.Context, req models.SuiGetCheckpointsRequest) (models.PaginatedCheckpointsResponse, error)
	SuiGetTransactionBlock(ctx context.Context, req models.SuiGetTransactionBlockRequest) (models.SuiTransactionBlockResponse, error)
}

type SUIClientHandler interface {
	SUIWSClient
	SUIRPCClient
}
