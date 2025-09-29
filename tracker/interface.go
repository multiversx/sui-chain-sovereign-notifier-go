package tracker

import (
	"context"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/multiversx/mx-chain-core-go/data/sovereign"
)

// IncomingHeaderCreator defines an incoming header creator behavior
type IncomingHeaderCreator interface {
	CreateIncomingHeader(event models.SuiEventResponse) (sovereign.IncomingHeaderHandler, error)
	IsInterfaceNil() bool
}

type SUIWSClient interface {
	SubscribeEvent(ctx context.Context, req models.SuiXSubscribeEventsRequest, msgCh chan models.SuiEventResponse) error
}

type SUIRPCClient interface {
	SuiGetLatestCheckpointSequenceNumber(ctx context.Context) (uint64, error)
	SuiGetCheckpoint(ctx context.Context, req models.SuiGetCheckpointRequest) (models.CheckpointResponse, error)
	SuiGetTransactionBlock(ctx context.Context, req models.SuiGetTransactionBlockRequest) (models.SuiTransactionBlockResponse, error)
}

type SUIClientHandler interface {
	SUIWSClient
	SUIRPCClient
}
