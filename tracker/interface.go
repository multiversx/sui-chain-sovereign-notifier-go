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

// SUIWSClient defines a ws SUI client
type SUIWSClient interface {
	SubscribeEvent(ctx context.Context, req models.SuiXSubscribeEventsRequest, msgCh chan models.SuiEventResponse) error
}

// SUIRPCClient defines an rpc SUI client
type SUIRPCClient interface {
	SuiGetLatestCheckpointSequenceNumber(ctx context.Context) (uint64, error)
	SuiGetCheckpoints(ctx context.Context, req models.SuiGetCheckpointsRequest) (models.PaginatedCheckpointsResponse, error)
}

// Storer storer defines a db storer
type Storer interface {
	Put(key, val []byte) error
	Get(key []byte) ([]byte, error)
	Close() error
	IsInterfaceNil() bool
}
