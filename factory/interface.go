package factory

import (
	"context"

	"github.com/multiversx/mx-chain-core-go/core/sovereign"
)

// SUIClient defines what a SUI client should be able to do
type SUIClient interface {
	Start(ctx context.Context) error
	RegisterHandler(handler sovereign.IncomingHeaderSubscriber) error
	Close() error
	IsInterfaceNil() bool
}
