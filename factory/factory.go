package factory

import (
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/multiversx/mx-chain-core-go/core/sovereign"
	hashingFactory "github.com/multiversx/mx-chain-core-go/hashing/factory"
	"github.com/multiversx/mx-chain-core-go/marshal/factory"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/config"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/tracker"
)

// CreateSUIClientNotifier creates a sui client notifier
func CreateSUIClientNotifier(cfg config.Config) (SUIClient, error) {
	marshaller, err := factory.NewMarshalizer(cfg.MarshallerType)
	if err != nil {
		return nil, err
	}

	hasher, err := hashingFactory.NewHasher(cfg.HasherType)
	if err != nil {
		return nil, err
	}

	headersNotifier, err := sovereign.NewHeadersNotifier(marshaller, hasher)
	if err != nil {
		return nil, err
	}

	return tracker.NewSUITrackerNotifier(tracker.ArgsSuiTrackerNotifier{
		WSClient:              sui.NewSuiWebsocketClient(cfg.ClientConfig.WSUrl),
		RPCClient:             sui.NewSuiClient(cfg.ClientConfig.RPCUrl),
		IncomingHeaderCreator: tracker.NewIncomingHeadersCreator(),
		HeadersNotifier:       headersNotifier,
	})
}
