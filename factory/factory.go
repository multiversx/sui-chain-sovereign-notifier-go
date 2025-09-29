package factory

import (
	"github.com/block-vision/sui-go-sdk/constant"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/config"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/tracker"
)

// CreateSUIClientNotifier creates a sui client notifier
func CreateSUIClientNotifier(_ config.Config) (SUIClient, error) {
	return tracker.NewSUITrackerNotifier(tracker.ArgsSuiTrackerNotifier{
		WSClient:              sui.NewSuiWebsocketClient("wss://rpc-mainnet.suiscan.xyz/websocket"),
		RPCClient:             sui.NewSuiClient(constant.BvMainnetEndpoint),
		IncomingHeaderCreator: tracker.NewIncomingHeadersCreator(),
	})
}
