package factory

import (
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/config"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/tracker"
)

// CreateSUIClientNotifier creates a sui client notifier
func CreateSUIClientNotifier(_ config.Config) (SUIClient, error) {

	// create a websocket client, connect to the mainnet websocket endpoint
	var cli = sui.NewSuiWebsocketClient("wss://rpc-mainnet.suiscan.xyz/websocket") // constant.WssSuiTestnetEndpoint)

	return tracker.NewSUITrackerNotifier(tracker.ArgsSuiTrackerNotifier{
		Client:                cli,
		IncomingHeaderCreator: tracker.NewIncomingHeadersCreator(),
	})
}
