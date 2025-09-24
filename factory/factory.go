package factory

import (
	"context"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/utils"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/config"
)

// CreateSUIClientNotifier creates a sui client notifier
func CreateSUIClientNotifier(_ config.Config) (SUIClient, error) {

	var ctx = context.Background()
	// create a websocket client, connect to the mainnet websocket endpoint
	var cli = sui.NewSuiWebsocketClient("wss://rpc-mainnet.suiscan.xyz/websocket") // constant.WssSuiTestnetEndpoint)

	// receiveMsgCh is a channel to receive Sui event
	receiveMsgCh := make(chan models.SuiEventResponse, 10)

	// SubscribeEvent implements the method `suix_subscribeEvent`, subscribe to a stream of Sui event.
	err := cli.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
		SuiEventFilter: map[string]interface{}{
			"All": []string{},
		},
	}, receiveMsgCh)
	if err != nil {
		panic(err)
	}

	for {
		select {
		// receive Sui event
		case msg := <-receiveMsgCh:
			utils.PrettyPrint(msg)
		case <-ctx.Done():
			return nil, nil
		}
	}
	return nil, nil
}
