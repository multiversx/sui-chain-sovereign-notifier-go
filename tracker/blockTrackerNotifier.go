package tracker

import (
	"context"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/utils"
	logger "github.com/multiversx/mx-chain-logger-go"
)

var log = logger.GetOrCreate("sui-tracker")

type SUIClient interface {
	SubscribeEvent(ctx context.Context, req models.SuiXSubscribeEventsRequest, msgCh chan models.SuiEventResponse) error
}

type ArgsSuiTrackerNotifier struct {
	Client                SUIClient
	IncomingHeaderCreator IncomingHeaderCreator
}

type blockTrackerNotifier struct {
	client                SUIClient
	incomingHeaderCreator IncomingHeaderCreator
}

func NewSUITrackerNotifier(args ArgsSuiTrackerNotifier) (*blockTrackerNotifier, error) {
	return &blockTrackerNotifier{
		client:                args.Client,
		incomingHeaderCreator: args.IncomingHeaderCreator,
	}, nil
}

func (btn *blockTrackerNotifier) Start(ctx context.Context) error {
	// receiveMsgCh is a channel to receive Sui event
	receiveMsgCh := make(chan models.SuiEventResponse, 10)

	// SubscribeEvent implements the method `suix_subscribeEvent`, subscribe to a stream of Sui event.
	err := btn.client.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
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
			err = btn.processEvent(msg)
			log.LogIfError(err)
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (btn *blockTrackerNotifier) processEvent(event models.SuiEventResponse) error {
	if log.GetLevel() == logger.LogTrace {
		utils.PrettyPrint(event)
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)
	return nil
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
