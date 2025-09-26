package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/block-vision/sui-go-sdk/constant"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
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

	//depositEventType := "0x2::coin::TransferEvent"

	// SubscribeEvent implements the method `suix_subscribeEvent`, subscribe to a stream of Sui event.
	err := btn.client.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
		SuiEventFilter: map[string]interface{}{
			"MoveEventType": "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809::order_info::OrderPlaced",
			//"MoveEventType": depositEventType,
			//"Sender": "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd",
			//"All": []string{},
		},
	}, receiveMsgCh)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			err := dsa2(ctx)
			log.LogIfError(err)

			if err != nil {
				return
			}

			log.Error("AOLEO")
			time.Sleep(time.Second)
		}
	}()

	for {
		select {
		// receive Sui event
		case msg := <-receiveMsgCh:
			err = btn.processEvent2(msg)
			log.LogIfError(err)
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func (btn *blockTrackerNotifier) dsa(digest string) {
	var cli = sui.NewSuiClient(constant.BvMainnetEndpoint)
	rsp, err := cli.SuiGetTransactionBlock(context.Background(), models.SuiGetTransactionBlockRequest{
		Digest: digest,
	})

	if err != nil {
		log.LogIfError(err)
		return
	}

	log.Info("got event from checpoint", "sequence", rsp.Checkpoint)

}

func dsa2(ctx context.Context) error {
	var cli = sui.NewSuiClient(constant.BvMainnetEndpoint)
	latestCheckPoint, err := cli.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	log.Info("Starting with latest checkpoint sequence", "number", latestCheckPoint)

	sampleSize := uint64(50)

	for {
		time.Sleep(10 * time.Second)

		currCheckPoint, err := cli.SuiGetLatestCheckpointSequenceNumber(ctx)
		log.LogIfError(err, "value", true)
		if err != nil {
			continue
		}

		passedCheckPoints := currCheckPoint - latestCheckPoint
		if passedCheckPoints < sampleSize {
			log.Error("not enough checkpoints passed: ", "number", currCheckPoint)
			continue
		}

		numBatches := passedCheckPoints / sampleSize
		if numBatches < 2 {
			latestCheckPoint += sampleSize
			// send here checkpoint
			log.Info("sending checkpoint numBatches < 2", "latestCheckPoint", latestCheckPoint, "numBatches", numBatches)
			continue
		}

		checkPointsToSend := make([]uint64, numBatches)
		for i := uint64(0); i < numBatches; i++ {
			checkPointsToSend[i] = latestCheckPoint
			latestCheckPoint += sampleSize
		}

		log.Info("sending checkpoint numBatches > 2", "checkPointsToSend", checkPointsToSend, "numBatches", numBatches)
		// send here checkpoints

	}

}

func (btn *blockTrackerNotifier) processEvent2(event models.SuiEventResponse) error {
	parsed := event.ParsedJson

	price, _ := parsed["price"].(string)

	if len(price) > 5 && price[0] == '4' { // && price[1] == '0' {
		utils.PrettyPrint(event)
	} else {
		return nil
		//	log.Error("ACTUALLY", "from", from, "to", to, "amount", parsed["amount"])
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	btn.dsa(event.Id.TxDigest)

	return nil
}

func (btn *blockTrackerNotifier) processEvent(event models.SuiEventResponse) error {
	utils.PrettyPrint(event)

	accountAddr := "0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd"

	parsed := event.ParsedJson

	from, _ := parsed["sender"].(string)
	to, _ := parsed["recipient"].(string)

	if from == accountAddr || to == accountAddr {
		msg := fmt.Sprintf("Token movement relevant: TxDigest=%s, From=%s, To=%s, Amount=%v\n",
			event.Id.TxDigest, from, to, parsed["amount"])

		log.Error(msg)
		utils.PrettyPrint(event)
	} else {
		//	log.Error("ACTUALLY", "from", from, "to", to, "amount", parsed["amount"])
	}

	return nil

	if log.GetLevel() == logger.LogTrace {
		utils.PrettyPrint(event)
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	btn.dsa(event.Id.TxDigest)

	return nil
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
