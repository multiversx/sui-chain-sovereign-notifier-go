package tracker

import (
	"context"
	"strconv"
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
	//receiveMsgCh := make(chan models.SuiEventResponse, 10)

	// SubscribeEvent implements the method `suix_subscribeEvent`, subscribe to a stream of Sui event.
	//err := btn.client.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
	//	SuiEventFilter: map[string]interface{}{
	//		"All": []string{},
	//	},
	//}, receiveMsgCh)
	//if err != nil {
	//	panic(err)
	//}

	dsa2("dsa")

	//for {
	//	select {
	//	// receive Sui event
	//	case msg := <-receiveMsgCh:
	//		err = btn.processEvent(msg)
	//		log.LogIfError(err)
	//	case <-ctx.Done():
	//		return nil
	//	}
	//}

	return nil
}

func dsa(digest string) {
	var cli = sui.NewSuiClient(constant.BvMainnetEndpoint)

	ctx := context.Background()
	latestCheckPoint := 0
	for {
		time.Sleep(1 * time.Second)

		checkPoints, err := cli.SuiGetCheckpoints(ctx, models.SuiGetCheckpointsRequest{
			Limit:           50,
			DescendingOrder: true,
		})
		log.LogIfError(err)
		_ = err

		for _, cp := range checkPoints.Data {
			currCheckPoint, _ := strconv.Atoi(cp.SequenceNumber)

			if currCheckPoint > latestCheckPoint {
				// keep them
				log.Info("checkpoint: ", "number", cp.SequenceNumber)
			} else {
				// discarded
				log.Error("checkpoint: ", "number", cp.SequenceNumber)
				break
			}

		}

		latestCheckPoint, err = strconv.Atoi(checkPoints.Data[0].SequenceNumber)

		log.Info("################################")
	}
	/*

		cli.get
		rsp, err := cli.SuiGetTransactionBlock(ctx, models.SuiGetTransactionBlockRequest{
			Digest: digest,
		})

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		cli.Check SuiGetEvents()

		log.Info("HERE")
		utils.PrettyPrint(rsp)

	*/
}

func dsa2(digest string) error {
	var cli = sui.NewSuiClient(constant.BvMainnetEndpoint)

	ctx := context.Background()
	latestCheckPoint, err := cli.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	log.Info("Starting with latest checkpoint sequence", "number", latestCheckPoint)

	sampleSize := uint64(5)

	for {
		time.Sleep(5 * time.Second)

		currCheckPoint, err := cli.SuiGetLatestCheckpointSequenceNumber(ctx)
		if err != nil {
			return err
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

func (btn *blockTrackerNotifier) processEvent(event models.SuiEventResponse) error {
	if log.GetLevel() == logger.LogTrace {
		utils.PrettyPrint(event)
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	dsa(event.Id.TxDigest)

	return nil
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
