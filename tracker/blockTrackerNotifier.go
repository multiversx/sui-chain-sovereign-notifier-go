package tracker

import (
	"context"
	"strconv"
	"time"

	"github.com/block-vision/sui-go-sdk/models"
	logger "github.com/multiversx/mx-chain-logger-go"
)

var log = logger.GetOrCreate("sui-tracker")

type ArgsSuiTrackerNotifier struct {
	WSClient              SUIWSClient
	RPCClient             SUIRPCClient
	IncomingHeaderCreator IncomingHeaderCreator
}

type blockTrackerNotifier struct {
	wsClient              SUIWSClient
	rpcClient             SUIRPCClient
	incomingHeaderCreator IncomingHeaderCreator
}

func NewSUITrackerNotifier(args ArgsSuiTrackerNotifier) (*blockTrackerNotifier, error) {
	return &blockTrackerNotifier{
		rpcClient:             args.RPCClient,
		wsClient:              args.WSClient,
		incomingHeaderCreator: args.IncomingHeaderCreator,
	}, nil
}

func (btn *blockTrackerNotifier) Start(ctx context.Context) error {
	receiveMsgCh := make(chan models.SuiEventResponse, 10)

	incomingChan := make(chan *checkPoint, 10)

	err := btn.wsClient.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
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

	latestCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	go heapManager(ctx, latestCheckPoint, incomingChan)

	go func() {
		for {
			err = btn.trackCheckpoints(ctx, incomingChan)
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
			err = btn.processEvent2(ctx, incomingChan, msg)
			log.LogIfError(err)
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func (btn *blockTrackerNotifier) trackCheckpoints(ctx context.Context, incomingChan chan<- *checkPoint) error {
	latestCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	log.Info("Starting with latest checkpoint sequence", "number", latestCheckPoint)

	sampleSize := uint64(50)

	for {
		time.Sleep(10 * time.Second)

		currCheckPoint, errGet := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
		if errGet != nil {
			log.Error("DSDSADSA", "error", errGet, "value", true)
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

			// change this

			incomingChan <- &checkPoint{
				checkpoint: latestCheckPoint,
				events:     nil,
				ready:      true,
			}

			continue
		}

		checkPointsToSend := make([]uint64, numBatches)
		for i := uint64(0); i < numBatches; i++ {
			incomingChan <- &checkPoint{
				checkpoint: latestCheckPoint,
				events:     nil,
				ready:      true,
			}

			checkPointsToSend[i] = latestCheckPoint
			latestCheckPoint += sampleSize
		}

		log.Info("sending checkpoint numBatches > 2", "checkPointsToSend", checkPointsToSend, "numBatches", numBatches)
		// send here checkpoints

	}

}

func (btn *blockTrackerNotifier) processEvent2(ctx context.Context, incomingChan chan<- *checkPoint, event models.SuiEventResponse) error {
	parsed := event.ParsedJson

	price, _ := parsed["price"].(string)

	if len(price) > 5 { //&& price[0] == '4' { // && price[1] == '0' {
		//utils.PrettyPrint(event)
	} else {
		return nil
		//	log.Error("ACTUALLY", "from", from, "to", to, "amount", parsed["amount"])
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	cp := &checkPoint{
		digest: event.Id.TxDigest,
		events: []models.SuiEventResponse{event},
		ready:  false,
	}

	incomingChan <- cp

	btn.getCheckPoint(ctx, cp, event.Id.TxDigest)

	return nil
}

func (btn *blockTrackerNotifier) getCheckPoint(ctx context.Context, cp *checkPoint, digest string) {

	var rsp models.SuiTransactionBlockResponse
	var err error
	for i := 0; i < 10; i++ {
		rsp, err = btn.rpcClient.SuiGetTransactionBlock(ctx, models.SuiGetTransactionBlockRequest{
			Digest: digest,
		})

		if err == nil && rsp.Checkpoint != "" {
			checkPointNumber, err := strconv.Atoi(rsp.Checkpoint)
			if err != nil {
				// return error here
				return
			}

			log.Info("marked as ready", "checkPointNumber", checkPointNumber)
			cp.MarkReady(uint64(checkPointNumber))
			break
		}

		time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
		log.Error("retrying getCheckPoint", "error", err)
	}

	if err != nil { // or here we passed all retrials, return error
		log.LogIfError(err)
		return
	}

	//log.Info("got event from checpoint", "sequence", rsp.Checkpoint)

}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
