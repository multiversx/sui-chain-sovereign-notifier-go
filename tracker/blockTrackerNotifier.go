package tracker

import (
	"context"
	"strconv"
	"sync"
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

	mutex sync.RWMutex

	pendingEvents map[string][]models.SuiEventResponse
}

func NewSUITrackerNotifier(args ArgsSuiTrackerNotifier) (*blockTrackerNotifier, error) {
	return &blockTrackerNotifier{
		rpcClient:             args.RPCClient,
		wsClient:              args.WSClient,
		incomingHeaderCreator: args.IncomingHeaderCreator,
		pendingEvents:         make(map[string][]models.SuiEventResponse),
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

	//latestCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
	//if err != nil {
	//	return err
	//}

	//	go heapManager(ctx, latestCheckPoint, incomingChan)

	go func() {
		for {
			err = btn.trackCheckpointsFull(ctx, incomingChan)
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

func (btn *blockTrackerNotifier) trackCheckpointsFull(ctx context.Context, incomingChan chan<- *checkPoint) error {
	latestCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	log.Info("Starting with latest checkpoint sequence", "number", latestCheckPoint)

	sampleSize := uint64(50)

	for {
		time.Sleep(1 * time.Second)

		currCheckPoints, errGet := btn.rpcClient.SuiGetCheckpoints(ctx,
			models.SuiGetCheckpointsRequest{
				//	Cursor:          strconv.Itoa(int(latestCheckPoint)),
				Limit:           50,
				DescendingOrder: true,
			},
		)
		if errGet != nil {
			log.Error("DSDSADSA", "error", errGet, "value", true)
			continue
		}

		if len(currCheckPoints.Data) == 0 {
			log.Error("currCheckPoints.Data", "len is zero", true)
			continue
		}

		latestSequenceNumber1, err := strconv.Atoi(currCheckPoints.Data[0].SequenceNumber)
		if err != nil {
			log.Error("latestSequenceNumber", "error", err)
			continue
		}

		checkPointsToSend := make([]uint64, 0)
		for idx := len(currCheckPoints.Data) - 1; idx >= 0; idx-- {
			cp := currCheckPoints.Data[idx]
			cpSeqNumber, _ := strconv.Atoi(cp.SequenceNumber)
			if uint64(cpSeqNumber) <= latestCheckPoint {
				continue
			}

			for _, digest := range cp.Transactions {

				btn.mutex.RLock()
				events, found := btn.pendingEvents[digest]
				btn.mutex.RUnlock()
				if !found {
					continue
				}

				_ = events
				log.Info("FOUND CHECKPOINTS TO SEND", "digest", digest, "checkpoint", cp.SequenceNumber)

				checkPointsToSend = append(checkPointsToSend, uint64(cpSeqNumber))
			}
		}

		if len(checkPointsToSend) > 0 {
			latestCheckPoint = checkPointsToSend[len(checkPointsToSend)-1]
		}

		latestSequenceNumber := uint64(latestSequenceNumber1)
		passedCheckPoints := latestSequenceNumber - latestCheckPoint
		if passedCheckPoints < sampleSize {
			log.Error("not enough checkpoints passed: ", "number", latestSequenceNumber)
			continue
		}

		numBatches := passedCheckPoints / sampleSize
		if numBatches < 2 {
			latestCheckPoint += sampleSize
			// send here checkpoint
			log.Info("sending checkpoint numBatches < 2", "latestCheckPoint", latestCheckPoint, "numBatches", numBatches)

			// change this
			/*
				incomingChan <- &checkPoint{
					checkpoint: latestCheckPoint,
					events:     nil,
					ready:      true,
				}

			*/

			continue
		}

		for i := uint64(0); i < numBatches; i++ {
			/*
				incomingChan <- &checkPoint{
					checkpoint: latestCheckPoint,
					events:     nil,
					ready:      true,
				}
			*/
			checkPointsToSend = append(checkPointsToSend, latestCheckPoint)
			latestCheckPoint += sampleSize
		}

		log.Info("sending checkpoint numBatches > 2", "checkPointsToSend", checkPointsToSend, "numBatches", numBatches)
		// send here checkpoints

	}

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

	if len(price) > 5 && price[0] == '4' { // && price[1] == '0' {
		//utils.PrettyPrint(event)
	} else {
		return nil
		//	log.Error("ACTUALLY", "from", from, "to", to, "amount", parsed["amount"])
	}

	log.Debug("received new SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	btn.mutex.Lock()
	if _, found := btn.pendingEvents[event.Id.TxDigest]; !found {
		btn.pendingEvents[event.Id.TxDigest] = []models.SuiEventResponse{event}
	} else {
		btn.pendingEvents[event.Id.TxDigest] = append(btn.pendingEvents[event.Id.EventSeq], event)
	}
	btn.mutex.Unlock()

	return nil

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
