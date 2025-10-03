package tracker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/block-vision/sui-go-sdk/models"
	logger "github.com/multiversx/mx-chain-logger-go"
)

type SUILightCheckpoint struct {
	Checkpoint uint64                    `json:"checkpoint"`
	Epoch      string                    `json:"epoch"`
	Events     []models.SuiEventResponse `json:"events"`
}

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

	pendingEvents           map[string][]models.SuiEventResponse
	sampleSize              uint64
	lastSentBatchCheckPoint uint64
}

func NewSUITrackerNotifier(args ArgsSuiTrackerNotifier) (*blockTrackerNotifier, error) {
	return &blockTrackerNotifier{
		rpcClient:             args.RPCClient,
		wsClient:              args.WSClient,
		incomingHeaderCreator: args.IncomingHeaderCreator,
		pendingEvents:         make(map[string][]models.SuiEventResponse),
		sampleSize:            10,
	}, nil
}

func (btn *blockTrackerNotifier) Start(ctx context.Context) error {
	receiveMsgCh := make(chan models.SuiEventResponse, 10)

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

	go func() {
		for {
			err = btn.fetchCheckpoints(ctx)
			log.LogIfError(err)

			log.Error("AOLEO")
			time.Sleep(time.Second)
		}
	}()

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

func (btn *blockTrackerNotifier) fetchCheckpoints(ctx context.Context) error {
	lastSentCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
	if err != nil {
		return err
	}

	log.Info("Starting with latest checkpoint sequence", "number", lastSentCheckPoint)

	if btn.lastSentBatchCheckPoint == 0 {
		btn.lastSentBatchCheckPoint = lastSentCheckPoint
	}
	for {
		time.Sleep(5 * time.Second)

		currCheckPoints, errGet := btn.rpcClient.SuiGetCheckpoints(ctx,
			models.SuiGetCheckpointsRequest{
				Cursor:          fmt.Sprintf("%d", btn.lastSentBatchCheckPoint),
				Limit:           50,
				DescendingOrder: false,
			},
		)
		if errGet != nil {
			log.Error("blockTrackerNotifier.btn.rpcClient.SuiGetCheckpoints", "error", errGet)
			continue
		}

		if len(currCheckPoints.Data) == 0 {
			log.Debug("blockTrackerNotifier.currCheckPoints.Data", "len is zero", true)
			continue
		}

		log.Error("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", "num", len(currCheckPoints.Data))

		checkPoints, errProcess := btn.processCheckPoints(currCheckPoints, lastSentCheckPoint)
		if errProcess != nil {
			log.Error("blockTrackerNotifier.processCheckPoints", "error", errProcess)
		}

		if len(checkPoints) > 0 {
			lastSentCheckPoint = checkPoints[len(checkPoints)-1].Checkpoint
		}
	}

}

func (btn *blockTrackerNotifier) processCheckPoints(
	currCheckPoints models.PaginatedCheckpointsResponse,
	lastSentCheckPoint uint64,
) ([]SUILightCheckpoint, error) {
	latestSequenceNumber, errConvert := strconv.Atoi(currCheckPoints.Data[len(currCheckPoints.Data)-1].SequenceNumber)
	if errConvert != nil {
		log.Error("blockTrackerNotifier: error trying to get latestSequenceNumber", "error", errConvert)
		return nil, errConvert
	}
	passedCheckPoints := uint64(latestSequenceNumber) - lastSentCheckPoint
	numBatches := passedCheckPoints / btn.sampleSize
	if numBatches == 0 {

		log.Error("NUM BATCHES IS ZERO", "latestSequenceNumber", latestSequenceNumber, "lastSentCheckPoint", lastSentCheckPoint)

		return nil, nil
	}

	toStopNextBatch := lastSentCheckPoint + numBatches*btn.sampleSize

	checkPointsToSend := make([]SUILightCheckpoint, 0)
	allCheckPointsMap := make(map[int]SUILightCheckpoint)
	checkPointsWithIncomingEventsMap := make(map[int]struct{})

	log.Error("STARTING", "start", lastSentCheckPoint, "end", toStopNextBatch, "numBatches", numBatches, "lastSentCheckPoint", lastSentCheckPoint, "latestSequenceNumber", latestSequenceNumber)

	for idx := range currCheckPoints.Data {
		currCheckPoint := currCheckPoints.Data[idx]
		cpSeqNumber, _ := strconv.Atoi(currCheckPoint.SequenceNumber)
		if uint64(cpSeqNumber) <= lastSentCheckPoint {
			continue
		}

		if uint64(cpSeqNumber) > toStopNextBatch {
			break
		}

		allCheckPointsMap[cpSeqNumber] = SUILightCheckpoint{
			Checkpoint: uint64(cpSeqNumber),
			Epoch:      currCheckPoint.Epoch,
		}

		saved := false
		incomingEvents := btn.getIncomingEvents(currCheckPoint)
		if len(incomingEvents) > 0 {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				Checkpoint: uint64(cpSeqNumber),
				Epoch:      currCheckPoint.Epoch,
				Events:     incomingEvents,
			})

			checkPointsWithIncomingEventsMap[cpSeqNumber] = struct{}{}
		} else if uint64(cpSeqNumber) == btn.lastSentBatchCheckPoint+btn.sampleSize {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				Checkpoint: uint64(cpSeqNumber),
				Epoch:      currCheckPoint.Epoch,
				Events:     nil,
			})

			log.Error("HERRERERERERERRERER", "cpSeqNumber", cpSeqNumber)

			btn.lastSentBatchCheckPoint += btn.sampleSize
			saved = true
		}

		if !saved && uint64(cpSeqNumber) == btn.lastSentBatchCheckPoint+btn.sampleSize {
			btn.lastSentBatchCheckPoint += btn.sampleSize
		}

	}

	for _, cp := range checkPointsToSend {
		log.Info("cp", "checkpoint", cp.Checkpoint, "len events", len(cp.Events))
	}

	return checkPointsToSend, nil
}

func (btn *blockTrackerNotifier) getBatchedCheckpoints(
	passedCheckPoints uint64,
	allCheckPointsMap map[int]SUILightCheckpoint,
	checkPointsWithIncomingEventsMap map[int]struct{},
) ([]SUILightCheckpoint, error) {
	numBatches := passedCheckPoints / btn.sampleSize
	batchedCheckPointsToSend := make([]SUILightCheckpoint, 0)

	for i := uint64(0); i < numBatches; i++ {
		btn.lastSentBatchCheckPoint += btn.sampleSize

		checkPointData, found := allCheckPointsMap[int(btn.lastSentBatchCheckPoint)]
		if !found {
			log.Error("checkPointData not found", "last checkpoint", btn.lastSentBatchCheckPoint)
			//return nil, errors.New("checkPointData not found")
		}

		if _, alreadyExists := checkPointsWithIncomingEventsMap[int(btn.lastSentBatchCheckPoint)]; alreadyExists {
			continue
		}

		batchedCheckPointsToSend = append(batchedCheckPointsToSend, checkPointData)
	}
	log.Info("sending checkpoints", "numBatches", numBatches)
	return batchedCheckPointsToSend, nil
}

func (btn *blockTrackerNotifier) getIncomingEvents(checkPoint models.CheckpointResponse) []models.SuiEventResponse {
	incomingEvents := make([]models.SuiEventResponse, 0)
	for _, digest := range checkPoint.Transactions {

		btn.mutex.RLock()
		events, found := btn.pendingEvents[digest]
		btn.mutex.RUnlock()
		if !found {
			continue
		}

		btn.mutex.Lock()
		delete(btn.pendingEvents, digest)
		btn.mutex.Unlock()

		incomingEvents = append(incomingEvents, events...)
		log.Debug("blockTrackerNotifier: found incoming events",
			"digest", digest, "checkpoint",
			checkPoint.SequenceNumber, "num events", len(events),
		)
	}

	return incomingEvents
}

func (btn *blockTrackerNotifier) processEvent(event models.SuiEventResponse) error {
	parsed := event.ParsedJson
	price, _ := parsed["price"].(string)

	if len(price) > 5 { //&& price[0] == '4' { // && price[1] == '0' {
	} else {
		return nil
	}

	log.Debug("received incoming SUI event", "event seq", event.Id.EventSeq, "digest", event.Id.TxDigest)

	btn.mutex.Lock()
	if _, found := btn.pendingEvents[event.Id.TxDigest]; !found {
		btn.pendingEvents[event.Id.TxDigest] = []models.SuiEventResponse{event}
	} else {
		btn.pendingEvents[event.Id.TxDigest] = append(btn.pendingEvents[event.Id.TxDigest], event)
	}
	btn.mutex.Unlock()

	return nil
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
