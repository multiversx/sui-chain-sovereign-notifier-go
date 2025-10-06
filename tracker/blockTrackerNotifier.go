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
	HeadersNotifier       IncomingHeadersNotifierHandler
}

type blockTrackerNotifier struct {
	wsClient              SUIWSClient
	rpcClient             SUIRPCClient
	incomingHeaderCreator IncomingHeaderCreator
	headersNotifier       IncomingHeadersNotifierHandler

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
		headersNotifier:       args.HeadersNotifier,
		sampleSize:            10,
	}, nil
}

func (btn *blockTrackerNotifier) Start(ctx context.Context) error {
	go func() {
		for {
			err := btn.fetchCheckpoints(ctx)
			log.Error("blockTrackerNotifier.fetchCheckpoints", "error", err)
			time.Sleep(time.Second)
		}
	}()

	receiveMsgCh := make(chan models.SuiEventResponse, 10)
	err := btn.wsClient.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
		SuiEventFilter: map[string]interface{}{
			"MoveEventType": "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809::order_info::OrderPlaced",
		},
	}, receiveMsgCh)
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-receiveMsgCh:
			err = btn.processEvent(msg)
			log.LogIfError(err)
		case <-ctx.Done():
			return nil
		}
	}
}

func (btn *blockTrackerNotifier) fetchCheckpoints(ctx context.Context) error {
	if btn.lastSentBatchCheckPoint == 0 {
		lastSentCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
		if err != nil {
			return err
		}

		btn.lastSentBatchCheckPoint = lastSentCheckPoint
	}

	log.Info("starting with latest checkpoint sequence", "number", btn.lastSentBatchCheckPoint)

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

		log.Debug("fetched checkpoints", "len", len(currCheckPoints.Data))

		checkPoints, errProcess := btn.processCheckPoints(currCheckPoints)
		if errProcess != nil {
			log.Error("blockTrackerNotifier.processCheckPoints", "error", errProcess)
		}

		err := btn.notifyIncomingHeaders(checkPoints)
		if err != nil {
			log.Error("blockTrackerNotifier.notifyIncomingHeaders", "error", err)
			return err
		}
	}

}

func (btn *blockTrackerNotifier) processCheckPoints(
	currCheckPoints models.PaginatedCheckpointsResponse,
) ([]SUILightCheckpoint, error) {
	latestSequenceNumber, errConvert := strconv.Atoi(currCheckPoints.Data[len(currCheckPoints.Data)-1].SequenceNumber)
	if errConvert != nil {
		log.Error("blockTrackerNotifier: error trying to get latestSequenceNumber", "error", errConvert)
		return nil, errConvert
	}

	passedCheckPoints := uint64(latestSequenceNumber) - btn.lastSentBatchCheckPoint
	numBatches := passedCheckPoints / btn.sampleSize
	if numBatches == 0 {
		log.Debug("processCheckPoints: num batches == 0",
			"latestSequenceNumber", latestSequenceNumber,
			"lastSentCheckPoint", btn.lastSentBatchCheckPoint,
			"passedCheckPoints", passedCheckPoints,
		)

		return nil, nil
	}

	endNextBatchSeqNumber := btn.lastSentBatchCheckPoint + numBatches*btn.sampleSize
	log.Debug("processCheckPoints",
		"start", btn.lastSentBatchCheckPoint,
		"end", endNextBatchSeqNumber,
		"numBatches", numBatches,
		"latestSequenceNumber", latestSequenceNumber,
	)

	checkPointsToSend := make([]SUILightCheckpoint, 0)
	for idx := range currCheckPoints.Data {
		currCheckPoint := currCheckPoints.Data[idx]
		cpSeqNumber, err := strconv.Atoi(currCheckPoint.SequenceNumber)
		if err != nil {
			return nil, err
		}

		if uint64(cpSeqNumber) <= btn.lastSentBatchCheckPoint {
			continue
		}

		if uint64(cpSeqNumber) > endNextBatchSeqNumber {
			break
		}

		incomingEvents := btn.getIncomingEvents(currCheckPoint)
		if uint64(cpSeqNumber) == btn.lastSentBatchCheckPoint+btn.sampleSize {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				Checkpoint: uint64(cpSeqNumber),
				Epoch:      currCheckPoint.Epoch,
				Events:     incomingEvents,
			})
			btn.lastSentBatchCheckPoint += btn.sampleSize
		} else if len(incomingEvents) > 0 {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				Checkpoint: uint64(cpSeqNumber),
				Epoch:      currCheckPoint.Epoch,
				Events:     incomingEvents,
			})
		}
	}

	return checkPointsToSend, nil
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
			"digest", digest,
			"checkpoint", checkPoint.SequenceNumber,
			"num events", len(events),
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

func (btn *blockTrackerNotifier) notifyIncomingHeaders(checkPoints []SUILightCheckpoint) error {
	for _, cp := range checkPoints {
		log.Info("cp", "checkpoint", cp.Checkpoint, "len events", len(cp.Events))
	}

	for _, checkPoint := range checkPoints {
		incomingHeader, err := btn.incomingHeaderCreator.CreateIncomingHeader(checkPoint)
		if err != nil {
			return err
		}

		err = btn.headersNotifier.NotifyHeaderSubscribers(incomingHeader)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
}
