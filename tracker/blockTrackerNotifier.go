package tracker

import (
	"context"
	"errors"
	"sort"
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

	pendingEvents map[string][]models.SuiEventResponse
	sampleSize    uint64
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

	for {
		time.Sleep(5 * time.Second)

		currCheckPoints, errGet := btn.rpcClient.SuiGetCheckpoints(ctx,
			models.SuiGetCheckpointsRequest{
				Limit:           50,
				DescendingOrder: true,
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

		err = btn.processCheckPoints(currCheckPoints, lastSentCheckPoint)
		if err != nil {
			log.Error("blockTrackerNotifier.processCheckPoints", "error", err)
		}
	}

}

func (btn *blockTrackerNotifier) processCheckPoints(currCheckPoints models.PaginatedCheckpointsResponse, lastSentCheckPoint uint64) error {
	latestSequenceNumber, errConvert := strconv.Atoi(currCheckPoints.Data[0].SequenceNumber)
	if errConvert != nil {
		log.Error("blockTrackerNotifier: error trying to get latestSequenceNumber", "error", errConvert)
		return errConvert
	}

	checkPointsToSend := make([]SUILightCheckpoint, 0)
	allCheckPointsMap := make(map[int]SUILightCheckpoint)
	checkPointsWithIncomingEventsMap := make(map[int]struct{})
	for idx := len(currCheckPoints.Data) - 1; idx >= 0; idx-- {
		currCheckPoint := currCheckPoints.Data[idx]
		cpSeqNumber, _ := strconv.Atoi(currCheckPoint.SequenceNumber)
		if uint64(cpSeqNumber) <= lastSentCheckPoint {
			continue
		}

		allCheckPointsMap[cpSeqNumber] = SUILightCheckpoint{
			Checkpoint: uint64(cpSeqNumber),
			Epoch:      currCheckPoint.Epoch,
		}

		incomingEvents := btn.getIncomingEvents(currCheckPoint)
		if len(incomingEvents) > 0 {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				Checkpoint: uint64(cpSeqNumber),
				Epoch:      currCheckPoint.Epoch,
				Events:     incomingEvents,
			})

			checkPointsWithIncomingEventsMap[cpSeqNumber] = struct{}{}
		}
	}

	passedCheckPoints := uint64(latestSequenceNumber) - lastSentCheckPoint
	if passedCheckPoints < btn.sampleSize {
		if len(checkPointsToSend) > 0 {
			lastSentCheckPoint = checkPointsToSend[len(checkPointsToSend)-1].Checkpoint
		}

		// SEND HERE
		log.Error("ERLY EXIT")

		return nil
	}

	numBatches := passedCheckPoints / btn.sampleSize
	batchedCheckPointsToSend := make([]SUILightCheckpoint, 0)
	for i := uint64(0); i < numBatches; i++ {
		lastSentCheckPoint += btn.sampleSize

		checkPointData, found := allCheckPointsMap[int(lastSentCheckPoint)]
		if !found {
			return errors.New("checkPointData not found")
		}

		if _, alreadyExists := checkPointsWithIncomingEventsMap[int(lastSentCheckPoint)]; alreadyExists {
			continue
		}

		batchedCheckPointsToSend = append(batchedCheckPointsToSend, checkPointData)
	}

	checkPointsToSend = append(checkPointsToSend, batchedCheckPointsToSend...)

	sort.SliceStable(checkPointsToSend, func(i, j int) bool {
		return checkPointsToSend[i].Checkpoint < checkPointsToSend[j].Checkpoint
	})

	log.Info("sending checkpoint numBatches > 2", "numBatches", numBatches)

	for _, cp := range checkPointsToSend {
		log.Info("cp", "checkpoint", cp.Checkpoint, "len events", len(cp.Events))
	}

	return nil
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
