package tracker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/core/closing"
	"github.com/multiversx/mx-chain-core-go/core/sovereign"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/sui-chain-sovereign-notifier-go/config"
)

type SUILightCheckpoint struct {
	Checkpoint uint64                    `json:"checkpoint"`
	Epoch      string                    `json:"epoch"`
	Events     []models.SuiEventResponse `json:"events"`
}

var log = logger.GetOrCreate("sui-tracker")

type ArgsSuiTrackerNotifier struct {
	PoolingTime        uint8 `toml:"pooling_time"`
	BatchSize          uint64
	StartingCheckpoint uint64

	SubscribedEvents []config.SubscribedEvent

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

	closer core.SafeCloser
	mutex  sync.RWMutex

	pendingEvents           map[string][]models.SuiEventResponse
	subscribedEvents        map[string]interface{}
	sampleSize              uint64
	lastSentBatchCheckPoint uint64
	startingCheckpoint      uint64
	poolingTime             uint8
}

func NewSUITrackerNotifier(args ArgsSuiTrackerNotifier) (*blockTrackerNotifier, error) {
	err := checkArgs(args)
	if err != nil {
		return nil, err
	}

	return &blockTrackerNotifier{
		rpcClient:             args.RPCClient,
		closer:                closing.NewSafeChanCloser(),
		wsClient:              args.WSClient,
		incomingHeaderCreator: args.IncomingHeaderCreator,
		pendingEvents:         make(map[string][]models.SuiEventResponse),
		headersNotifier:       args.HeadersNotifier,
		sampleSize:            args.BatchSize,
		startingCheckpoint:    args.StartingCheckpoint,
		poolingTime:           args.PoolingTime,
		subscribedEvents:      createSubScribedEvents(args.SubscribedEvents),
	}, nil
}

func checkArgs(args ArgsSuiTrackerNotifier) error {
	if args.PoolingTime == 0 {
		return fmt.Errorf("%w for pooling time", errZeroValue)
	}
	if args.BatchSize == 0 {
		return fmt.Errorf("%w for batch size", errZeroValue)
	}
	if args.WSClient == nil {
		return errNilWSClient
	}
	if args.RPCClient == nil {
		return errNilRPCClient
	}
	if args.IncomingHeaderCreator == nil {
		return errNilIncomingHeadersCreator
	}
	if args.HeadersNotifier == nil {
		return errNilHeadersNotifier
	}

	return nil
}

func createSubScribedEvents(subscribedEvents []config.SubscribedEvent) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, event := range subscribedEvents {
		ret[event.EventType] = event.Value
	}
	return ret
}

func (btn *blockTrackerNotifier) Start(ctx context.Context) error {
	btn.closer = closing.NewSafeChanCloser()

	btn.waitUntilStartingCheckPoint(ctx)

	go func() {
		for {
			err := btn.trackCheckPoints(ctx)
			log.Error("blockTrackerNotifier.fetchCheckpoints", "error", err)
			time.Sleep(time.Second)
		}
	}()

	return btn.trackEvents(ctx)
}

func (btn *blockTrackerNotifier) waitUntilStartingCheckPoint(ctx context.Context) {
	timer := time.NewTicker(time.Duration(btn.poolingTime) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-btn.closer.ChanClose():
			log.Debug("blockTrackerNotifier.trackCheckPoints: closing channel")
			return
		case <-ctx.Done():
			log.Debug("blockTrackerNotifier.trackCheckPoints: context done")
			return
		case <-timer.C:
			latestCheckPoint, err := btn.rpcClient.SuiGetLatestCheckpointSequenceNumber(ctx)
			if err != nil {
				log.Error("blockTrackerNotifier.waitUntilStartCheckPoint", "error", err)
				continue
			}

			if btn.startingCheckpoint == 0 {
				btn.lastSentBatchCheckPoint = latestCheckPoint - btn.sampleSize
				log.Info("SUI waitUntilStartCheckPoint finished, starting notifying with latest checkpoint",
					"starting checkpoint", btn.lastSentBatchCheckPoint,
					"latestCheckPoint", latestCheckPoint,
				)
				return
			}

			if latestCheckPoint >= btn.startingCheckpoint {
				btn.lastSentBatchCheckPoint = btn.startingCheckpoint - btn.sampleSize
				log.Info("SUI waitUntilStartCheckPoint finished",
					"starting checkpoint", btn.lastSentBatchCheckPoint,
					"latestCheckPoint", latestCheckPoint,
				)
				return
			}

			log.Debug("waiting, latest SUI checkpoint is less than starting checkpoint",
				"latest checkpoint", latestCheckPoint,
				"starting checkpoint", btn.startingCheckpoint,
			)
		}
	}

}

func (btn *blockTrackerNotifier) trackCheckPoints(ctx context.Context) error {
	log.Info("starting with latest checkpoint sequence", "number", btn.lastSentBatchCheckPoint)

	timer := time.NewTicker(time.Duration(btn.poolingTime) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-btn.closer.ChanClose():
			log.Debug("blockTrackerNotifier.trackCheckPoints: closing channel")
			return nil
		case <-ctx.Done():
			log.Debug("blockTrackerNotifier.trackCheckPoints: context done")
			return nil
		case <-timer.C:
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

			checkPoints, errProcess := btn.processCheckPoints(currCheckPoints)
			if errProcess != nil {
				log.Error("blockTrackerNotifier.processCheckPoints", "error", errProcess)
				return errProcess
			}

			err := btn.notifyIncomingHeaders(checkPoints)
			if err != nil {
				log.Error("blockTrackerNotifier.notifyIncomingHeaders", "error", err)
				return err
			}
		}
	}

}

func (btn *blockTrackerNotifier) processCheckPoints(
	currCheckPoints models.PaginatedCheckpointsResponse,
) ([]SUILightCheckpoint, error) {
	numCheckPoints := len(currCheckPoints.Data)
	log.Debug("fetched checkpoints", "len", numCheckPoints)

	if numCheckPoints == 0 {
		log.Debug("blockTrackerNotifier.currCheckPoints.Data", "len is zero", true)
		return nil, nil
	}

	latestSequenceNumber, errConvert := strconv.Atoi(currCheckPoints.Data[numCheckPoints-1].SequenceNumber)
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
	txDigest := event.Id.TxDigest
	log.Debug("received incoming SUI event", "event seq", event.Id.EventSeq, "digest", txDigest)

	btn.mutex.Lock()
	if _, found := btn.pendingEvents[txDigest]; !found {
		btn.pendingEvents[txDigest] = []models.SuiEventResponse{event}
	} else {
		btn.pendingEvents[txDigest] = append(btn.pendingEvents[txDigest], event)
	}
	btn.mutex.Unlock()

	return nil
}

func (btn *blockTrackerNotifier) notifyIncomingHeaders(checkPoints []SUILightCheckpoint) error {
	for _, checkPoint := range checkPoints {
		log.Info("sui tracker notifier: notifying incoming headers",
			"checkpoint", checkPoint.Checkpoint,
			"num events", len(checkPoint.Events),
		)

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

func (btn *blockTrackerNotifier) trackEvents(ctx context.Context) error {
	receiveMsgCh := make(chan models.SuiEventResponse, 10)
	err := btn.wsClient.SubscribeEvent(ctx, models.SuiXSubscribeEventsRequest{
		SuiEventFilter: btn.subscribedEvents,
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
			log.Debug("blockTrackerNotifier.trackEvents: context done")
			return nil
		case <-btn.closer.ChanClose():
			log.Debug("blockTrackerNotifier.trackCheckPoints: closing channel")
			return nil
		}
	}
}

// RegisterHandler will register an incoming header subscriber
func (btn *blockTrackerNotifier) RegisterHandler(handler sovereign.IncomingHeaderSubscriber) error {
	return btn.headersNotifier.RegisterSubscriber(handler)
}

// Close will close the underlying client and closer chan
func (btn *blockTrackerNotifier) Close() {
	defer btn.closer.Close() // should always be last
}
