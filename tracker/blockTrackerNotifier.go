package tracker

import (
	"context"
	"encoding/binary"
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

const (
	nonceKey      = "nonce"
	checkPointKey = "checkpoint"
)

var log = logger.GetOrCreate("sui-tracker")

// ArgsSuiTrackerNotifier is a struct placeholder for args needed to create a sui checkpoint tracker
type ArgsSuiTrackerNotifier struct {
	PoolingTime        uint8
	BatchSize          uint64
	StartingCheckpoint uint64

	SubscribedEvents []config.SubscribedEvent

	WSClient              SUIWSClient
	RPCClient             SUIRPCClient
	IncomingHeaderCreator IncomingHeaderCreator
	HeadersNotifier       IncomingHeadersNotifierHandler

	NonceStorer Storer
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
	batchSize               uint64
	lastSentBatchCheckPoint uint64
	startingCheckpoint      uint64
	poolingTime             uint8

	incomingNonce uint64
	nonceStorer   Storer
}

// NewSUITrackerNotifier creates a new sui checkpoint tracker that will notify incoming headers to sovereign chain
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
		batchSize:             args.BatchSize,
		startingCheckpoint:    args.StartingCheckpoint,
		poolingTime:           args.PoolingTime,
		subscribedEvents:      createSubScribedEvents(args.SubscribedEvents),
		incomingNonce:         getUintValueFromStorage(args.NonceStorer, nonceKey),
		nonceStorer:           args.NonceStorer,
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
	if args.NonceStorer == nil {
		return errNilStorer
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

func getUintValueFromStorage(db Storer, key string) uint64 {
	storedValue, _ := db.Get([]byte(key))
	var val uint64
	if storedValue != nil {
		val = binary.BigEndian.Uint64(storedValue)
	} else {
		val = 1
	}

	log.Debug("sui notifier: getUintValueFromStorage", "key", key, "value", val)
	return val
}

// Start launches the main tracking loop responsible for monitoring Sui checkpoints
// and forwarding them, in order, to the sovereign chain. It will wait until the
// start checkpoint provided is reached(if set to 0, it will start from the latest checkpoint).
//
// The process runs continuously and performs the following tasks:
//
//  1. Periodically fetches recent checkpoints from the Sui RPC endpoint based on
//     the configured pooling time interval.
//
//  2. Collects and organizes checkpoint data(via websocket subscription) together with any Sui events that
//     occurred within those checkpoints, ensuring strictly ordered transmission.
//
//  3. Sends checkpoints immediately when they contain relevant on-chain events,
//     ensuring real-time updates to the sovereign chain.
//
//  4. For empty or inactive checkpoints, batches them according to the configured
//     batch size, ensuring the sovereign chain remains synchronized even during
//     periods of low activity. For example, if `batch_size = 100`, it will
//     send checkpoints 1001, 1101, 1201, and so on.
//
//  5. If new events appear between these batch intervals, intermediate checkpoints
//     (e.g. 1050, 1075) are also sent immediately, ensuring no event is delayed
//     or skipped between two batch boundaries.
//
// This hybrid design provides a balance between responsiveness (event-based triggers)
// and efficiency (batch transmission for quiet periods).
//
// The tracker is resilient to transient RPC errors — any temporary failure will be
// retried on the next polling cycle.
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
				btn.lastSentBatchCheckPoint = latestCheckPoint - btn.batchSize
				log.Info("SUI waitUntilStartCheckPoint finished, starting notifying with latest checkpoint",
					"starting checkpoint", btn.lastSentBatchCheckPoint,
					"latestCheckPoint", latestCheckPoint,
				)
				return
			}

			if latestCheckPoint >= btn.startingCheckpoint {
				btn.lastSentBatchCheckPoint = btn.startingCheckpoint - btn.batchSize
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
	numBatches := passedCheckPoints / btn.batchSize
	if numBatches == 0 {
		log.Debug("processCheckPoints: num batches == 0",
			"latestSequenceNumber", latestSequenceNumber,
			"lastSentCheckPoint", btn.lastSentBatchCheckPoint,
			"passedCheckPoints", passedCheckPoints,
		)

		return nil, nil
	}

	endNextBatchSeqNumber := btn.lastSentBatchCheckPoint + numBatches*btn.batchSize
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
		if uint64(cpSeqNumber) == btn.lastSentBatchCheckPoint+btn.batchSize {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				SequenceNumber: uint64(cpSeqNumber),
				Epoch:          currCheckPoint.Epoch,
				Events:         incomingEvents,
			})
			btn.lastSentBatchCheckPoint += btn.batchSize
		} else if len(incomingEvents) > 0 {
			checkPointsToSend = append(checkPointsToSend, SUILightCheckpoint{
				SequenceNumber: uint64(cpSeqNumber),
				Epoch:          currCheckPoint.Epoch,
				Events:         incomingEvents,
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

func (btn *blockTrackerNotifier) notifyIncomingHeaders(checkPoints []SUILightCheckpoint) error {
	for _, checkPoint := range checkPoints {
		log.Info("sui tracker notifier: notifying incoming headers",
			"checkpoint", checkPoint.SequenceNumber,
			"num events", len(checkPoint.Events),
			"incoming nonce", btn.incomingNonce,
		)

		checkPoint.IncomingNonce = btn.incomingNonce
		incomingHeader, err := btn.incomingHeaderCreator.CreateIncomingHeader(checkPoint)
		if err != nil {
			return err
		}

		err = btn.headersNotifier.NotifyHeaderSubscribers(incomingHeader)
		if err != nil {
			return err
		}

		btn.incomingNonce++

		err = btn.saveNonce()
		if err != nil {
			return err
		}
	}

	return nil
}

func (btn *blockTrackerNotifier) saveNonce() error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, btn.incomingNonce)
	return btn.nonceStorer.Put([]byte(nonceKey), buf)
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

// RegisterHandler will register an incoming header subscriber
func (btn *blockTrackerNotifier) RegisterHandler(handler sovereign.IncomingHeaderSubscriber) error {
	return btn.headersNotifier.RegisterSubscriber(handler)
}

// Close will close the underlying mechanisms for rpc/ws tasks
func (btn *blockTrackerNotifier) Close() error {
	defer btn.closer.Close() // should always be last
	return btn.nonceStorer.Close()
}

// IsInterfaceNil checks if the underlying pointer is nil
func (btn *blockTrackerNotifier) IsInterfaceNil() bool {
	return btn == nil
}
