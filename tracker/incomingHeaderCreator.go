package tracker

import (
	"encoding/json"
	"strconv"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/multiversx/mx-chain-core-go/data/sovereign"
)

type incomingHeaderCreator struct {
}

// NewIncomingHeadersCreator creates a new incoming header creator
func NewIncomingHeadersCreator() *incomingHeaderCreator {
	return &incomingHeaderCreator{}
}

// CreateIncomingHeader will create an incoming header for MVX chain, based on the provided ETH header with its incoming logs
// For now, the proof represents the json bytes of the ETH header.
func (ihc *incomingHeaderCreator) CreateIncomingHeader(event models.SuiEventResponse) (sovereign.IncomingHeaderHandler, error) {
	bytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	nonce, err := strconv.Atoi(event.Id.EventSeq)
	if err != nil {
		return nil, err
	}

	return &sovereign.IncomingHeader{
		Proof: bytes,
		Nonce: uint64(nonce),
		//IncomingEvents: createIncomingEvents(logs),
	}, nil
}

/*
func createIncomingEvents(logs []types.Log) []*transaction.Event {
	incomingEvents := make([]*transaction.Event, len(logs))

	for idx, ethLog := range logs {
		incomingEvents[idx] = &transaction.Event{
			Address:    ethLog.Address.Bytes(),
			Identifier: nil, // todo
			Topics:     getTopics(ethLog.Topics),
			Data:       ethLog.Data,
		}
	}

	return incomingEvents
}

*/

// IsInterfaceNil checks if the underlying pointer is nil
func (ihc *incomingHeaderCreator) IsInterfaceNil() bool {
	return ihc == nil
}
