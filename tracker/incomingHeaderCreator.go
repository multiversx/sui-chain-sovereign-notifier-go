package tracker

import (
	"encoding/json"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/multiversx/mx-chain-core-go/data/sovereign"
	"github.com/multiversx/mx-chain-core-go/data/sovereign/dto"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
)

type incomingHeaderCreator struct {
}

// NewIncomingHeadersCreator creates a new incoming header creator
func NewIncomingHeadersCreator() *incomingHeaderCreator {
	return &incomingHeaderCreator{}
}

// CreateIncomingHeader will create an incoming header for MVX chain, based on the provided SUI checkpoint with its incoming events
// For now, the proof represents the json bytes of a light SUI checkpoint.
func (ihc *incomingHeaderCreator) CreateIncomingHeader(checkPoint SUILightCheckpoint) (sovereign.IncomingHeaderHandler, error) {
	bytes, err := json.Marshal(checkPoint)
	if err != nil {
		return nil, err
	}

	return &sovereign.IncomingHeader{
		SourceChainID:  dto.SUI,
		Proof:          bytes,
		Nonce:          checkPoint.SequenceNumber,
		IncomingEvents: createIncomingEvents(checkPoint.Events),
	}, nil
}

func createIncomingEvents(events []models.SuiEventResponse) []*transaction.Event {
	incomingEvents := make([]*transaction.Event, len(events))

	for idx, event := range events {
		incomingEvents[idx] = &transaction.Event{
			Address: []byte(event.Sender),
			// TODO: Rest of the fields should be taken from event.ParsedJson like:
			// parsed := event.ParsedJson
			// token, _ := parsed["token"].(string)
		}
	}

	return incomingEvents
}

// IsInterfaceNil checks if the underlying pointer is nil
func (ihc *incomingHeaderCreator) IsInterfaceNil() bool {
	return ihc == nil
}
