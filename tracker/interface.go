package tracker

import (
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/multiversx/mx-chain-core-go/data/sovereign"
)

// IncomingHeaderCreator defines an incoming header creator behavior
type IncomingHeaderCreator interface {
	CreateIncomingHeader(event models.SuiEventResponse) (sovereign.IncomingHeaderHandler, error)
	IsInterfaceNil() bool
}

/*
type clientWrapper struct {

}

func dsa() {
	var cli = sui.NewSuiClient(constant.BvTestnetEndpoint)

	ctx := context.Background()
	rsp, err := cli.SuiGetCheckpoints(ctx, models.SuiGetCheckpointsRequest{
		Limit:           10,
		DescendingOrder: true,
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	utils.PrettyPrint(rsp)
	cli.SuiGetCheckpoints()
	// fetch Checkpoint 1628214 and print details.
	rsp2, err := cli.SuiGetTransactionBlock(ctx, models.SuiGetCheckpointRequest{
		CheckpointID: "1628214",
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	utils.PrettyPrint(rsp2)

}
}

*/
