package tracker

import "github.com/block-vision/sui-go-sdk/models"

// SUILightCheckpoint holds minimum required data from a SUI checkpoint to create an incoming sovereign header
type SUILightCheckpoint struct {
	Checkpoint uint64                    `json:"checkpoint"`
	Epoch      string                    `json:"epoch"`
	Events     []models.SuiEventResponse `json:"events"`
}
