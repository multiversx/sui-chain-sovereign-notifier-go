package factory

import "context"

// SUIClient defines what a SUI client should be able to do
type SUIClient interface {
	Start(ctx context.Context) error
	Close()
}
