package config

// Config holds notifier general config
type Config struct {
	MarshallerType string `toml:"marshaller_type"`
	HasherType     string `toml:"hasher_type"`

	PoolingTime        uint8  `toml:"pooling_time"`
	BatchSize          uint64 `toml:"batch_size"`
	StartingCheckpoint uint64 `toml:"starting_checkpoint"`

	SubscribedEvents []SubscribedEvent `toml:"subscribed_events"`
	ClientConfig     SUIClientConfig   `toml:"client_config"`
}

type SubscribedEvent struct {
	EventType string `toml:"event_type"`
	Value     string `toml:"value"`
}

type SUIClientConfig struct {
	RPCUrl string `toml:"rpc_url"`
	WSUrl  string `toml:"ws_url"`
}
