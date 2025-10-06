package config

// Config holds notifier general config
type Config struct {
	MarshallerType string `toml:"marshaller_type"`
	HasherType     string `toml:"hasher_type"`

	ClientConfig SUIClientConfig `toml:"client_config"`
}

type SUIClientConfig struct {
	RPCUrl string `toml:"rpc_url"`
	WSUrl  string `toml:"ws_url"`
}
