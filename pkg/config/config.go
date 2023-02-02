package config

import "time"

type Consumer struct {
	Concurrency   int
	Topics        []string
	ConsumerGroup string
	Exception     Exception
}

type Exception struct {
	Topic       string
	MaxRetry    int
	Cron        string
	Duration    time.Duration
	Concurrency int
}

type Kafka struct {
	Servers            string
	IsSecureCluster    bool
	ScramUsername      string
	ScramPassword      string
	RootCAPath         string
	IntermediateCAPath string
	Rack               string
}
