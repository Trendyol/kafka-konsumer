package kafka

import "fmt"

// processError identifies a failed processing or delivery attempt. It must not
// allow the source message's offset to be committed.
type processError struct {
	Stage       string
	SourceTopic string
	TargetTopic string
	Partition   int
	Offset      int64
	Cause       error
	Stack       []byte
}

func (e *processError) Error() string {
	return fmt.Sprintf("consumer %s failed: source=%s target=%s partition=%d offset=%d: %v",
		e.Stage, e.SourceTopic, e.TargetTopic, e.Partition, e.Offset, e.Cause)
}

func (e *processError) Unwrap() error { return e.Cause }

type processRequest struct {
	message *Message
	skip    bool
}

type processResult struct {
	err error
}
