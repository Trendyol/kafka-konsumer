package kafka

import "github.com/segmentio/kafka-go"

// offsetStash holds the latest committed offsets by topic => partition => offset.
type offsetStash map[string]map[int]int64

// UpdateWithNewestCommittedOffsets calculates latest committed, and it
// updates the latest offset value by looking topic => partition => offset
func (o offsetStash) UpdateWithNewestCommittedOffsets(messages []kafka.Message) {
	for i := range messages {
		offsetsByPartition, ok := o[messages[i].Topic]
		if !ok {
			offsetsByPartition = map[int]int64{}
			o[messages[i].Topic] = offsetsByPartition
		}

		// Because of incoming messages is already committed, we need to increase their offsets by 1
		messageOffset := messages[i].Offset + 1

		if offset, ok := offsetsByPartition[messages[i].Partition]; !ok || messageOffset > offset {
			offsetsByPartition[messages[i].Partition] = messageOffset
		}
	}
}

// IgnoreAlreadyCommittedMessages When committing messages in consumer groups, the message with the highest offset for
// a given topic/partition determines the value of the committed offset for that partition. For example, if messages at
// offset 1, 2, and 3 of a single partition and if we commit with message offset 3 it will also result in committing the
// messages at offsets 1 and 2 for that partition. It means we can safely ignore the messages which have offsets 1 and 2.
func (o offsetStash) IgnoreAlreadyCommittedMessages(messages []kafka.Message) []kafka.Message {
	willBeCommitted := make([]kafka.Message, 0, len(messages))
	for i := range messages {
		offsetsByPartition := o[messages[i].Topic]
		offset := offsetsByPartition[messages[i].Partition]

		// it means, we already committed this message, so we can safely ignore it
		if messages[i].Offset <= offset {
			continue
		}

		willBeCommitted = append(willBeCommitted, messages[i])
	}

	return willBeCommitted
}
