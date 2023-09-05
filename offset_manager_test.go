package kafka

import (
	"reflect"
	"testing"

	segmentio "github.com/segmentio/kafka-go"
)

func Test_offsetStash_SetWithNewestCommittedOffsets(t *testing.T) {
	// Given
	offsets := offsetStash{}
	messages := []segmentio.Message{
		{Topic: "a", Partition: 0, Offset: 10},
		{Topic: "a", Partition: 0, Offset: 5},

		{Topic: "a", Partition: 1, Offset: 11},
		{Topic: "a", Partition: 1, Offset: 20},

		{Topic: "a", Partition: 2, Offset: 12},

		{Topic: "b", Partition: 0, Offset: 15},
		{Topic: "b", Partition: 0, Offset: 20},

		{Topic: "c", Partition: 2, Offset: 1},
		{Topic: "c", Partition: 2, Offset: 2},
		{Topic: "c", Partition: 2, Offset: 3},
	}

	// When
	offsets.UpdateWithNewestCommittedOffsets(messages)

	// Then
	assertFunc := func(actual map[int]int64, expected map[int]int64) {
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal(actual, "is not equal to", expected)
		}
	}

	assertFunc(offsets["a"], map[int]int64{0: 11, 1: 21, 2: 13})
	assertFunc(offsets["b"], map[int]int64{0: 21})
	assertFunc(offsets["c"], map[int]int64{2: 4})
}

func Test_offsetStash_IgnoreAlreadyCommittedMessages(t *testing.T) {
	// Given
	offsets := offsetStash{
		"a": map[int]int64{
			0: 10,
		},
		"b": map[int]int64{
			0: 5,
			1: 6,
		},
		"c": map[int]int64{
			10: 15,
		},
	}
	messages := []segmentio.Message{
		{Topic: "a", Partition: 0, Offset: 8},  // ignore
		{Topic: "a", Partition: 0, Offset: 9},  // ignore
		{Topic: "a", Partition: 0, Offset: 10}, // ignore

		{Topic: "b", Partition: 0, Offset: 15}, // update
		{Topic: "b", Partition: 1, Offset: 5},  // ignore

		{Topic: "c", Partition: 1, Offset: 5},   // update
		{Topic: "c", Partition: 10, Offset: 10}, // ignore
	}

	// When
	actual := offsets.IgnoreAlreadyCommittedMessages(messages)

	// Then
	if len(actual) != 2 {
		t.Fatal("Actual must be length of 2")
	}

	if actual[0].Topic != "b" && actual[0].Partition == 0 && actual[0].Offset == 15 {
		t.Fatalf("Actual %v is not equal to expected", actual[0])
	}

	if actual[1].Topic != "c" && actual[1].Partition == 1 && actual[0].Offset == 5 {
		t.Fatalf("Actual %v is not equal to expected", actual[1])
	}
}
