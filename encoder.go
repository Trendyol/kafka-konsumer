package kafka

import (
	"context"
)

type Encoder interface {
	Produce(ctx context.Context, v any, messages ...Message) error
}

type EncoderFunc func(v any) ([]byte, error)

type encoder struct {
	p  Producer
	fn EncoderFunc
}

func NewEncoder(p Producer, fn EncoderFunc) Encoder {
	return &encoder{p, fn}
}

func (d *encoder) Produce(ctx context.Context, v any, messages ...Message) error {
	bytes, err := d.fn(v)
	if err != nil {
		return err
	}

	message := Message{}
	if len(messages) > 0 {
		message = messages[0]
	}

	message.Value = bytes
	return d.p.Produce(ctx, message)
}
