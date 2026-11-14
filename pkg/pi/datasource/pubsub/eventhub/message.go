package eventhub

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

type Message struct {
	event     *azeventhubs.ReceivedEventData
	processor *azeventhubs.ProcessorPartitionClient
	logger    Logger
}

func (a *Message) Commit() {
	_ = a.CommitContext(context.Background())
}

func (a *Message) CommitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Update the checkpoint with the latest event received
	if a.processor != nil {
		err := a.processor.UpdateCheckpoint(ctx, a.event, nil)
		if err != nil {
			a.logger.Errorf("failed to acknowledge event with eventID %v: %v", a.event.MessageID, err)
			return err
		}

		a.logger.Debugf("Message committed via processor checkpoint (MessageID: %v)", a.event.MessageID)
	} else {
		a.logger.Debugf("Message acknowledged (direct read mode)")
	}
	return nil
}
