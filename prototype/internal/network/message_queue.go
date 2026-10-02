package network

import (
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// MessageQueue is the message queue manager
type MessageQueue struct {
	conn          *nats.Conn
	subscriptions map[string]*nats.Subscription
	mu            sync.Mutex
	nodeID        string
}

// NewMessageQueue creates a message queue manager
func NewMessageQueue(nodeID, natsURL string) (*MessageQueue, error) {
	// Connect to the NATS server
	conn, err := nats.Connect(natsURL,
		nats.Name("sfchain-"+nodeID),
		nats.ReconnectWait(5*time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[message queue] NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[message queue] NATS reconnected: %s", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("[message queue] NATS error: %v", err)
		}),
	)

	if err != nil {
		return &MessageQueue{
			nodeID:        nodeID,
			subscriptions: make(map[string]*nats.Subscription),
		}, nil
	}

	return &MessageQueue{
		conn:          conn,
		nodeID:        nodeID,
		subscriptions: make(map[string]*nats.Subscription),
	}, nil
}

// Publish publishes a message to the given subject
func (mq *MessageQueue) Publish(subject string, data []byte) error {
	if mq.conn == nil {
		return nats.ErrConnectionClosed
	}

	return mq.conn.Publish(subject, data)
}

// Subscribe subscribes to messages on the given subject
func (mq *MessageQueue) Subscribe(subject string, handler nats.MsgHandler) error {
	if mq.conn == nil {
		return nats.ErrConnectionClosed
	}

	mq.mu.Lock()
	defer mq.mu.Unlock()

	// Check whether already subscribed
	if _, exists := mq.subscriptions[subject]; exists {
		return nil
	}

	sub, err := mq.conn.Subscribe(subject, handler)
	if err != nil {
		return err
	}

	mq.subscriptions[subject] = sub
	return nil
}

// Unsubscribe unsubscribes from the given subject
func (mq *MessageQueue) Unsubscribe(subject string) error {
	if mq.conn == nil {
		return nats.ErrConnectionClosed
	}

	mq.mu.Lock()
	defer mq.mu.Unlock()

	sub, exists := mq.subscriptions[subject]
	if !exists {
		return nil
	}

	err := sub.Unsubscribe()
	if err != nil {
		return err
	}

	delete(mq.subscriptions, subject)
	return nil
}

// Close closes the message queue connection
func (mq *MessageQueue) Close() {
	if mq.conn != nil {
		mq.mu.Lock()
		for subject, sub := range mq.subscriptions {
			sub.Unsubscribe()
			delete(mq.subscriptions, subject)
		}
		mq.mu.Unlock()

		mq.conn.Close()
	}
}

// IsConnected reports whether the message queue is connected
func (mq *MessageQueue) IsConnected() bool {
	return mq.conn != nil && mq.conn.IsConnected()
}
