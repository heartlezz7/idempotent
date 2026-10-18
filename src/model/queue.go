package model

import "time"

// Message is one row of the simulated broker (queue_messages). A single
// Postgres table stands in for one partition of a topic: at-least-once by
// construction, since a lease can expire and be reclaimed by another
// worker.
type Message struct {
	ID             int64
	Topic          string
	DedupeKey      *string
	Payload        []byte
	PayloadHash    []byte
	Status         string // pending | leased | done
	Attempts       int
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	AvailableAt    time.Time
	CreatedAt      time.Time
}

const (
	StatusPending = "pending"
	StatusLeased  = "leased"
	StatusDone    = "done"
)

// OutboxEvent is a row written in the same transaction as a business
// effect and later relayed onto the queue. Removes the dual-write problem
// at its root: the effect and the intent to notify commit together.
type OutboxEvent struct {
	ID        int64
	Topic     string
	Payload   []byte
	Published bool
	CreatedAt time.Time
}
