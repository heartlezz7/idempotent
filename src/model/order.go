package model

import "time"

// Order is the resource that needs no dedup table at all: a status update
// is naturally idempotent once guarded by a monotonic version.
type Order struct {
	ID        string
	Status    string
	Version   int64
	UpdatedAt time.Time
}

// Notification is the append-only side effect that IS NOT naturally
// idempotent -- there is no assignment that makes sending twice equal to
// sending once. This is the resource that genuinely needs the inbox
// pattern in front of it.
type Notification struct {
	ID        int64
	OrderID   string
	MessageID int64
	Body      string
	SentAt    time.Time
}
