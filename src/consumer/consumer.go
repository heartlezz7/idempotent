// Package consumer implements naive and safe message handlers over the
// simulated broker in src/queue. It is the "beyond HTTP" counterpart to
// the REST idempotency lab: the same discipline (claim atomically, commit
// effect and receipt together, release rather than save on failure)
// applied to at-least-once message delivery instead of client retries.
package consumer

import "errors"

// Chaos selects a fault-injection point inside a safe consumer, mirroring
// the HTTP lab's X-Chaos header. It exists to make the crash windows from
// the doc's "Races and recovery" table reproducible on demand instead of
// waiting for a real pod to get OOM-killed.
type Chaos string

const (
	ChaosNone                  Chaos = ""
	ChaosCrashBeforeCommit     Chaos = "crash_before_commit"
	ChaosCrashAfterCommitNoAck Chaos = "crash_after_commit_before_ack"
)

// ErrChaosCrashBeforeCommit is returned by a safe consumer when
// ChaosCrashBeforeCommit fires. It is not a real failure: the transaction
// was deliberately rolled back to prove the retry path, and the caller
// should report the Outcome rather than treat this as a 500.
var ErrChaosCrashBeforeCommit = errors.New("chaos: simulated crash before commit")

// Outcome describes what a single Process call actually did, for the lab
// control plane to render back to whoever is driving the experiment.
type Outcome struct {
	Consumer  string `json:"consumer"` // "naive" | "safe"
	Resource  string `json:"resource"` // "order" | "notification"
	MessageID int64  `json:"message_id"`
	Applied   bool   `json:"applied"`
	Acked     bool   `json:"acked"`
	Detail    string `json:"detail"`
}
