package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/heartlezz7/idempotent/src/consumer"
	"github.com/heartlezz7/idempotent/src/model"
	"github.com/heartlezz7/idempotent/src/outbox"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultLeaseSeconds = 5

type publishReq struct {
	Topic     string          `json:"topic" binding:"required"`
	DedupeKey *string         `json:"dedupe_key"`
	Payload   json.RawMessage `json:"payload" binding:"required"`
}

// Publish is a manual stand-in for an external producer. dedupe_key is
// optional -- omit it to send a fire-and-forget message, same as the
// doc's "accept absence, process normally" policy for a missing
// Idempotency-Key.
func (h *Handler) Publish(c *gin.Context) {
	var req publishReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	id, deduped, err := queue.Publish(c.Request.Context(), h.pool, req.Topic, req.DedupeKey, req.Payload)
	if errors.Is(err, queue.ErrDedupeConflict) {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "dedupe_key_reused_with_different_payload"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"message_id": id, "deduped": deduped})
}

// NaiveConsume claims one message from ?topic= and runs it through the
// unprotected handler for that resource: every delivery lands, so
// replaying the same message inflates audit_log once per delivery.
func (h *Handler) NaiveConsume(c *gin.Context) {
	topic := c.Query("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic query param required"})
		return
	}
	ctx := c.Request.Context()
	msg, err := queue.Claim(ctx, h.pool, topic, "naive-"+uuid.NewString(), leaseSeconds(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if msg == nil {
		c.JSON(http.StatusOK, gin.H{"message": "no messages available"})
		return
	}

	outcome, err := naiveProcessor(topic)(ctx, h.pool, msg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, outcome)
}

func naiveProcessor(topic string) func(ctx context.Context, pool *pgxpool.Pool, msg *model.Message) (consumer.Outcome, error) {
	switch topic {
	case "notifications":
		return consumer.ProcessNotificationNaive
	default:
		return consumer.ProcessOrderNaive
	}
}

// SafeConsume claims one message from ?topic= and runs it through the
// protected handler for that resource. ?chaos= selects a fault injection
// point (crash_before_commit | crash_after_commit_before_ack) so the
// crash windows from the doc's races table can be exercised on demand.
func (h *Handler) SafeConsume(c *gin.Context) {
	topic := c.Query("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic query param required"})
		return
	}
	chaos := consumer.Chaos(c.Query("chaos"))
	ctx := c.Request.Context()
	msg, err := queue.Claim(ctx, h.pool, topic, "safe-"+uuid.NewString(), leaseSeconds(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if msg == nil {
		c.JSON(http.StatusOK, gin.H{"message": "no messages available"})
		return
	}

	var outcome consumer.Outcome
	switch topic {
	case "notifications":
		outcome, err = consumer.ProcessNotificationSafe(ctx, h.pool, msg, chaos)
	default:
		outcome, err = consumer.ProcessOrderSafe(ctx, h.pool, msg, chaos)
	}
	if err != nil && !errors.Is(err, consumer.ErrChaosCrashBeforeCommit) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// A chaos-triggered rollback is an intentional lab outcome, not a
	// real failure -- report it as 200 with the outcome describing what
	// happened, same as everything else.
	c.JSON(http.StatusOK, outcome)
}

// RelayRun runs the transactional outbox relay once, publishing up to
// ?limit= (default 10) pending outbox rows onto the queue.
func (h *Handler) RelayRun(c *gin.Context) {
	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			limit = n
		}
	}
	n, err := outbox.RunOnce(c.Request.Context(), h.pool, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"published": n})
}

type messageIDReq struct {
	MessageID int64 `json:"message_id" binding:"required"`
}

// ForceRedeliver is lab-only: simulates a broker redelivering a message
// the consumer already fully processed (an ack that never reached the
// broker, or a rebalance replay). Use it to prove the SAFE consumers
// absorb a duplicate; the naive ones will visibly duplicate the effect.
func (h *Handler) ForceRedeliver(c *gin.Context) {
	var req messageIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := queue.ForceRedeliver(c.Request.Context(), h.pool, req.MessageID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message_id": req.MessageID, "status": "pending"})
}

// ForceExpireLease is lab-only: backdates a message's lease so the next
// Claim can steal it immediately, to exercise the stuck-in-progress
// recovery path without waiting out the real lease duration.
func (h *Handler) ForceExpireLease(c *gin.Context) {
	var req messageIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := queue.ForceExpireLease(c.Request.Context(), h.pool, req.MessageID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message_id": req.MessageID, "lease": "expired"})
}

func parsePositiveInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}

func leaseSeconds(c *gin.Context) int {
	if v := c.Query("lease_seconds"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			return n
		}
	}
	return defaultLeaseSeconds
}
