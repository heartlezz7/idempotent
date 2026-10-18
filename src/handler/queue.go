package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

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
//
//	@Summary		Publish a message
//	@Description	Stand-in for an external producer. dedupe_key is optional; reusing it with the same payload returns the existing message (deduped=true), with a different payload returns 422.
//	@Tags			producer
//	@Accept			json
//	@Produce		json
//	@Param			body	body		model.PublishRequest	true	"Message to publish"
//	@Success		201		{object}	model.PublishResponse
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		422		{object}	model.ErrorResponse	"dedupe_key_reused_with_different_payload"
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/publish [post]
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
		fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"message_id": id, "deduped": deduped})
}

// NaiveConsume claims one message from ?topic= and runs it through the
// unprotected handler for that resource: every delivery lands, so
// replaying the same message inflates audit_log once per delivery.
//
//	@Summary		Consume one message (naive)
//	@Description	Claims one message and applies it unconditionally: every redelivery duplicates the effect.
//	@Tags			consumers
//	@Produce		json
//	@Param			topic			query		string				true	"Topic to claim from"	Enums(orders, notifications)
//	@Param			lease_seconds	query		int					false	"Lease length in seconds (default 5)"
//	@Success		200				{object}	consumer.Outcome	"Processed (or model.NoMessagesResponse when the topic is empty)"
//	@Failure		400				{object}	model.ErrorResponse
//	@Failure		500				{object}	model.ErrorResponse
//	@Router			/naive/consume [post]
func (h *Handler) NaiveConsume(c *gin.Context) {
	topic := c.Query("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic query param required"})
		return
	}
	ctx := c.Request.Context()
	msg, err := queue.Claim(ctx, h.pool, topic, "naive-"+uuid.NewString(), leaseSeconds(c))
	if err != nil {
		fail(c, err)
		return
	}
	if msg == nil {
		c.JSON(http.StatusOK, gin.H{"message": "no messages available"})
		return
	}

	outcome, err := naiveProcessor(topic)(ctx, h.pool, msg)
	if err != nil {
		fail(c, err)
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
//
//	@Summary		Consume one message (safe)
//	@Description	Orders use a monotonic version guard plus the transactional outbox; notifications use the inbox table. chaos injects a crash before commit or after commit but before ack.
//	@Tags			consumers
//	@Produce		json
//	@Param			topic			query		string				true	"Topic to claim from"	Enums(orders, notifications)
//	@Param			lease_seconds	query		int					false	"Lease length in seconds (default 5)"
//	@Param			chaos			query		string				false	"Fault injection point"	Enums(crash_before_commit, crash_after_commit_before_ack)
//	@Success		200				{object}	consumer.Outcome	"Processed, skipped, or chaos outcome (or model.NoMessagesResponse when the topic is empty)"
//	@Failure		400				{object}	model.ErrorResponse
//	@Failure		500				{object}	model.ErrorResponse
//	@Router			/safe/consume [post]
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
		fail(c, err)
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
		fail(c, err)
		return
	}
	// A chaos-triggered rollback is an intentional lab outcome, not a
	// real failure -- report it as 200 with the outcome describing what
	// happened, same as everything else.
	c.JSON(http.StatusOK, outcome)
}

// RelayRun runs the transactional outbox relay once, publishing up to
// ?limit= (default 10) pending outbox rows onto the queue.
//
//	@Summary		Run the outbox relay once
//	@Description	Publishes up to limit unpublished outbox_events rows onto the queue with dedupe_key outbox:{id}.
//	@Tags			relay
//	@Produce		json
//	@Param			limit	query		int	false	"Max rows to publish (default 10)"
//	@Success		200		{object}	model.RelayResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/relay/run [post]
func (h *Handler) RelayRun(c *gin.Context) {
	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			limit = n
		}
	}
	n, err := outbox.RunOnce(c.Request.Context(), h.pool, limit)
	if err != nil {
		fail(c, err)
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
//
//	@Summary		Force a redelivery (lab-only)
//	@Description	Resets a message to pending regardless of status, including done -- simulates a broker replay.
//	@Tags			chaos
//	@Accept			json
//	@Produce		json
//	@Param			body	body		model.MessageIDRequest	true	"Message to redeliver"
//	@Success		200		{object}	model.RedeliverResponse
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/_duplicate [post]
func (h *Handler) ForceRedeliver(c *gin.Context) {
	var req messageIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := queue.ForceRedeliver(c.Request.Context(), h.pool, req.MessageID); err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message_id": req.MessageID, "status": "pending"})
}

// ForceExpireLease is lab-only: backdates a message's lease so the next
// Claim can steal it immediately, to exercise the stuck-in-progress
// recovery path without waiting out the real lease duration.
//
//	@Summary		Expire a lease now (lab-only)
//	@Description	Backdates a leased message so the next claim can steal it.
//	@Tags			chaos
//	@Accept			json
//	@Produce		json
//	@Param			body	body		model.MessageIDRequest	true	"Leased message"
//	@Success		200		{object}	model.ExpireLeaseResponse
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/_expire_lease [post]
func (h *Handler) ForceExpireLease(c *gin.Context) {
	var req messageIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := queue.ForceExpireLease(c.Request.Context(), h.pool, req.MessageID); err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message_id": req.MessageID, "lease": "expired"})
}
