package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Stats is the scoreboard. Run a request three times, then read this.
//
// An idempotent endpoint leaves users/information/audit_entries flat
// after the first call. A broken one increments them every time.
//
//	@Summary		Scoreboard
//	@Description	Row counts. An idempotent endpoint leaves them flat after the first call.
//	@Tags			lab
//	@Produce		json
//	@Success		200	{object}	model.StatsResponse
//	@Failure		500	{object}	model.ErrorResponse
//	@Router			/_stats [get]
func (h *Handler) Stats(c *gin.Context) {
	var s struct {
		Users          int `json:"users_live"`
		UsersDeleted   int `json:"users_deleted"`
		Information    int `json:"information_live"`
		AuditEntries   int `json:"audit_entries"`
		KeysTotal      int `json:"idempotency_keys"`
		KeysInProgress int `json:"idempotency_keys_in_progress"`
		KeysCompleted  int `json:"idempotency_keys_completed"`
	}
	err := h.pool.QueryRow(c, `
		SELECT (SELECT count(*) FROM users       WHERE deleted_at IS NULL),
		       (SELECT count(*) FROM users       WHERE deleted_at IS NOT NULL),
		       (SELECT count(*) FROM information WHERE deleted_at IS NULL),
		       (SELECT count(*) FROM audit_log),
		       (SELECT count(*) FROM idempotency_keys),
		       (SELECT count(*) FROM idempotency_keys WHERE state = 'in_progress'),
		       (SELECT count(*) FROM idempotency_keys WHERE state = 'completed')`).
		Scan(&s.Users, &s.UsersDeleted, &s.Information,
			&s.AuditEntries, &s.KeysTotal, &s.KeysInProgress, &s.KeysCompleted)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, s)
}

// Reset wipes everything so you can re-run an experiment from zero.
//
//	@Summary	Truncate every table
//	@Tags		lab
//	@Produce	json
//	@Success	200	{object}	model.ResetResponse
//	@Failure	500	{object}	model.ErrorResponse
//	@Router		/_reset [post]
func (h *Handler) Reset(c *gin.Context) {
	if _, err := h.pool.Exec(c, `
		TRUNCATE users, information, audit_log, idempotency_keys RESTART IDENTITY`); err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"reset": true})
}

// Sweep deletes expired idempotency keys. In production this is a cron
// or a background goroutine, batched so it does not hold long locks.
//
//	@Summary		Delete expired idempotency keys
//	@Description	Batched (10000 rows). Production would run this on a schedule.
//	@Tags			lab
//	@Produce		json
//	@Success		200	{object}	model.SweepResponse
//	@Failure		500	{object}	model.ErrorResponse
//	@Router			/_sweep [post]
func (h *Handler) Sweep(c *gin.Context) {
	tag, err := h.pool.Exec(c, `
		DELETE FROM idempotency_keys
		 WHERE ctid IN (
		   SELECT ctid FROM idempotency_keys WHERE expires_at < now() LIMIT 10000
		 )`)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"swept": tag.RowsAffected()})
}

// ExpireKeys forces every stored key to look expired, so you can watch
// a "retry after the TTL" turn into a brand-new operation without
// waiting 24 hours. Lab-only, obviously.
//
//	@Summary		Force every key to look expired
//	@Description	Lab-only: lets you test the TTL boundary without waiting 24h.
//	@Tags			lab
//	@Produce		json
//	@Success		200	{object}	model.ExpireKeysResponse
//	@Failure		500	{object}	model.ErrorResponse
//	@Router			/_expire_keys [post]
func (h *Handler) ExpireKeys(c *gin.Context) {
	tag, err := h.pool.Exec(c, `
		UPDATE idempotency_keys
		   SET expires_at = now() - interval '1 second',
		       lease_expires_at = now() - interval '1 second'`)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"expired": tag.RowsAffected()})
}
