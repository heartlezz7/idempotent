package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type auditCount struct {
	Consumer string `json:"consumer"`
	Op       string `json:"op"`
	Count    int64  `json:"count"`
}

type queueCount struct {
	Topic  string `json:"topic"`
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

// Stats is the scoreboard: run a message through naive and safe consumers
// the same number of times, then compare audit_log counts here. A
// correct consumer applies an effect once per logical change; a naive one
// applies it once per delivery.
func (h *Handler) Stats(c *gin.Context) {
	ctx := c.Request.Context()

	rows, err := h.pool.Query(ctx, `
		SELECT consumer, op, count(*) FROM audit_log GROUP BY consumer, op ORDER BY consumer, op`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var audits []auditCount
	for rows.Next() {
		var a auditCount
		if err := rows.Scan(&a.Consumer, &a.Op, &a.Count); err != nil {
			rows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		audits = append(audits, a)
	}
	rows.Close()

	qrows, err := h.pool.Query(ctx, `
		SELECT topic, status, count(*) FROM queue_messages GROUP BY topic, status ORDER BY topic, status`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var queues []queueCount
	for qrows.Next() {
		var q queueCount
		if err := qrows.Scan(&q.Topic, &q.Status, &q.Count); err != nil {
			qrows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		queues = append(queues, q)
	}
	qrows.Close()

	var orders, notificationsSent, inboxRows, outboxPending, outboxPublished int64
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&orders); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM notifications_sent`).Scan(&notificationsSent); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM inbox_processed`).Scan(&inboxRows); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE NOT published`).Scan(&outboxPending); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published`).Scan(&outboxPublished); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"audit_log":          audits,
		"queue_messages":     queues,
		"orders":             orders,
		"notifications_sent": notificationsSent,
		"inbox_processed":    inboxRows,
		"outbox_pending":     outboxPending,
		"outbox_published":   outboxPublished,
	})
}

// Reset truncates every lab table without restarting the process --
// cheaper than a full docker compose cycle between manual experiments.
func (h *Handler) Reset(c *gin.Context) {
	_, err := h.pool.Exec(c.Request.Context(), `
		TRUNCATE queue_messages, inbox_processed, orders, notifications_sent,
		         outbox_events, audit_log RESTART IDENTITY`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"reset": true})
}
