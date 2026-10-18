package model

import "encoding/json"

// Request/response shapes for the Swagger spec only. The handlers still
// bind their own unexported structs and write gin.H literals; these types
// just give swag something named to point at, so handler logic stays
// untouched.

// ErrorResponse is every non-2xx JSON body in the lab.
type ErrorResponse struct {
	Error string `json:"error" example:"topic query param required"`
}

// PublishRequest is the body of POST /publish.
type PublishRequest struct {
	Topic     string          `json:"topic" binding:"required" example:"orders"`
	DedupeKey *string         `json:"dedupe_key" example:"order-123:v2"`
	Payload   json.RawMessage `json:"payload" binding:"required" swaggertype:"object"`
}

type PublishResponse struct {
	MessageID int64 `json:"message_id" example:"1"`
	Deduped   bool  `json:"deduped"`
}

// NoMessagesResponse is what a consume call returns when nothing is
// claimable right now.
type NoMessagesResponse struct {
	Message string `json:"message" example:"no messages available"`
}

type RelayResponse struct {
	Published int `json:"published"`
}

type MessageIDRequest struct {
	MessageID int64 `json:"message_id" binding:"required" example:"1"`
}

type RedeliverResponse struct {
	MessageID int64  `json:"message_id" example:"1"`
	Status    string `json:"status" example:"pending"`
}

type ExpireLeaseResponse struct {
	MessageID int64  `json:"message_id" example:"1"`
	Lease     string `json:"lease" example:"expired"`
}

type AuditCount struct {
	Consumer string `json:"consumer" example:"safe"`
	Op       string `json:"op" example:"order.apply"`
	Count    int64  `json:"count"`
}

type QueueCount struct {
	Topic  string `json:"topic" example:"orders"`
	Status string `json:"status" example:"done"`
	Count  int64  `json:"count"`
}

// StatsResponse is the GET /_stats scoreboard.
type StatsResponse struct {
	AuditLog          []AuditCount `json:"audit_log"`
	QueueMessages     []QueueCount `json:"queue_messages"`
	Orders            int64        `json:"orders"`
	NotificationsSent int64        `json:"notifications_sent"`
	InboxProcessed    int64        `json:"inbox_processed"`
	OutboxPending     int64        `json:"outbox_pending"`
	OutboxPublished   int64        `json:"outbox_published"`
}

type ResetResponse struct {
	Reset bool `json:"reset" example:"true"`
}
