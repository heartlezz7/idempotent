// Package handler is the lab's control plane: plain HTTP endpoints used
// to drive and observe the queue/consumer idempotency experiments. They
// are not themselves the subject under test -- REST idempotency lives on
// another branch. Here HTTP is just the remote control for publishing
// messages, stepping consumers, and reading back the audit trail.
package handler

import "github.com/jackc/pgx/v5/pgxpool"

type Handler struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Handler {
	return &Handler{pool: pool}
}
