package model

// Response shapes for the Swagger spec only. The handlers still write
// gin.H literals; these types just give swag something named to point
// at, so the business logic stays untouched.

// ErrorResponse is every non-2xx JSON body in the lab.
type ErrorResponse struct {
	Error string `json:"error" example:"precondition_failed"`
	Hint  string `json:"hint,omitempty" example:"GET the resource first and send its ETag as If-Match"`
}

// StatsResponse is the GET /_stats scoreboard.
type StatsResponse struct {
	UsersLive                 int `json:"users_live"`
	UsersDeleted              int `json:"users_deleted"`
	InformationLive           int `json:"information_live"`
	AuditEntries              int `json:"audit_entries"`
	IdempotencyKeys           int `json:"idempotency_keys"`
	IdempotencyKeysInProgress int `json:"idempotency_keys_in_progress"`
	IdempotencyKeysCompleted  int `json:"idempotency_keys_completed"`
}

type ResetResponse struct {
	Reset bool `json:"reset" example:"true"`
}

type SweepResponse struct {
	Swept int64 `json:"swept"`
}

type ExpireKeysResponse struct {
	Expired int64 `json:"expired"`
}
