package handler

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/heartlezz7/idempotent/src/route/middleware"
	"github.com/jackc/pgx/v5/pgconn"
)

// Helpers shared by the users, information and lab handlers.

// audit appends to the observable side-effect log. Count these rows to
// see whether an endpoint really is idempotent.
func audit(db middleware.DB, c *gin.Context, op, resource, id string) {
	_, _ = db.Exec(c,
		`INSERT INTO audit_log (op, resource, resource_id, route) VALUES ($1,$2,$3,$4)`,
		op, resource, id, c.Request.URL.Path)
}

func etag(version int) string { return fmt.Sprintf(`"v%d"`, version) }

// parseETag turns `"v41"` (or `W/"v41"`) back into 41.
func parseETag(s string) (int, bool) {
	s = strings.TrimPrefix(s, "W/")
	s = strings.Trim(s, `"`)
	s = strings.TrimPrefix(s, "v")
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// chaos lets you simulate a failure AFTER the write but BEFORE the
// response. Send `X-Chaos: fail-after-write`.
//
//	/naive -> the insert already autocommitted; the retry duplicates it.
//	/safe  -> the transaction rolls back, the key is released, and the
//	          retry re-runs cleanly. Exactly one row either way.
func chaos(c *gin.Context) bool {
	if c.GetHeader("X-Chaos") == "fail-after-write" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "simulated_failure_after_write"})
		return true
	}
	return false
}

// fail answers 500 with the raw error. Lab-only: a real service would log
// err and return a generic message instead of leaking driver details.
func fail(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

// notFound answers 404 when no row matches the id.
func notFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
}

// invalidBody answers 400 when the JSON body fails to bind.
func invalidBody(c *gin.Context) {
	c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
}

const uniqueViolation = "23505"

// isUniqueViolation reports whether err is Postgres error 23505, the
// signal that a UNIQUE index refused a duplicate row.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}
