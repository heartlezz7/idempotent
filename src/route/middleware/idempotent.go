// Package middleware Package idem implements Idempotency-Key handling as gin middleware.
//
// The design goal is that the business handler stays completely unaware
// of idempotency. The exact same handler function is mounted on /naive
// (no middleware) and /safe (with middleware) -- all the protection
// lives here.
//
// The one thing the handler must cooperate on: it writes through
// idem.DBFrom(c, pool) instead of using the pool directly, so its work
// lands in the same transaction as the receipt.
package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ctxTxKey     = "idem.tx"
	maxBodyBytes = 1 << 20 // 1 MiB
	maxKeyLen    = 255
)

// Headers that are worth replaying alongside a stored response body.
var replayableHeaders = []string{"Content-Type", "Location", "ETag"}

// DB is satisfied by both *pgxpool.Pool and pgx.Tx, which is what lets
// one handler run either inside our transaction or standalone.
type DB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// DBFrom returns the transaction this request is running inside, if the
// middleware started one, and otherwise the plain pool (autocommit).
func DBFrom(c *gin.Context, pool *pgxpool.Pool) DB {
	if v, ok := c.Get(ctxTxKey); ok {
		if tx, ok := v.(pgx.Tx); ok {
			return tx
		}
	}
	return pool
}

// ---------------------------------------------------------------------
// Buffered response writer
//
// gin normally streams straight to the socket, which would mean the
// client sees "201 Created" BEFORE we commit. If the commit then failed
// we would have promised something that never happened. So we buffer,
// commit, and only then flush.
// ---------------------------------------------------------------------

type buffered struct {
	gin.ResponseWriter
	status int
	body   bytes.Buffer
}

func (b *buffered) Write(p []byte) (int, error)       { return b.body.Write(p) }
func (b *buffered) WriteString(s string) (int, error) { return b.body.WriteString(s) }
func (b *buffered) WriteHeader(code int)              { b.status = code }
func (b *buffered) WriteHeaderNow()                   {} // suppressed until commit
func (b *buffered) Written() bool                     { return false }
func (b *buffered) Size() int                         { return b.body.Len() }

func (b *buffered) Status() int {
	if b.status == 0 {
		return http.StatusOK
	}
	return b.status
}

// ---------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------

// claimSQL is the whole concurrency story in one statement.
//
// Plain INSERT wins  -> we own this operation.
// Conflict + expired lease + same fingerprint -> the previous owner
//
//	died, we take the claim over.
//
// Conflict otherwise -> zero rows, someone else owns it or finished it.
//
// Never SELECT-then-INSERT. That gap is where the duplicate is born.
const claimSQL = `
INSERT INTO idempotency_keys
      (tenant_id, endpoint, key, request_hash, state,
       lease_expires_at, expires_at)
VALUES ($1, $2, $3, $4, 'in_progress',
        now() + interval '60 seconds',
        now() + interval '24 hours')
ON CONFLICT (tenant_id, endpoint, key) DO UPDATE
   SET lease_expires_at = now() + interval '60 seconds'
 WHERE idempotency_keys.state            = 'in_progress'
   AND idempotency_keys.lease_expires_at < now()
   AND idempotency_keys.request_hash     = EXCLUDED.request_hash
RETURNING true`

const loadSQL = `
SELECT request_hash, state,
       COALESCE(response_code, 0),
       COALESCE(response_body, ''),
       COALESCE(response_headers, ''),
       lease_expires_at > now()
  FROM idempotency_keys
 WHERE tenant_id = $1 AND endpoint = $2 AND key = $3`

const completeSQL = `
UPDATE idempotency_keys
   SET state = 'completed', response_code = $4,
       response_body = $5, response_headers = $6
 WHERE tenant_id = $1 AND endpoint = $2 AND key = $3`

// releaseSQL deletes an in_progress claim so a retry can genuinely
// re-run. Persisting a 5xx as 'completed' would poison the key forever.
const releaseSQL = `
DELETE FROM idempotency_keys
 WHERE tenant_id = $1 AND endpoint = $2 AND key = $3
   AND state = 'in_progress'`

// ---------------------------------------------------------------------

func Middleware(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		// --- 1. Extract and validate the key ---------------------------
		key := c.GetHeader("Idempotency-Key")
		if key == "" || len(key) > maxKeyLen {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "idempotency_key_required",
				"hint":  "send a per-operation UUID in the Idempotency-Key header",
			})
			return
		}

		// Scope is the security boundary, not just a uniqueness trick.
		// Without it, tenant B can replay tenant A's response body.
		tenant := c.GetHeader("X-Api-Key")
		if tenant == "" {
			tenant = "demo"
		}
		endpoint := c.Request.Method + " " + c.FullPath()

		// --- 2. Fingerprint the request --------------------------------
		raw, err := io.ReadAll(io.LimitReader(c.Request.Body, maxBodyBytes))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "unreadable_body"})
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(raw)) // give it back to the handler
		fp := fingerprint(c.Request.Method, c.Request.URL.Path, raw)

		// --- 3. Claim the key atomically -------------------------------
		var owned bool
		err = pool.QueryRow(ctx, claimSQL, tenant, endpoint, key, fp).Scan(&owned)

		if errors.Is(err, pgx.ErrNoRows) {
			// --- 4. We lost. Replay, reject, or ask them to wait. ------
			resolveExisting(c, pool, tenant, endpoint, key, fp)
			return
		}
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "idempotency_store_unavailable"})
			return
		}

		// --- 5. We own it. Run the handler inside one transaction. -----
		tx, err := pool.Begin(ctx)
		if err != nil {
			release(pool, tenant, endpoint, key)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "begin_failed"})
			return
		}
		defer tx.Rollback(ctx) // no-op once committed

		c.Set(ctxTxKey, tx)

		orig := c.Writer
		buf := &buffered{ResponseWriter: orig}
		c.Writer = buf

		c.Next()

		c.Writer = orig
		status := buf.Status()

		// --- 6. Transient failure: release, never save. ----------------
		// Deterministic 4xx could legitimately be cached. Anything 5xx
		// must be released so the client's retry actually re-executes.
		if status >= 500 || len(c.Errors) > 0 {
			_ = tx.Rollback(ctx)
			release(pool, tenant, endpoint, key)
			flush(orig, status, buf.body.Bytes())
			return
		}

		// --- 7. Write the receipt in the SAME transaction. -------------
		hdrs, _ := json.Marshal(capturedHeaders(orig))
		if _, err := tx.Exec(ctx, completeSQL,
			tenant, endpoint, key, status, buf.body.String(), string(hdrs)); err != nil {
			_ = tx.Rollback(ctx)
			release(pool, tenant, endpoint, key)
			flush(orig, http.StatusInternalServerError,
				[]byte(`{"error":"receipt_write_failed"}`))
			return
		}

		// --- 8. Commit, then respond. ----------------------------------
		// If the process dies between here and the client receiving the
		// bytes, the retry finds a completed receipt and replays it.
		if err := tx.Commit(ctx); err != nil {
			release(pool, tenant, endpoint, key)
			flush(orig, http.StatusInternalServerError,
				[]byte(`{"error":"commit_failed"}`))
			return
		}
		flush(orig, status, buf.body.Bytes())
	}
}

// resolveExisting handles the three "we did not get the claim" outcomes.
func resolveExisting(c *gin.Context, pool *pgxpool.Pool, tenant, endpoint, key string, fp []byte) {
	var (
		hash    []byte
		state   string
		code    int
		body    string
		hdrsRaw string
		live    bool
	)
	err := pool.QueryRow(c.Request.Context(), loadSQL, tenant, endpoint, key).
		Scan(&hash, &state, &code, &body, &hdrsRaw, &live)
	if err != nil {
		// Extremely narrow race: the row was swept between our failed
		// claim and this read. Telling the client to retry is correct.
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "idempotency_key_in_use"})
		return
	}

	switch {
	// Same key, different payload: a client bug, not a retry. Say so
	// loudly rather than silently handing back an unrelated response.
	case !bytes.Equal(hash, fp):
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
			"error": "key_reused_with_different_payload",
		})

	// The happy path this whole package exists for.
	case state == "completed":
		var hdrs map[string]string
		_ = json.Unmarshal([]byte(hdrsRaw), &hdrs)
		for k, v := range hdrs {
			c.Header(k, v)
		}
		c.Header("Idempotent-Replayed", "true")
		c.Abort()
		if body == "" {
			c.Writer.WriteHeader(code)
			c.Writer.WriteHeaderNow()
			return
		}
		c.Data(code, "application/json; charset=utf-8", []byte(body))

	// Genuinely in flight right now. Do not block a connection on it.
	default:
		_ = live
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "idempotency_key_in_use",
			"hint":  "an identical request is still running; retry with backoff",
		})
	}
}

func fingerprint(method, path string, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte("\n"))
	h.Write([]byte(path))
	h.Write([]byte("\n"))
	// A production version canonicalises the JSON here (sort keys,
	// normalise numbers) and strips volatile fields such as trace IDs,
	// otherwise a reordered map makes a legitimate retry fail with 422.
	h.Write(body)
	return h.Sum(nil)
}

func capturedHeaders(w gin.ResponseWriter) map[string]string {
	out := map[string]string{}
	for _, k := range replayableHeaders {
		if v := w.Header().Get(k); v != "" {
			out[k] = v
		}
	}
	return out
}

func flush(w gin.ResponseWriter, status int, body []byte) {
	w.WriteHeader(status)
	w.WriteHeaderNow()
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

// release runs on a fresh context: the request context may already be
// cancelled by the client hanging up, and we still need the cleanup.
func release(pool *pgxpool.Pool, tenant, endpoint, key string) {
	_, _ = pool.Exec(context.Background(), releaseSQL, tenant, endpoint, key)
}
