package handler

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/heartlezz7/idempotent/src/model"
	"github.com/heartlezz7/idempotent/src/route/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Handler { return &Handler{pool: pool} }

// db returns the middleware's transaction when one exists, otherwise the
// pool. Handlers never care which they got.
func (h *Handler) db(c *gin.Context) middleware.DB { return middleware.DBFrom(c, h.pool) }

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

// =====================================================================
// GET  -- safe and idempotent by construction. Identical on both trees.
// =====================================================================

func (h *Handler) ListUsers(c *gin.Context) {
	rows, err := h.db(c).Query(c, `
		SELECT id, name, version FROM users
		 WHERE deleted_at IS NULL ORDER BY created_at`)
	if err != nil {
		fail(c, err)
		return
	}
	defer rows.Close()

	out := []model.User{}
	for rows.Next() {
		var u model.User
		if err := rows.Scan(&u.ID, &u.Name, &u.Version); err != nil {
			fail(c, err)
			return
		}
		out = append(out, u)
	}
	c.JSON(http.StatusOK, out)
}

// GetUser demonstrates the conditional GET. The handler performs no
// writes at all -- no view counter, no lazy provisioning -- because
// prefetchers, crawlers and link unfurlers will call it uninvited.
func (h *Handler) GetUser(c *gin.Context) {
	var u model.User
	err := h.db(c).QueryRow(c, `
		SELECT id, name, version FROM users
		 WHERE id = $1::uuid AND deleted_at IS NULL`, c.Param("id")).
		Scan(&u.ID, &u.Name, &u.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	tag := etag(u.Version)
	c.Header("ETag", tag)
	c.Header("Cache-Control", "private, max-age=30")
	if c.GetHeader("If-None-Match") == tag {
		c.Status(http.StatusNotModified)
		return
	}
	c.JSON(http.StatusOK, u)
}

// =====================================================================
// POST -- not idempotent by construction: the server picks the identity.
//
// ONE handler, mounted twice. /safe wraps it in idem.Middleware; /naive
// does not. Nothing in the business logic below differs.
// =====================================================================

func (h *Handler) CreateUser(c *gin.Context) {
	var in model.CreateUserReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	id := uuid.NewString()

	if _, err := db.Exec(c, `
		INSERT INTO users (id, name, content_hash)
		VALUES ($1::uuid, $2::text, sha256(convert_to($2::text,'UTF8')))`,
		id, in.Name); err != nil {
		fail(c, err)
		return
	}
	audit(db, c, "create", "users", id)

	if chaos(c) {
		return
	}

	c.Header("Location", c.Request.URL.Path+"/"+id)
	c.JSON(http.StatusCreated, model.User{ID: id, Name: in.Name, Version: 1})
}

// =====================================================================
// PUT -- idempotent because it is an assignment, x = v, not an
// operation on the previous value. Both versions upsert; only one of
// them keeps the promise.
// =====================================================================

// PutUserNaive: an upsert that still manages to be non-idempotent.
// The version counter climbs on every call and the audit log grows,
// so three identical writes leave three different observable states.
func (h *Handler) PutUserNaive(c *gin.Context) {
	var in model.PutUserReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	var u model.User
	err := db.QueryRow(c, `
		INSERT INTO users (id, name, content_hash, version)
		VALUES ($1::uuid, $2::text, sha256(convert_to($2::text,'UTF8')), 1)
		ON CONFLICT (id) DO UPDATE
		   SET name         = EXCLUDED.name,
		       content_hash = EXCLUDED.content_hash,
		       version      = users.version + 1   -- <-- the bug
		RETURNING id, name, version`, c.Param("id"), in.Name).
		Scan(&u.ID, &u.Name, &u.Version)
	if err != nil {
		fail(c, err)
		return
	}

	audit(db, c, "replace", "users", u.ID) // <-- and this one
	c.JSON(http.StatusOK, u)
}

// PutUserSafe: full replacement, guarded by a content hash.
//
//   - version advances only when the content genuinely changed, so a
//     retry does not invalidate every other client's If-Match
//   - the audit row is appended only on a real change
//   - 201 on create, 200 on replace: different status codes, identical
//     server state, which is exactly what idempotency permits
//   - If-Match / If-None-Match are honoured for concurrency control
func (h *Handler) PutUserSafe(c *gin.Context) {
	var in model.PutUserReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}
	id := c.Param("id")
	db := h.db(c)

	// Preconditions are evaluated against the current row, if any.
	var curVersion int
	exists := true
	if err := db.QueryRow(c,
		`SELECT version FROM users WHERE id = $1::uuid AND deleted_at IS NULL`, id).
		Scan(&curVersion); errors.Is(err, pgx.ErrNoRows) {
		exists = false
	} else if err != nil {
		fail(c, err)
		return
	}

	if inm := c.GetHeader("If-None-Match"); inm == "*" && exists {
		c.JSON(http.StatusPreconditionFailed, gin.H{"error": "already_exists"})
		return
	}
	if im := c.GetHeader("If-Match"); im != "" {
		if !exists || im != etag(curVersion) {
			c.JSON(http.StatusPreconditionFailed, gin.H{"error": "precondition_failed"})
			return
		}
	}

	// prev sees the pre-update snapshot, so `changed` is true on a real
	// modification and on a fresh insert, false on an identical rewrite.
	var (
		u        model.User
		inserted bool
		changed  bool
	)
	err := db.QueryRow(c, `
		WITH prev AS (
			SELECT content_hash FROM users WHERE id = $1::uuid
		), up AS (
			INSERT INTO users (id, name, content_hash, version)
			VALUES ($1::uuid, $2::text, sha256(convert_to($2::text,'UTF8')), 1)
			ON CONFLICT (id) DO UPDATE
			   SET name         = EXCLUDED.name,
			       content_hash = EXCLUDED.content_hash,
			       version      = CASE
			           WHEN users.content_hash = EXCLUDED.content_hash
			           THEN users.version
			           ELSE users.version + 1 END
			RETURNING (xmax = 0) AS inserted, id, name, version
		)
		SELECT up.inserted, up.id, up.name, up.version,
		       ((SELECT content_hash FROM prev)
		         IS DISTINCT FROM sha256(convert_to($2::text,'UTF8'))) AS changed
		  FROM up`, id, in.Name).
		Scan(&inserted, &u.ID, &u.Name, &u.Version, &changed)
	if err != nil {
		fail(c, err)
		return
	}

	if changed {
		audit(db, c, "replace", "users", u.ID)
	}

	c.Header("ETag", etag(u.Version))
	if inserted {
		c.Header("Location", c.Request.URL.Path)
		c.JSON(http.StatusCreated, u)
		return
	}
	c.JSON(http.StatusOK, u)
}

// =====================================================================
// PATCH -- idempotent or not depending entirely on the patch document.
// =====================================================================

// PatchUserNaive applies an accumulating operation. Nothing here can be
// retried safely, because the result depends on the previous value.
func (h *Handler) PatchUserNaive(c *gin.Context) {
	var in model.PatchUserNaiveReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	var u model.User
	err := db.QueryRow(c, `
		UPDATE users
		   SET name    = name || $2::text,   -- accumulation, not assignment
		       version = version + 1
		 WHERE id = $1::uuid AND deleted_at IS NULL
		RETURNING id, name, version`, c.Param("id"), in.AppendName).
		Scan(&u.ID, &u.Name, &u.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	audit(db, c, "patch", "users", u.ID)
	c.JSON(http.StatusOK, u)
}

// PatchUserSafe implements JSON Merge Patch semantics with a mandatory
// If-Match precondition.
//
// The important trick: an identical re-patch does not advance the
// version. That means the retry's now-stale If-Match still matches, and
// the call succeeds as a true no-op instead of returning a confusing
// 412 for an operation that actually worked.
func (h *Handler) PatchUserSafe(c *gin.Context) {
	if ct := c.ContentType(); ct != "" &&
		ct != "application/merge-patch+json" && ct != "application/json" {
		c.Header("Accept-Patch", "application/merge-patch+json")
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": "unsupported_patch_format"})
		return
	}

	var in model.PatchUserReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	// RFC 6585: demand the precondition rather than silently allowing
	// a lost update.
	im := c.GetHeader("If-Match")
	if im == "" {
		c.JSON(http.StatusPreconditionRequired, gin.H{
			"error": "precondition_required",
			"hint":  "GET the resource first and send its ETag as If-Match",
		})
		return
	}
	wantVersion, ok := parseETag(im)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed_if_match"})
		return
	}

	db := h.db(c)
	var (
		u       model.User
		changed bool
	)
	// Version check and write in one statement: no read-modify-write gap
	// for a concurrent patch to slip through.
	//
	// Note RETURNING exposes the NEW row, so `changed` has to be derived
	// from a CTE that captured the pre-update snapshot. In the SET clause
	// bare column references still mean the OLD values, which is what
	// makes the version CASE work.
	err := db.QueryRow(c, `
		WITH prev AS (
			SELECT name FROM users WHERE id = $1::uuid
		), up AS (
			UPDATE users
			   SET name         = COALESCE($3::text, name),
			       content_hash = sha256(convert_to(COALESCE($3::text, name),'UTF8')),
			       version      = CASE
			           WHEN name IS NOT DISTINCT FROM COALESCE($3::text, name)
			           THEN version ELSE version + 1 END
			 WHERE id = $1::uuid AND version = $2 AND deleted_at IS NULL
			RETURNING id, name, version
		)
		SELECT up.id, up.name, up.version,
		       ((SELECT name FROM prev) IS DISTINCT FROM up.name) AS changed
		  FROM up`,
		c.Param("id"), wantVersion, in.Name).
		Scan(&u.ID, &u.Name, &u.Version, &changed)

	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusPreconditionFailed, gin.H{
			"error": "precondition_failed",
			"hint":  "the resource is gone or someone else advanced its version",
		})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	if changed {
		audit(db, c, "patch", "users", u.ID)
	}
	c.Header("ETag", etag(u.Version))
	c.JSON(http.StatusOK, u)
}

// =====================================================================
// DELETE -- idempotent because its postcondition is absence.
// =====================================================================

// DeleteUserNaive hard-deletes and fires its side effect unconditionally,
// so calling it three times writes three audit rows for one deletion.
func (h *Handler) DeleteUserNaive(c *gin.Context) {
	db := h.db(c)
	tag, err := db.Exec(c, `DELETE FROM users WHERE id = $1::uuid`, c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}

	// Unguarded. In a real service this is the refund, or the
	// "your account was closed" email, going out on every retry.
	audit(db, c, "delete", "users", c.Param("id"))

	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	c.Status(http.StatusNoContent)
}

// DeleteUserSafe soft-deletes behind a guard.
//
//	204 -- deleted just now, side effects fired exactly once
//	410 -- you already deleted this (tombstone found)
//	404 -- this never existed
//
// The 410/404 split is what lets a client write correct retry logic:
// "404 might be my typo, 410 means my earlier call worked."
func (h *Handler) DeleteUserSafe(c *gin.Context) {
	db := h.db(c)
	id := c.Param("id")

	tag, err := db.Exec(c, `
		UPDATE users SET deleted_at = now()
		 WHERE id = $1::uuid AND deleted_at IS NULL`, id)
	if err != nil {
		fail(c, err)
		return
	}

	if tag.RowsAffected() == 0 {
		var tombstone bool
		if err := db.QueryRow(c,
			`SELECT deleted_at IS NOT NULL FROM users WHERE id = $1::uuid`, id).
			Scan(&tombstone); errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
			return
		} else if err != nil {
			fail(c, err)
			return
		}
		c.JSON(http.StatusGone, gin.H{"error": "already_deleted"})
		return
	}

	// Reached only on the first successful delete.
	audit(db, c, "delete", "users", id)
	c.Status(http.StatusNoContent)
}

func fail(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}
