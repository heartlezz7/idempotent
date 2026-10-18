package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/heartlezz7/idempotent/src/model"
	"github.com/jackc/pgx/v5"
)

// =====================================================================
// information exists to demonstrate the OTHER mechanism.
//
// Unlike users, this resource has a naturally unique field: phone.
// That single index gives POST idempotency for free -- no key table,
// no TTL sweeper, no lease, no state machine. The database refuses the
// duplicate row on its own.
//
// The only design question left is what you RETURN when the constraint
// fires, and that is where the naive and safe versions differ.
// =====================================================================

// @Summary		List information
// @Description	Pure read; identical on both trees.
// @Tags			information
// @Produce		json
// @Success		200	{array}		model.Information
// @Failure		500	{object}	model.ErrorResponse
// @Router			/naive/information [get]
// @Router			/safe/information [get]
func (h *Handler) ListInformation(c *gin.Context) {
	rows, err := h.db(c).Query(c, `
		SELECT id, phone, age, version FROM information
		 WHERE deleted_at IS NULL ORDER BY created_at`)
	if err != nil {
		fail(c, err)
		return
	}
	defer rows.Close()

	out := []model.Information{}
	for rows.Next() {
		var i model.Information
		if err := rows.Scan(&i.ID, &i.Phone, &i.Age, &i.Version); err != nil {
			fail(c, err)
			return
		}
		out = append(out, i)
	}
	c.JSON(http.StatusOK, out)
}

// @Summary	Get information (conditional GET)
// @Tags		information
// @Produce	json
// @Param		id				path		string	true	"Information ID (uuid)"
// @Param		If-None-Match	header		string	false	"ETag from a previous GET"
// @Success	200				{object}	model.Information
// @Success	304				"Not modified"
// @Failure	404				{object}	model.ErrorResponse
// @Failure	500				{object}	model.ErrorResponse
// @Router		/naive/information/{id} [get]
// @Router		/safe/information/{id} [get]
func (h *Handler) GetInformation(c *gin.Context) {
	var i model.Information
	err := h.db(c).QueryRow(c, `
		SELECT id, phone, age, version FROM information
		 WHERE id = $1::uuid AND deleted_at IS NULL`, c.Param("id")).
		Scan(&i.ID, &i.Phone, &i.Age, &i.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	tag := etag(i.Version)
	c.Header("ETag", tag)
	if c.GetHeader("If-None-Match") == tag {
		c.Status(http.StatusNotModified)
		return
	}
	c.JSON(http.StatusOK, i)
}

// CreateInformationNaive does not handle the constraint violation.
//
// The DATA is still safe -- Postgres refuses the second row either way.
// But the API is not: the retry gets a 500 with a leaked constraint
// name, and the client has no way to tell "your request already
// succeeded" from "the database is broken, keep retrying".
//
// This is the common half-fix. The constraint protects the table; the
// error handling is what makes the endpoint usable.
//
//	@Summary		Create information (naive)
//	@Description	The unique phone index still blocks the duplicate, but the retry gets a 500 that leaks the constraint name.
//	@Tags			information
//	@Accept			json
//	@Produce		json
//	@Param			body	body		model.CreateInformationReq	true	"New record"
//	@Success		201		{object}	model.Information
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/naive/information [post]
func (h *Handler) CreateInformationNaive(c *gin.Context) {
	var in model.CreateInformationReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	id := uuid.NewString()
	if _, err := db.Exec(c, `
		INSERT INTO information (id, phone, age) VALUES ($1::uuid, $2, $3)`,
		id, in.Phone, in.Age); err != nil {
		fail(c, err) // leaks: information_phone_live violation
		return
	}

	audit(db, c, "create", "information", id)
	c.JSON(http.StatusCreated, model.Information{ID: id, Phone: in.Phone, Age: in.Age, Version: 1})
}

// CreateInformationSafe catches 23505 and returns the resource that
// already exists, with 200 instead of 201.
//
// No Idempotency-Key header, no receipt table, no lease. The unique
// index IS the dedup. Reach for this before building key infrastructure.
//
//	@Summary		Create information (safe)
//	@Description	No Idempotency-Key needed: the unique phone index is the dedup. A repeat returns 200 with the existing record.
//	@Tags			information
//	@Accept			json
//	@Produce		json
//	@Param			body	body		model.CreateInformationReq	true	"New record"
//	@Param			X-Chaos	header		string						false	"fail-after-write: return 500 after the insert"	Enums(fail-after-write)
//	@Success		200		{object}	model.Information			"Already existed (Idempotent-Replayed: true)"
//	@Success		201		{object}	model.Information			"Created"
//	@Header			200,201	{string}	Location					"URI of the record"
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/safe/information [post]
func (h *Handler) CreateInformationSafe(c *gin.Context) {
	var in model.CreateInformationReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	id := uuid.NewString()
	_, err := db.Exec(c, `
		INSERT INTO information (id, phone, age) VALUES ($1::uuid, $2, $3)`,
		id, in.Phone, in.Age)

	if isUniqueViolation(err) {
		var existing model.Information
		if err := db.QueryRow(c, `
			SELECT id, phone, age, version FROM information
			 WHERE phone = $1 AND deleted_at IS NULL`, in.Phone).
			Scan(&existing.ID, &existing.Phone, &existing.Age, &existing.Version); err != nil {
			fail(c, err)
			return
		}
		// 200, not 201: nothing was created by THIS call.
		c.Header("Location", c.Request.URL.Path+"/"+existing.ID)
		c.Header("Idempotent-Replayed", "true")
		c.JSON(http.StatusOK, existing)
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	audit(db, c, "create", "information", id)

	if chaos(c) {
		return
	}

	c.Header("Location", c.Request.URL.Path+"/"+id)
	c.JSON(http.StatusCreated, model.Information{ID: id, Phone: in.Phone, Age: in.Age, Version: 1})
}

// PutInformationSafe: full replacement at a client-known URI. Both
// fields are always supplied, so this is a pure assignment.
//
//	@Summary		Replace information
//	@Description	Pure assignment at a client-known URI: 201 on create, 200 on replace.
//	@Tags			information
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"Information ID (uuid)"
//	@Param			body	body		model.CreateInformationReq	true	"Full replacement"
//	@Success		200		{object}	model.Information
//	@Success		201		{object}	model.Information
//	@Header			200,201	{string}	ETag	"Current version"
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		409		{object}	model.ErrorResponse	"phone_already_taken"
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/safe/information/{id} [put]
func (h *Handler) PutInformationSafe(c *gin.Context) {
	var in model.CreateInformationReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	var (
		i        model.Information
		inserted bool
		changed  bool
	)
	err := db.QueryRow(c, `
		WITH prev AS (
			SELECT phone, age FROM information WHERE id = $1::uuid
		), up AS (
			INSERT INTO information (id, phone, age, version)
			VALUES ($1::uuid, $2::text, $3::int, 1)
			ON CONFLICT (id) DO UPDATE
			   SET phone   = EXCLUDED.phone,
			       age     = EXCLUDED.age,
			       version = CASE
			           WHEN information.phone = EXCLUDED.phone
			            AND information.age   = EXCLUDED.age
			           THEN information.version
			           ELSE information.version + 1 END
			RETURNING (xmax = 0) AS inserted, id, phone, age, version
		)
		SELECT up.inserted, up.id, up.phone, up.age, up.version,
		       ((SELECT phone FROM prev) IS DISTINCT FROM up.phone
		     OR  (SELECT age   FROM prev) IS DISTINCT FROM up.age) AS changed
		  FROM up`, c.Param("id"), in.Phone, in.Age).
		Scan(&inserted, &i.ID, &i.Phone, &i.Age, &i.Version, &changed)
	if isUniqueViolation(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "phone_already_taken"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	if changed {
		audit(db, c, "replace", "information", i.ID)
	}

	c.Header("ETag", etag(i.Version))
	if inserted {
		c.JSON(http.StatusCreated, i)
		return
	}
	c.JSON(http.StatusOK, i)
}

// PatchInformationNaive: `age = age + n`. The textbook non-idempotent
// patch. Three retries age the person by three years.
//
//	@Summary		Patch information (naive)
//	@Description	age = age + age_increment. Three retries age the person three times.
//	@Tags			information
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string							true	"Information ID (uuid)"
//	@Param			body	body		model.PatchInformationNaiveReq	true	"Accumulating patch"
//	@Success		200		{object}	model.Information
//	@Failure		400		{object}	model.ErrorResponse
//	@Failure		404		{object}	model.ErrorResponse
//	@Failure		500		{object}	model.ErrorResponse
//	@Router			/naive/information/{id} [patch]
func (h *Handler) PatchInformationNaive(c *gin.Context) {
	var in model.PatchInformationNaiveReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

	db := h.db(c)
	var i model.Information
	err := db.QueryRow(c, `
		UPDATE information
		   SET age     = age + $2::int,   -- accumulation
		       version = version + 1
		 WHERE id = $1::uuid AND deleted_at IS NULL
		RETURNING id, phone, age, version`, c.Param("id"), in.AgeIncrement).
		Scan(&i.ID, &i.Phone, &i.Age, &i.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	audit(db, c, "patch", "information", i.ID)
	c.JSON(http.StatusOK, i)
}

// PatchInformationSafe: merge patch with absolute values, guarded by
// If-Match. Sending {"age": 31} ten times leaves age at 31.
//
//	@Summary		Patch information (safe)
//	@Description	Merge patch with absolute values and a mandatory If-Match.
//	@Tags			information
//	@Accept			json
//	@Produce		json
//	@Param			id			path		string						true	"Information ID (uuid)"
//	@Param			body		body		model.PatchInformationReq	true	"Merge patch"
//	@Param			If-Match	header		string						true	"ETag from GET"
//	@Success		200			{object}	model.Information
//	@Header			200			{string}	ETag	"Current version"
//	@Failure		400			{object}	model.ErrorResponse
//	@Failure		409			{object}	model.ErrorResponse	"phone_already_taken"
//	@Failure		412			{object}	model.ErrorResponse
//	@Failure		428			{object}	model.ErrorResponse
//	@Failure		500			{object}	model.ErrorResponse
//	@Router			/safe/information/{id} [patch]
func (h *Handler) PatchInformationSafe(c *gin.Context) {
	var in model.PatchInformationReq
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body"})
		return
	}

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
		i       model.Information
		changed bool
	)
	err := db.QueryRow(c, `
		WITH prev AS (
			SELECT phone, age FROM information WHERE id = $1::uuid
		), up AS (
			UPDATE information
			   SET phone   = COALESCE($3::text, phone),
			       age     = COALESCE($4::int,  age),
			       version = CASE
			           WHEN phone IS NOT DISTINCT FROM COALESCE($3::text, phone)
			            AND age   IS NOT DISTINCT FROM COALESCE($4::int,  age)
			           THEN version ELSE version + 1 END
			 WHERE id = $1::uuid AND version = $2 AND deleted_at IS NULL
			RETURNING id, phone, age, version
		)
		SELECT up.id, up.phone, up.age, up.version,
		       ((SELECT phone FROM prev) IS DISTINCT FROM up.phone
		     OR  (SELECT age   FROM prev) IS DISTINCT FROM up.age) AS changed
		  FROM up`, c.Param("id"), wantVersion, in.Phone, in.Age).
		Scan(&i.ID, &i.Phone, &i.Age, &i.Version, &changed)

	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusPreconditionFailed, gin.H{
			"error": "precondition_failed",
			"hint":  "gone, or someone else advanced the version",
		})
		return
	}
	if isUniqueViolation(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "phone_already_taken"})
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	if changed {
		audit(db, c, "patch", "information", i.ID)
	}
	c.Header("ETag", etag(i.Version))
	c.JSON(http.StatusOK, i)
}

// @Summary		Delete information (naive)
// @Description	Hard delete; audits on every call.
// @Tags			information
// @Produce		json
// @Param			id	path	string	true	"Information ID (uuid)"
// @Success		204	"Deleted"
// @Failure		404	{object}	model.ErrorResponse
// @Failure		500	{object}	model.ErrorResponse
// @Router			/naive/information/{id} [delete]
func (h *Handler) DeleteInformationNaive(c *gin.Context) {
	db := h.db(c)
	tag, err := db.Exec(c, `DELETE FROM information WHERE id = $1::uuid`, c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	audit(db, c, "delete", "information", c.Param("id")) // unguarded

	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	c.Status(http.StatusNoContent)
}

// @Summary		Delete information (safe)
// @Description	Guarded soft delete: 204, then 410.
// @Tags			information
// @Produce		json
// @Param			id	path	string	true	"Information ID (uuid)"
// @Success		204	"Deleted just now"
// @Failure		404	{object}	model.ErrorResponse
// @Failure		410	{object}	model.ErrorResponse
// @Failure		500	{object}	model.ErrorResponse
// @Router			/safe/information/{id} [delete]
func (h *Handler) DeleteInformationSafe(c *gin.Context) {
	db := h.db(c)
	id := c.Param("id")

	tag, err := db.Exec(c, `
		UPDATE information SET deleted_at = now()
		 WHERE id = $1::uuid AND deleted_at IS NULL`, id)
	if err != nil {
		fail(c, err)
		return
	}

	if tag.RowsAffected() == 0 {
		var tombstoned bool
		err := db.QueryRow(c,
			`SELECT deleted_at IS NOT NULL FROM information WHERE id = $1::uuid`, id).
			Scan(&tombstoned)
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
			return
		}
		if err != nil {
			fail(c, err)
			return
		}
		c.JSON(http.StatusGone, gin.H{"error": "already_deleted"})
		return
	}

	audit(db, c, "delete", "information", id)
	c.Status(http.StatusNoContent)
}
