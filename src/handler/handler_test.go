package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/heartlezz7/idempotent/src/handler"
	"github.com/heartlezz7/idempotent/src/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	h := handler.New(pool)
	r := gin.New()
	r.GET("/_stats", h.Stats)
	r.POST("/_reset", h.Reset)
	r.POST("/publish", h.Publish)
	r.POST("/naive/consume", h.NaiveConsume)
	r.POST("/safe/consume", h.SafeConsume)
	r.POST("/relay/run", h.RelayRun)
	r.POST("/_duplicate", h.ForceRedeliver)
	r.POST("/_expire_lease", h.ForceExpireLease)
	return r
}

func do(t *testing.T, r *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return m
}

func TestPublish_MissingTopicIsBadRequest(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	rec := do(t, r, http.MethodPost, "/publish", map[string]any{"payload": map[string]any{"x": 1}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a missing topic, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPublish_CreatesAClaimableMessage(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	rec := do(t, r, http.MethodPost, "/publish", map[string]any{
		"topic":   "orders",
		"payload": map[string]any{"order_id": "o-1", "status": "paid", "version": 1},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["deduped"] != false {
		t.Fatalf("expected deduped=false on first publish, got %v", body)
	}
}

func TestPublish_DedupeConflictReturns422(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	req := map[string]any{
		"topic":      "orders",
		"dedupe_key": "dup-key",
		"payload":    map[string]any{"amount": 100},
	}
	if rec := do(t, r, http.MethodPost, "/publish", req); rec.Code != http.StatusCreated {
		t.Fatalf("first publish: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	req["payload"] = map[string]any{"amount": 999}
	rec := do(t, r, http.MethodPost, "/publish", req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for a dedupe_key reused with a different payload, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestNaiveConsume_NoMessagesAvailable(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	rec := do(t, r, http.MethodPost, "/naive/consume?topic=orders", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 even with nothing to claim, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["message"] != "no messages available" {
		t.Fatalf("expected the no-messages response, got %v", body)
	}
}

func TestSafeConsume_EndToEndAppliesOnce(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	do(t, r, http.MethodPost, "/publish", map[string]any{
		"topic":   "orders",
		"payload": map[string]any{"order_id": "o-2", "status": "paid", "version": 1},
	})
	rec := do(t, r, http.MethodPost, "/safe/consume?topic=orders", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["applied"] != true || body["acked"] != true {
		t.Fatalf("expected applied=true, acked=true, got %v", body)
	}
}

func TestSafeConsume_ChaosCrashBeforeCommitReportsUnappliedNotAcked(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	do(t, r, http.MethodPost, "/publish", map[string]any{
		"topic":   "orders",
		"payload": map[string]any{"order_id": "o-3", "status": "paid", "version": 1},
	})
	rec := do(t, r, http.MethodPost, "/safe/consume?topic=orders&chaos=crash_before_commit", nil)
	// A chaos-triggered rollback is a deliberate lab outcome, not a server error.
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a simulated chaos crash, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["applied"] != false || body["acked"] != false {
		t.Fatalf("expected applied=false, acked=false after a simulated crash, got %v", body)
	}
}

func TestForceRedeliverAndForceExpireLease_RequireMessageID(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	if rec := do(t, r, http.MethodPost, "/_duplicate", map[string]any{}); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without message_id, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, r, http.MethodPost, "/_expire_lease", map[string]any{}); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without message_id, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStatsAndReset(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	do(t, r, http.MethodPost, "/publish", map[string]any{
		"topic":   "orders",
		"payload": map[string]any{"order_id": "o-4", "status": "paid", "version": 1},
	})
	do(t, r, http.MethodPost, "/safe/consume?topic=orders", nil)

	rec := do(t, r, http.MethodGet, "/_stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /_stats, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["orders"] != float64(1) {
		t.Fatalf("expected 1 order before reset, got %v", body["orders"])
	}

	if rec := do(t, r, http.MethodPost, "/_reset", nil); rec.Code != http.StatusOK {
		t.Fatalf("reset: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = do(t, r, http.MethodGet, "/_stats", nil)
	body = decode(t, rec)
	if body["orders"] != float64(0) {
		t.Fatalf("expected 0 orders after reset, got %v", body["orders"])
	}
}

func TestRelayRun_PublishesPendingOutboxEvents(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	r := newRouter(pool)

	do(t, r, http.MethodPost, "/publish", map[string]any{
		"topic":   "orders",
		"payload": map[string]any{"order_id": "o-5", "status": "paid", "version": 1},
	})
	do(t, r, http.MethodPost, "/safe/consume?topic=orders", nil)

	rec := do(t, r, http.MethodPost, "/relay/run", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["published"] != float64(1) {
		t.Fatalf("expected 1 event published, got %v", body)
	}
}
