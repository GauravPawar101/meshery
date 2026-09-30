package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gorilla/mux"
	"github.com/meshery/meshery/server/models"
	"github.com/meshery/meshkit/database"
	"github.com/meshery/meshkit/models/events"
)

const (
	// deleteContextMissingID is a well-formed id that no context was ever saved
	// under, so the local provider's lookup fails.
	deleteContextMissingID = "11111111-2222-3333-4444-555555555555"

	// deleteContextProviderError is the text the local provider (gorm) returns
	// for that failed lookup. It belongs in the server log only: it must not
	// reach the HTTP response or a persisted/broadcast event.
	deleteContextProviderError = "record not found"

	// deleteContextClientDetail is the fixed detail the handler substitutes for
	// the provider's error in everything a client can see.
	deleteContextClientDetail = "unable to fetch kubernetes context"
)

// runDeleteContextLookupFailure calls DeleteContext for a context that does not
// exist, over an in-memory database and the real local provider (no stub, so the
// error is the one the provider genuinely returns). It returns the recorded
// response, every event persisted afterwards, and a subscription to the
// requesting user's event stream.
func runDeleteContextLookupFailure(t *testing.T) (*httptest.ResponseRecorder, []events.Event, <-chan interface{}) {
	t.Helper()

	db, err := database.New(database.Options{Engine: database.SQLITE, Filename: ":memory:"})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := db.AutoMigrate(models.K8sContext{}, events.Event{}); err != nil {
		t.Fatalf("migrate tables: %v", err)
	}

	systemID := uuid.Must(uuid.NewV4())
	userID := uuid.Must(uuid.NewV4())
	h := &Handler{
		config:   &models.HandlerConfig{EventBroadcaster: models.NewBroadcaster("test")},
		log:      newTestLogger(t),
		SystemID: &systemID,
	}
	provider := &models.DefaultLocalProvider{
		MesheryK8sContextPersister: &models.MesheryK8sContextPersister{DB: &db},
		EventsPersister:            &models.EventsPersister{DB: &db},
	}

	published, unsubscribe := h.config.EventBroadcaster.Subscribe(userID)
	t.Cleanup(unsubscribe)

	req := httptest.NewRequest(http.MethodDelete, "/api/system/kubernetes/contexts/"+deleteContextMissingID, nil)
	req = mux.SetURLVars(req, map[string]string{"id": deleteContextMissingID})
	req = req.WithContext(context.WithValue(req.Context(), models.TokenCtxKey, "test-token"))
	rec := httptest.NewRecorder()

	// Before the fix the handler fell through into machine initialisation with an
	// empty context. Report that as a failing test rather than a crashed binary.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DeleteContext panicked after a failed context lookup: %v", r)
			}
		}()
		h.DeleteContext(rec, req, nil, &models.User{ID: userID}, provider)
	}()

	var persisted []events.Event
	if err := db.Find(&persisted).Error; err != nil {
		t.Fatalf("read persisted events: %v", err)
	}
	return rec, persisted, published
}

// TestDeleteContext_LookupFailureReturnsClientSafeError pins the HTTP side of the
// fix. A failed lookup must end the request with a 500 MeshKit error, and the
// body must carry only the fixed detail - never the provider's own error text,
// which ErrGetK8sContexts would otherwise copy into longDescription.
func TestDeleteContext_LookupFailureReturnsClientSafeError(t *testing.T) {
	rec, _, _ := runDeleteContextLookupFailure(t)
	body := rec.Body.String()

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusInternalServerError, body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response is not JSON: %v. body: %s", err, body)
	}
	if payload["code"] != ErrGetK8sContextsCode {
		t.Errorf("code = %v, want %s", payload["code"], ErrGetK8sContextsCode)
	}
	if !strings.Contains(body, deleteContextClientDetail) {
		t.Errorf("response is missing the fixed detail %q. body: %s", deleteContextClientDetail, body)
	}
	if strings.Contains(body, deleteContextProviderError) {
		t.Errorf("response leaks the provider error %q. body: %s", deleteContextProviderError, body)
	}
}

// TestDeleteContext_LookupFailurePersistsAndPublishesOneErrorEvent pins the event
// side. Exactly one Error event, addressed to the requested context, must be
// persisted and broadcast to the user. It must not be overwritten by, or
// followed by, the "Delete request received" informational event the success
// path emits, and it must not carry the provider's error text either.
func TestDeleteContext_LookupFailurePersistsAndPublishesOneErrorEvent(t *testing.T) {
	_, persisted, published := runDeleteContextLookupFailure(t)

	if len(persisted) != 1 {
		t.Fatalf("persisted %d events, want exactly 1: %+v", len(persisted), persisted)
	}
	got := persisted[0]

	if got.Severity != events.Error {
		t.Errorf("severity = %q, want %q", got.Severity, events.Error)
	}
	if got.ActedUpon.String() != deleteContextMissingID {
		t.Errorf("actedUpon = %s, want %s", got.ActedUpon, deleteContextMissingID)
	}
	if !strings.Contains(got.Description, deleteContextMissingID) {
		t.Errorf("description %q does not name context %s", got.Description, deleteContextMissingID)
	}
	if strings.Contains(got.Description, "Delete request received") {
		t.Errorf("description %q is the success-path event, so the handler fell through", got.Description)
	}

	metadata, err := json.Marshal(got.Metadata)
	if err != nil {
		t.Fatalf("marshal event metadata: %v", err)
	}
	if !strings.Contains(string(metadata), deleteContextClientDetail) {
		t.Errorf("event metadata is missing the fixed detail %q: %s", deleteContextClientDetail, metadata)
	}
	if strings.Contains(string(metadata), deleteContextProviderError) {
		t.Errorf("event metadata leaks the provider error %q: %s", deleteContextProviderError, metadata)
	}

	// Publish runs in its own goroutine, so wait for delivery.
	select {
	case msg := <-published:
		evt, ok := msg.(*events.Event)
		if !ok {
			t.Fatalf("published %T, want *events.Event", msg)
		}
		if evt.Severity != events.Error {
			t.Errorf("published severity = %q, want %q", evt.Severity, events.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event was published to the user's stream")
	}
}
