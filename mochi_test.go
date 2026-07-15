package mochi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-analytics/mochi-go"
)

type call struct {
	url  string
	body map[string]any
}

// mockTransport records calls and returns the status/headers chosen by responder
// for each call index. It unmarshals the wire body so tests can assert on it.
func mockTransport(responder func(index int) (int, http.Header)) (*[]call, mochi.Transport) {
	var mu sync.Mutex
	calls := []call{}
	t := func(url string, body []byte) (int, string, http.Header, error) {
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		mu.Lock()
		index := len(calls)
		calls = append(calls, call{url: url, body: m})
		mu.Unlock()
		status, headers := responder(index)
		return status, "{}", headers, nil
	}
	return &calls, t
}

func newClient(t mochi.Transport, opts mochi.Options) (*mochi.Client, *[]error) {
	var mu sync.Mutex
	errs := []error{}
	opts.URL = "http://localhost:9999/"
	opts.APIKey = "mochi_sk_test"
	opts.FlushInterval = time.Minute // effectively disabled; tests flush manually
	opts.MaxRetries = 2
	opts.Transport = t
	opts.OnError = func(e error) {
		mu.Lock()
		errs = append(errs, e)
		mu.Unlock()
	}
	return mochi.New(opts), &errs
}

func eventNames(calls []call) []string {
	var names []string
	for _, c := range calls {
		for _, e := range c.body["events"].([]any) {
			names = append(names, e.(map[string]any)["name"].(string))
		}
	}
	return names
}

func TestBatchesEventsIntoOneRequest(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{})

	c.Track(mochi.Event{Type: mochi.EventCommand, Name: "play", UserID: "1"})
	c.Track(mochi.Event{Type: mochi.EventGuildJoin, GuildID: "2"})
	c.Flush()
	c.Shutdown()

	if len(*calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(*calls))
	}
	if got := (*calls)[0].url; got != "http://localhost:9999/api/v1/ingest" {
		t.Fatalf("unexpected url %q", got)
	}
	events := (*calls)[0].body["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	first := events[0].(map[string]any)
	if first["userId"] != "1" {
		t.Fatalf("want userId camelCase 1, got %v", first["userId"])
	}
	if first["ts"] == nil || first["ts"] == "" {
		t.Fatalf("ts should default to enqueue time")
	}
}

func TestAutoFlushesWhenBatchSizeReached(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{MaxBatchSize: 5})

	for i := 0; i < 5; i++ {
		c.Track(mochi.Event{Type: mochi.EventCommand, Name: "x"})
	}
	c.Flush()
	c.Shutdown()

	total := 0
	for _, cl := range *calls {
		total += len(cl.body["events"].([]any))
	}
	if total != 5 {
		t.Fatalf("want 5 events total, got %d", total)
	}
}

func TestSplitsOversizedQueueIntoMultipleBatches(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{MaxBatchSize: 10})

	for i := 0; i < 25; i++ {
		c.Track(mochi.Event{Type: mochi.EventCommand, Name: "x"})
	}
	c.Flush()
	c.Shutdown()

	total := 0
	for _, cl := range *calls {
		total += len(cl.body["events"].([]any))
	}
	if total != 25 {
		t.Fatalf("want 25 events total, got %d", total)
	}
	if len(*calls) < 3 {
		t.Fatalf("want >=3 batches, got %d", len(*calls))
	}
}

func TestRetriesRetryableFailureThenSucceeds(t *testing.T) {
	calls, transport := mockTransport(func(index int) (int, http.Header) {
		if index == 0 {
			return 503, nil
		}
		return 202, nil
	})
	c, errs := newClient(transport, mochi.Options{})

	c.Track(mochi.Event{Type: mochi.EventCommand, Name: "play"})
	c.Flush()
	c.Shutdown()

	if len(*calls) != 2 {
		t.Fatalf("want 2 calls (1 retry), got %d", len(*calls))
	}
	if len(*errs) != 0 {
		t.Fatalf("want no errors, got %v", *errs)
	}
}

func TestDropsBatchAndReportsOnNonRetryableError(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 400, nil })
	c, errs := newClient(transport, mochi.Options{})

	c.Track(mochi.Event{Type: mochi.EventCommand, Name: "play"})
	c.Flush()
	c.Shutdown()

	if len(*calls) != 1 {
		t.Fatalf("want 1 call (no retries on 400), got %d", len(*calls))
	}
	if len(*errs) != 1 {
		t.Fatalf("want 1 reported error, got %d", len(*errs))
	}
}

func TestDropsOldestEventsOnQueueOverflow(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, errs := newClient(transport, mochi.Options{MaxQueueSize: 3, MaxBatchSize: 100})

	for i := 0; i < 5; i++ {
		c.Track(mochi.Event{Type: mochi.EventCustom, Name: []string{"event-0", "event-1", "event-2", "event-3", "event-4"}[i]})
	}
	c.Flush()
	c.Shutdown()

	if len(*errs) == 0 {
		t.Fatalf("want overflow errors reported")
	}
	names := eventNames(*calls)
	want := []string{"event-2", "event-3", "event-4"}
	if len(names) != len(want) {
		t.Fatalf("want %v, got %v", want, names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("want %v, got %v", want, names)
		}
	}
}

func TestSendsSnapshotsImmediately(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{})

	c.Snapshot(mochi.Snapshot{GuildCount: 42, WsPingMs: mochi.Ptr(30)})
	c.Shutdown()

	if len(*calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(*calls))
	}
	if got := (*calls)[0].url; got != "http://localhost:9999/api/v1/snapshot" {
		t.Fatalf("unexpected url %q", got)
	}
	if (*calls)[0].body["guildCount"] != float64(42) {
		t.Fatalf("want guildCount 42, got %v", (*calls)[0].body["guildCount"])
	}
}

func TestSnapshotAutoAttachesResources(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{})

	c.Snapshot(mochi.Snapshot{GuildCount: 1})
	c.Shutdown()

	mem, ok := (*calls)[0].body["memoryMb"].(float64)
	if !ok || mem <= 0 {
		t.Fatalf("want positive memoryMb, got %v", (*calls)[0].body["memoryMb"])
	}
}

func TestSnapshotCallerValueWinsOverMeasurement(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{})

	c.Snapshot(mochi.Snapshot{GuildCount: 1, MemoryMb: mochi.Ptr(7), CPUPercent: mochi.Ptr(0.0)})
	c.Shutdown()

	if got := (*calls)[0].body["memoryMb"]; got != float64(7) {
		t.Fatalf("want caller memoryMb 7, got %v", got)
	}
	if got := (*calls)[0].body["cpuPercent"]; got != float64(0) {
		t.Fatalf("want caller cpuPercent 0, got %v", got)
	}
}

func TestHonorsRetryAfterOn429(t *testing.T) {
	var mu sync.Mutex
	count := 0
	transport := func(url string, body []byte) (int, string, http.Header, error) {
		mu.Lock()
		i := count
		count++
		mu.Unlock()
		if i == 0 {
			h := http.Header{}
			h.Set("Retry-After", "1")
			return 429, "{}", h, nil
		}
		return 202, "{}", nil, nil
	}
	c, errs := newClient(transport, mochi.Options{})

	start := time.Now()
	c.Track(mochi.Event{Type: mochi.EventCommand, Name: "play"})
	c.Flush()
	c.Shutdown()

	if count != 2 {
		t.Fatalf("want 2 calls, got %d", count)
	}
	if len(*errs) != 0 {
		t.Fatalf("want no errors, got %v", *errs)
	}
	// Retry-After: 1s must dominate the 500ms computed backoff.
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("want >=900ms (honored Retry-After), got %v", elapsed)
	}
}

func TestNeverPanicsWhenOnErrorPanics(t *testing.T) {
	_, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c := mochi.New(mochi.Options{
		URL:           "http://localhost:9999/",
		APIKey:        "mochi_sk_test",
		FlushInterval: time.Minute,
		MaxQueueSize:  1,
		Transport:     transport,
		OnError:       func(error) { panic("handler blew up") },
	})
	defer c.Shutdown()

	// Overflow reports from inside Track(); a panicking handler must not escape.
	c.Track(mochi.Event{Type: mochi.EventCustom, Name: "a"})
	c.Track(mochi.Event{Type: mochi.EventCustom, Name: "b"})
}

// TestSerializationOmitsUnsetKeepsFalsy pins the tricky wire cases from the
// core spec: unset optionals are omitted, but shardId:0 / success:false are kept.
func TestSerializationOmitsUnsetKeepsFalsy(t *testing.T) {
	calls, transport := mockTransport(func(int) (int, http.Header) { return 202, nil })
	c, _ := newClient(transport, mochi.Options{})

	c.Track(mochi.Event{
		Type:    mochi.EventCommand,
		Name:    "ping",
		ShardID: mochi.Ptr(0),
		Success: mochi.Ptr(false),
	})
	c.Flush()
	c.Shutdown()

	e := (*calls)[0].body["events"].([]any)[0].(map[string]any)
	if _, ok := e["guildId"]; ok {
		t.Fatalf("unset guildId should be omitted, got %v", e["guildId"])
	}
	if e["shardId"] != float64(0) {
		t.Fatalf("shardId:0 should be kept, got %v (present=%v)", e["shardId"], e["shardId"] != nil)
	}
	if e["success"] != false {
		t.Fatalf("success:false should be kept, got %v", e["success"])
	}
}

func TestDefaultTransportSetsAuthAndPath(t *testing.T) {
	var mu sync.Mutex
	var gotAuth, gotCT, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		mu.Unlock()
		w.WriteHeader(202)
		_, _ = w.Write([]byte("{}"))
	}))
	defer srv.Close()

	c := mochi.New(mochi.Options{URL: srv.URL, APIKey: "mochi_sk_test", FlushInterval: time.Minute})
	c.Track(mochi.Event{Type: mochi.EventCommand, Name: "play", UserID: "1"})
	c.Flush()
	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer mochi_sk_test" {
		t.Fatalf("want auth header, got %q", gotAuth)
	}
	if gotPath != "/api/v1/ingest" {
		t.Fatalf("want ingest path, got %q", gotPath)
	}
	if gotCT != "application/json" {
		t.Fatalf("want json content-type, got %q", gotCT)
	}
	if events, ok := gotBody["events"].([]any); !ok || len(events) != 1 {
		t.Fatalf("want 1 event in body, got %v", gotBody)
	}
}
