// Package mochi is the Go core client for Mochi, self-hosted analytics for
// Discord bots. It batches events and delivers them to a Mochi instance over
// HTTP with retries.
//
// It conforms to the Mochi core spec v1.0.0 (see the `core` repo). Its
// overriding design constraint: analytics must never crash, block, or slow the
// host bot. Track is non-blocking and no method panics into the caller;
// failures are routed to OnError.
//
// A library adapter (for discordgo, etc.) sits on top of this core and maps
// gateway events to Track calls.
package mochi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

// rfc3339Milli matches the millisecond-precision, Z-suffixed timestamps the
// reference cores emit, e.g. "2026-07-07T12:00:00.000Z".
const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

// EventType is the kind of an analytics event.
type EventType string

const (
	EventCommand    EventType = "command"
	EventGuildJoin  EventType = "guild_join"
	EventGuildLeave EventType = "guild_leave"
	EventError      EventType = "error"
	EventCustom     EventType = "custom"
)

// ChannelType describes where an event occurred.
type ChannelType string

const (
	ChannelGuildText  ChannelType = "guild_text"
	ChannelGuildVoice ChannelType = "guild_voice"
	ChannelThread     ChannelType = "thread"
	ChannelDM         ChannelType = "dm"
	ChannelGroupDM    ChannelType = "group_dm"
	ChannelOther      ChannelType = "other"
)

// Event is a single analytics event. It serializes to the camelCase Mochi wire
// format; unset optional fields are omitted. Scalar optionals that have a
// meaningful zero value (ShardID, Success, DurationMs) are pointers so that
// e.g. shardId:0 and success:false are sent rather than dropped by omitempty.
type Event struct {
	Type EventType `json:"type"`
	// Name is the command or custom-event name. Required for command/custom/error.
	Name        string         `json:"name,omitempty"`
	GuildID     string         `json:"guildId,omitempty"`
	UserID      string         `json:"userId,omitempty"` // hashed server-side, never stored raw
	ChannelType ChannelType    `json:"channelType,omitempty"`
	ShardID     *int           `json:"shardId,omitempty"`
	Success     *bool          `json:"success,omitempty"`
	DurationMs  *int           `json:"durationMs,omitempty"`
	Meta        map[string]any `json:"meta,omitempty"`
	// TS is an ISO 8601 timestamp; defaults to enqueue time when empty.
	TS string `json:"ts,omitempty"`
}

// Snapshot is a guild-count / health sample. GuildCount is always sent (even 0);
// the other fields are omitted when unset.
type Snapshot struct {
	GuildCount           int      `json:"guildCount"`
	ShardID              *int     `json:"shardId,omitempty"`
	TotalShards          *int     `json:"totalShards,omitempty"`
	ApproximateMemberSum *int     `json:"approximateMemberSum,omitempty"`
	WsPingMs             *int     `json:"wsPingMs,omitempty"`
	// CPUPercent is process CPU usage normalized to 0-100 across all cores, and
	// MemoryMb is resident memory in megabytes. Both are filled in automatically
	// by Snapshot when nil; set them to override the auto-measurement.
	CPUPercent *float64 `json:"cpuPercent,omitempty"`
	MemoryMb   *int     `json:"memoryMb,omitempty"`
	TS         string   `json:"ts,omitempty"`
}

type ingestBody struct {
	Events []Event `json:"events"`
}

// Transport sends one request and returns the status, response body, and
// response headers (used to read Retry-After). It is injectable for testing;
// the default uses net/http.
type Transport func(url string, body []byte) (status int, respBody string, headers http.Header, err error)

// Options configures a Client. Zero-valued numeric fields take their default.
type Options struct {
	// URL is the base URL of the Mochi instance, e.g. https://mochi.example.com.
	URL    string
	APIKey string
	// FlushInterval is the background flush cadence. Default 5s.
	FlushInterval time.Duration
	// MaxBatchSize is events per request; clamped to <=100. Default 100.
	MaxBatchSize int
	// MaxQueueSize bounds the queue; overflow drops oldest-first. Default 10000.
	MaxQueueSize int
	// MaxRetries is retry attempts for retryable failures. Default 3.
	MaxRetries int
	// OnError is called on drops and permanent failures. It is guarded: a
	// panicking handler can never crash the bot.
	OnError func(error)
	// Transport is injectable for testing. Defaults to net/http.
	Transport Transport
	// HTTPClient backs the default transport. Defaults to a 10s-timeout client.
	HTTPClient *http.Client
}

var retryableStatus = map[int]bool{429: true, 500: true, 502: true, 503: true, 504: true}

// Client is a batching, non-blocking analytics client. Construct it with New.
type Client struct {
	ingestURL     string
	snapshotURL   string
	apiKey        string
	flushInterval time.Duration
	maxBatchSize  int
	maxQueueSize  int
	maxRetries    int
	onError       func(error)
	transport     Transport

	mu       sync.Mutex // guards queue and shutdown
	queue    []Event
	shutdown bool

	flushMu sync.Mutex    // serializes flushes so drains never run in parallel
	trigger chan struct{} // batch-full signal to the background loop
	stop    chan struct{}
	done    chan struct{}

	// CPU baseline for the delta between snapshots. CPU is a rate, so the first
	// snapshot only records memory and seeds these.
	cpuMu     sync.Mutex
	lastCPU   float64
	lastCPUAt time.Time
}

// New builds a Client and starts its background flush loop. Call Shutdown on
// exit to flush remaining events.
func New(opts Options) *Client {
	flush := opts.FlushInterval
	if flush <= 0 {
		flush = 5 * time.Second
	}
	batch := opts.MaxBatchSize
	if batch <= 0 {
		batch = 100
	}
	if batch > 100 {
		batch = 100
	}
	queue := opts.MaxQueueSize
	if queue <= 0 {
		queue = 10_000
	}
	retries := opts.MaxRetries
	if retries <= 0 {
		retries = 3
	}
	onError := opts.OnError
	if onError == nil {
		onError = func(error) {}
	}

	base := strings.TrimRight(opts.URL, "/")
	c := &Client{
		ingestURL:     base + "/api/v1/ingest",
		snapshotURL:   base + "/api/v1/snapshot",
		apiKey:        opts.APIKey,
		flushInterval: flush,
		maxBatchSize:  batch,
		maxQueueSize:  queue,
		maxRetries:    retries,
		onError:       onError,
		transport:     opts.Transport,
		trigger:       make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	if c.transport == nil {
		httpClient := opts.HTTPClient
		if httpClient == nil {
			httpClient = &http.Client{Timeout: 10 * time.Second}
		}
		c.transport = defaultTransport(httpClient, c.apiKey)
	}

	go c.loop()
	return c
}

// Track queues an event. It returns immediately; sending happens in the
// background. It never panics.
func (c *Client) Track(e Event) {
	c.mu.Lock()
	if c.shutdown {
		c.mu.Unlock()
		return
	}
	if e.TS == "" {
		e.TS = time.Now().UTC().Format(rfc3339Milli)
	}
	c.queue = append(c.queue, e)
	var overflowErr error
	if overflow := len(c.queue) - c.maxQueueSize; overflow > 0 {
		// Drop oldest-first; copy so the trimmed backing array can be freed.
		c.queue = append([]Event(nil), c.queue[overflow:]...)
		overflowErr = errors.New("mochi: event queue overflow, dropped oldest")
	}
	full := len(c.queue) >= c.maxBatchSize
	c.mu.Unlock()

	if overflowErr != nil {
		c.report(overflowErr)
	}
	if full {
		select {
		case c.trigger <- struct{}{}:
		default: // a flush is already pending; nothing to do
		}
	}
}

// TrackCommand is a convenience wrapper for a command event. ctx supplies any
// additional context fields (guild, user, duration, …).
func (c *Client) TrackCommand(name string, ctx Event) {
	ctx.Type = EventCommand
	ctx.Name = name
	c.Track(ctx)
}

// Snapshot sends a guild-count / health snapshot immediately (with retries).
// It never returns an error into the caller; failures go to OnError.
//
// Process CPU and memory are measured and attached automatically; values set
// on s take precedence.
func (c *Client) Snapshot(s Snapshot) {
	cpu, mem := c.collectResources()
	if s.CPUPercent == nil {
		s.CPUPercent = cpu
	}
	if s.MemoryMb == nil {
		s.MemoryMb = mem
	}
	if err := c.send(c.snapshotURL, s); err != nil {
		c.report(err)
	}
}

// collectResources samples process CPU (normalized to 0-100 across all cores,
// relative to the previous snapshot) and memory obtained from the OS. Both are
// best-effort: an unsupported metric yields nil rather than a bogus number.
func (c *Client) collectResources() (cpuPercent *float64, memoryMb *int) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Sys is total memory obtained from the OS; the closest stdlib proxy for RSS.
	mb := int(m.Sys / (1024 * 1024))
	memoryMb = &mb

	cpuSecs, ok := readCPUSeconds()
	if !ok {
		return cpuPercent, memoryMb
	}
	now := time.Now()
	c.cpuMu.Lock()
	defer c.cpuMu.Unlock()
	if !c.lastCPUAt.IsZero() {
		elapsed := now.Sub(c.lastCPUAt).Seconds()
		cores := runtime.NumCPU()
		if elapsed > 0 && cores > 0 {
			pct := (cpuSecs - c.lastCPU) / elapsed / float64(cores) * 100
			pct = math.Round(math.Max(0, pct)*10) / 10
			cpuPercent = &pct
		}
	}
	c.lastCPU = cpuSecs
	c.lastCPUAt = now
	return cpuPercent, memoryMb
}

// readCPUSeconds reads cumulative process CPU seconds via runtime/metrics.
// The metric is unavailable on older Go toolchains, in which case ok is false.
func readCPUSeconds() (float64, bool) {
	const name = "/cpu/classes/total:cpu-seconds"
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64 {
		return 0, false
	}
	return sample[0].Value.Float64(), true
}

// Flush drains the queue now. It is safe to call concurrently; overlapping
// flushes are serialized so a drain never runs in parallel.
func (c *Client) Flush() {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.drain()
}

// Shutdown stops the background loop and flushes remaining events. After
// Shutdown, Track is a no-op. Safe to call more than once.
func (c *Client) Shutdown() {
	c.mu.Lock()
	if c.shutdown {
		c.mu.Unlock()
		return
	}
	c.shutdown = true
	c.mu.Unlock()

	close(c.stop)
	<-c.done // wait for the loop to exit before the final flush
	c.Flush()
}

func (c *Client) loop() {
	defer close(c.done)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.Flush()
		case <-c.trigger:
			c.Flush()
		}
	}
}

func (c *Client) drain() {
	for {
		c.mu.Lock()
		if len(c.queue) == 0 {
			c.mu.Unlock()
			return
		}
		n := c.maxBatchSize
		if n > len(c.queue) {
			n = len(c.queue)
		}
		batch := make([]Event, n)
		copy(batch, c.queue[:n])
		c.queue = append([]Event(nil), c.queue[n:]...)
		c.mu.Unlock()

		if err := c.send(c.ingestURL, ingestBody{Events: batch}); err != nil {
			// Batch is dropped; don't spin on a failing endpoint.
			c.report(err)
			return
		}
	}
}

func (c *Client) send(url string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("mochi: encode failed: %w", err) // permanent
	}
	var lastErr error
	retryAfter := time.Duration(-1) // <0 means "no server-provided delay"
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(500*math.Pow(2, float64(attempt-1))) * time.Millisecond
			if retryAfter >= 0 {
				// Honor Retry-After (from a prior 429) over the computed backoff.
				delay = retryAfter
				retryAfter = -1
			}
			time.Sleep(delay)
		}
		status, respBody, headers, terr := c.transport(url, data)
		if terr != nil {
			lastErr = terr
			continue // network error → retry
		}
		if status >= 200 && status < 300 {
			return nil
		}
		if !retryableStatus[status] {
			return fmt.Errorf("mochi: request rejected (%d) %s", status, respBody)
		}
		if status == 429 {
			if ra := parseRetryAfter(headers.Get("Retry-After")); ra >= 0 {
				retryAfter = ra
			}
		}
		lastErr = fmt.Errorf("mochi: server returned %d", status)
	}
	if lastErr == nil {
		lastErr = errors.New("mochi: request failed")
	}
	return lastErr
}

// report routes an error to OnError, guarding it: a handler must never crash
// the bot.
func (c *Client) report(err error) {
	defer func() { _ = recover() }()
	c.onError(err)
}

// parseRetryAfter parses a Retry-After header (delta-seconds or HTTP-date) into
// a duration, or a negative duration when absent/unparseable.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return -1
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs < 0 {
			return -1
		}
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
		return 0
	}
	return -1
}

func defaultTransport(hc *http.Client, apiKey string) Transport {
	return func(url string, body []byte) (int, string, http.Header, error) {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, "", nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := hc.Do(req)
		if err != nil {
			return 0, "", nil, err
		}
		defer resp.Body.Close()
		text, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(text), resp.Header, nil
	}
}

// Ptr returns a pointer to v — a convenience for setting optional scalar fields
// like ShardID or Success, e.g. mochi.Ptr(0).
func Ptr[T any](v T) *T { return &v }
