package discordgo

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	dgo "github.com/bwmarrin/discordgo"
	mochi "github.com/mochi-analytics/mochi-go"
)

// capture records the wire payloads the core client sends, so these tests
// exercise the full adapter → core → serialization path.
type capture struct {
	mu        sync.Mutex
	events    []map[string]any
	snapshots []map[string]any
}

func (c *capture) transport(url string, body []byte) (int, string, http.Header, error) {
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasSuffix(url, "/api/v1/snapshot") {
		c.snapshots = append(c.snapshots, payload)
	} else if raw, ok := payload["events"].([]any); ok {
		for _, e := range raw {
			c.events = append(c.events, e.(map[string]any))
		}
	}
	return 202, "{}", nil, nil
}

func (c *capture) allEvents() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.events...)
}

func (c *capture) allSnapshots() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.snapshots...)
}

func newHarness(t *testing.T, opts Options) (*adapter, *dgo.Session, *capture) {
	t.Helper()
	rec := &capture{}
	client := mochi.New(mochi.Options{
		URL:           "http://localhost:9999",
		APIKey:        "mochi_sk_test",
		FlushInterval: time.Minute, // tests flush manually
		Transport:     rec.transport,
	})
	t.Cleanup(client.Shutdown)
	a := newAdapter(client, opts)
	t.Cleanup(func() { a.stopOnce.Do(func() { close(a.stop) }) })
	session := &dgo.Session{ShardID: 0, ShardCount: 2, State: dgo.NewState()}
	return a, session, rec
}

func slashInteraction(guildID, userID, channelID, name string, options ...*dgo.ApplicationCommandInteractionDataOption) *dgo.InteractionCreate {
	i := &dgo.Interaction{
		Type:      dgo.InteractionApplicationCommand,
		GuildID:   guildID,
		ChannelID: channelID,
		Data: dgo.ApplicationCommandInteractionData{
			Name:        name,
			CommandType: dgo.ChatApplicationCommand,
			Options:     options,
		},
	}
	if guildID != "" {
		i.Member = &dgo.Member{User: &dgo.User{ID: userID}}
	} else {
		i.User = &dgo.User{ID: userID}
	}
	return &dgo.InteractionCreate{Interaction: i}
}

func meta(e map[string]any) map[string]any {
	m, _ := e["meta"].(map[string]any)
	return m
}

func TestTracksSlashCommandInGuild(t *testing.T) {
	a, s, rec := newHarness(t, Options{})

	a.onInteraction(s, slashInteraction("g1", "u1", "c1", "play"))
	a.client.Flush()

	events := rec.allEvents()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	e := events[0]
	if e["type"] != "command" || e["name"] != "play" {
		t.Fatalf("unexpected type/name: %v", e)
	}
	if e["guildId"] != "g1" || e["userId"] != "u1" {
		t.Fatalf("want guildId g1 / userId u1, got %v / %v", e["guildId"], e["userId"])
	}
	if e["channelType"] != "guild_text" {
		t.Fatalf("want guild_text fallback, got %v", e["channelType"])
	}
	if e["shardId"] != float64(0) {
		t.Fatalf("shardId 0 must be sent, got %v", e["shardId"])
	}
	if meta(e)["source"] != "slash" {
		t.Fatalf("want source slash, got %v", meta(e))
	}
}

func TestTracksDMCommandUsingUserField(t *testing.T) {
	a, s, rec := newHarness(t, Options{})

	a.onInteraction(s, slashInteraction("", "u9", "d1", "help"))
	a.client.Flush()

	e := rec.allEvents()[0]
	if _, ok := e["guildId"]; ok {
		t.Fatalf("DM must omit guildId, got %v", e["guildId"])
	}
	if e["userId"] != "u9" {
		t.Fatalf("want userId from User field, got %v", e["userId"])
	}
	if e["channelType"] != "dm" {
		t.Fatalf("want dm, got %v", e["channelType"])
	}
}

func TestTracksContextMenuSource(t *testing.T) {
	a, s, rec := newHarness(t, Options{})

	ic := slashInteraction("g1", "u1", "c1", "Report Message")
	ic.Data = dgo.ApplicationCommandInteractionData{
		Name:        "Report Message",
		CommandType: dgo.MessageApplicationCommand,
	}
	a.onInteraction(s, ic)
	a.client.Flush()

	if got := meta(rec.allEvents()[0])["source"]; got != "context_menu" {
		t.Fatalf("want context_menu, got %v", got)
	}
}

func TestIgnoresListedCommands(t *testing.T) {
	a, s, rec := newHarness(t, Options{IgnoreCommands: []string{"play"}})

	a.onInteraction(s, slashInteraction("g1", "u1", "c1", "play"))
	a.onInteraction(s, slashInteraction("g1", "u1", "c1", "stop"))
	a.client.Flush()

	events := rec.allEvents()
	if len(events) != 1 || events[0]["name"] != "stop" {
		t.Fatalf("want only 'stop' tracked, got %v", events)
	}
}

func TestDisableAutoTrackCommands(t *testing.T) {
	a, s, rec := newHarness(t, Options{DisableAutoTrackCommands: true})

	a.onInteraction(s, slashInteraction("g1", "u1", "c1", "play"))
	a.client.Flush()

	if got := len(rec.allEvents()); got != 0 {
		t.Fatalf("want no auto-tracked events, got %d", got)
	}
}

func TestFullCommandNameWalksSubcommandGroup(t *testing.T) {
	data := dgo.ApplicationCommandInteractionData{
		Name: "config",
		Options: []*dgo.ApplicationCommandInteractionDataOption{{
			Name: "user",
			Type: dgo.ApplicationCommandOptionSubCommandGroup,
			Options: []*dgo.ApplicationCommandInteractionDataOption{{
				Name: "set",
				Type: dgo.ApplicationCommandOptionSubCommand,
			}},
		}},
	}
	if got := fullCommandName(data); got != "config user set" {
		t.Fatalf(`want "config user set", got %q`, got)
	}

	flat := dgo.ApplicationCommandInteractionData{
		Name: "play",
		Options: []*dgo.ApplicationCommandInteractionDataOption{
			{Name: "query", Type: dgo.ApplicationCommandOptionString},
		},
	}
	if got := fullCommandName(flat); got != "play" {
		t.Fatalf(`want "play", got %q`, got)
	}
}

func TestMapChannelType(t *testing.T) {
	cases := map[dgo.ChannelType]mochi.ChannelType{
		dgo.ChannelTypeGuildText:          mochi.ChannelGuildText,
		dgo.ChannelTypeGuildNews:          mochi.ChannelGuildText,
		dgo.ChannelTypeDM:                 mochi.ChannelDM,
		dgo.ChannelTypeGroupDM:            mochi.ChannelGroupDM,
		dgo.ChannelTypeGuildVoice:         mochi.ChannelGuildVoice,
		dgo.ChannelTypeGuildStageVoice:    mochi.ChannelGuildVoice,
		dgo.ChannelTypeGuildPublicThread:  mochi.ChannelThread,
		dgo.ChannelTypeGuildPrivateThread: mochi.ChannelThread,
		dgo.ChannelTypeGuildNewsThread:    mochi.ChannelThread,
		dgo.ChannelTypeGuildForum:         mochi.ChannelOther,
		dgo.ChannelTypeGuildCategory:      mochi.ChannelOther,
	}
	for in, want := range cases {
		if got := mapChannelType(in); got != want {
			t.Errorf("mapChannelType(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestChannelTypeFromState(t *testing.T) {
	a, s, rec := newHarness(t, Options{})
	if err := s.State.GuildAdd(&dgo.Guild{ID: "g1", Name: "G"}); err != nil {
		t.Fatalf("GuildAdd: %v", err)
	}
	if err := s.State.ChannelAdd(&dgo.Channel{ID: "c1", GuildID: "g1", Type: dgo.ChannelTypeGuildVoice}); err != nil {
		t.Fatalf("ChannelAdd: %v", err)
	}

	a.onInteraction(s, slashInteraction("g1", "u1", "c1", "join"))
	a.client.Flush()

	if got := rec.allEvents()[0]["channelType"]; got != "guild_voice" {
		t.Fatalf("want guild_voice from state, got %v", got)
	}
}

func TestGuildCreateSuppressedBeforeReadyAndForKnownGuilds(t *testing.T) {
	a, s, rec := newHarness(t, Options{})

	// Arrives before Ready: part of the initial sync, not a join.
	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "g1", MemberCount: 5}})

	// Ready seeds the known set with the guilds we already have.
	a.markReady([]string{"g1", "g2"})

	// Replayed / outage-recovery GuildCreate for known guilds: still not joins.
	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "g1", MemberCount: 5}})
	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "g2", MemberCount: 7}})
	a.client.Flush()

	if got := len(rec.allEvents()); got != 0 {
		t.Fatalf("initial sync must not emit guild_join, got %d events", got)
	}
}

func TestGuildJoinAfterReadyIsTracked(t *testing.T) {
	a, s, rec := newHarness(t, Options{IncludeGuildNames: true})
	a.markReady([]string{"g1"})

	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "new", Name: "New Guild", MemberCount: 42}})
	a.client.Flush()

	events := rec.allEvents()
	if len(events) != 1 {
		t.Fatalf("want 1 guild_join, got %d", len(events))
	}
	e := events[0]
	if e["type"] != "guild_join" || e["guildId"] != "new" {
		t.Fatalf("unexpected event %v", e)
	}
	if meta(e)["memberCount"] != float64(42) || meta(e)["name"] != "New Guild" {
		t.Fatalf("unexpected meta %v", meta(e))
	}
}

func TestGuildDeleteUnavailableIsNotALeave(t *testing.T) {
	a, s, rec := newHarness(t, Options{})
	a.markReady([]string{"g1"})

	a.onGuildDelete(s, &dgo.GuildDelete{Guild: &dgo.Guild{ID: "g1", Unavailable: true}})
	a.client.Flush()

	if got := len(rec.allEvents()); got != 0 {
		t.Fatalf("outage must not emit guild_leave, got %d", got)
	}
}

func TestGuildLeaveTracked(t *testing.T) {
	a, s, rec := newHarness(t, Options{IncludeGuildNames: true})
	a.markReady([]string{"g1"})

	a.onGuildDelete(s, &dgo.GuildDelete{
		Guild:        &dgo.Guild{ID: "g1"},
		BeforeDelete: &dgo.Guild{ID: "g1", Name: "Old Guild"},
	})
	a.client.Flush()

	events := rec.allEvents()
	if len(events) != 1 || events[0]["type"] != "guild_leave" {
		t.Fatalf("want guild_leave, got %v", events)
	}
	if got := meta(events[0])["name"]; got != "Old Guild" {
		t.Fatalf("want name from BeforeDelete, got %v", got)
	}

	// The guild is forgotten, so rejoining counts as a fresh join.
	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "g1", MemberCount: 3}})
	a.client.Flush()
	if got := len(rec.allEvents()); got != 2 {
		t.Fatalf("want rejoin tracked, got %d events", got)
	}
}

func TestWrapHandlerRecordsSuccessAndDuration(t *testing.T) {
	a, s, rec := newHarness(t, Options{DisableAutoTrackCommands: true})
	called := false
	handler := WrapHandler(a.client, func(*dgo.Session, *dgo.InteractionCreate) {
		called = true
		time.Sleep(5 * time.Millisecond)
	})

	handler(s, slashInteraction("g1", "u1", "c1", "play"))
	a.client.Flush()

	if !called {
		t.Fatal("inner handler not called")
	}
	e := rec.allEvents()[0]
	if e["success"] != true {
		t.Fatalf("want success true, got %v", e["success"])
	}
	if d, ok := e["durationMs"].(float64); !ok || d < 1 {
		t.Fatalf("want durationMs >=1, got %v", e["durationMs"])
	}
}

func TestWrapHandlerRecordsPanicAsFailureAndRepanics(t *testing.T) {
	a, s, rec := newHarness(t, Options{DisableAutoTrackCommands: true})
	handler := WrapHandler(a.client, func(*dgo.Session, *dgo.InteractionCreate) {
		panic("handler exploded")
	})

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("panic must be re-raised to the caller")
			}
		}()
		handler(s, slashInteraction("g1", "u1", "c1", "play"))
	}()
	a.client.Flush()

	e := rec.allEvents()[0]
	if e["success"] != false {
		t.Fatalf("want success false on panic, got %v", e["success"])
	}
}

func TestSnapshotUsesState(t *testing.T) {
	a, s, rec := newHarness(t, Options{})
	if err := s.State.GuildAdd(&dgo.Guild{ID: "g1", MemberCount: 10}); err != nil {
		t.Fatalf("GuildAdd: %v", err)
	}
	if err := s.State.GuildAdd(&dgo.Guild{ID: "g2", MemberCount: 32}); err != nil {
		t.Fatalf("GuildAdd: %v", err)
	}

	a.sendSnapshot(s)

	snaps := rec.allSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("want 1 snapshot, got %d", len(snaps))
	}
	snap := snaps[0]
	if snap["guildCount"] != float64(2) {
		t.Fatalf("want guildCount 2, got %v", snap["guildCount"])
	}
	if snap["approximateMemberSum"] != float64(42) {
		t.Fatalf("want memberSum 42, got %v", snap["approximateMemberSum"])
	}
	if snap["totalShards"] != float64(2) {
		t.Fatalf("want totalShards 2, got %v", snap["totalShards"])
	}
	if snap["shardId"] != float64(0) {
		t.Fatalf("shardId 0 must be sent, got %v", snap["shardId"])
	}
	if ping, ok := snap["wsPingMs"].(float64); !ok || ping < 0 {
		t.Fatalf("want non-negative wsPingMs, got %v", snap["wsPingMs"])
	}
}

func TestOnReadySeedsGuildsAndStartsSnapshotLoopOnce(t *testing.T) {
	a, s, rec := newHarness(t, Options{SnapshotInterval: time.Hour})

	a.onReady(s, &dgo.Ready{Guilds: []*dgo.Guild{{ID: "g1"}}})
	a.onReady(s, &dgo.Ready{Guilds: []*dgo.Guild{{ID: "g1"}}}) // reconnect

	// The loop sends one snapshot immediately; startOnce prevents a second loop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(rec.allSnapshots()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(rec.allSnapshots()); got != 1 {
		t.Fatalf("want exactly 1 snapshot from a single loop, got %d", got)
	}

	// g1 was seeded by Ready, so its GuildCreate is sync, not a join.
	a.onGuildCreate(s, &dgo.GuildCreate{Guild: &dgo.Guild{ID: "g1"}})
	a.client.Flush()
	if got := len(rec.allEvents()); got != 0 {
		t.Fatalf("Ready-seeded guild must not emit join, got %d", got)
	}
}

func TestAttachDetachIsCleanAndIdempotent(t *testing.T) {
	rec := &capture{}
	client := mochi.New(mochi.Options{
		URL: "http://localhost:9999", APIKey: "k",
		FlushInterval: time.Minute, Transport: rec.transport,
	})
	defer client.Shutdown()

	session := &dgo.Session{ShardID: 0, ShardCount: 1, State: dgo.NewState()}
	detach := Attach(session, client, Options{})
	detach()
	detach() // must not panic on a double close
}
