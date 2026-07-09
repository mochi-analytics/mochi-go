// Package discordgo is the discordgo adapter for Mochi analytics.
//
// It auto-instruments application commands, guild joins/leaves, and periodic
// health snapshots, mirroring @mochi-analytics/discordjs and
// mochi-analytics-discordpy so all three SDKs report identically.
//
// Because this package is named discordgo, import bwmarrin's library under an
// alias when using both:
//
//	import (
//	    dgo "github.com/bwmarrin/discordgo"
//	    mochidgo "github.com/mochi-analytics/mochi-go/discordgo"
//	)
package discordgo

import (
	"strings"
	"sync"
	"time"

	dgo "github.com/bwmarrin/discordgo"
	mochi "github.com/mochi-analytics/mochi-go"
)

const defaultSnapshotInterval = time.Hour

// Options configures Attach. The zero value is the default behavior.
type Options struct {
	// IncludeGuildNames puts guild names in join/leave event metadata.
	IncludeGuildNames bool
	// IgnoreCommands lists command names to skip entirely.
	IgnoreCommands []string
	// SnapshotInterval is the gap between guild-count snapshots. Default 1h.
	SnapshotInterval time.Duration
	// DisableAutoTrackCommands stops command events being recorded automatically
	// on interaction. Use WrapHandler for accurate success/duration instead.
	// (This is the inverse of autoTrackCommands in the JS/Python SDKs, so that
	// the Go zero value keeps auto-tracking on.)
	DisableAutoTrackCommands bool
}

type adapter struct {
	client           *mochi.Client
	opts             Options
	ignored          map[string]bool
	snapshotInterval time.Duration

	mu    sync.Mutex
	seen  map[string]bool // guild ids known to us; distinguishes sync from join
	ready bool

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
}

func newAdapter(client *mochi.Client, opts Options) *adapter {
	interval := opts.SnapshotInterval
	if interval <= 0 {
		interval = defaultSnapshotInterval
	}
	ignored := make(map[string]bool, len(opts.IgnoreCommands))
	for _, name := range opts.IgnoreCommands {
		ignored[name] = true
	}
	return &adapter{
		client:           client,
		opts:             opts,
		ignored:          ignored,
		snapshotInterval: interval,
		seen:             make(map[string]bool),
		stop:             make(chan struct{}),
	}
}

// Attach hooks a Mochi client into a discordgo session. It returns a detach
// function that removes every handler and stops the snapshot loop.
func Attach(s *dgo.Session, client *mochi.Client, opts Options) (detach func()) {
	a := newAdapter(client, opts)

	removers := []func(){
		s.AddHandler(a.onInteraction),
		s.AddHandler(a.onGuildCreate),
		s.AddHandler(a.onGuildDelete),
		s.AddHandler(a.onReady),
	}

	// If the session is already connected we never see Ready, so seed from state.
	if ids, ok := readyGuildIDs(s); ok {
		a.markReady(ids)
		a.startSnapshots(s)
	}

	return func() {
		for _, remove := range removers {
			remove()
		}
		a.stopOnce.Do(func() { close(a.stop) })
	}
}

// WrapHandler wraps a command handler so Mochi records accurate duration and
// success. Use together with Options.DisableAutoTrackCommands. A panicking
// handler is recorded as a failure and the panic is re-raised unchanged.
func WrapHandler(
	client *mochi.Client,
	handler func(*dgo.Session, *dgo.InteractionCreate),
) func(*dgo.Session, *dgo.InteractionCreate) {
	return func(s *dgo.Session, ic *dgo.InteractionCreate) {
		if ic == nil || ic.Interaction == nil || ic.Type != dgo.InteractionApplicationCommand {
			handler(s, ic)
			return
		}
		data := ic.ApplicationCommandData()
		startedAt := time.Now()
		success := true

		defer func() {
			r := recover()
			if r != nil {
				success = false
			}
			event := commandEvent(s, ic.Interaction, data)
			event.Success = mochi.Ptr(success)
			event.DurationMs = mochi.Ptr(int(time.Since(startedAt).Milliseconds()))
			client.Track(event)
			if r != nil {
				panic(r)
			}
		}()

		handler(s, ic)
	}
}

// -- handlers ---------------------------------------------------------------

func (a *adapter) onInteraction(s *dgo.Session, ic *dgo.InteractionCreate) {
	if a.opts.DisableAutoTrackCommands || ic == nil || ic.Interaction == nil {
		return
	}
	if ic.Type != dgo.InteractionApplicationCommand {
		return
	}
	data := ic.ApplicationCommandData()
	if a.ignored[data.Name] {
		return
	}
	a.client.Track(commandEvent(s, ic.Interaction, data))
}

func (a *adapter) onGuildCreate(s *dgo.Session, g *dgo.GuildCreate) {
	if g == nil || g.Guild == nil {
		return
	}
	a.mu.Lock()
	known, ready := a.seen[g.ID], a.ready
	a.seen[g.ID] = true
	a.mu.Unlock()

	// discordgo replays a GuildCreate for every guild during the initial sync
	// after connect, and again when a guild recovers from an outage. Neither is
	// a join, so only guilds we have never seen post-Ready count.
	if known || !ready {
		return
	}

	meta := map[string]any{"memberCount": g.MemberCount}
	if a.opts.IncludeGuildNames {
		meta["name"] = g.Name
	}
	a.client.Track(mochi.Event{
		Type:    mochi.EventGuildJoin,
		GuildID: g.ID,
		ShardID: mochi.Ptr(s.ShardID),
		Meta:    meta,
	})
}

func (a *adapter) onGuildDelete(s *dgo.Session, g *dgo.GuildDelete) {
	if g == nil || g.Guild == nil {
		return
	}
	if g.Unavailable {
		return // a Discord outage, not a leave
	}
	a.mu.Lock()
	delete(a.seen, g.ID)
	a.mu.Unlock()

	var meta map[string]any
	if a.opts.IncludeGuildNames {
		// A GuildDelete payload carries only the id; the name lives in the
		// pre-delete cached copy.
		name := g.Name
		if name == "" && g.BeforeDelete != nil {
			name = g.BeforeDelete.Name
		}
		if name != "" {
			meta = map[string]any{"name": name}
		}
	}
	a.client.Track(mochi.Event{
		Type:    mochi.EventGuildLeave,
		GuildID: g.ID,
		ShardID: mochi.Ptr(s.ShardID),
		Meta:    meta,
	})
}

func (a *adapter) onReady(s *dgo.Session, r *dgo.Ready) {
	ids := make([]string, 0, len(r.Guilds))
	for _, g := range r.Guilds {
		ids = append(ids, g.ID)
	}
	a.markReady(ids)
	a.startSnapshots(s)
}

// -- snapshots --------------------------------------------------------------

func (a *adapter) markReady(guildIDs []string) {
	a.mu.Lock()
	for _, id := range guildIDs {
		a.seen[id] = true
	}
	a.ready = true
	a.mu.Unlock()
}

// startSnapshots is idempotent: Ready fires again on every reconnect.
func (a *adapter) startSnapshots(s *dgo.Session) {
	a.startOnce.Do(func() { go a.snapshotLoop(s) })
}

func (a *adapter) snapshotLoop(s *dgo.Session) {
	a.sendSnapshot(s)
	ticker := time.NewTicker(a.snapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-ticker.C:
			a.sendSnapshot(s)
		}
	}
}

func (a *adapter) sendSnapshot(s *dgo.Session) {
	guildCount, memberSum := guildStats(s)
	ping := int(s.HeartbeatLatency().Milliseconds())
	if ping < 0 {
		ping = 0 // no heartbeat acked yet
	}
	a.client.Snapshot(mochi.Snapshot{
		GuildCount:           guildCount,
		ShardID:              mochi.Ptr(s.ShardID),
		TotalShards:          mochi.Ptr(totalShards(s)),
		ApproximateMemberSum: mochi.Ptr(memberSum),
		WsPingMs:             mochi.Ptr(ping),
	})
}

// -- helpers ----------------------------------------------------------------

func commandEvent(
	s *dgo.Session,
	i *dgo.Interaction,
	data dgo.ApplicationCommandInteractionData,
) mochi.Event {
	return mochi.Event{
		Type:        mochi.EventCommand,
		Name:        fullCommandName(data),
		GuildID:     i.GuildID,
		UserID:      userIDOf(i),
		ChannelType: channelTypeOf(s, i),
		ShardID:     mochi.Ptr(s.ShardID),
		Meta:        map[string]any{"source": commandSource(data)},
	}
}

// fullCommandName joins the command with any subcommand-group/subcommand path,
// e.g. "config user set".
func fullCommandName(data dgo.ApplicationCommandInteractionData) string {
	parts := []string{data.Name}
	options := data.Options
	for {
		var next *dgo.ApplicationCommandInteractionDataOption
		for _, option := range options {
			if option == nil {
				continue
			}
			if option.Type == dgo.ApplicationCommandOptionSubCommand ||
				option.Type == dgo.ApplicationCommandOptionSubCommandGroup {
				next = option
				break
			}
		}
		if next == nil {
			return strings.Join(parts, " ")
		}
		parts = append(parts, next.Name)
		options = next.Options
	}
}

func commandSource(data dgo.ApplicationCommandInteractionData) string {
	if data.CommandType == dgo.ChatApplicationCommand {
		return "slash"
	}
	return "context_menu"
}

func userIDOf(i *dgo.Interaction) string {
	if i.Member != nil && i.Member.User != nil {
		return i.Member.User.ID
	}
	if i.User != nil {
		return i.User.ID
	}
	return ""
}

func channelTypeOf(s *dgo.Session, i *dgo.Interaction) mochi.ChannelType {
	if s.State != nil && i.ChannelID != "" {
		if channel, err := s.State.Channel(i.ChannelID); err == nil && channel != nil {
			return mapChannelType(channel.Type)
		}
	}
	if i.GuildID != "" {
		return mochi.ChannelGuildText
	}
	return mochi.ChannelDM
}

func mapChannelType(t dgo.ChannelType) mochi.ChannelType {
	switch t {
	case dgo.ChannelTypeDM:
		return mochi.ChannelDM
	case dgo.ChannelTypeGroupDM:
		return mochi.ChannelGroupDM
	case dgo.ChannelTypeGuildVoice, dgo.ChannelTypeGuildStageVoice:
		return mochi.ChannelGuildVoice
	case dgo.ChannelTypeGuildNewsThread,
		dgo.ChannelTypeGuildPublicThread,
		dgo.ChannelTypeGuildPrivateThread:
		return mochi.ChannelThread
	case dgo.ChannelTypeGuildText, dgo.ChannelTypeGuildNews:
		return mochi.ChannelGuildText
	default:
		return mochi.ChannelOther
	}
}

func guildStats(s *dgo.Session) (guildCount, memberSum int) {
	if s.State == nil {
		return 0, 0
	}
	s.State.RLock()
	defer s.State.RUnlock()
	for _, guild := range s.State.Guilds {
		memberSum += guild.MemberCount
	}
	return len(s.State.Guilds), memberSum
}

func totalShards(s *dgo.Session) int {
	if s.ShardCount > 0 {
		return s.ShardCount
	}
	return 1
}

// readyGuildIDs reports the state's guild ids when the session is already
// connected, so Attach can skip waiting for a Ready that already fired.
func readyGuildIDs(s *dgo.Session) ([]string, bool) {
	if s.State == nil {
		return nil, false
	}
	s.State.RLock()
	defer s.State.RUnlock()
	if s.State.SessionID == "" {
		return nil, false
	}
	ids := make([]string, 0, len(s.State.Guilds))
	for _, guild := range s.State.Guilds {
		ids = append(ids, guild.ID)
	}
	return ids, true
}
