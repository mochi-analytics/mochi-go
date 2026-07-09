# Mochi Go SDK

Go SDK for [Mochi](https://github.com/mochi-analytics/mochi), self-hosted
analytics for Discord bots.

## Modules

This repo is a Go workspace with two independently versioned modules, mirroring
the core/adapter split of `mochi-js` and `mochi-py`:

- `github.com/mochi-analytics/mochi-go` — the **core client**: a batching,
  non-blocking HTTP client for the Mochi ingest and snapshot APIs. Conforms to
  the language-agnostic [core spec](https://github.com/mochi-analytics/core)
  v1.0.0. **Zero dependencies** (stdlib only).
- `github.com/mochi-analytics/mochi-go/discordgo` — the
  [discordgo](https://github.com/bwmarrin/discordgo) adapter.

The adapter is a nested module so the core stays dependency-free: importing the
core never pulls discordgo into your module graph.

## Install

```sh
go get github.com/mochi-analytics/mochi-go
```

```go
import "github.com/mochi-analytics/mochi-go"

client := mochi.New(mochi.Options{
    URL:    "https://mochi.example.com",
    APIKey: os.Getenv("MOCHI_API_KEY"),
})
defer client.Shutdown() // flush remaining events on exit

client.TrackCommand("play", mochi.Event{
    GuildID: "935512380767846400",
    UserID:  "102992563902441472",
    ShardID: mochi.Ptr(0),
})
```

`Track` returns immediately; events are queued and flushed by a background
goroutine (every `FlushInterval`, or when a batch fills). Analytics never
crashes, blocks, or slows the bot: no method panics into the caller and delivery
failures are routed to `OnError`.

## discordgo adapter

```sh
go get github.com/mochi-analytics/mochi-go/discordgo
```

```go
import (
    dgo "github.com/bwmarrin/discordgo"
    mochi "github.com/mochi-analytics/mochi-go"
    mochidgo "github.com/mochi-analytics/mochi-go/discordgo"
)

session, _ := dgo.New("Bot " + token)
client := mochi.New(mochi.Options{URL: mochiURL, APIKey: apiKey})

detach := mochidgo.Attach(session, client, mochidgo.Options{})
defer detach()
```

`Attach` instruments application commands, guild joins/leaves, and an hourly
guild-count snapshot. For accurate success/duration, set
`Options{DisableAutoTrackCommands: true}` and wrap your handlers with
`mochidgo.WrapHandler(client, handler)`.

Two discordgo-specific behaviors worth knowing:

- **`GuildCreate` is replayed for every guild on connect.** The adapter seeds a
  known-guild set from `Ready`, so a restart never emits phantom `guild_join`
  events. Only guilds first seen *after* `Ready` count as joins.
- **`GuildDelete` with `Unavailable: true` is a Discord outage**, not a leave,
  and is ignored.

## Design notes

- **Optional scalars are pointers.** `ShardID`, `Success`, and `DurationMs` are
  `*int` / `*bool` so a meaningful zero (`shardId: 0`, `success: false`) is sent
  rather than dropped by `omitempty`. Use `mochi.Ptr(v)` to set them.
- **Snapshot is synchronous.** `Snapshot` sends immediately with retries (as the
  spec requires) and swallows failures to `OnError`. Call it in a goroutine if
  you don't want to block.
- **Retry-After is honored** on `429` (delta-seconds or HTTP-date) in preference
  to the exponential backoff.

## Development

`go test ./...` is module-scoped, so run each module. The `go.work` file wires
the adapter to the local core (no publish needed):

```sh
go test ./...                       # core
(cd discordgo && go test ./...)     # adapter
go vet ./... && gofmt -l .
```

The core suite mirrors the `@mochi-analytics/core` and `mochi-analytics` suites
and adds the spec's tricky serialization vectors.

`discordgo/go.mod` carries a `replace` pointing at the core while it is
unpublished; drop it once the core is tagged. (Consumers ignore `replace`
directives in dependency `go.mod` files.)

## License

Apache-2.0
