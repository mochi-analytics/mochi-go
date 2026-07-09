module github.com/mochi-analytics/mochi-go/discordgo

go 1.21

require (
	github.com/bwmarrin/discordgo v0.29.0
	github.com/mochi-analytics/mochi-go v1.0.0
)

// Local development only; replace directives in dependency go.mod files are
// ignored by consumers of github.com/mochi-analytics/mochi-go/discordgo.
replace github.com/mochi-analytics/mochi-go => ../
