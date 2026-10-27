package main

import "testing"

func TestRedactWebhook(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"inside quotes", `x "https://discord.com/api/webhooks/123/FAKE" y`,
			`x "https://discord.com/api/webhooks/<redacted>" y`},
		{"end of string", "post https://discord.com/api/webhooks/123/FAKE",
			"post https://discord.com/api/webhooks/<redacted>"},
		{"discordapp host", "https://discordapp.com/api/webhooks/123/FAKE next",
			"https://discordapp.com/api/webhooks/<redacted> next"},
		{"no url", "discord: got 404 (giving up)", "discord: got 404 (giving up)"},
		{"url error", `Post "https://discord.com/api/webhooks/123/FAKE": context deadline exceeded`,
			`Post "https://discord.com/api/webhooks/<redacted>": context deadline exceeded`},
		{"two urls", "https://discord.com/api/webhooks/1/A and https://discord.com/api/webhooks/2/B",
			"https://discord.com/api/webhooks/<redacted> and https://discord.com/api/webhooks/<redacted>"},
	}
	for _, c := range cases {
		if got := redactWebhook(c.in); got != c.want {
			t.Errorf("%s: redactWebhook(%q)=%q want %q", c.name, c.in, got, c.want)
		}
	}
}
