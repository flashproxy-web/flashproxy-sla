package main

import "github.com/flashproxy/flashproxy-status/internal/alertgate"

// seedConfirmed returns the confirmed status a package starts the monitor loop with.
// lastMsg is the message of its newest status_change row in sla.events ("" if none),
// current the first verdict observed after start.
//
// With a recorded row the monitor continues from it, so the normal debounce path then
// writes the closing/changing row (degraded -> operational) or, when nothing changed,
// nothing at all. Without one, a non-healthy package is treated as coming from
// operational so its first row is an ordinary "operational -> degraded".
func seedConfirmed(lastMsg, current string) string {
	if _, status, _, ok := alertgate.ParseMessage(lastMsg); ok {
		return status
	}
	if current == "degraded" || current == "down" {
		return "operational"
	}
	return current
}
