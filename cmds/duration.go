package cmds

import "time"

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}
