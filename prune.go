package jorb

import (
	"time"

	"github.com/b1naryth1ef/jorb/db"
)

// Prune removes stalled, stale or expired jobs
func (c *Client) Prune(keep time.Duration) error {
	err := db.CancelStalledJobs(c.conn)
	if err != nil {
		return err
	}

	return db.PruneJobs(c.conn, time.Now().Add(keep*-1))
}
