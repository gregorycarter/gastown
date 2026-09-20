package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/util"
)

const (
	defaultDoltBackupInterval = 15 * time.Minute
	// doltBackupTimeout is generous so a large commit delta on a big database
	// (e.g. hq under a wisp flood) does not blow the deadline mid-sync. The old
	// 120s ceiling produced spurious exit-1 backup failures (gt-ye21).
	doltBackupTimeout = 5 * time.Minute
	// doltBackupRetries / doltBackupRetryDelay retry a failed sync after a short
	// pause, so a transient lock (a concurrent dolt op holding the db) does not
	// fail the whole backup cycle.
	doltBackupRetries    = 1
	doltBackupRetryDelay = 5 * time.Second
)

// doltBackupInterval returns the configured backup interval, or the default (15m).
func doltBackupInterval(config *DaemonPatrolConfig) time.Duration {
	if config != nil && config.Patrols != nil && config.Patrols.DoltBackup != nil {
		if config.Patrols.DoltBackup.IntervalStr != "" {
			if d, err := time.ParseDuration(config.Patrols.DoltBackup.IntervalStr); err == nil && d > 0 {
				return d
			}
		}
	}
	return defaultDoltBackupInterval
}

// syncDoltBackups syncs each production database to its configured backup location.
// Non-fatal: errors are logged but don't stop the daemon.
func (d *Daemon) syncDoltBackups() {
	// Dolt backup uses iCloud Drive for offsite sync — only available on macOS.
	// On Linux this generates HIGH priority escalation spam every ~15 minutes.
	if runtime.GOOS != "darwin" {
		return
	}
	if !d.isPatrolActive("dolt_backup") {
		return
	}

	// One durable checkpoint replaces a new tracking molecule per backup tick.
	// This avoids DB churn during persistent filesystem/permission failures.
	stateFile := filepath.Join(d.config.TownRoot, ".runtime", "dolt-backup-state.json")
	state, err := readDoltBackupState(stateFile)
	if err != nil {
		d.logger.Printf("dolt_backup: checkpoint unavailable: %v", err)
		return
	}
	defer func() {
		if err := writeDoltBackupState(stateFile, state); err != nil {
			d.logger.Printf("dolt_backup: checkpoint write failed: %v", err)
		}
	}()

	// Resolve data dir: use DoltServerManager if available, else conventional path.
	var dataDir string
	if d.doltServer != nil && d.doltServer.IsEnabled() && d.doltServer.config.DataDir != "" {
		dataDir = d.doltServer.config.DataDir
	} else {
		dataDir = filepath.Join(d.config.TownRoot, ".dolt-data")
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		d.logger.Printf("dolt_backup: data dir %s does not exist, skipping", dataDir)
		return
	}

	config := d.patrolConfig.Patrols.DoltBackup
	databases := config.Databases
	if len(databases) == 0 {
		databases = d.discoverDatabasesWithBackups(dataDir)
	}

	if len(databases) == 0 {
		d.logger.Printf("dolt_backup: no databases with backup remotes found")
		return
	}

	d.logger.Printf("dolt_backup: syncing %d database(s)", len(databases))

	synced := 0
	var failures []string
	var successful []string
	for _, db := range databases {
		if !validDBName.MatchString(db) {
			d.logger.Printf("dolt_backup: invalid database name")
			continue
		}
		previous := state[db]
		if time.Now().Before(previous.NextAttempt) {
			d.logger.Printf("dolt_backup: %s: backing off until %s", db, previous.NextAttempt.UTC().Format(time.RFC3339))
			continue
		}
		backupName := db + "-backup"
		if err := d.syncBackup(dataDir, db, backupName); err != nil {
			d.logger.Printf("dolt_backup: %s: sync failed: %v", db, err)
			failures = append(failures, db)
			state[db] = nextDoltBackupState(previous, time.Now(), err)
		} else {
			synced++
			successful = append(successful, db)
			state[db] = nextDoltBackupState(previous, time.Now(), nil)
		}
	}

	d.logger.Printf("dolt_backup: synced %d/%d database(s)", synced, len(databases))

	if len(failures) > 0 {
		d.logger.Printf("dolt_backup: failed databases: %s", strings.Join(failures, ", "))
	}
	if synced > 0 {
		for _, destination := range d.doltBackupReplicaDestinations() {
			key := "replica:" + destination
			previous := state[key]
			if !time.Now().Before(previous.NextAttempt) {
				err := d.syncOffsiteBackup(successful, destination)
				state[key] = nextDoltBackupState(previous, time.Now(), err)
				if err != nil {
					d.logger.Printf("dolt_backup: replica failed: %v", err)
				}
			} else {
				d.logger.Printf("dolt_backup: replica %s backing off until %s", destination, previous.NextAttempt.UTC().Format(time.RFC3339))
			}
		}
	}

}

// syncBackup runs `dolt backup sync <backup-name>` for a single database,
// retrying once on failure so a transient lock or large delta does not fail the
// cycle (gt-ye21).
func (d *Daemon) syncBackup(dataDir, db, backupName string) error {
	parentCtx := d.ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	dbDir := filepath.Join(dataDir, db)

	var lastErr error
	for attempt := 0; attempt <= doltBackupRetries; attempt++ {
		if attempt > 0 {
			d.logger.Printf("dolt_backup: %s: retry %d/%d after error: %v", db, attempt, doltBackupRetries, lastErr)
			timer := time.NewTimer(doltBackupRetryDelay)
			select {
			case <-timer.C:
			case <-parentCtx.Done():
				timer.Stop()
				return parentCtx.Err()
			}
		}

		ctx, cancel := context.WithTimeout(parentCtx, doltBackupTimeout)
		cmd := exec.CommandContext(ctx, "dolt", "backup", "sync", backupName)
		cmd.Dir = dbDir
		util.SetProcessGroup(cmd)

		output, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			d.logger.Printf("dolt_backup: %s: synced to %s", db, backupName)
			return nil
		}
		lastErr = fmt.Errorf("%s: %s", err, strings.TrimSpace(string(output)))
		if permanentBackupFailure(lastErr) {
			break
		}
	}
	return lastErr
}

// syncOffsiteBackup rsyncs the local backup directory to iCloud Drive.
// iCloud automatically syncs to Apple's cloud, providing offsite replication.
// Non-fatal: if iCloud is unavailable or rsync fails, we just log and continue.
func (d *Daemon) doltBackupReplicaDestinations() []string {
	var result []string
	if destination := d.patrolConfig.Patrols.DoltBackup.OffsiteDir; destination != "" {
		result = append(result, destination)
	}
	if home, err := os.UserHomeDir(); err == nil {
		cloud := filepath.Join(home, "Library", "Mobile Documents", "com~apple~CloudDocs", "gt-dolt-backup")
		if len(result) == 0 || result[0] != cloud {
			result = append(result, cloud)
		}
	}
	return result
}

func (d *Daemon) syncOffsiteBackup(databases []string, icloudDir string) error {
	backupDir := filepath.Join(d.config.TownRoot, ".dolt-backup")
	if _, err := os.Stat(backupDir); err != nil {
		return err
	}

	if !filepath.IsAbs(icloudDir) || filepath.Clean(icloudDir) == filepath.Clean(backupDir) {
		return fmt.Errorf("invalid replica destination")
	}
	if err := os.MkdirAll(icloudDir, 0700); err != nil {
		return fmt.Errorf("cannot create replica dir: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Copy only successfully synced databases. Never delete a previous backup
	// because another database failed or was omitted from this cycle.
	var failures []error
	for _, db := range databases {
		source := filepath.Join(backupDir, db)
		if _, err := os.Stat(source); err != nil {
			failures = append(failures, fmt.Errorf("replica source unavailable for %s: %w", db, err))
			continue
		}
		cmd := exec.CommandContext(ctx, "rsync", "-a", source, icloudDir+"/")
		util.SetDetachedProcessGroup(cmd)
		if output, err := cmd.CombinedOutput(); err != nil {
			failures = append(failures, fmt.Errorf("replica failed for %s: %w (%s)", db, err, strings.TrimSpace(string(output))))
		} else {
			d.logger.Printf("dolt_backup: replicated %s to %s", db, icloudDir)
		}
	}
	return errors.Join(failures...)
}

// discoverDatabasesWithBackups lists databases in the data directory
// that have a <name>-backup backup remote configured.
func (d *Daemon) discoverDatabasesWithBackups(dataDir string) []string {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		d.logger.Printf("dolt_backup: error reading data dir: %v", err)
		return nil
	}

	var databases []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		// Check if this directory has a <name>-backup configured
		backupName := name + "-backup"
		if d.hasBackupRemote(dataDir, name, backupName) {
			databases = append(databases, name)
		}
	}

	return databases
}

// hasBackupRemote checks if a database has the specified backup remote configured.
func (d *Daemon) hasBackupRemote(dataDir, db, backupName string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbDir := dataDir + "/" + db
	cmd := exec.CommandContext(ctx, "dolt", "backup")
	cmd.Dir = dbDir
	util.SetDetachedProcessGroup(cmd)

	output, err := cmd.Output()
	if err != nil {
		return false
	}

	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(line) == backupName {
			return true
		}
	}
	return false
}

// Persistent backoff survives daemon restart and retains the last good time.
type doltBackupState struct {
	Failures    int       `json:"failures"`
	LastSuccess time.Time `json:"lastSuccess,omitempty"`
	LastAttempt time.Time `json:"lastAttempt"`
	NextAttempt time.Time `json:"nextAttempt,omitempty"`
	Error       string    `json:"error,omitempty"`
}

func permanentBackupFailure(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "operation not permitted") || strings.Contains(text, "permission denied") || strings.Contains(text, "read-only file system")
}
func nextDoltBackupState(previous doltBackupState, now time.Time, err error) doltBackupState {
	if err == nil {
		return doltBackupState{LastSuccess: now, LastAttempt: now}
	}
	n := previous.Failures + 1
	if n > 6 {
		n = 6
	}
	delay := 15 * time.Minute * time.Duration(1<<uint(n-1))
	if delay > 4*time.Hour {
		delay = 4 * time.Hour
	}
	return doltBackupState{Failures: n, LastSuccess: previous.LastSuccess, LastAttempt: now, NextAttempt: now.Add(delay), Error: err.Error()}
}
func readDoltBackupState(file string) (map[string]doltBackupState, error) {
	data, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return map[string]doltBackupState{}, nil
	}
	if err != nil {
		return nil, err
	}
	var state map[string]doltBackupState
	err = json.Unmarshal(data, &state)
	if state == nil && err == nil {
		err = fmt.Errorf("invalid null backup checkpoint")
	}
	return state, err
}
func writeDoltBackupState(file string, state map[string]doltBackupState) error {
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(file), ".backup-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), file)
}
