package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Manifest is manifest.json: what ran, where, and with which pins. Each
// profile's run merges its own entry under Profiles.
type Manifest struct {
	Schema    int                   `json:"schema"`
	RunID     string                `json:"run_id"`
	GitCommit string                `json:"git_commit"`
	GitDirty  bool                  `json:"git_dirty"`
	Host      map[string]string     `json:"host"`
	Tools     map[string]string     `json:"tools"`
	Fleet     json.RawMessage       `json:"fleet"`
	Profiles  map[string]ProfileRun `json:"profiles"`
}

// ProfileRun is one profile's part of the manifest.
type ProfileRun struct {
	Start         time.Time         `json:"start"`
	End           time.Time         `json:"end"`
	Cluster       string            `json:"cluster"`
	FeatureGates  map[string]bool   `json:"feature_gates"`
	RuntimeConfig map[string]string `json:"runtime_config"`
	Images        map[string]string `json:"images"`
	Manifests     map[string]string `json:"manifests"`
	Scheduler     map[string]string `json:"scheduler"`
	// ClockOffset*S is the API server's creation timestamp minus the host's
	// clock, in seconds; the server stamps whole seconds, so |offset| < 1 is
	// as close as this can tell. ClockOffsetEndS is nil, and absent from the
	// file, when the end-of-run check measured nothing.
	ClockOffsetStartS float64  `json:"clock_offset_start_s"`
	ClockOffsetEndS   *float64 `json:"clock_offset_end_s,omitempty"`
	WatchErrors       int      `json:"watch_errors"`
}

// MergeManifest writes runDir/manifest.json with base's run-level fields
// and pr under Profiles[profile], keeping any other profile already there.
// It refuses a manifest that belongs to another run ID.
func MergeManifest(runDir string, base Manifest, profile string, pr ProfileRun) error {
	path := filepath.Join(runDir, ManifestFile)
	profiles := map[string]ProfileRun{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		var old Manifest
		if err := json.Unmarshal(data, &old); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if old.RunID != base.RunID {
			return fmt.Errorf("%s belongs to run %s, not %s", path, old.RunID, base.RunID)
		}
		for k, v := range old.Profiles {
			profiles[k] = v
		}
	case !errors.Is(err, fs.ErrNotExist):
		return err
	}
	profiles[profile] = pr
	base.Schema = 1
	base.Profiles = profiles
	out, err := json.MarshalIndent(base, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// PrepareProfileDir creates runDir/profile and refuses one that already
// holds an events file, so a repeated run ID never overwrites recorded data.
func PrepareProfileDir(runDir, profile string) (string, error) {
	dir := filepath.Join(runDir, profile)
	if _, err := os.Stat(filepath.Join(dir, EventsFile)); err == nil {
		return "", fmt.Errorf("%s already holds %s from an earlier run; choose another run ID", dir, EventsFile)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	return dir, os.MkdirAll(dir, 0o755)
}
