// Package claims loads a scheduler's documented claims: for each
// capability, what the scheduler's documentation says, where, the outcome
// that claim implies, and how the suite's profile exercises it.
package claims

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

// Claim is one capability's documented claim.
type Claim struct {
	Claim     string          `json:"claim"`
	Citation  string          `json:"citation"`
	Expected  verdict.Outcome `json:"expected"`
	Mechanism string          `json:"mechanism"`
}

// File is claims/<scheduler>.yaml.
type File struct {
	Scheduler string           `json:"scheduler"`
	Version   string           `json:"version"`
	Claims    map[string]Claim `json:"claims"`
}

// Parse decodes and validates a claims file: every capability in
// verdict.Capabilities must have a non-empty claim and mechanism, an https
// citation and a valid expected outcome, and no other capability may appear.
func Parse(data []byte) (File, error) {
	var f File
	if err := yaml.UnmarshalStrict(data, &f); err != nil {
		return File{}, fmt.Errorf("claims: %w", err)
	}
	if f.Scheduler == "" || f.Version == "" {
		return File{}, errors.New("claims: scheduler and version must both be set")
	}
	known := map[string]bool{}
	for _, c := range verdict.Capabilities {
		known[c] = true
	}
	names := make([]string, 0, len(f.Claims))
	for name := range f.Claims {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !known[name] {
			return File{}, fmt.Errorf("claims: unknown capability %q", name)
		}
	}
	for _, c := range verdict.Capabilities {
		cl, ok := f.Claims[c]
		switch {
		case !ok:
			return File{}, fmt.Errorf("claims: missing capability %q", c)
		case strings.TrimSpace(cl.Claim) == "":
			return File{}, fmt.Errorf("claims: capability %q: claim is empty", c)
		case !strings.HasPrefix(cl.Citation, "https://"):
			return File{}, fmt.Errorf("claims: capability %q: citation %q is not an https URL", c, cl.Citation)
		case !cl.Expected.Valid():
			return File{}, fmt.Errorf("claims: capability %q: expected %q, want supported, partial or not_supported", c, cl.Expected)
		case strings.TrimSpace(cl.Mechanism) == "":
			return File{}, fmt.Errorf("claims: capability %q: mechanism is empty", c)
		}
	}
	return f, nil
}

// Load reads and validates the claims file at path.
func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	f, err := Parse(data)
	if err != nil {
		return File{}, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}
