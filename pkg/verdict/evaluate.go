package verdict

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// Capability names, as used in probe labels, probes.json and claims files.
const (
	CapGang        = "gang"
	CapGangControl = "gang-control"
	CapPriority    = "priority"
	CapPreemption  = "preemption"
	CapFairShare   = "fair-share"
	CapBackfill    = "backfill"
	CapTopology    = "topology"
)

// Capabilities lists the six capabilities, in report order. Controls such
// as CapGangControl are not capabilities and have no documented claim.
var Capabilities = []string{CapGang, CapPriority, CapPreemption, CapFairShare, CapBackfill, CapTopology}

// Probe is one entry of probes.json: which probe ran, its parameters, and
// the end of its evidence window. Events observed after WindowEnd are
// ignored, so a verdict does not depend on the probe's cleanup.
type Probe struct {
	Capability string          `json:"capability"`
	Params     json.RawMessage `json:"params"`
	WindowEnd  time.Time       `json:"window_end"`
}

// Evaluate computes the verdict of one probe from a run's events.
func Evaluate(evs []events.Event, pr Probe) (Verdict, error) {
	v, err := evaluate(evs, pr)
	if err != nil {
		return Verdict{}, fmt.Errorf("%s: %w", pr.Capability, err)
	}
	v.Capability = pr.Capability
	return v, nil
}

func evaluate(evs []events.Event, pr Probe) (Verdict, error) {
	if pr.WindowEnd.IsZero() {
		return Verdict{}, fmt.Errorf("window_end is missing")
	}
	t := Build(evs, pr.Capability, pr.WindowEnd)
	switch pr.Capability {
	case CapGang, CapGangControl:
		var p GangParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Gang(t, p)
	case CapPriority:
		var p PriorityParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Priority(t, p)
	case CapPreemption:
		var p PreemptionParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Preemption(t, p)
	case CapFairShare:
		var p FairShareParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return FairShare(t, p)
	case CapBackfill:
		var p BackfillParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Backfill(t, p)
	case CapTopology:
		var p TopologyParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Topology(t, p)
	}
	return Verdict{}, fmt.Errorf("unknown capability")
}

func decode(raw json.RawMessage, into any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("params: %w", err)
	}
	return nil
}
