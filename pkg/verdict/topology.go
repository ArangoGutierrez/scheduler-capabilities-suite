package verdict

import (
	"errors"
	"fmt"
	"sort"
)

// TopologyParams are the topology probe's parameters: one Job per attempt,
// its pod count, and the rack of every node in the fleet.
type TopologyParams struct {
	AttemptJobs []string          `json:"attempt_jobs"`
	PodsPerJob  int               `json:"pods_per_job"`
	RackOf      map[string]string `json:"rack_of"`
}

func (p TopologyParams) validate() error {
	if len(p.AttemptJobs) == 0 || len(p.RackOf) == 0 {
		return errors.New("attempt_jobs and rack_of must both be set")
	}
	if p.PodsPerJob < 1 {
		return fmt.Errorf("pods_per_job %d, want 1 or more", p.PodsPerJob)
	}
	return nil
}

// Topology judges whether each attempt placed its first PodsPerJob
// scheduled pods in a single rack. An attempt with fewer scheduled pods
// (stuck, or held by a queue) does not count as one rack.
func Topology(t Timeline, p TopologyParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	oneRack := 0
	groups := [][]*Pod{t.jobSeqs(p.AttemptJobs...)}
	for _, job := range p.AttemptJobs {
		ps, seen := t.job(job)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", job)
		}
		groups = append(groups, ps)
		var sched []*Pod
		for _, q := range ps {
			if !q.Scheduled.IsZero() {
				sched = append(sched, q)
			}
		}
		if len(sched) < p.PodsPerJob {
			continue
		}
		sort.SliceStable(sched, func(i, j int) bool { return sched[i].Scheduled.Before(sched[j].Scheduled) })
		racks := map[string]bool{}
		for _, q := range sched[:p.PodsPerJob] {
			r, ok := p.RackOf[q.Node]
			if !ok {
				return Verdict{}, fmt.Errorf("node %q is not in rack_of", q.Node)
			}
			racks[r] = true
		}
		if len(racks) == 1 {
			oneRack++
		}
	}
	n := len(p.AttemptJobs)
	v := Verdict{
		Measurements: map[string]float64{"attempts": float64(n), "attempts_one_rack": float64(oneRack)},
		EvidenceSeq:  evidence(groups...),
	}
	switch oneRack {
	case n:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("all %d attempts placed every pod in one rack", n)
	case 0:
		v.Outcome = NotSupported
		v.Reason = "no attempt placed its pods in one rack"
	default:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d of %d attempts placed every pod in one rack", oneRack, n)
	}
	return v, nil
}
