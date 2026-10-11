package verdict

import (
	"errors"
	"fmt"
	"time"
)

// BackfillParams are the backfill probe's parameters: the large gang, the
// blocker Jobs whose end sets the gang's earliest possible start, the short
// Jobs that may run in the gap, and the start tolerance.
type BackfillParams struct {
	BigJob       string   `json:"big_job"`
	BigPods      int      `json:"big_pods"`
	BlockerJobs  []string `json:"blocker_jobs"`
	SmallJobs    []string `json:"small_jobs"`
	ToleranceSec float64  `json:"tolerance_sec"`
}

func (p BackfillParams) validate() error {
	if p.BigJob == "" || len(p.BlockerJobs) == 0 || len(p.SmallJobs) == 0 {
		return errors.New("big_job, blocker_jobs and small_jobs must all be set")
	}
	if p.BigPods < 1 {
		return fmt.Errorf("big_pods %d, want 1 or more", p.BigPods)
	}
	if p.ToleranceSec < 0 {
		return fmt.Errorf("tolerance_sec %s is negative", fmtSec(p.ToleranceSec))
	}
	return nil
}

// Backfill judges whether short jobs ran in the gap before the large gang
// without delaying it. The gang starts when its BigPods-th pod is
// scheduled; its earliest possible start is the last blocker pod's end. A
// short job or gang seen only as a Job (a queue still held it) never ran.
func Backfill(t Timeline, p BackfillParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	big, seen := t.job(p.BigJob)
	if !seen {
		return Verdict{}, fmt.Errorf("no events recorded for job %s", p.BigJob)
	}
	blockers, missing := ofJobs(t.Pods, p.BlockerJobs)
	if missing != "" {
		return Verdict{}, fmt.Errorf("no pods recorded for blocker job %s", missing)
	}
	var smalls []*Pod
	var byJob [][]*Pod
	for _, j := range p.SmallJobs {
		ps, seen := t.job(j)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", j)
		}
		smalls = append(smalls, ps...)
		byJob = append(byJob, ps)
	}
	var earliest time.Time
	for _, b := range blockers {
		if b.End.IsZero() {
			return Verdict{}, fmt.Errorf("blocker job %s never finished", b.Job)
		}
		if b.End.After(earliest) {
			earliest = b.End
		}
	}
	tbig, started := nthScheduled(big, p.BigPods)
	// a short job ran first when its first pod was scheduled before the gang
	before := 0
	for _, ps := range byJob {
		if first, sched := nthScheduled(ps, 1); sched && (!started || first.Before(tbig)) {
			before++
		}
	}
	v := Verdict{
		Measurements: map[string]float64{"small_before_big": float64(before)},
		EvidenceSeq:  evidence(big, blockers, smalls, t.jobSeqs(append(append([]string{p.BigJob}, p.BlockerJobs...), p.SmallJobs...)...)),
	}
	delay := 0.0
	if started {
		delay = tbig.Sub(earliest).Seconds()
		v.Measurements["big_start_delay_s"] = delay
	}
	switch {
	case before == 0 && started:
		v.Outcome = NotSupported
		v.Reason = "no short job ran before the gang started"
	case before == 0:
		v.Outcome = NotSupported
		v.Reason = "no short job ran, and the gang never started"
	case !started:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d short jobs ran, and the gang never started", before)
	case delay > p.ToleranceSec:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d short jobs ran before the gang, which started %s s after its earliest possible start", before, fmtSec(delay))
	default:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("%d short jobs ran before the gang, which started %s s after its earliest possible start", before, fmtSec(delay))
	}
	return v, nil
}
