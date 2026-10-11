package events_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

func TestEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "events") {
		t.Fatal("events specs failed")
	}
}

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func ev(seq int64, tr events.Transition) events.Event {
	return events.Event{
		Schema: 1, Seq: seq, RunID: "r1", ObservedAt: t0.Add(time.Duration(seq) * time.Second),
		Kind: "Pod", Namespace: "scs-gang", Name: "p", UID: "u",
		Probe: "gang", Job: "gang-a", Tenant: "t1", GPUs: 8, Transition: tr,
	}
}

const line1 = `{"schema":1,"seq":1,"run_id":"r1","observed_at":"2026-10-10T10:00:01Z","kind":"Pod","namespace":"scs-gang","name":"p","uid":"u","probe":"gang","job":"gang-a","tenant":"t1","priority":0,"gpus":8,"transition":"created"}`

var _ = Describe("events", func() {
	It("writes one JSON object per line and reads the same events back", func() {
		var buf bytes.Buffer
		enc := events.NewEncoder(&buf)
		apiTime := time.Date(2026, 10, 10, 10, 0, 1, 0, time.UTC)
		second := ev(2, events.Scheduled)
		second.APITime = &apiTime
		second.APITimePrecision = "s"
		second.Node = "n1"
		Expect(enc.Encode(ev(1, events.Created))).To(Succeed())
		Expect(enc.Encode(second)).To(Succeed())
		Expect(enc.Flush()).To(Succeed())
		Expect(buf.String()).To(Equal(line1 + "\n" +
			`{"schema":1,"seq":2,"run_id":"r1","observed_at":"2026-10-10T10:00:02Z","api_time":"2026-10-10T10:00:01Z","api_time_precision":"s","kind":"Pod","namespace":"scs-gang","name":"p","uid":"u","probe":"gang","job":"gang-a","tenant":"t1","priority":0,"gpus":8,"transition":"scheduled","node":"n1"}` + "\n"))

		got, err := events.Read(&buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(got[0].Transition).To(Equal(events.Created))
		Expect(got[1].Node).To(Equal("n1"))
		Expect(got[1].APITime.Equal(apiTime)).To(BeTrue())
		Expect(got[1].ObservedAt.Equal(t0.Add(2 * time.Second))).To(BeTrue())
	})

	It("rejects a gap in seq and names the line", func() {
		in := line1 + "\n" + strings.Replace(line1, `"seq":1`, `"seq":3`, 1) + "\n"
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 2: seq 3, want 2"))
	})

	It("rejects a truncated last line and names it", func() {
		in := line1 + "\n" + `{"schema":1,"seq":2,"run_id":"r1",` + "\n"
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 2: unexpected EOF"))
	})

	It("rejects two events joined on one line", func() {
		in := line1 + strings.Replace(line1, `"seq":1`, `"seq":2`, 1) + "\n"
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 1: unexpected data after the event"))
	})

	It("rejects an unknown transition", func() {
		in := strings.Replace(line1, `"created"`, `"teleported"`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError(`line 1: seq 1: unknown transition "teleported"`))
	})

	It("rejects a schema version other than 1", func() {
		in := strings.Replace(line1, `"schema":1`, `"schema":2`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 1: seq 1: schema 2, want 1"))
	})

	It("rejects a field the schema does not define", func() {
		in := strings.Replace(line1, `"kind":"Pod"`, `"kind":"Pod","colour":"red"`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError(`line 1: json: unknown field "colour"`))
	})

	It("refuses to write an event with seq 0", func() {
		enc := events.NewEncoder(&bytes.Buffer{})
		Expect(enc.Encode(ev(0, events.Created))).To(MatchError("seq 0: seq must be 1 or more"))
	})

	It("refuses to write api_time without a precision", func() {
		e := ev(1, events.Created)
		apiTime := t0
		e.APITime = &apiTime
		enc := events.NewEncoder(&bytes.Buffer{})
		Expect(enc.Encode(e)).To(MatchError(`seq 1: api_time_precision "", want s or us`))
	})

	It("only ever writes whole lines to the underlying writer", func() {
		var log writeLog
		enc := events.NewEncoder(&log)
		// each line is about 220 bytes (line1 is 217), so 100 lines are about
		// 22000 bytes: the 4096-byte buffer fills at least 5 times with no Flush
		for seq := int64(1); seq <= 100; seq++ {
			Expect(enc.Encode(ev(seq, events.Created))).To(Succeed())
		}
		Expect(len(log.writes)).To(BeNumerically(">=", 5))
		for i, w := range log.writes {
			Expect(string(w)).To(HaveSuffix("\n"), "write %d", i)
		}
		Expect(enc.Flush()).To(Succeed())
		var all []byte
		for i, w := range log.writes {
			Expect(string(w)).To(HaveSuffix("\n"), "write %d", i)
			all = append(all, w...)
		}
		got, err := events.Read(bytes.NewReader(all))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(100))
	})
})

// writeLog keeps a copy of every Write call made to it.
type writeLog struct{ writes [][]byte }

func (w *writeLog) Write(p []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), p...))
	return len(p), nil
}
