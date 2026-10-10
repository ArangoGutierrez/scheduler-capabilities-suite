// Package labels holds the label keys the suite puts on every object it
// creates, so the recorder can find and attribute them.
package labels

// Prefix is the suite's label prefix, under a domain Carlos controls.
const Prefix = "scs.arangogutierrez.github.io/"

// Keys and values.
const (
	PartOf      = "app.kubernetes.io/part-of"
	PartOfValue = "scheduler-capabilities-suite"
	Probe       = Prefix + "probe"
	Tenant      = Prefix + "tenant"
	Job         = Prefix + "job"
	RunID       = Prefix + "run-id"
	Rack        = Prefix + "rack"
	Block       = Prefix + "block"
)

// Selector selects every object the suite created.
const Selector = PartOf + "=" + PartOfValue
