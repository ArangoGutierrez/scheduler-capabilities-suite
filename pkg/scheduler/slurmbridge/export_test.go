package slurmbridge

import (
	"io"
	"time"
)

// Unexported steps of Install, Submit and Cleanup, exposed to the specs only.
var CheckSum = checkSum
var KeyMaterial = keyMaterial
var NewKey = newKey
var InstallObjects = installObjects
var FailOnDelete = failOnDelete
var RegisterNodes = registerNodes
var RestAPI = restAPI
var NodesReady = nodesReady
var WaitReady = waitReady
var LiveJobs = liveJobs
var WaitJobsGone = waitJobsGone
var PriorityClasses = priorityClasses
var NodeGPUs = nodeGPUs

// RestGet is the type of RestAPI's result.
type RestGet = restGet

// NewWith is New with the key source and Cleanup's bound given.
func NewWith(random io.Reader, cleanup time.Duration) *Profile {
	return &Profile{random: random, cleanupTimeout: cleanup}
}

// KeySource and CleanupBound read what New set.
func KeySource(p *Profile) io.Reader        { return p.random }
func CleanupBound(p *Profile) time.Duration { return p.cleanupTimeout }

// ProfileObjects is what Install applies before the nodes.
var ProfileObjects = (*Profile).objects
