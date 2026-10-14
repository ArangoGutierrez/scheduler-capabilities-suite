package slurmbridge

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
