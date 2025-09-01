package neilats_refactor_go

import "sync"

var RttMatrixFileLock sync.Mutex
var LatencyFileLock sync.Mutex
