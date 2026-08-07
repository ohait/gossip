package net

import (
	"sync"
)

var Shutdown sync.WaitGroup

var ShuttingDown = make(chan struct{})
