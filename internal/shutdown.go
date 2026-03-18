package gossip

import (
	"sync"
)

var Shutdown sync.WaitGroup

var ShuttingDown = make(chan struct{})
