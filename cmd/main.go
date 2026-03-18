package main

import (
	"os"
	"os/signal"
	"syscall"

	"oha.it/gossip/internal"
)

func main() {
	s := &gossip.Service{
		LogsFolder: "logs",
	}
	err := s.Init()
	if err != nil {
		panic(err)
	}
	if _, err = s.Bind(":7950"); err != nil {
		panic(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGQUIT)
	<-quit
	close(gossip.ShuttingDown)
	gossip.Shutdown.Wait()
}
