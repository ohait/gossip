package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"oha.it/gossip/internal"
)

func main() {
	addr := flag.String("l", ":7950", "listen address")
	flag.Parse()

	logsFolder := flag.Arg(0)
	if logsFolder == "" {
		fmt.Fprintln(os.Stderr, "usage: gossip [-l addr] <logs-folder>")
		os.Exit(1)
	}

	s := &gossip.Service{
		LogsFolder: logsFolder,
	}
	err := s.Init()
	if err != nil {
		panic(err)
	}
	log.Printf("init ok...")
	lAddr, err := s.Bind(*addr)
	if err != nil {
		panic(err)
	}
	log.Printf("listening on %s", lAddr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGQUIT)
	log.Printf("press Ctrl+C to quit")
	<-quit
	close(gossip.ShuttingDown)
	gossip.Shutdown.Wait()
}
