package main

import (
	. "Master/Master"
	"os"
	"os/signal"
)

func main() {
	var master Master
	master.Start()
	defer master.Stop()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
