package main

import (
	. "Monitor/monitor"
	"os"
	"os/signal"
)

func main() {
	var monitor Monitor
	monitor.Start()
	defer monitor.Stop()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
