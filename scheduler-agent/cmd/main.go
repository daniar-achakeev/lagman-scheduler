package main

import (
	"os"

	"github.com/daniar-achakeev/scheduler/scheduler-agent/internal"
	"github.com/daniar-achakeev/scheduler/scheduler-agent/log"
)

const port = 28001

func main() {
	lgr := log.New(os.Stdout)
	server := internal.NewResultReceiver(lgr)
	err := server.Run(port)
	if err != nil {
		lgr.Logf("server exited with error: %v", err)
		os.Exit(1)
	}

}
