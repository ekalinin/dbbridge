package main

import (
	"flag"
	"log"

	// Register drivers statically
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/clickhouse"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/mysql"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/oracle"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/postgres"
)

func main() {
	configPath := flag.String("config", "configs/dbbridge.yaml", "Path to config file")
	flag.Parse()

	log.Printf("Starting dbbridge with config: %s", *configPath)

	// main is the only place that exits: the wiring steps report their failures
	// instead of calling log.Fatalf from inside their own frame.
	a, err := newApp(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	a.serve()
	a.awaitSignals()
	a.Close()
}
