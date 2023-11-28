package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var id = flag.String("id", "", "Consumer ID")

func main() {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()

	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{"127.0.0.1:29092"},
			Topic:          "demo",
			GroupID:        "my-group",
			SessionTimeout: time.Second * 6,
		})
		defer reader.Close()

		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Consumer: %s Parition: %d Offset: %d Key: %s\n", *id, msg.Partition, msg.Offset, msg.Key)
			time.Sleep(300 * time.Millisecond)

		}
	}()

	<-ctx.Done()
}
