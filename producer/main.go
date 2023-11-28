package main

import (
	"context"
	"crypto/rand"
	"flag"
	"log"
	"strconv"

	"github.com/segmentio/kafka-go"
)

var async = flag.Bool("a", false, "use async")

func main() {
	flag.Parse()

	ctx := context.Background()

	logger := log.Default()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:     []string{"127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092"},
		Topic:       "demo",
		Async:       *async,
		Logger:      kafka.LoggerFunc(logger.Printf),
		ErrorLogger: kafka.LoggerFunc(logger.Printf),
		BatchSize:   2000,

		// CompressionCodec: &compress.Lz4Codec,

		Balancer: &SimpleBalancer{},
	})
	defer writer.Close()

	m := make([]byte, 500)

	rand.Read(m)

	for i := 0; i < 524_288; i++ {
		err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(strconv.Itoa(i)), Value: m})
		if err != nil {
			log.Fatal(err)
		}
	}
}
