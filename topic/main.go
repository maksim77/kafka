package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

var (
	command    = flag.String("c", "", "create or delete")
	brokers    = flag.String("b", "127.0.0.1:29092", "Brokers list")
	name       = flag.String("n", "demo", "topic name")
	partitions = flag.Int("p", 1, "paritions numbers")
	replicas   = flag.Int("r", 1, "replicas numbers")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	conn, err := kafka.DialContext(ctx, "tcp", *brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	switch *command {
	case "create":
		err := controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             *name,
			NumPartitions:     *partitions,
			ReplicationFactor: *replicas,
			ConfigEntries: []kafka.ConfigEntry{
				// {ConfigName: "min.insync.replicas", ConfigValue: "2"},
				{ConfigName: "segment.bytes", ConfigValue: "2097152"},
				// {ConfigName: "retention.bytes", ConfigValue: "3145728"},
			},
		})
		if err != nil {
			log.Fatal(err)
		}
	case "delete":
		err := controllerConn.DeleteTopics(*name)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("command must be create or delete")
	}
}
