package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

const (
	// EnvGoogleProjectID is XXX
	EnvGoogleProjectID = "GOOGLE_PROJECT_ID"
	// EnvPubSubTopic is XXX
	EnvPubSubTopic = "TOPIC"
)

func main() {
	projectID := os.Getenv(EnvGoogleProjectID)
	if projectID == "" {
		fmt.Println("required env:", EnvGoogleProjectID)
		os.Exit(1)
	}

	psTopic := os.Getenv(EnvPubSubTopic)
	if psTopic == "" {
		fmt.Println("required env:", EnvPubSubTopic)
		os.Exit(1)
	}

	var check, sub string
	flag.StringVar(&check, "check", "", "check type\npublish or subscribe")
	flag.StringVar(&sub, "sub", "", "subscription name")
	flag.Parse()

	ctx := context.Background()

	psCli, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		fmt.Println("error new client", err)
		os.Exit(1)
	}
	defer psCli.Close()

	topic := psCli.Topic(psTopic)

	switch check {
	case "pub":
		result := topic.Publish(ctx, &pubsub.Message{
			ID:   fmt.Sprint(time.Now().Unix(), "aaa"),
			Data: []byte(string(time.Now().String())),
		})

		fmt.Println(time.Now().String(), "publish")
		<-result.Ready()
		fmt.Println(time.Now().String(), "publish ready")

		serverID, err := result.Get(ctx)
		if err != nil {
			fmt.Println("result get failed", err)
		}

		fmt.Println("server id:", serverID)
	case "sub":
		if sub == "" {
			fmt.Println("sub is required")
			os.Exit(1)
		}

		s := psCli.Subscription(sub)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		err := s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			fmt.Println("message id:", msg.ID, " data:", string(msg.Data))
			cancel()
		})
		if err != nil {
			fmt.Println("subscribe failed", err)
			os.Exit(1)
		}
	default:
		fmt.Println("not defined check case:", check)
		os.Exit(1)
	}
}
