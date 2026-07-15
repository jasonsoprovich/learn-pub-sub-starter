package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerGameLog(gameLog routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	err := gamelogic.WriteLog(gameLog)
	if err != nil {
		fmt.Println("Failed to write log:", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}

func main() {
	const connString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to create channel:", err)
		os.Exit(1)
	}
	defer ch.Close()

	fmt.Println("Connected to RabbitMQ.")

	// game_logs
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		routing.GameLogSlug+".*",
		pubsub.Durable,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind game_logs queue:", err)
		os.Exit(1)
	}
	fmt.Println("Game logs queue declared and bound.")

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handlerGameLog,
	)
	if err != nil {
		fmt.Println("Failed to subscribe to game logs:", err)
		os.Exit(1)
	}
	fmt.Println("Subscribed to game logs")

	// moves
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"army_moves",
		routing.ArmyMovesPrefix+".*",
		pubsub.Durable,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind army_moves queue:", err)
		os.Exit(1)
	}
	fmt.Println("Army moves queue declared and bound.")

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		command := words[0]

		switch command {
		case "pause":
			fmt.Println("Sending pause message.")
			pauseState := routing.PlayingState{IsPaused: true}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, pauseState)
			if err != nil {
				fmt.Println("Failed to publish pause message:", err)
			}
		case "resume":
			fmt.Println("Sending resume message.")
			resumeState := routing.PlayingState{IsPaused: false}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, resumeState)
			if err != nil {
				fmt.Println("Failed to publish resume message:", err)
			}
		case "quit":
			fmt.Println("Exiting.")
			return
		default:
			fmt.Println("Unkown command:", command)
		}
	}
}
