package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(m gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(m)

		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			// Don't process moves from same player
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			// Safe move, process normally
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// War detected, but still process it
			return pubsub.Ack
		default:
			// Unknown outcome, discard
			return pubsub.NackDiscard
		}
	}
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to get username:", err)
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(username)

	// pause
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to pause message:", err)
		os.Exit(1)
	}

	fmt.Printf("Subscribed to pause messages on queue: pause.%s\n", username)

	// move
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("army_moves.%s", username),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to army moves:", err)
		os.Exit(1)
	}

	fmt.Printf("Subscribed to army moves on queue: army_moves.%s\n", username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		command := words[0]

		switch command {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println("Error spawning unit:", err)
			}
		case "move":
			msg, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("Error moving unit:", err)
			} else {
				err = pubsub.PublishJSON(
					ch,
					routing.ExchangePerilTopic,
					fmt.Sprintf("army_moves.%s", username),
					msg,
				)
				if err != nil {
					fmt.Println("Failed to publish move:", err)
				} else {
					fmt.Println("Move published successfully.")
				}
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
