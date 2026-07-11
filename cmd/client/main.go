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

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(m gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(m)

		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			war := gamelogic.RecognitionOfWar{
				Attacker: m.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				war,
			)
			if err != nil {
				fmt.Println("Failed to publish war message.", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			fmt.Println("Unknown war outcome.")
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
		handlerMove(gameState, ch),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to army moves:", err)
		os.Exit(1)
	}

	fmt.Printf("Subscribed to army moves on queue: army_moves.%s\n", username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gameState),
	)
	if err != nil {
		fmt.Println("Failed to subscribe to war messages:", err)
		os.Exit(1)
	}
	fmt.Println("Subscribed to war messages on shared queue: war")

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
