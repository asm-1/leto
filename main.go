package main

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"encoding/json"
	"os"
)

type Configuration struct {
	SERVER_RABBITMQ string
}

func main() {
	file, err := os.ReadFile("config/config.json")
	failOnError(err, "Failed to read settings file")

	configuration := Configuration{}
	err = json.Unmarshal(file, &configuration)
	failOnError(err, "Failed to decode settings into structure")

	fmt.Println("Hello RabbitMQ")

	serverConnectionRabbit(&configuration)

	fmt.Println("Successfully connected to our RabbitMQ")
}

func serverConnectionRabbit(conf *Configuration) {
	conn, err := amqp.Dial(conf.SERVER_RABBITMQ)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// The connection abstracts the socket connection
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//entryInQueuing(ch)
	//readingFromQueue(ch)
}

func entryInQueuing(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := "Hello World!"
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
	})
	failOnError(err, "Failed to publish a message")
}

func readingFromQueue(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
	}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}