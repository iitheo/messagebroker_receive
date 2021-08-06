package main

import (
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	//Setting up is the same as the publisher;
	//we open a connection and a channel, and declare the queue from which
	//we're going to consume. Note this matches up with the queue that send publishes to.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
	}

	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		failOnError(err, "Failed to open a channel")
	}

	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	if err != nil {
		failOnError(err, "Failed to declare a queue")
	}


	//Note that we declare the queue here, as well.
	//Because we might start the consumer before the publisher,
	//we want to make sure the queue exists before we try to consume messages from it.
	//
	//We're about to tell the server to deliver us the messages from the queue.
	//Since it will push us messages asynchronously, we will read the messages from a channel
	//(returned by amqp::Consume) in a goroutine.
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	if err != nil {
		failOnError(err, "Failed to register a consumer")
	}


	forever := make(chan bool)

	//Our consumer listens for messages from RabbitMQ,
	//so unlike the publisher which publishes a single message,
	//we'll keep the consumer running to listen for messages and print them out.


	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	//The consumer will print the message it gets from the publisher via RabbitMQ.
	//The consumer will keep running, waiting for messages (Use Ctrl-C to stop it),
	//so try running the publisher from another terminal.
	//
	//If you want to check on the queue, try using rabbitmqctl list_queues.
}
