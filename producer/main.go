package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func main() {

	brokers := []string{"localhost:9092"}
	topic := "test_topic"

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalf("Erro ao criar produtor: %v", err)
	}
	defer producer.Close()

	fmt.Println("Produtor Kafka iniciado. Digite mensagens para enviar ao Kafka:")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		message := scanner.Text()
		if message == "exit" {
			fmt.Println("Encerrando produtor.")
			break
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Erro ao enviar mensagem: %v", err)
		} else {
			log.Printf("Mensagem enviada para partição %d com offset %d", partition, offset)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Erro ao ler entrada do terminal: %v", err)
	}
}
