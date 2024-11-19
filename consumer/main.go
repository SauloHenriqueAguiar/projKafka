package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {

	brokers := []string{"localhost:9092"}
	topic := "test_topic"

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Erro ao criar consumidor: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Erro ao consumir partição: %v", err)
	}
	defer partitionConsumer.Close()

	log.Println("Consumidor Kafka iniciado. Aguardando mensagens...")

	for message := range partitionConsumer.Messages() {
		log.Printf("Mensagem recebida: %s", string(message.Value))
	}
}
