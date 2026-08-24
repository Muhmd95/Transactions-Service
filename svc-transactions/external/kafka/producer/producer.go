package producer

import (
	"context"
	"encoding/json"
	"time" // if Net timeouts / Producer.Timeout configured here

	"github.com/IBM/sarama" // the client itself

	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)

type Publisher struct {
	syncProducer sarama.SyncProducer
	topic        string
}

func NewPublisher(ctx context.Context, brokers []string, topic string) (*Publisher, error) { // not *transactions.transactionEvent because of close function
	log := logger.Ctx(ctx)
	cfg := sarama.NewConfig()
	//acks=all for all to ack (1 in production)
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	// REQUIRED for sync client
	cfg.Producer.Return.Successes = true
	// "short timeout"
	cfg.Net.DialTimeout = time.Second * 5
	cfg.Net.ReadTimeout = time.Second * 5
	cfg.Net.WriteTimeout = time.Second * 5

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't create the sync producer")
		return nil, err
	}

	return &Publisher{
		syncProducer: producer,
		topic:        topic,
	}, nil

}

func (p *Publisher) PublishTransactionEvent(ctx context.Context, evt *transactions.TransactionEvent) error {
	log := logger.Ctx(ctx)
	bytes, err := json.Marshal(evt)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't marshal the event")
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(evt.WalletID), // the events in a partition is order by the walletid
		// all events of the same wallet goes to the same partition
		Value: sarama.ByteEncoder(bytes),
	}

	_, _, err = p.syncProducer.SendMessage(msg)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't send the message")
		return err
	}
	return nil
}

// producer.Close() at shutdown
func (p *Publisher) Close() error {
	return p.syncProducer.Close()

}
