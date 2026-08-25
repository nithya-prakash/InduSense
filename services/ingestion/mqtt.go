package main

import (
	"log"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// inboundMessage carries an MQTT message into the worker pool along with its
// Ack function. The message is only acked after it has been durably handed
// off to Kafka (or dead-lettered) — if ingestion crashes before that point,
// the persistent MQTT session redelivers it on reconnect, preserving
// at-least-once delivery end to end.
type inboundMessage struct {
	topic   string
	payload []byte
	ack     func()
}

func connectMQTT(cfg Config, connected *atomic.Bool, jobs chan<- inboundMessage) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBrokerURL).
		SetClientID(cfg.MQTTClientID).
		SetCleanSession(false). // persistent session: broker redelivers on reconnect if we never acked
		SetAutoAckDisabled(true).
		SetOrderMatters(false). // per-device ordering is preserved downstream via Kafka partitioning + sequence_number
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			connected.Store(true)
			metricMQTTConnected.Set(1)
			log.Printf("mqtt: connected to %s", cfg.MQTTBrokerURL)
			subscribe(c, cfg, jobs)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			connected.Store(false)
			metricMQTTConnected.Set(0)
			log.Printf("mqtt: connection lost: %v (will auto-reconnect)", err)
		})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return nil, errTimeout("mqtt connect")
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return client, nil
}

func subscribe(client mqtt.Client, cfg Config, jobs chan<- inboundMessage) {
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case jobs <- inboundMessage{topic: msg.Topic(), payload: msg.Payload(), ack: msg.Ack}:
		case <-time.After(5 * time.Second):
			// Queue has been full for 5s straight: don't ack, so the broker
			// redelivers later — this is the ingestion-side backpressure
			// signal propagating all the way back to MQTT delivery.
			log.Printf("ingestion: queue saturated, leaving message on topic %s unacked", msg.Topic())
		}
	}

	topics := map[string]byte{
		"factory/+/machine/+/sensor/+/telemetry": cfg.MQTTQoS,
		"factory/+/machine/+/status":             cfg.MQTTQoS,
		"factory/+/machine/+/events":             cfg.MQTTQoS,
	}
	for topic, qos := range topics {
		if token := client.Subscribe(topic, qos, handler); token.Wait() && token.Error() != nil {
			log.Printf("mqtt: failed to subscribe to %s: %v", topic, token.Error())
		} else {
			log.Printf("mqtt: subscribed to %s (qos=%d)", topic, qos)
		}
	}
}

type errTimeout string

func (e errTimeout) Error() string { return string(e) + " timed out" }
