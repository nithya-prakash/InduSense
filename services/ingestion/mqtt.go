package main

import (
	"context"
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

// connectMQTT wires the message handler to guard against the shutdown race
// a pre-GitHub audit found: paho invokes message handlers on their own,
// untracked goroutines, so a message could still arrive and be dispatched
// after main() has started shutting down. Rather than track those
// goroutines (a sync.WaitGroup can't safely do that here — see main.go's
// shutdown sequence comment for why), jobs is simply never closed, so a
// send from any of them is always safe no matter when it happens. ctx is
// threaded through so a handler already in its select when shutdown begins
// still bails out promptly via ctx.Done() instead of blocking.
func connectMQTT(ctx context.Context, cfg Config, connected *atomic.Bool, jobs chan<- inboundMessage) (mqtt.Client, error) {
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
			subscribe(ctx, c, cfg, jobs)
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

func subscribe(ctx context.Context, client mqtt.Client, cfg Config, jobs chan<- inboundMessage) {
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case jobs <- inboundMessage{topic: msg.Topic(), payload: msg.Payload(), ack: msg.Ack}:
		case <-ctx.Done():
			// Shutting down: deliberately do not send to jobs and do not
			// ack — the persistent MQTT session redelivers this message
			// after reconnect, same as any other unacked message.
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
