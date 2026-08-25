package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mqttPublisher wraps a paho MQTT client configured for automatic
// reconnection, so a transient broker outage doesn't kill the simulator.
type mqttPublisher struct {
	client mqtt.Client
	qos    byte
}

func newMQTTPublisher(brokerURL, clientID string, qos byte) (*mqttPublisher, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetOnConnectHandler(func(mqtt.Client) {
			log.Printf("mqtt: connected to %s", brokerURL)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("mqtt: connection lost: %v (will auto-reconnect)", err)
		})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return nil, fmt.Errorf("mqtt: connect timed out")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt: connect failed: %w", err)
	}

	return &mqttPublisher{client: client, qos: qos}, nil
}

func (p *mqttPublisher) publish(topic string, payload []byte, timeout time.Duration) error {
	token := p.client.Publish(topic, p.qos, false, payload)
	if !token.WaitTimeout(timeout) {
		return fmt.Errorf("publish to %s timed out after %s", topic, timeout)
	}
	return token.Error()
}

func (p *mqttPublisher) disconnect() {
	p.client.Disconnect(1000)
}
