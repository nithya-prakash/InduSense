package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// fakeToken satisfies mqtt.Token without a real broker.
type fakeToken struct{}

func (fakeToken) Wait() bool                     { return true }
func (fakeToken) WaitTimeout(time.Duration) bool { return true }
func (fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (fakeToken) Error() error { return nil }

// fakeMessage satisfies mqtt.Message without a real broker.
type fakeMessage struct {
	topic   string
	payload []byte
}

func (m *fakeMessage) Duplicate() bool   { return false }
func (m *fakeMessage) Qos() byte         { return 1 }
func (m *fakeMessage) Retained() bool    { return false }
func (m *fakeMessage) Topic() string     { return m.topic }
func (m *fakeMessage) MessageID() uint16 { return 0 }
func (m *fakeMessage) Payload() []byte   { return m.payload }
func (m *fakeMessage) Ack()              {}

// fakeClient satisfies mqtt.Client, capturing whatever handler subscribe()
// registers so the test can invoke it directly — including, critically,
// invoking it on a goroutine started *after* the test's simulated shutdown
// sequence has already run, the way paho's own router can (see the comment
// in main.go's shutdown sequence: paho spawns an untracked goroutine per
// inbound message and its own docs warn Disconnect does not wait for them).
type fakeClient struct {
	mu       sync.Mutex
	handlers []mqtt.MessageHandler
}

func (c *fakeClient) IsConnected() bool       { return true }
func (c *fakeClient) IsConnectionOpen() bool  { return true }
func (c *fakeClient) Connect() mqtt.Token     { return fakeToken{} }
func (c *fakeClient) Disconnect(quiesce uint) {}
func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	return fakeToken{}
}
func (c *fakeClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	c.mu.Lock()
	c.handlers = append(c.handlers, callback)
	c.mu.Unlock()
	return fakeToken{}
}
func (c *fakeClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	c.mu.Lock()
	c.handlers = append(c.handlers, callback)
	c.mu.Unlock()
	return fakeToken{}
}
func (c *fakeClient) Unsubscribe(topics ...string) mqtt.Token             { return fakeToken{} }
func (c *fakeClient) AddRoute(topic string, callback mqtt.MessageHandler) {}
func (c *fakeClient) OptionsReader() mqtt.ClientOptionsReader             { return mqtt.ClientOptionsReader{} }

func (c *fakeClient) handler() mqtt.MessageHandler {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handlers[0]
}

// TestSubscribe_ShutdownWhileMessagesArriving_NoPanic reproduces, without a
// real broker, exactly the race a pre-GitHub audit found: MQTT messages
// still arriving (and being dispatched on their own goroutines, mirroring
// paho's router) at the same moment main() is running its shutdown
// sequence. It deliberately includes a handler invocation that doesn't run
// until *after* the shutdown sequence (cancel + Disconnect) has already
// happened — the exact scenario paho's own "Disconnect may return before
// all activities have completed" warning describes — to prove that jobs
// being left open (never closed, never waited on via a WaitGroup the
// handler itself feeds) makes that scenario safe rather than merely
// unlikely. Run with -race: the historical bug was a send on a closed
// channel, and an earlier draft of this fix's own inFlight-WaitGroup
// tracking was itself caught by -race as a WaitGroup misuse (Add
// concurrent with Wait) — this version has neither.
func TestSubscribe_ShutdownWhileMessagesArriving_NoPanic(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		ctx, cancel := context.WithCancel(context.Background())
		jobs := make(chan inboundMessage, 4) // small buffer: forces handlers to actually contend
		client := &fakeClient{}

		subscribe(ctx, client, Config{MQTTQoS: 1}, jobs)
		handler := client.handler()

		var wg sync.WaitGroup
		var lateStarted atomic.Bool

		// Simulate ordinary in-flight messages: dispatched (goroutine
		// started) before shutdown begins, exactly like paho's router.
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handler(client, &fakeMessage{topic: "factory/1/machine/1/sensor/1/telemetry", payload: []byte("x")})
			}()
		}

		// Simulate the one paho itself warns about: a dispatch goroutine
		// that gets *created* before/while Disconnect runs, but doesn't
		// actually start executing until after main()'s shutdown sequence
		// has already finished. Modeled here as a goroutine that blocks
		// until told the shutdown sequence has run.
		shutdownSequenceDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-shutdownSequenceDone
			lateStarted.Store(true)
			handler(client, &fakeMessage{topic: "factory/1/machine/1/sensor/1/telemetry", payload: []byte("late")})
		}()

		// main()'s shutdown sequence, per main.go: cancel, then
		// "Disconnect" (no-op here) — critically, jobs is never closed and
		// nothing waits on the handler goroutines before proceeding.
		cancel()
		close(shutdownSequenceDone) // now let the "late" goroutine run, deliberately after shutdown "completed"

		// A worker with the exact same select/ctx.Done()/drain shape as
		// the real worker() in main.go — reimplemented locally rather than
		// calling worker() itself so this test stays hermetic (the real
		// one calls processMessage, which needs a live Kafka connection
		// via kafkaSink; that's orthogonal to the shutdown race this test
		// targets).
		var workerWG sync.WaitGroup
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for {
				select {
				case job := <-jobs:
					job.ack()
				case <-ctx.Done():
					for {
						select {
						case job := <-jobs:
							job.ack()
						default:
							return
						}
					}
				}
			}
		}()

		wg.Wait()       // all simulated handler goroutines (including the late one) have run
		workerWG.Wait() // worker has drained and exited via ctx.Done()

		if !lateStarted.Load() {
			t.Fatalf("iteration %d: late handler goroutine never ran — test didn't exercise the scenario it's meant to", iter)
		}
	}
}
