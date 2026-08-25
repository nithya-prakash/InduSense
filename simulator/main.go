// Command simulator generates realistic telemetry for every sensor seeded in
// Postgres and publishes it over MQTT, with configurable fault injection
// (duplicates, out-of-order delivery, network delay, sensor failure, and
// machine shutdowns) so downstream services have real, imperfect data to
// contend with instead of a clean synthetic stream.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nithya-prakash/indusense/pkg/events"
)

type publishJob struct {
	topic   string
	payload []byte
}

type stats struct {
	published    atomic.Uint64
	duplicates   atomic.Uint64
	delayed      atomic.Uint64
	outOfOrder   atomic.Uint64
	anomalies    atomic.Uint64
	dropped      atomic.Uint64
	sensorFailed atomic.Uint64
	publishErrs  atomic.Uint64
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("simulator: loading sensor catalog from postgres (limit=%d)", cfg.SensorCount)
	loadCtx, loadCancel := context.WithTimeout(ctx, 30*time.Second)
	catalog, err := loadSensorCatalog(loadCtx, cfg.PostgresDSN, cfg.SensorCount)
	loadCancel()
	if err != nil {
		log.Fatalf("simulator: failed to load sensor catalog: %v", err)
	}
	if len(catalog) == 0 {
		log.Fatalf("simulator: no sensors found — run `make seed` first")
	}
	log.Printf("simulator: loaded %d sensors", len(catalog))

	pub, err := newMQTTPublisher(cfg.MQTTBrokerURL, cfg.MQTTClientID, cfg.MQTTQoS)
	if err != nil {
		log.Fatalf("simulator: failed to connect to MQTT broker: %v", err)
	}
	defer pub.disconnect()

	jobs := make(chan publishJob, cfg.QueueCapacity)
	var st stats

	var workerWG sync.WaitGroup
	for i := 0; i < cfg.PublisherWorkers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for job := range jobs {
				if err := pub.publish(job.topic, job.payload, 5*time.Second); err != nil {
					st.publishErrs.Add(1)
				} else {
					st.published.Add(1)
				}
			}
		}()
	}

	enqueue := func(topic string, v any) {
		payload, err := json.Marshal(v)
		if err != nil {
			log.Printf("simulator: marshal error: %v", err)
			return
		}
		select {
		case jobs <- publishJob{topic: topic, payload: payload}:
		case <-time.After(50 * time.Millisecond):
			st.dropped.Add(1) // backpressure: queue is saturated, drop rather than grow unbounded
		}
	}

	machines := map[string]*machineController{}
	var machinesMu sync.Mutex
	getMachine := func(deviceID string) *machineController {
		machinesMu.Lock()
		defer machinesMu.Unlock()
		mc, ok := machines[deviceID]
		if !ok {
			mc = newMachineController()
			machines[deviceID] = mc
		}
		return mc
	}

	interval := time.Duration(len(catalog)) * time.Second / time.Duration(max(cfg.MessagesPerSec, 1))
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	log.Printf("simulator: target rate %d msg/s across %d sensors (~%s per sensor)",
		cfg.MessagesPerSec, len(catalog), interval)

	var sensorWG sync.WaitGroup
	for idx, entry := range catalog {
		entry := entry
		seed := time.Now().UnixNano() + int64(idx)*7919
		rng := rand.New(rand.NewSource(seed))
		gen := newSensorGenerator(rng, entry.MinValue, entry.MaxValue, cfg.AnomalyRate)
		mc := getMachine(entry.DeviceID)

		sensorWG.Add(1)
		go runSensor(ctx, &sensorWG, cfg, entry, rng, gen, mc, enqueue, &st, interval)
	}

	// One controller goroutine per unique machine drives shutdown/recovery
	// transitions and announces them on the machine status/events topics.
	var machineWG sync.WaitGroup
	seenDevices := map[string]bool{}
	deviceToFactoryMachine := map[string][2]string{}
	for _, e := range catalog {
		if !seenDevices[e.DeviceID] {
			seenDevices[e.DeviceID] = true
			deviceToFactoryMachine[e.DeviceID] = [2]string{e.FactoryID, e.MachineID}
			machineWG.Add(1)
			go runMachineController(ctx, &machineWG, e.DeviceID, e.FactoryID, e.MachineID,
				getMachine(e.DeviceID), enqueue, rand.New(rand.NewSource(time.Now().UnixNano())))
		}
	}

	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				log.Printf("simulator stats: published=%d duplicates=%d delayed=%d out_of_order=%d anomalies=%d dropped=%d sensor_failures_active=%d publish_errors=%d",
					st.published.Load(), st.duplicates.Load(), st.delayed.Load(), st.outOfOrder.Load(),
					st.anomalies.Load(), st.dropped.Load(), st.sensorFailed.Load(), st.publishErrs.Load())
			}
		}
	}()

	<-ctx.Done()
	log.Println("simulator: shutdown signal received, draining in-flight sensors...")
	sensorWG.Wait()
	machineWG.Wait()
	close(jobs)
	workerWG.Wait()

	log.Printf("simulator: final stats: published=%d duplicates=%d delayed=%d out_of_order=%d anomalies=%d dropped=%d publish_errors=%d",
		st.published.Load(), st.duplicates.Load(), st.delayed.Load(), st.outOfOrder.Load(),
		st.anomalies.Load(), st.dropped.Load(), st.publishErrs.Load())
}

func runSensor(
	ctx context.Context,
	wg *sync.WaitGroup,
	cfg Config,
	entry SensorCatalogEntry,
	rng *rand.Rand,
	gen *sensorGenerator,
	mc *machineController,
	enqueue func(topic string, v any),
	st *stats,
	baseInterval time.Duration,
) {
	defer wg.Done()

	telemetryTopic := fmt.Sprintf("factory/%s/machine/%s/sensor/%s/telemetry",
		entry.FactoryID, entry.MachineID, entry.SensorID)
	eventsTopic := fmt.Sprintf("factory/%s/machine/%s/events", entry.FactoryID, entry.MachineID)

	var seq uint64
	failed := false

	jitter := func() time.Duration {
		frac := 0.8 + rng.Float64()*0.4 // +/-20%
		return time.Duration(float64(baseInterval) * frac)
	}

	timer := time.NewTimer(jitter())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(jitter())
		}

		if !mc.isRunning() {
			continue // machine is shut down: no readings, i.e. a missing-reading gap
		}

		wasFailed := failed
		failed = sensorShouldFail(rng, failed, cfg.SensorFailureRate)
		if failed != wasFailed {
			if failed {
				st.sensorFailed.Add(1)
				enqueue(eventsTopic, events.MachineEvent{
					FactoryID: entry.FactoryID, MachineID: entry.MachineID,
					DeviceID: entry.DeviceID, SensorID: entry.SensorID,
					EventType: "SENSOR_FAILURE", Timestamp: time.Now().UTC(),
				})
			} else {
				enqueue(eventsTopic, events.MachineEvent{
					FactoryID: entry.FactoryID, MachineID: entry.MachineID,
					DeviceID: entry.DeviceID, SensorID: entry.SensorID,
					EventType: "SENSOR_RECOVERED", Timestamp: time.Now().UTC(),
				})
			}
		}
		if failed {
			continue // sensor is down: missing reading
		}

		seq++
		value, isAnomaly := gen.next()
		if isAnomaly {
			st.anomalies.Add(1)
		}

		evt := events.TelemetryEvent{
			EventID:          uuid.NewString(),
			OrganizationID:   entry.OrganizationID,
			FactoryID:        entry.FactoryID,
			ProductionLineID: entry.ProductionLineID,
			MachineID:        entry.MachineID,
			DeviceID:         entry.DeviceID,
			SensorID:         entry.SensorID,
			Timestamp:        time.Now().UTC(),
			SequenceNumber:   seq,
			Metric:           entry.Metric,
			Value:            value,
			Unit:             entry.Unit,
		}

		fd := decideFaults(rng, cfg)
		publishEvent(ctx, evt, telemetryTopic, fd, enqueue, st)
	}
}

func publishEvent(ctx context.Context, evt events.TelemetryEvent, topic string, fd faultDecision, enqueue func(string, any), st *stats) {
	send := func(e events.TelemetryEvent) { enqueue(topic, e) }

	if fd.Delayed {
		st.delayed.Add(1)
		if fd.OutOfOrder {
			st.outOfOrder.Add(1)
		}
		go func() {
			select {
			case <-time.After(fd.DelayFor):
				send(evt)
				if fd.Duplicate {
					st.duplicates.Add(1)
					send(evt)
				}
			case <-ctx.Done():
			}
		}()
		return
	}

	send(evt)
	if fd.Duplicate {
		st.duplicates.Add(1)
		send(evt)
	}
}

func runMachineController(
	ctx context.Context,
	wg *sync.WaitGroup,
	deviceID, factoryID, machineID string,
	mc *machineController,
	enqueue func(topic string, v any),
	rng *rand.Rand,
) {
	defer wg.Done()
	statusTopic := fmt.Sprintf("factory/%s/machine/%s/status", factoryID, machineID)
	eventsTopic := fmt.Sprintf("factory/%s/machine/%s/events", factoryID, machineID)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			running := mc.isRunning()
			if shouldToggle(rng, running) {
				mc.setRunning(!running)
				newStatus := "STOPPED"
				if !running {
					newStatus = "RUNNING"
				}
				enqueue(statusTopic, events.MachineStatusEvent{
					FactoryID: factoryID, MachineID: machineID,
					Status: newStatus, Timestamp: time.Now().UTC(),
				})
				enqueue(eventsTopic, events.MachineEvent{
					FactoryID: factoryID, MachineID: machineID, DeviceID: deviceID,
					EventType: "MACHINE_" + newStatus, Timestamp: time.Now().UTC(),
				})
			}
		}
	}
}
