"""
Phase 3 — Kafka Producer
========================
Reads from TelemetryGenerator and publishes events to the Kafka topic
`player_encounters` at the configured rate.

Key design decisions
--------------------
* Partitioning: boss_id is used as the Kafka message key. This guarantees
  all events for the same boss land on the same partition, enabling
  per-boss ordered processing in the consumer.
* Delivery reports: async callback logs failed deliveries to the DLQ topic.
* Back-pressure: uses confluent-kafka's poll() to drain the delivery
  callback queue without blocking the main produce loop.
* Graceful shutdown: handles SIGINT/SIGTERM; flushes in-flight messages
  before exiting.
"""

import json
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Producer, KafkaException
from loguru import logger

from src.config import KafkaConfig, TelemetryConfig
from src.streaming.telemetry_generator import TelemetryGenerator


# ── Kafka producer configuration ──────────────────────────────────────────────

PRODUCER_CONFIG = {
    "bootstrap.servers":        KafkaConfig.BOOTSTRAP_SERVERS,
    "acks":                     "all",         # wait for full ISR acknowledgement
    "retries":                  5,
    "retry.backoff.ms":         200,
    "compression.type":         "lz4",         # fast compression for JSON payloads
    "linger.ms":                5,             # micro-batching for throughput
    "batch.size":               65536,         # 64 KB batch size
    "queue.buffering.max.ms":   10,
    "delivery.timeout.ms":      30000,
}


# ── Delivery callbacks ────────────────────────────────────────────────────────

_failed_count = 0
_success_count = 0


def _delivery_report(err, msg) -> None:
    """Called by confluent-kafka for every produced message (async)."""
    global _failed_count, _success_count
    if err is not None:
        _failed_count += 1
        logger.warning(
            f"Delivery failed for event on partition {msg.partition()}: {err}"
        )
    else:
        _success_count += 1


# ── Main producer class ───────────────────────────────────────────────────────

class EncounterProducer:
    """
    Wraps confluent-kafka Producer and the TelemetryGenerator.

    Usage
    -----
        producer = EncounterProducer()
        producer.run()          # runs until SIGINT or duration expires
    """

    def __init__(self):
        self.producer = Producer(PRODUCER_CONFIG)
        self.generator = TelemetryGenerator()
        self._running = True

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame) -> None:
        logger.info(f"Signal {signum} received — shutting down producer…")
        self._running = False

    def run(self) -> None:
        """
        Main produce loop.
        Generates events and publishes them to `player_encounters`.
        """
        topic = KafkaConfig.TOPIC_ENCOUNTERS
        eps = TelemetryConfig.EVENTS_PER_SECOND
        interval = 1.0 / eps if eps > 0 else 0.0
        report_every = 5000   # print stats every N events
        produced = 0

        logger.info(
            f"Starting producer → topic={topic}  "
            f"target={eps} events/s  bootstrap={KafkaConfig.BOOTSTRAP_SERVERS}"
        )

        for event in self.generator.stream():
            if not self._running:
                break

            t0 = time.monotonic()
            payload = json.dumps(event).encode("utf-8")

            # Use boss_id as partition key for ordered per-boss processing
            key = str(event["boss_id"]).encode("utf-8")

            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    on_delivery=_delivery_report,
                )
            except BufferError:
                # Internal queue full — poll to drain delivery callbacks
                self.producer.poll(timeout=0.1)
                self.producer.produce(topic=topic, key=key, value=payload,
                                      on_delivery=_delivery_report)

            # Non-blocking poll to drain delivery callbacks
            self.producer.poll(timeout=0)
            produced += 1

            if produced % report_every == 0:
                logger.info(
                    f"Produced {produced:,} events  "
                    f"(success={_success_count:,}, failed={_failed_count:,})"
                )

            elapsed = time.monotonic() - t0
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Flush remaining in-flight messages
        logger.info(f"Flushing {self.producer.__len__()} in-flight messages…")
        self.producer.flush(timeout=30)
        logger.info(
            f"Producer shut down. Total produced: {produced:,}  "
            f"success={_success_count:,}  failed={_failed_count:,}"
        )


if __name__ == "__main__":
    EncounterProducer().run()
