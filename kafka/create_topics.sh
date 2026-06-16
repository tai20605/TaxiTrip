#!/bin/bash
# ================================================================
# Create Kafka topics for Boston Rideshare Platform
# Run inside the kafka container:
#   docker exec boston_kafka bash /opt/kafka_topics/create_topics.sh
#
# apache/kafka image has tools at: /opt/kafka/bin/
# ================================================================

set -e

BOOTSTRAP=localhost:9092
KAFKA_BIN=/opt/kafka/bin

echo "============================================"
echo " Boston Rideshare - Kafka Topic Setup"
echo "============================================"

echo "Waiting for Kafka broker to be ready..."
until $KAFKA_BIN/kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP &>/dev/null 2>&1; do
  echo "  Kafka not ready yet — retrying in 3s..."
  sleep 3
done
echo "  ✓ Kafka broker is ready"
echo ""

# ── Main topic: rideshare_events ─────────────────────────────────
echo "[1/2] Creating topic: rideshare_events"
$KAFKA_BIN/kafka-topics.sh \
  --create \
  --bootstrap-server $BOOTSTRAP \
  --topic rideshare_events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000 \
  --config retention.bytes=2147483648 \
  --config compression.type=lz4 \
  --config max.message.bytes=10485760

echo "  ✓ rideshare_events created"
echo "    - Partitions : 3 (keyed by source neighborhood)"
echo "    - Retention  : 7 days / 2 GB"
echo "    - Compression: lz4"

# ── DLQ topic: rideshare_events_dlq ─────────────────────────────
echo ""
echo "[2/2] Creating topic: rideshare_events_dlq"
$KAFKA_BIN/kafka-topics.sh \
  --create \
  --bootstrap-server $BOOTSTRAP \
  --topic rideshare_events_dlq \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=2592000000

echo "  ✓ rideshare_events_dlq created"
echo "    - Partitions: 1"
echo "    - Retention : 30 days"

# ── Describe topics ───────────────────────────────────────────────
echo ""
echo "============================================"
echo " Topic Summary"
echo "============================================"
$KAFKA_BIN/kafka-topics.sh \
  --describe \
  --bootstrap-server $BOOTSTRAP \
  --topic rideshare_events

echo ""
$KAFKA_BIN/kafka-topics.sh \
  --describe \
  --bootstrap-server $BOOTSTRAP \
  --topic rideshare_events_dlq

echo ""
echo "  All topics:"
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP

echo ""
echo "✅ All topics created successfully!"
