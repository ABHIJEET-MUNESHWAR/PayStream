//! Config defaults + an ignored real-Kafka round-trip (run with a broker via `--ignored`).
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

use paystream_infra::AppConfig;

#[test]
fn app_config_has_sensible_defaults() {
    let cfg = AppConfig::default();
    assert_eq!(cfg.http_port, 8082);
    assert_eq!(cfg.input_topic, "payments.events");
    assert_eq!(cfg.output_topic, "payments.enriched");
    assert_eq!(cfg.partition, 0);
    assert!(!cfg.kafka_brokers.is_empty());
}

/// Real end-to-end Kafka round-trip. Ignored by default because it needs a broker; run with:
/// `PAYSTREAM_KAFKA=localhost:9092 cargo test -p paystream-infra -- --ignored`.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn kafka_source_sink_round_trip() {
    use chrono::Utc;
    use paystream_core::ports::{EventSink, EventSource};
    use paystream_infra::{KafkaEventSink, KafkaEventSource};
    use paystream_types::{Currency, Direction, Money, PaymentEvent, PaymentStatus};
    use uuid::Uuid;

    let brokers = std::env::var("PAYSTREAM_KAFKA")
        .unwrap_or_else(|_| "localhost:9092".to_string())
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    let topic = format!("paystream-it-{}", Uuid::new_v4());

    let enrich = |e: PaymentEvent| paystream_core::enrich(e, Utc::now());
    let event = PaymentEvent {
        payment_id: Uuid::new_v4(),
        direction: Direction::PayIn,
        account: "acct-it".to_string(),
        amount: Money::new(Currency::MXN, 1_000_00),
        status: PaymentStatus::Completed,
        occurred_at: Utc::now(),
    };

    let sink = KafkaEventSink::connect(brokers.clone(), &topic, 0)
        .await
        .unwrap();
    sink.publish(&enrich(event.clone())).await.unwrap();

    let source = KafkaEventSource::connect(brokers, &topic, 0, 0)
        .await
        .unwrap();
    let batch = source.next_batch(10).await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].payment_id, event.payment_id);
}
