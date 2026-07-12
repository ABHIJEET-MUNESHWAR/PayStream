//! Criterion microbenchmarks for the pipeline's CPU hot paths: enrichment and aggregation.
//!
//! Run: `cargo bench -p paystream-core`
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

use chrono::Utc;
use criterion::{criterion_group, criterion_main, Criterion};
use paystream_core::{enrich, Aggregator};
use paystream_types::{Currency, Direction, Money, PaymentEvent, PaymentStatus};
use std::hint::black_box;
use uuid::Uuid;

fn sample_event() -> PaymentEvent {
    PaymentEvent {
        payment_id: Uuid::new_v4(),
        direction: Direction::PayOut,
        account: "acct-bench".to_string(),
        amount: Money::new(Currency::MXN, 1_000_00),
        status: PaymentStatus::Completed,
        occurred_at: Utc::now(),
    }
}

fn bench_enrich(c: &mut Criterion) {
    let event = sample_event();
    let now = Utc::now();
    c.bench_function("enrich_single_event", |b| {
        b.iter(|| enrich(black_box(event.clone()), black_box(now)));
    });
}

fn bench_aggregate(c: &mut Criterion) {
    let enriched = enrich(sample_event(), Utc::now());
    let aggregator = Aggregator::new();
    c.bench_function("aggregator_update", |b| {
        b.iter(|| aggregator.update(black_box(&enriched)));
    });
}

criterion_group!(benches, bench_enrich, bench_aggregate);
criterion_main!(benches);
