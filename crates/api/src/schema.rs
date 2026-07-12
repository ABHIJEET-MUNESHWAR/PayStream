//! GraphQL schema exposing the pipeline's real-time aggregates (the stream read model).

use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};
use paystream_core::Aggregator;
use std::sync::Arc;

/// Concrete schema type used across the API and node.
pub type PayStreamSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

#[derive(SimpleObject)]
struct StatusCount {
    status: String,
    count: u64,
}

#[derive(SimpleObject)]
struct DirectionCount {
    direction: String,
    count: u64,
}

#[derive(SimpleObject)]
struct CurrencyTotal {
    currency: String,
    settled_minor_units: i64,
}

/// GraphQL view of the aggregate snapshot.
#[derive(SimpleObject)]
struct Aggregates {
    total_processed: u64,
    max_risk_score: u32,
    by_status: Vec<StatusCount>,
    by_direction: Vec<DirectionCount>,
    settled: Vec<CurrencyTotal>,
}

/// Root query.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Liveness marker.
    async fn health(&self) -> &'static str {
        "UP"
    }

    /// Current real-time aggregates over the processed event stream.
    async fn aggregates(&self, ctx: &Context<'_>) -> Aggregates {
        let snapshot = ctx.data_unchecked::<Arc<Aggregator>>().snapshot();
        Aggregates {
            total_processed: snapshot.total_processed,
            max_risk_score: u32::from(snapshot.max_risk_score),
            by_status: snapshot
                .by_status
                .into_iter()
                .map(|(status, count)| StatusCount {
                    status: format!("{status:?}"),
                    count,
                })
                .collect(),
            by_direction: snapshot
                .by_direction
                .into_iter()
                .map(|(direction, count)| DirectionCount {
                    direction: format!("{direction:?}"),
                    count,
                })
                .collect(),
            settled: snapshot
                .settled_minor_by_currency
                .into_iter()
                .map(|(currency, settled_minor_units)| CurrencyTotal {
                    currency: currency.to_string(),
                    settled_minor_units,
                })
                .collect(),
        }
    }
}

/// Builds the schema with the shared aggregator injected as context data.
#[must_use]
pub fn build_schema(aggregator: Arc<Aggregator>) -> PayStreamSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(aggregator)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use paystream_core::enrich;
    use paystream_types::{Currency, Direction, Money, PaymentEvent, PaymentStatus};
    use uuid::Uuid;

    #[tokio::test]
    async fn aggregates_query_reflects_processed_events() {
        let aggregator = Arc::new(Aggregator::new());
        aggregator.update(&enrich(
            PaymentEvent {
                payment_id: Uuid::new_v4(),
                direction: Direction::PayIn,
                account: "a".to_string(),
                amount: Money::new(Currency::MXN, 1_000_00),
                status: PaymentStatus::Completed,
                occurred_at: Utc::now(),
            },
            Utc::now(),
        ));
        let schema = build_schema(aggregator);

        let response = schema
            .execute(
                "{ health aggregates { totalProcessed settled { currency settledMinorUnits } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        let data = response.data.to_string();
        assert!(data.contains("totalProcessed"));
        assert!(data.contains("MXN"));
    }
}
