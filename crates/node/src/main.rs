//! PayStream node — the composition root. Wires the rskafka source/sink to the streaming pipeline
//! and serves the GraphQL API, with structured tracing, Prometheus metrics and graceful shutdown.
#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use paystream_api::{build_router, build_schema};
use paystream_core::{Aggregator, Pipeline, PipelineConfig};
use paystream_infra::{install_prometheus, AppConfig, KafkaEventSink, KafkaEventSource};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json())
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let prometheus = install_prometheus();

    let config = AppConfig::from_env()?;
    tracing::info!(?config, "starting PayStream node");

    // Connect the Kafka source and sink (real broker required; provided by docker-compose).
    let source = Arc::new(
        KafkaEventSource::connect(
            config.kafka_brokers.clone(),
            &config.input_topic,
            config.partition,
            0,
        )
        .await?,
    );
    let sink = Arc::new(
        KafkaEventSink::connect(
            config.kafka_brokers.clone(),
            &config.output_topic,
            config.partition,
        )
        .await?,
    );
    let aggregator = Arc::new(Aggregator::new());

    let pipeline = Arc::new(Pipeline::new(
        source,
        sink,
        aggregator.clone(),
        PipelineConfig::default(),
    ));
    let shutdown = CancellationToken::new();

    // Run the streaming pipeline in the background.
    let pipeline_task = {
        let pipeline = pipeline.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move { pipeline.run(shutdown).await })
    };

    // Serve the GraphQL API + metrics.
    let schema = build_schema(aggregator);
    let router = build_router(schema, prometheus);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.http_port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP API listening (GraphiQL at /graphiql, metrics at /metrics)");

    let server_shutdown = shutdown.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received");
            server_shutdown.cancel();
        })
        .await?;

    // Await the pipeline's clean stop.
    let _ = pipeline_task.await;
    tracing::info!("PayStream node stopped");
    Ok(())
}
