//! Axum HTTP surface: GraphQL endpoint, GraphiQL playground, health and Prometheus metrics.

use crate::schema::PayStreamSchema;
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::Extension,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use metrics_exporter_prometheus::PrometheusHandle;

async fn graphql_handler(
    schema: Extension<PayStreamSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn health() -> &'static str {
    "UP"
}

async fn metrics(handle: Extension<PrometheusHandle>) -> String {
    handle.render()
}

/// Builds the HTTP router wiring the GraphQL schema and Prometheus handle.
pub fn build_router(schema: PayStreamSchema, prometheus: PrometheusHandle) -> Router {
    Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphiql", get(graphiql))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .layer(Extension(schema))
        .layer(Extension(prometheus))
}
