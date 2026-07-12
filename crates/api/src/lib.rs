//! PayStream GraphQL API over axum.
#![forbid(unsafe_code)]
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

pub mod schema;
pub mod server;

pub use schema::{build_schema, PayStreamSchema, QueryRoot};
pub use server::build_router;
