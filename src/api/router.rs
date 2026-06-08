use std::sync::Arc;

use axum::routing::post;
use axum::{Extension, Router};

use crate::api::{ApiContext, controllers};

pub fn router(ctx: Arc<ApiContext>) -> Router {
    Router::new()
        .route(
            "/v1/transfers/search",
            post(controllers::post_transfers_search),
        )
        .route(
            "/v1/token-transfers/search",
            post(controllers::post_token_transfers_search),
        )
        .layer(Extension(ctx))
}
