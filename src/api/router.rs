use std::sync::Arc;

use axum::routing::{get, post};
use axum::{Extension, Router};

use crate::api::{ApiContext, controllers};

pub fn router(ctx: Arc<ApiContext>) -> Router {
    Router::new()
        .route(
            "/v1/transfers/search",
            post(controllers::post_transfers_search),
        )
        .route("/v1/transfers/{tx_hash}", get(controllers::get_transfer))
        .layer(Extension(ctx))
}
