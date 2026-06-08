use std::sync::Arc;

use axum::Extension;
use axum::Json;

use crate::api::models::{TransferResponse, TransferSearchRequest, TransferSearchResponse};
use crate::api::{ApiContext, Result};

pub async fn post_transfers_search(
    Extension(ctx): Extension<Arc<ApiContext>>,
    Json(req): Json<TransferSearchRequest>,
) -> Result<Json<TransferSearchResponse>> {
    let need_total_count = req.need_total_count;
    let search = req.try_into()?;
    let transfers = ctx.sqlx_client.search_native_transfers(&search).await?;
    let transfers = transfers_to_response(transfers)?;

    let total_count = if need_total_count {
        Some(ctx.sqlx_client.count_native_transfers(&search).await?)
    } else {
        None
    };

    let response = TransferSearchResponse {
        total_count,
        transfers,
    };

    Ok(Json(response))
}

pub async fn post_token_transfers_search(
    Extension(ctx): Extension<Arc<ApiContext>>,
    Json(req): Json<TransferSearchRequest>,
) -> Result<Json<TransferSearchResponse>> {
    let need_total_count = req.need_total_count;
    let search = req.try_into()?;
    let transfers = ctx.sqlx_client.search_token_transfers(&search).await?;
    let transfers = transfers_to_response(transfers)?;

    let total_count = if need_total_count {
        Some(ctx.sqlx_client.count_token_transfers(&search).await?)
    } else {
        None
    };

    let response = TransferSearchResponse {
        total_count,
        transfers,
    };

    Ok(Json(response))
}

fn transfers_to_response(
    transfers: Vec<crate::db::TransferFromDb>,
) -> anyhow::Result<Vec<TransferResponse>> {
    transfers
        .into_iter()
        .map(TransferResponse::try_from)
        .collect()
}
