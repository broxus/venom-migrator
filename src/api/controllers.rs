use std::sync::Arc;

use std::str::FromStr;

use axum::Extension;
use axum::Json;
use axum::extract::Path;
use tycho_types::cell::HashBytes;

use crate::api::models::{TransferResponse, TransferSearchRequest, TransferSearchResponse};
use crate::api::{ApiContext, InvalidRequest, NotFound, Result};

pub async fn post_transfers_search(
    Extension(ctx): Extension<Arc<ApiContext>>,
    Json(req): Json<TransferSearchRequest>,
) -> Result<Json<TransferSearchResponse>> {
    let need_total_count = req.need_total_count;
    let search = req.try_into()?;
    let transfers = ctx.sqlx_client.search_transfers(&search).await?;
    let transfers = transfers_to_response(transfers)?;

    let total_count = if need_total_count {
        Some(ctx.sqlx_client.count_transfers(&search).await?)
    } else {
        None
    };

    let response = TransferSearchResponse {
        total_count,
        transfers,
    };

    Ok(Json(response))
}

pub async fn get_transfer(
    Extension(ctx): Extension<Arc<ApiContext>>,
    Path(tx_hash): Path<String>,
) -> Result<Json<TransferResponse>> {
    let tx_hash = HashBytes::from_str(&tx_hash)
        .map_err(|_| InvalidRequest("invalid txHash"))?
        .to_string();
    let transfer = ctx
        .sqlx_client
        .get_transfer(&tx_hash)
        .await?
        .ok_or(NotFound("transfer not found"))?;

    Ok(Json(TransferResponse::try_from(transfer)?))
}

fn transfers_to_response(
    transfers: Vec<crate::db::TransferFromDb>,
) -> anyhow::Result<Vec<TransferResponse>> {
    transfers
        .into_iter()
        .map(TransferResponse::try_from)
        .collect()
}
