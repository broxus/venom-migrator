use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::db::SqlxClient;

mod controllers;
pub mod models;
mod router;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct ApiConfig {
    pub listen_addr: SocketAddr,
}

impl Default for ApiConfig {
    #[inline]
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
        }
    }
}

#[derive(Clone)]
pub struct ApiContext {
    sqlx_client: SqlxClient,
}

pub async fn serve(config: &ApiConfig, sqlx_client: SqlxClient) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .with_context(|| format!("failed to bind api listener on {}", config.listen_addr))?;

    let router = router::router(Arc::new(ApiContext { sqlx_client }));

    tracing::info!(addr = %config.listen_addr, "starting api server");

    axum::serve(listener, router)
        .await
        .context("api server stopped")?;

    Ok(())
}

type Result<T> = std::result::Result<T, Error>;

pub struct Error(anyhow::Error);

impl<E> From<E> for Error
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    error_message: String,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = if self.0.downcast_ref::<InvalidRequest>().is_some() {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };

        (
            status,
            Json(ErrorResponse {
                error_message: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct InvalidRequest(pub &'static str);
