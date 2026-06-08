use std::str::FromStr;

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use tycho_types::cell::HashBytes;
use tycho_types::models::StdAddr;

use crate::api::InvalidRequest;
use crate::db::{TransferFromDb, TransfersSearch, TransfersSearchOrdering};

const MAX_LIMIT: i64 = 100;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSearchRequest {
    pub limit: i64,
    pub offset: i64,
    pub ordering: TransferSearchOrdering,
    pub user_address: Option<String>,
    pub need_total_count: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSearchResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_count: Option<i64>,
    pub transfers: Vec<TransferResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferResponse {
    pub tx_hash: String,
    pub from_address: String,
    pub to_address: String,
    pub token: Option<TokenResponse>,
    pub amount: String,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenResponse {
    pub symbol: String,
    pub address: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum TransferSearchOrdering {
    CreatedAtAscending,
    CreatedAtDescending,
}

impl From<TransferSearchOrdering> for TransfersSearchOrdering {
    fn from(value: TransferSearchOrdering) -> Self {
        match value {
            TransferSearchOrdering::CreatedAtAscending => Self::CreatedAtAscending,
            TransferSearchOrdering::CreatedAtDescending => Self::CreatedAtDescending,
        }
    }
}

impl TryFrom<TransferSearchRequest> for TransfersSearch {
    type Error = anyhow::Error;

    fn try_from(value: TransferSearchRequest) -> Result<Self, Self::Error> {
        let limit = value.limit;
        if limit <= 0 || limit > MAX_LIMIT {
            anyhow::bail!(InvalidRequest("limit must be between 1 and 100"));
        }

        let offset = value.offset;
        if offset < 0 {
            anyhow::bail!(InvalidRequest("offset must be non-negative"));
        }

        let user_address = value
            .user_address
            .as_deref()
            .map(StdAddr::from_str)
            .transpose()
            .map_err(|_| InvalidRequest("invalid userAddress"))?;

        Ok(Self {
            user_address,
            ordering: value.ordering.into(),
            limit,
            offset,
        })
    }
}

impl TryFrom<TransferFromDb> for TransferResponse {
    type Error = anyhow::Error;

    fn try_from(value: TransferFromDb) -> Result<Self, Self::Error> {
        let from_address = StdAddr::new(
            i8::try_from(value.sender_wc)?,
            HashBytes::from_str(&value.sender_account)?,
        );
        let to_address = StdAddr::new(
            i8::try_from(value.recipient_wc)?,
            HashBytes::from_str(&value.recipient_account)?,
        );

        let token = match (
            value.token_symbol,
            value.token_address_wc,
            value.token_address_account,
        ) {
            (Some(symbol), Some(workchain), Some(account)) => {
                let address =
                    StdAddr::new(i8::try_from(workchain)?, HashBytes::from_str(&account)?);
                Some(TokenResponse {
                    symbol,
                    address: address.to_string(),
                })
            }
            _ => None,
        };

        Ok(Self {
            tx_hash: value.transaction_hash,
            from_address: from_address.to_string(),
            to_address: to_address.to_string(),
            token,
            amount: value
                .amount
                .to_u128()
                .map(|amount| amount.to_string())
                .unwrap_or_else(|| value.amount.to_string()),
            status: value.status,
            created_at: value.created_at.and_utc().timestamp(),
        })
    }
}
