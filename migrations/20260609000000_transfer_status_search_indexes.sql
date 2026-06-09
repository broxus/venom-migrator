CREATE INDEX transfers_search_status_created_at_idx
    ON transfers (status, created_at, transaction_hash);

CREATE INDEX transfers_search_sender_status_created_at_idx
    ON transfers (sender_wc, sender_account, status, created_at, transaction_hash);

CREATE INDEX token_transfers_search_status_created_at_idx
    ON token_transfers (status, created_at, transaction_hash);

CREATE INDEX token_transfers_search_sender_status_created_at_idx
    ON token_transfers (sender_wc, sender_account, status, created_at, transaction_hash);
